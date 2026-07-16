"""OMPS Reader"""

import xarray as xr

from .base import GriddedReader, _scientific_hygiene, register_reader
from .sat_utils import (
    add_time_coord,
    standardize_satellite_coords,
    tai93_to_datetime,
    update_history,
)


@register_reader("omps")
class OMPSReader(GriddedReader):
    """
    Reader for OMPS (Ozone Mapping and Profiler Suite) data.
    Supports Level 2 (Nadir Mapper) and Level 3 daily products.
    """

    def open_dataset(
        self,
        files: str | list[str],
        product: str = "nmto3_l2",
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads OMPS data.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s) or URL(s).
        product : str, optional
            OMPS product: 'nmto3_l2' (default) or 'nmto3_l3'.
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr, by default False.
        virtualizarr_file : str or None, optional
            Path to the VirtualiZarr file, by default None.
        virtualizarr_backend : str, optional
            VirtualiZarr backend, by default "kerchunk".
        icechunk_repo : str or None, optional
            Path to the Icechunk repository, by default None.
        use_icechunk : bool, optional
            Whether to use Icechunk, by default False.
        icechunk_url : str or None, optional
            Path to the Icechunk repository, by default None.
        use_dask : bool, optional
            Whether to use Dask for lazy loading, by default False.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The OMPS dataset.
        """
        if "preprocess" not in kwargs:
            from functools import partial

            kwargs["preprocess"] = partial(omps_preprocess, product=product)

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        # L2 data often has different cross-track dimensions if not carefully selected,
        # but standard products should be consistent.
        ds = super().open_dataset(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser="hdf5",
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            **kwargs,
        )

        # Update history
        ds = update_history(ds, f"Read OMPS {product} data.")

        return ds


def omps_preprocess(ds: xr.Dataset, product: str = "nmto3_l2") -> xr.Dataset:
    """
    Preprocess OMPS dataset lazily.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    product : str, optional
        Product type, by default "nmto3_l2".

    Returns
    -------
    xr.Dataset
        Processed dataset.

    Examples
    --------
    >>> import xarray as xr
    >>> from monetio.readers.omps import omps_preprocess
    >>> ds = xr.Dataset({"ColumnAmountO3": (("scanline", "ground_pixel"), [[250, 260]])})
    >>> ds = omps_preprocess(ds, product="nmto3_l2")
    """
    if product == "nmto3_l2":
        ds = _preprocess_nmto3_l2(ds)
    elif product == "nmto3_l3":
        ds = _preprocess_nmto3_l3(ds)

    # Standardize coordinates
    ds = standardize_satellite_coords(ds)

    # Scientific Hygiene
    ds = _scientific_hygiene(ds)

    return ds


def _preprocess_nmto3_l2(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess OMPS Level 2 Nadir Mapper Total Ozone.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Processed dataset with standard names and lazily converted time.

    Examples
    --------
    >>> ds = _preprocess_nmto3_l2(ds)
    """
    # Basic mapping of variables if they are in groups
    mapping = {
        "GeolocationData/Latitude": "latitude",
        "GeolocationData/Longitude": "longitude",
        "GeolocationData/Time": "time_raw",
        "ScienceData/ColumnAmountO3": "ozone_column",
        "ScienceData/QualityFlags": "quality_flags",
        "ScienceData/RadiativeCloudFraction": "cloud_fraction",
        "AncillaryData/APrioriLayerO3": "apriori",
        "ScienceData/LayerEfficiency": "layer_efficiency",
        "DimPressureLevel": "pressure",
    }

    for old, new in mapping.items():
        if old in ds.variables:
            ds = ds.rename({old: new})

    # If already renamed or in root
    root_mapping = {
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Time": "time_raw",
        "ColumnAmountO3": "ozone_column",
        "QualityFlags": "quality_flags",
        "RadiativeCloudFraction": "cloud_fraction",
        "APrioriLayerO3": "apriori",
        "LayerEfficiency": "layer_efficiency",
    }
    for old, new in root_mapping.items():
        if old in ds.variables and new not in ds.variables:
            ds = ds.rename({old: new})

    # Handle Time (Lazy)
    if "time_raw" in ds.variables:
        ds["time"] = tai93_to_datetime(ds["time_raw"])
        ds = ds.set_coords("time")
        if "time_raw" in ds.variables:
            ds = ds.drop_vars("time_raw")

    # Apply Quality Control (Lazy)
    if "ozone_column" in ds.data_vars:
        col = ds["ozone_column"]
        mask = (col >= 50.0) & (col <= 700.0)

        if "cloud_fraction" in ds.variables:
            mask = mask & (ds["cloud_fraction"] <= 0.3)

        if "quality_flags" in ds.variables:
            # According to original code: flags >= 138 are bad
            mask = mask & (ds["quality_flags"] < 138)

        ds["ozone_column"] = ds["ozone_column"].where(mask)
        if "layer_efficiency" in ds.data_vars:
            ds["layer_efficiency"] = ds["layer_efficiency"].where(mask)

    # Standardize dimensions to y, x if they are something else
    # L2 OMPS uses (nscan, nstep) or similar.
    # We'll let standardize_satellite_coords handle it if we add common names.

    # Update history
    ds = update_history(ds, "Preprocessed OMPS L2 data.")

    return ds


def _preprocess_nmto3_l3(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess OMPS Level 3 Nadir Mapper Total Ozone.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Processed dataset with standard names and 2D coordinates.

    Examples
    --------
    >>> ds = _preprocess_nmto3_l3(ds)
    """
    root_mapping = {
        "Latitude": "lat",
        "Longitude": "lon",
        "ColumnAmountO3": "ozone_column",
        "RadiativeCloudFraction": "cloud_fraction",
    }
    for old, new in root_mapping.items():
        if old in ds.variables:
            ds = ds.rename({old: new})

    # L3 often has 1D lat/lon coordinates and 2D data.
    # But standardize_satellite_coords expects 2D lat/lon if they are coordinates.
    # Let's see if they are 1D.
    if "lat" in ds.dims and "lon" in ds.dims:
        # Generate 2D meshgrid lazily
        lon2d, lat2d = xr.broadcast(ds.lon, ds.lat)
        ds = ds.assign_coords(
            latitude=lat2d.assign_attrs({"units": "degrees_north"}),
            longitude=lon2d.assign_attrs({"units": "degrees_east"}),
        )

    # Handle Time from attributes if available
    if "time" not in ds.coords:
        ds = add_time_coord(ds, time_attr="Date")

    # Masking
    if "ozone_column" in ds.data_vars:
        col = ds["ozone_column"]
        mask = col >= 0
        if "cloud_fraction" in ds.variables:
            mask = mask & (ds["cloud_fraction"] <= 0.3)
        ds["ozone_column"] = ds["ozone_column"].where(mask)

    # Update history
    ds = update_history(ds, "Preprocessed OMPS L3 data.")

    return ds
