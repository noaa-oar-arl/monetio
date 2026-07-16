"""NCEP GRIB Reader"""

from typing import Any

import numpy as np
import xarray as xr

from .base import GriddedReader, _ensure_time_dimension, _scientific_hygiene, register_reader
from .sat_utils import update_history


@register_reader("ncep_grib")
class NCEPGribReader(GriddedReader):
    """
    Reader for NCEP GRIB files.
    """

    def open_dataset(
        self,
        files: str | list[str],
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Reads NCEP GRIB files.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s), URL(s), or glob pattern.
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr to create a virtual Zarr dataset, by default False.
        virtualizarr_file : str or None, optional
            Path to save/load the VirtualiZarr reference JSON file, by default None.
        virtualizarr_parser : str or None, optional
            The VirtualiZarr parser to use (e.g. 'grib2'), by default None.
        virtualizarr_backend : str, optional
            Backend for VirtualiZarr references ("kerchunk" or "icechunk"), by default "kerchunk".
        icechunk_repo : str or None, optional
            Path to the Icechunk repository, by default None.
        use_icechunk : bool, optional
            Whether to use Icechunk for VirtualiZarr references, by default False.
        icechunk_url : str or None, optional
            Path to the Icechunk repository, by default None.
        use_dask : bool, optional
            Whether to use Dask for lazy loading, by default False.
        **kwargs : Any
            Additional arguments passed to xarray.open_mfdataset or the driver.

        Returns
        -------
        xarray.Dataset
            The processed NCEP GRIB dataset.

        Examples
        --------
        >>> from monetio.readers.ncep_grib import NCEPGribReader
        >>> reader = NCEPGribReader()
        >>> ds = reader.open_dataset("gfs.*.grib2", engine="grib2io")
        """
        # Default to grib2io engine
        if "engine" not in kwargs:
            kwargs["engine"] = "grib2io"
        # Also supports open_mfdataset logic
        if "concat_dim" not in kwargs:
            kwargs["concat_dim"] = "time"

        if "preprocess" not in kwargs:
            kwargs["preprocess"] = ncep_grib_preprocess

        ds = self.driver.open(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser="grib2",
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            **kwargs,
        )

        # Update history
        ds = update_history(ds, "Read NCEP GRIB data.")
        ds = _ensure_time_dimension(ds)

        return ds


def ncep_grib_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess function for a single NCEP GRIB file.

    Converts 1D latitude/longitude to 2D coordinates lazily and applies
    scientific hygiene.

    Parameters
    ----------
    ds : xarray.Dataset
        Input NCEP GRIB dataset.

    Returns
    -------
    xarray.Dataset
        Processed dataset with 'latitude' and 'longitude' coordinates on (y, x) dims.

    Examples
    --------
    >>> ds = ncep_grib_preprocess(ds)
    """
    # 1. Coordinate Renaming & Promotion
    # Some backends might have them as variables but not coords
    rename_dict = {}
    if "lat_0" in ds.variables:
        rename_dict["lat_0"] = "latitude"
    if "lon_0" in ds.variables:
        rename_dict["lon_0"] = "longitude"

    if rename_dict:
        ds = ds.rename(rename_dict)
        # Promote renamed variables to coordinates
        to_coord = [v for v in rename_dict.values() if v in ds.variables and v not in ds.coords]
        if to_coord:
            ds = ds.set_coords(to_coord)

    # Normalize valid_time -> time for GRIB interoperability.
    if "valid_time" in ds.coords or "valid_time" in ds.dims:
        if "time" in ds.coords or "time" in ds.dims or "time" in ds.variables:
            if "valid_time" in ds.variables:
                ds = ds.drop_vars("valid_time")
        else:
            if "valid_time" in ds.coords and "valid_time" not in ds.dims:
                valid_time_dims = ds["valid_time"].dims
                if len(valid_time_dims) == 1 and valid_time_dims[0] in ds.dims:
                    ds = ds.swap_dims({valid_time_dims[0]: "valid_time"})
            ds = ds.rename({"valid_time": "time"})

    # 2. Generate 2D Latitude and Longitude lazily
    if "latitude" in ds.coords and "longitude" in ds.coords:
        # Check if they are 1D
        if ds.latitude.ndim == 1 and ds.longitude.ndim == 1:
            lat_dim = ds.latitude.dims[0]
            lon_dim = ds.longitude.dims[0]

            # Create new DataArrays for broadcast to avoid alignment issues.
            # We preserve existing laziness (NumPy or Dask) without manual wrapping.
            lon1d = xr.DataArray(ds.longitude.data, dims="x", attrs=ds.longitude.attrs)
            lat1d = xr.DataArray(ds.latitude.data, dims="y", attrs=ds.latitude.attrs)

            if ds.chunks:
                # Align coordinate chunking with dataset chunks to maintain laziness.
                lon1d = lon1d.chunk({d: ds.chunks[d] for d in lon1d.dims if d in ds.chunks})
                lat1d = lat1d.chunk({d: ds.chunks[d] for d in lat1d.dims if d in ds.chunks})

            # Broadcast to 2D
            # xr.broadcast will handle both NumPy and Dask lazily
            lon2d, lat2d = xr.broadcast(lon1d, lat1d)

            # Ensure dimension order is (y, x) to match original meshgrid behavior
            # We use transpose to ensure the order is correct for the new dimensions
            if "y" in lon2d.dims and "x" in lon2d.dims:
                lon2d = lon2d.transpose("y", "x")
                lat2d = lat2d.transpose("y", "x")

            # Replace 1D coords in the dataset with index ranges to avoid dim/coord conflict
            ds = ds.assign_coords(
                **{
                    lat_dim: np.arange(ds.sizes[lat_dim]),
                    lon_dim: np.arange(ds.sizes[lon_dim]),
                }
            )
            # Rename dims to y, x
            ds = ds.rename({lat_dim: "y", lon_dim: "x"})

            # Assign 2D coordinates
            ds = ds.assign_coords(
                longitude=lon2d.assign_attrs(
                    {"long_name": "Longitude", "units": "degree_east", "standard_name": "longitude"}
                ),
                latitude=lat2d.assign_attrs(
                    {"long_name": "Latitude", "units": "degree_north", "standard_name": "latitude"}
                ),
            )

            ds = ds.set_coords(["latitude", "longitude"])
            ds = update_history(ds, "Generated 2D latitude/longitude coordinates lazily.")

    # 3. Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # Update history
    ds = update_history(ds, "Preprocessed NCEP GRIB data.")

    return ds
