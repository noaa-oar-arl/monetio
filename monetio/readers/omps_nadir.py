"""OMPS Nadir Reader"""

import datetime

import pandas as pd
import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import (
    add_time_coord,
    jpss_time_to_datetime,
    standardize_satellite_coords,
    update_history,
)


@register_reader("omps_nadir")
class OMPSNadirReader(GriddedReader):
    """
    Reader for OMPS (Ozone Mapping and Profiler Suite) Nadir products.
    Supports NOAA JPSS (V8TOZ, NP/TC SDR/GEO) and NASA (NMTO3) products.
    Available on AWS Open Data.
    """

    def open_dataset(
        self,
        files: str | list[str] = None,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str = None,
        satellite: str = "snpp",
        product: str = "v8toz",
        group: str | list[str] = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads OMPS Nadir data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File path(s) or URL(s).
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr to create a virtual Zarr dataset, by default False.
        virtualizarr_file : str or None, optional
            Path to save/load the VirtualiZarr reference JSON file, by default None.
        virtualizarr_parser : str or None, optional
            The VirtualiZarr parser to use (e.g. 'hdf5', 'netcdf3', 'zarr', 'grib2').
        virtualizarr_backend : str, optional
            Backend for VirtualiZarr references ("kerchunk" or "icechunk"), by default "kerchunk".
        icechunk_repo : str or None, optional
            Path to the Icechunk repository, by default None.
        use_icechunk : bool, optional
            Whether to use Icechunk, by default False.
        icechunk_url : str or None, optional
            Path to the Icechunk repository, by default None.
        use_dask : bool, optional
            Whether to use Dask for lazy loading, by default False.
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str], optional
            Dates to retrieve. If files is None, this is used to build URLs.
        satellite : str, optional
            Satellite identifier: 'snpp', 'n20' (NOAA-20/J01), or 'n21' (NOAA-21/J02).
            Default is 'snpp'.
        product : str, optional
            OMPS product type:
            - 'v8toz': NOAA Total Ozone EDR (default)
            - 'nmto3_l2': NASA Nadir Mapper Total Ozone L2
            - 'nmto3_l3': NASA Nadir Mapper Total Ozone L3
            - 'tc_sdr': NOAA Total Column SDR
            - 'np_sdr': NOAA Nadir Profiler SDR
        group : Union[str, List[str]], optional
            The NetCDF group(s) to open. If None, appropriate groups for the
            product will be selected (e.g. SDR + GEO for SDR products).
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The OMPS dataset.

        Examples
        --------
        Open standard V8TOZ product:
        >>> reader = OMPSNadirReader()
        >>> ds = reader.open_dataset(dates='2024-01-01', product='v8toz')
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files = self.build_urls(dates, satellite=satellite, product=product)

        if group is None:
            if product.lower() == "tc_sdr":
                groups = ["All_Data/OMPS-TC-SDR_All", "All_Data/OMPS-TC-GEO_All"]
            elif product.lower() == "np_sdr":
                groups = ["All_Data/OMPS-NP-SDR_All", "All_Data/OMPS-NP-GEO_All"]
            else:
                groups = [None]
        elif isinstance(group, str):
            groups = [group]
        else:
            groups = group

        user_preprocess = kwargs.pop("preprocess", None)

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        if "concat_dim" not in kwargs:
            kwargs["concat_dim"] = "time"
        if "combine" not in kwargs:
            kwargs["combine"] = "nested"
        dsets = []
        for g in groups:
            g_kwargs = kwargs.copy()
            if g:
                g_kwargs["group"] = g
                # Filter files by group to avoid open_mfdataset failure
                if isinstance(files, list) and len(files) > 1:
                    # Look for SDR or GEO keyword in group name
                    g_sdr = "SDR" in g.upper()
                    g_geo = "GEO" in g.upper()
                    g_files = [
                        f
                        for f in files
                        if (g_sdr and "SDR" in f.upper()) or (g_geo and "GEO" in f.upper())
                    ]
                    # If filter returns nothing, it might be a single granule pair, fallback to all
                    if not g_files:
                        g_files = files
                else:
                    g_files = files
            else:
                g_files = files

            try:
                # Open without the preprocessor at this stage
                ds_g = super().open_dataset(g_files, **g_kwargs, virtualizarr_parser="hdf5")
                dsets.append(ds_g)
            except (OSError, RuntimeError, ValueError):
                # Not all groups may be present in all files
                continue

        if not dsets:
            raise RuntimeError(f"No groups could be opened for product {product}.")

        # Merge groups
        ds = xr.merge(dsets, compat="no_conflicts")

        # Now apply OMPS preprocessing to the merged dataset
        ds = omps_nadir_preprocess(ds, product=product)

        if user_preprocess:
            ds = user_preprocess(ds)

        # Update history
        ds = update_history(ds, f"Read OMPS Nadir {product} data from {satellite}.")

        return ds

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        satellite: str = "snpp",
        product: str = "v8toz",
    ) -> list[str]:
        """
        Build S3 URLs for OMPS Nadir data based on dates.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        satellite : str, optional
            Satellite identifier ('snpp', 'n20', 'n21', 'j01', 'j02').
        product : str, optional
            OMPS product.

        Returns
        -------
        List[str]
            List of S3 URLs.
        """
        from .drivers import FileUtility

        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        sat_map = {
            "snpp": "noaa-nesdis-snpp-pds",
            "n20": "noaa-nesdis-n20-pds",
            "n21": "noaa-nesdis-n21-pds",
            "j01": "noaa-nesdis-n20-pds",
            "j02": "noaa-nesdis-n21-pds",
        }
        bucket = sat_map.get(satellite.lower())
        if not bucket:
            raise ValueError(f"Unknown satellite: {satellite}. Choose from {list(sat_map.keys())}")

        prod_map = {
            "v8toz": "OMPS_V8TOZ",
            "tc_sdr": "OMPS-TC-SDR",
            "tc_geo": "OMPS-TC-GEO",
            "np_sdr": "OMPS-NP-SDR",
            "np_geo": "OMPS-NP-GEO",
        }
        dir_name = prod_map.get(product.lower())
        if not dir_name:
            # Fallback for NASA or others if they ever follow this pattern,
            # but usually they don't.
            dir_name = product

        # Determine if we need to fetch multiple directories (SDR + GEO)
        dirs_to_search = [dir_name]
        if product.lower() == "tc_sdr":
            dirs_to_search.append("OMPS-TC-GEO")
        elif product.lower() == "np_sdr":
            dirs_to_search.append("OMPS-NP-GEO")

        urls = []
        for d in dates.floor("D").unique():
            for dn in dirs_to_search:
                prefix = f"s3://{bucket}/{dn}/{d.strftime('%Y/%m/%d')}/"
                try:
                    found = FileUtility.expand_paths(f"{prefix}*.nc")
                    # Also try .h5 for SDRs
                    if not found and "SDR" in dn:
                        found = FileUtility.expand_paths(f"{prefix}*.h5")
                    urls.extend(found)
                except Exception:
                    continue

        return sorted(urls)


def omps_nadir_preprocess(ds: xr.Dataset, product: str = "v8toz") -> xr.Dataset:
    """
    Preprocess OMPS Nadir dataset lazily.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    product : str, optional
        Product type, by default "v8toz".

    Returns
    -------
    xr.Dataset
        Processed dataset.

    Examples
    --------
    >>> ds = omps_nadir_preprocess(ds, product='v8toz')
    """
    if product == "v8toz":
        ds = _preprocess_v8toz(ds)
    elif product in ["nmto3_l2", "nmto3_l3"]:
        # Use existing logic from omps.py
        from .omps import omps_preprocess

        ds = omps_preprocess(ds, product=product)
        return ds
    elif product in ["tc_sdr", "np_sdr"]:
        ds = _preprocess_sdr(ds)

    # Standardize coordinates and dimensions
    ds = standardize_satellite_coords(ds)

    # Ensure a time dimension exists for stitching if not already present
    if "time" not in ds.dims:
        if "time" in ds.coords:
            if ds.coords["time"].ndim == 1:
                # If time is 1D and matches y or x, swap it
                # But jpss_time_to_datetime usually produces something indexed by y
                pass
            else:
                # Expand a new time dimension
                ds = ds.expand_dims("time")
        else:
            # Try to add from attributes
            ds = add_time_coord(ds, time_attr="time_coverage_start")

    return ds


def _preprocess_v8toz(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess NOAA V8TOZ Total Ozone EDR.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Processed dataset with standard names, lazily converted time, and quality masking.

    Examples
    --------
    >>> ds = _preprocess_v8toz(ds)
    """
    mapping = {
        "Latitude": "latitude",
        "Longitude": "longitude",
        "ScanTime": "time_raw",
        "ColumnAmountO3": "ozone_column",
        "AerosolIndex": "aerosol_index",
        "So2Index": "so2_index",
        "CloudFraction": "cloud_fraction",
        "QualityFlag": "quality_flag",
    }

    # Rename variables if they exist
    rename_dict = {old: new for old, new in mapping.items() if old in ds.variables}
    if rename_dict:
        ds = ds.rename(rename_dict)

    # Handle Time (Lazy)
    if "time_raw" in ds.variables:
        ds["time"] = jpss_time_to_datetime(ds["time_raw"])
        ds = ds.set_coords("time")

    # Quality Flagging (Lazy)
    if "quality_flag" in ds.variables and "ozone_column" in ds.variables:
        # According to OMPS V8TOZ docs, non-zero flags are usually bad
        ds["ozone_column"] = ds["ozone_column"].where(ds["quality_flag"] == 0)

    return ds


def _preprocess_sdr(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess NOAA SDR (Sensor Data Record).

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset, potentially merged from SDR and GEO groups.

    Returns
    -------
    xr.Dataset
        Processed dataset with standard names.

    Examples
    --------
    >>> ds = _preprocess_sdr(ds)
    """
    # SDRs usually have data in groups
    mapping = {
        "All_Data/OMPS-TC-SDR_All/Radiance": "radiance",
        "All_Data/OMPS-NP-SDR_All/Radiance": "radiance",
        "All_Data/OMPS-TC-GEO_All/Latitude": "latitude",
        "All_Data/OMPS-TC-GEO_All/Longitude": "longitude",
        "All_Data/OMPS-NP-GEO_All/Latitude": "latitude",
        "All_Data/OMPS-NP-GEO_All/Longitude": "longitude",
        # Names after merging groups (no prefix)
        "Radiance": "radiance",
        "Latitude": "latitude",
        "Longitude": "longitude",
    }
    # Ensure standard names are set even if renaming fails (e.g. they already exist)
    # We rename only if the source exists and target does not exist as a variable.
    rename_dict = {
        old: new
        for old, new in mapping.items()
        if old in ds.variables and new not in ds.variables and old != new
    }
    if rename_dict:
        ds = ds.rename(rename_dict)

    return ds
