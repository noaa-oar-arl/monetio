"""AirNow Reader"""

import os
from datetime import datetime
from functools import lru_cache, partial
from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    import dask.dataframe as dd
    import timezonefinder
from numpy import nan

from ..util import long_to_wide
from .base import PointReader, register_reader
from .drivers import FileUtility
from .epa_utils import add_monitor_metadata
from .sat_utils import update_history


@register_reader("airnow")
class AirNowReader(PointReader):
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
        dates: pd.DatetimeIndex | list[datetime] | datetime | str = None,
        download: bool = False,
        wide_fmt: bool = True,
        n_procs: int = 1,
        daily: bool = False,
        bad_utcoffset: str = "drop",
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load AirNow data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File path, list of paths, or glob pattern.
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
            Dates to retrieve if files are not provided.
        download : bool, optional
            Whether to download files to local directory, by default False.
        wide_fmt : bool, optional
            Whether to return data in wide format (pollutants as columns), by default True.
        n_procs : int, optional
            Number of processors for dask compute (if not lazy), by default 1.
        daily : bool, optional
            Whether to load daily data instead of hourly, by default False.
        bad_utcoffset : str, optional
            How to handle sites with zero UTC offset and large longitude.
            Options: 'drop', 'null', 'fix', 'leave'. By default 'drop'.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the driver.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded AirNow data.
        """

        if files is None and dates is not None:
            # Construct URLs from dates
            urls, fnames = build_urls(dates, daily=daily)

            if download:
                for url, fname in zip(urls, fnames):
                    retrieve(url, fname)
                files = fnames.tolist()
            else:
                files = urls.tolist()

        if not files:
            raise ValueError("Must provide either 'files' or 'dates'.")

        # Define per-file preprocessing
        storage_options = kwargs.get("storage_options")
        if not storage_options and any(str(f).startswith("s3://") for f in files):
            from .drivers import get_default_storage_options

            storage_options = get_default_storage_options(str(files[0]))

        read_func = partial(read_airnow_csv, daily=daily, storage_options=storage_options)

        # Use base class to open
        # We stay in long format if we are lazy to avoid expensive shuffles/computes
        # during the DataFrame stage.
        df = super().open_dataset(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser=virtualizarr_parser,
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            read_method=read_func,
            as_xarray=False,
            lazy=lazy,
            **kwargs,
        )

        # Post-processing
        # We only perform wide_fmt here if NOT lazy, to avoid the hidden compute in long_to_wide
        do_wide = wide_fmt and not lazy
        df = self._post_process(df, daily=daily, wide_fmt=do_wide, bad_utcoffset=bad_utcoffset)

        df = self.harmonize(df)

        if not lazy and hasattr(df, "compute") and not isinstance(df, pd.DataFrame):
            df = df.compute(num_workers=n_procs)

        if as_xarray:
            # Filter out expand2d from kwargs if present to avoid double-passing
            to_xr_kwargs = {k: v for k, v in kwargs.items() if k != "expand2d"}
            ds = self.to_xarray(df, expand2d=wide_fmt, **to_xr_kwargs)

            # Ensure history from _post_process and harmonize is there
            # even if lost in DataFrame stage (Dask doesn't support .attrs)
            for msg in [
                "Post-processed and filtered AirNow data.",
                "Harmonized and dropped NaN locations.",
            ]:
                if msg not in ds.attrs.get("history", ""):
                    ds = update_history(ds, msg)

            if bad_utcoffset == "fix" and "utcoffset" in ds.variables:
                ds = self._fix_utcoffset_xarray(ds)

            # Update history
            ds = update_history(ds, "Read AirNow data.")

            return ds

        return df

    def _fix_utcoffset_xarray(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Fix UTC offsets in an Xarray Dataset using a vectorized approach.

        Parameters
        ----------
        ds : xr.Dataset
            Input dataset.

        Returns
        -------
        xr.Dataset
            Dataset with fixed UTC offsets and updated local time.
        """

        def _get_uo_vec(lat, lon, current_uo):
            # Only fix if current_uo is 0 and longitude is far from 0
            # We use a vectorized approach
            mask = (current_uo == 0) & (np.abs(lon) > 20)
            # Use sum() or other dask-friendly reduction if we really wanted to check,
            # but in apply_ufunc we can just process it.
            # To avoid hidden compute from np.any(mask) if current_uo is dask,
            # we should avoid it. But here we are inside the worker function,
            # so mask is already a numpy array.
            # WAIT: if dask="parallelized", then _get_uo_vec is called on numpy chunks.
            # So np.any(mask) is fine here!
            if not np.any(mask):
                return current_uo

            res = current_uo.copy().astype(float)

            # Identify indices where mask is True
            idx = np.where(mask)

            # Get lat/lon at those indices
            lats_to_fix = lat[idx]
            lons_to_fix = lon[idx]

            # Find unique locations to fix to minimize TimezoneFinder calls
            locs = np.stack([lats_to_fix, lons_to_fix], axis=-1)
            unique_locs, inverse = np.unique(locs, axis=0, return_inverse=True)

            # Compute unique offsets
            unique_offsets = np.array([get_utcoffset(la, lo) for la, lo in unique_locs])

            # Broadcast unique offsets back to all masked positions
            res[idx] = unique_offsets[inverse]

            return res

        new_uo = xr.apply_ufunc(
            _get_uo_vec,
            ds.latitude,
            ds.longitude,
            ds.utcoffset,
            dask="parallelized",
            output_dtypes=[float],
        )

        ds["utcoffset"] = new_uo

        if "time_local" in ds.variables:
            # Re-calculate local time if it was already there
            # time_local = time + utcoffset
            # Xarray handles the broadcasting
            ds["time_local"] = ds.time + ds.utcoffset.astype("timedelta64[h]")

        return ds

    def _post_process(
        self,
        df: Union[pd.DataFrame, "dd.DataFrame"],
        daily: bool = False,
        wide_fmt: bool = True,
        bad_utcoffset: str = "drop",
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Internal post-processing logic.

        Parameters
        ----------
        df : Union[pd.DataFrame, dd.DataFrame]
            Input dataframe.
        daily : bool, optional
            Whether to load daily data instead of hourly, by default False.
        wide_fmt : bool, optional
            Whether to return data in wide format, by default True.
        bad_utcoffset : str, optional
            How to handle sites with zero UTC offset and large longitude, by default 'drop'.

        Returns
        -------
        Union[pd.DataFrame, dd.DataFrame]
            The post-processed dataframe.
        """
        # Determine backend
        try:
            import dask.dataframe as dd

            is_dask = isinstance(df, dd.DataFrame)
            lib = dd if is_dask else pd
        except ImportError:
            is_dask = False
            lib = pd

        # Time conversion (Backend-agnostic API)
        if daily:
            df["time"] = lib.to_datetime(df.date, format=r"%m/%d/%y")
        else:
            # We use string concatenation which works for both
            dt_str = df.date + " " + df.time
            df["time"] = lib.to_datetime(dt_str, format=r"%m/%d/%y %H:%M")
            df["time_local"] = df.time + lib.to_timedelta(df.utcoffset, unit="h")

        df = df.drop(columns=["date"])

        # Metadata (Already supports Dask)
        df = get_station_locations(df)

        savecols = [
            "time",
            "siteid",
            "site",
            "utcoffset",
            "variable",
            "units",
            "obs",
            "time_local",
            "latitude",
            "longitude",
            "cmsa_name",
            "msa_code",
            "msa_name",
            "state_name",
            "epa_region",
        ]

        if daily:
            cols = [col for col in savecols if col not in {"time_local", "utcoffset"}]
        else:
            cols = savecols

        # Filter columns that exist
        df = df[[c for c in cols if c in df.columns]]

        df = df.drop_duplicates()

        # If we are going to return Xarray and using Dask, we defer the 'fix' to the Xarray stage
        # where we have a better vectorized implementation.
        # Otherwise, we do it here (using the optimized DataFrame path in filter_bad_values).
        if is_dask and bad_utcoffset == "fix":
            df_bad_utcoffset = "leave"
        else:
            df_bad_utcoffset = bad_utcoffset

        df = filter_bad_values(df, bad_utcoffset=df_bad_utcoffset)

        if wide_fmt:
            # Warning: long_to_wide currently forces compute on Dask
            df = long_to_wide(df)
            subset = [c for c in ["time", "latitude", "longitude", "siteid"] if c in df.columns]
            df = df.drop_duplicates(subset=subset)

        # Update history
        try:
            df = update_history(df, "Post-processed and filtered AirNow data.")
        except AttributeError:
            # For dask DataFrames that don't support attrs
            pass

        return df


# -----------------------------------------------------------------------------
# Helper functions ported from monetio/obs/airnow.py
# -----------------------------------------------------------------------------


def build_urls(
    dates: pd.DatetimeIndex | list[datetime] | datetime | str, daily: bool = False
) -> tuple:
    """
    Construct AirNow S3 URLs and filenames for the given dates.

    Parameters
    ----------
    dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
        Dates to build URLs for.
    daily : bool, optional
        Whether to build URLs for daily data, by default False.

    Returns
    -------
    tuple
        (urls, filenames) as pandas.Series.

    Examples
    --------
    >>> urls, fnames = build_urls("2023-01-01")
    >>> print(urls[0])
    s3://files.airnowtech.org/airnow/2023/20230101/HourlyData_2023010100.dat
    """
    dates = pd.DatetimeIndex(np.atleast_1d(pd.to_datetime(dates)))
    if daily:
        dates = dates.floor("D").unique()
    else:  # hourly
        dates = dates.floor("h").unique()

    urls = []
    fnames = []
    # Use HTTPS by default as S3 access can be restricted or require region config
    base_url = "https://files.airnowtech.org/airnow/"
    for dt in dates:
        if daily:
            fname = "daily_data.dat"
        else:
            fname = dt.strftime(r"HourlyData_%Y%m%d%H.dat")
        url = base_url + dt.strftime(r"%Y/%Y%m%d/") + fname
        urls.append(url)
        fnames.append(fname)

    return pd.Series(urls, index=None), pd.Series(fnames, index=None)


def retrieve(url: str, fname: str) -> None:
    """
    Retrieve a file from a URL (S3 or HTTP) and save it locally.

    Parameters
    ----------
    url : str
        The URL to retrieve.
    fname : str
        The local filename to save to.

    Examples
    --------
    >>> retrieve("s3://files.airnowtech.org/airnow/2023/20230101/HourlyData_2023010100.dat", "HourlyData_2023010100.dat")
    """
    if not os.path.isfile(fname):
        if url.startswith("s3://"):
            fs = FileUtility.get_fs(url)
            fs.get(url, fname)
        elif url.startswith("http"):
            import requests

            r = requests.get(url)
            r.raise_for_status()
            with open(fname, "wb") as f:
                f.write(r.content)
        else:
            # Local file copy?
            pass
    else:
        pass


def read_airnow_csv(
    fn: str, daily: bool = False, storage_options: dict | None = None, **kwargs
) -> pd.DataFrame:
    """
    Read a single AirNow CSV file.

    Parameters
    ----------
    fn : str
        File path or URL.
    daily : bool, optional
        Whether the file contains daily data, by default False.
    storage_options : dict, optional
        Storage options for fsspec, by default None.
    **kwargs : dict
        Additional arguments passed to pd.read_csv.

    Returns
    -------
    pd.DataFrame
        The loaded data.

    Examples
    --------
    >>> df = read_airnow_csv("HourlyData_2023010100.dat")
    """
    hourly_cols = [
        "date",
        "time",
        "siteid",
        "site",
        "utcoffset",
        "variable",
        "units",
        "obs",
        "source",
    ]
    daily_cols = [
        "date",
        "siteid",
        "site",
        "variable",
        "units",
        "obs",
        "hours",
        "source",
    ]

    # Provide dtype hints for known numeric columns to avoid pandas type
    # inference overhead.  The column indices map to the pipe-delimited
    # AirNow format: 0=date, 1=time, 2=siteid, 3=site, 4=utcoffset,
    # 5=variable, 6=units, 7=obs, 8=source (hourly) or similar for daily.
    _dtype_hints = {4: "float32", 7: "float32"}

    try:
        dft = pd.read_csv(
            fn,
            delimiter="|",
            header=None,
            encoding="ISO-8859-1",
            on_bad_lines="warn",
            dtype=_dtype_hints,
            storage_options=storage_options,
        )
    except Exception:
        dft = pd.DataFrame(columns=hourly_cols)

    ncols = dft.columns.size
    if ncols == len(hourly_cols):
        dft.columns = hourly_cols
    elif ncols == len(hourly_cols) - 1:
        daily = True
        dft.columns = daily_cols
    else:
        # Return empty with correct cols if mismatch
        # Or raise
        if daily:
            return pd.DataFrame(columns=daily_cols)
        else:
            return pd.DataFrame(columns=hourly_cols)

    dft["obs"] = dft.obs.astype(float)
    # Ensure siteid is object (str) to avoid nullable string issues in Dask/Pandas 3.0
    dft["siteid"] = dft.siteid.astype(str).str.zfill(9).astype(object)

    if not daily and "utcoffset" in dft.columns:
        dft["utcoffset"] = dft.utcoffset.astype(int)

    return dft


def filter_bad_values(
    df: Union[pd.DataFrame, "dd.DataFrame"],
    max_val: float = 3000.0,
    bad_utcoffset: str = "drop",
) -> Union[pd.DataFrame, "dd.DataFrame"]:
    """
    Filter bad values and handle zero UTC offsets.

    Parameters
    ----------
    df : Union[pd.DataFrame, dd.DataFrame]
        Input dataframe.
    max : float, optional
        Maximum allowed observation value, by default 3000.
    bad_utcoffset : str, optional
        How to handle sites with zero UTC offset and large longitude,
        by default "drop".

    Returns
    -------
    Union[pd.DataFrame, dd.DataFrame]
        Filtered dataframe.

    Examples
    --------
    >>> df = filter_bad_values(df, max=1000.0, bad_utcoffset="fix")
    """
    from numpy import nan

    df["obs"] = df["obs"].where((df.obs <= max_val) & (df.obs >= 0), nan)

    if "utcoffset" in df.columns:
        bad_rows = df.query("utcoffset == 0 and abs(longitude) > 20")
        if bad_utcoffset == "null":
            # For dask compatibility
            df["utcoffset"] = df["utcoffset"].where(
                ~((df.utcoffset == 0) & (df.longitude.abs() > 20)), nan
            )
        elif bad_utcoffset == "drop":
            df = df.loc[~((df.utcoffset == 0) & (df.longitude.abs() > 20))]
        elif bad_utcoffset == "fix":
            # TimezoneFinder is slow, so only call it for unique locations
            unique_locs = bad_rows.drop_duplicates(subset=["latitude", "longitude"])
            if not unique_locs.empty:
                # Use vectorized-style approach even for DataFrame
                # Convert to numpy for the map
                lats = unique_locs.latitude.to_numpy()
                lons = unique_locs.longitude.to_numpy()
                offsets = np.array([get_utcoffset(la, lo) for la, lo in zip(lats, lons)])

                # Create a mapping DataFrame
                mapping = pd.DataFrame({"latitude": lats, "longitude": lons, "new_offset": offsets})

                # Merge back to bad_rows
                bad_rows_fixed = bad_rows.merge(mapping, on=["latitude", "longitude"], how="left")
                df.loc[bad_rows.index, "utcoffset"] = bad_rows_fixed.new_offset.to_numpy()
        elif bad_utcoffset == "leave":
            pass
        else:
            raise ValueError("`bad_utcoffset` must be one of: 'null', 'drop', 'fix', 'leave'")

    return df


@lru_cache(maxsize=1)
def _get_tf(*, in_memory: bool = True) -> "timezonefinder.TimezoneFinder":
    """Get the TimezoneFinder instance."""
    from ..util import _import_required

    timezonefinder = _import_required("timezonefinder")

    return timezonefinder.TimezoneFinder(in_memory=in_memory)


@lru_cache(maxsize=1024)
def get_utcoffset(lat: float, lon: float) -> float:
    """
    Get the UTC offset for a given latitude and longitude.

    Parameters
    ----------
    lat : float
        Latitude.
    lon : float
        Longitude.

    Returns
    -------
    float
        UTC offset in hours.

    Examples
    --------
    >>> offset = get_utcoffset(40.0, -80.0)
    """
    import warnings

    try:
        import pytz
    except ImportError:
        warnings.warn("pytz not installed, guessing UTC offset based on longitude")
        do_guess = True
    else:
        do_guess = False

    if do_guess:
        lon_ = (lon + 180) % 360 - 180
        return round(lon_ / 15, 0)

    else:
        finder = _get_tf()
        tz_str = finder.timezone_at(lng=lon, lat=lat)
        if tz_str:
            tz = pytz.timezone(tz_str)
            uo = tz.utcoffset(datetime(2020, 1, 1), is_dst=False).total_seconds() / 3600
            return uo
        else:
            return nan


def get_station_locations(
    df: Union[pd.DataFrame, "dd.DataFrame"],
) -> Union[pd.DataFrame, "dd.DataFrame"]:
    """
    Add site metadata to the dataframe.

    Parameters
    ----------
    df : Union[pd.DataFrame, dd.DataFrame]
        Input dataframe.

    Returns
    -------
    Union[pd.DataFrame, dd.DataFrame]
        Dataframe with site metadata.

    Examples
    --------
    >>> df = get_station_locations(df)
    """
    return add_monitor_metadata(df, airnow=True)
