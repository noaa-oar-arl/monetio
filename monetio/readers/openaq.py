"""OpenAQ Reader"""

import hashlib
import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Union

import numpy as np
import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd

logger = logging.getLogger(__name__)


@register_reader("openaq")
class OpenAQReader(PointReader):
    """
    OpenAQ Reader for real-time fetches (JSONL format).
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
        dates: pd.DatetimeIndex | list[datetime] | datetime | str = None,
        wide_fmt: bool = True,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load OpenAQ data.

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
        wide_fmt : bool, optional
            Whether to return data in wide format, by default True.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded OpenAQ data.

        Examples
        --------
        >>> from monetio.readers.openaq import OpenAQReader
        >>> reader = OpenAQReader()
        >>> ds = reader.open_dataset(dates='2023-01-01', lazy=True)
        """

        # For backward compatibility, if the first argument looks like dates, swap them.
        if (
            files is not None
            and dates is None
            and isinstance(files, pd.DatetimeIndex | datetime | pd.Timestamp | list | str)
        ):
            # If it's a string or list, it could be files.
            # But if it's a DatetimeIndex or a single datetime, it's definitely dates.
            if isinstance(files, pd.DatetimeIndex | datetime | pd.Timestamp):
                dates = files
                files = None
            elif isinstance(files, list) and len(files) > 0 and isinstance(files[0], datetime):
                dates = files
                files = None

        if files is None and dates is not None:
            files = build_urls(dates)

        # Use a more robust check for files
        has_files = files is not None
        if has_files:
            if isinstance(files, list | pd.Series | np.ndarray):
                has_files = len(files) > 0
            elif isinstance(files, str):
                has_files = True
            else:
                try:
                    has_files = bool(files)
                except ValueError:
                    has_files = len(files) > 0

        if not has_files:
            # Return empty object of correct type
            if lazy:
                import dask.dataframe as dd

                df = dd.from_pandas(pd.DataFrame(), npartitions=1)
            else:
                df = pd.DataFrame()

            if as_xarray:
                return xr.Dataset()
            return df

        # Use base class to open
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
            read_method=read_openaq_json,
            as_xarray=False,
            lazy=lazy,
            **kwargs,
        )

        # Post-processing (always long format to maintain laziness)
        df = self._post_process(df, dates=dates)
        df = self.harmonize(df)

        if as_xarray:
            # Pop expand2d from kwargs if present to avoid multiple values error
            exp2d = kwargs.pop("expand2d", wide_fmt)
            # Filter out expand2d from kwargs if still present (defensive)
            to_xr_kwargs = {k: v for k, v in kwargs.items() if k != "expand2d"}
            ds = self.to_xarray(df, expand2d=exp2d, **to_xr_kwargs)

            # Update history
            ds = update_history(ds, "Read OpenAQ data.")
            return ds

        if wide_fmt:
            from ..util import long_to_wide

            df = long_to_wide(df)

        return df

    def _post_process(
        self,
        df: Union[pd.DataFrame, "dd.DataFrame"],
        dates: pd.DatetimeIndex | list[datetime] | datetime | str = None,
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Internal post-processing logic (backend-agnostic).

        Parameters
        ----------
        df : Union[pd.DataFrame, dd.DataFrame]
            Input dataframe.
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str], optional
            Requested dates for filtering.

        Returns
        -------
        Union[pd.DataFrame, dd.DataFrame]
            The post-processed dataframe.
        """
        # Determine backend
        try:
            import dask.dataframe as dd

            is_dask = isinstance(df, dd.DataFrame)
        except ImportError:
            is_dask = False

        # Use .empty for Pandas and .npartitions for Dask
        if is_dask:
            if df.npartitions == 0:
                return df
        else:
            if df.empty:
                return df

        if dates is not None:
            dates = pd.DatetimeIndex(np.atleast_1d(pd.to_datetime(dates)))
            df = df.loc[(df.time >= dates.min()) & (df.time <= dates.max())]

        # 1. SITE ID (Lazy friendly)
        def _get_siteid(df_part: pd.DataFrame) -> pd.DataFrame:
            if df_part.empty:
                df_part["siteid"] = pd.Series(dtype=object)
                return df_part

            # to_hash might be missing some columns if the file was empty or different
            needed = ["location", "latitude", "longitude"]
            for col in needed:
                if col not in df_part.columns:
                    df_part["siteid"] = "unknown"
                    return df_part

            to_hash = (
                df_part.location.astype(str)
                + " "
                + df_part.latitude.astype(str)
                + " "
                + df_part.longitude.astype(str)
            )
            # Use country + hash
            country = df_part.country if "country" in df_part.columns else "XX"
            df_part["siteid"] = (
                country.astype(str)
                + "_"
                + to_hash.str.encode("utf-8")
                .apply(lambda b: hashlib.sha1(b).hexdigest() if pd.notnull(b) else "nan")
                .str.slice(0, 7)
            )
            return df_part

        if is_dask:
            df = df.map_partitions(_get_siteid)
        else:
            df = _get_siteid(df)

        # Rename parameter/unit to variable/units for MONETIO consistency
        df = df.rename(columns={"parameter": "variable", "unit": "units", "value": "obs"})

        # 2. Unit Conversions (Lazy friendly)
        ppm_to_ugm3 = {
            "o3": 1990,
            "co": 1160,
            "no2": 1900,
            "no": 1240,
            "so2": 2650,
            "ch4": 664,
            "co2": 1820,
        }
        ppm_to_ugm3["nox"] = ppm_to_ugm3["no2"]

        def _convert_units(df_part: pd.DataFrame) -> pd.DataFrame:
            if df_part.empty:
                return df_part
            for vn, f in ppm_to_ugm3.items():
                if "variable" in df_part.columns and "units" in df_part.columns:
                    is_ug = (df_part.variable == vn) & (df_part.units == "µg/m³")
                    df_part.loc[is_ug, "obs"] /= f
                    df_part.loc[is_ug, "units"] = "ppm"
            return df_part

        if is_dask:
            df = df.map_partitions(_convert_units)
        else:
            df = _convert_units(df)

        # Drop duplicates consistently for both paths to ensure reliable 2D expansion
        subset = [
            "time",
            "latitude",
            "longitude",
            "siteid",
            "variable",
        ]
        subset = [c for c in subset if c in df.columns]
        df = df.drop_duplicates(subset=subset)

        # Update history if attributes exist
        df = update_history(df, "Post-processed OpenAQ data (siteid, unit conversion).")

        return df

    def to_xarray(
        self,
        df: Union[pd.DataFrame, "dd.DataFrame"],
        expand2d: bool = True,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Convert OpenAQ DataFrame to Xarray Dataset, ensuring consistent naming.

        Parameters
        ----------
        df : Union[pd.DataFrame, dd.DataFrame]
            Input dataframe.
        expand2d : bool, optional
            Whether to expand to 2D (time, node) structure, by default True.
        **kwargs : dict
            Additional arguments passed to super().to_xarray.

        Returns
        -------
        xr.Dataset
            The loaded dataset.
        """
        ds = super().to_xarray(df, expand2d=expand2d, **kwargs)

        if expand2d:
            # If it was expanded via ds_to_2d, it will have o3, pm25 etc. as data vars
            # and o3_unit, pm25_unit etc. as well.
            # We want to rename them to o3_ppm, pm25_ugm3 to match consistent MONETIO naming.
            ppm_vars = ["o3", "co", "no2", "no", "so2", "ch4", "co2", "nox"]
            ugm3_vars = ["pm1", "pm25", "pm4", "pm10", "bc"]

            rename_dict = {}
            for v in ppm_vars:
                if v in ds.data_vars:
                    ds[v].attrs["units"] = "ppm"
                    rename_dict[v] = f"{v}_ppm"
                    if f"{v}_unit" in ds.data_vars:
                        ds = ds.drop_vars(f"{v}_unit")
            for v in ugm3_vars:
                if v in ds.data_vars:
                    ds[v].attrs["units"] = "ug/m3"
                    rename_dict[v] = f"{v}_ugm3"
                    if f"{v}_unit" in ds.data_vars:
                        ds = ds.drop_vars(f"{v}_unit")

            if rename_dict:
                ds = ds.rename(rename_dict)
                # Format units to LaTeX if applicable
                from .base import _format_units

                ds = _format_units(ds)
                ds = update_history(ds, f"Renamed variables: {list(rename_dict.values())}")

        return ds


def build_urls(dates: pd.DatetimeIndex | list[datetime] | datetime | str) -> list[str]:
    """
    Construct OpenAQ URLs for the given dates.
    Uses HTTPS by default for better compatibility in restricted environments.

    Parameters
    ----------
    dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
        Dates to build URLs for.

    Returns
    -------
    List[str]
        List of URLs.
    """
    from .drivers import FileUtility

    # We try S3 listing first, but fall back to assuming existence if it fails
    s3bucket = "openaq-fetches/realtime"

    try:
        fs = FileUtility.get_fs("s3://openaq-fetches")
        folders = fs.ls(s3bucket)
        use_s3 = True
    except Exception:
        use_s3 = False

    dates = pd.to_datetime(dates)
    if isinstance(dates, pd.Timestamp):
        dates = pd.DatetimeIndex([dates])
    dates = dates.floor("D").unique()

    if use_s3:
        days_available = [folder.split("/")[-1] for folder in folders]
    else:
        # Fallback: assume all requested dates might be available via HTTPS
        # Use .dt accessor if it's a Series, otherwise floor directly if it's a DatetimeIndex
        dates_dt = pd.to_datetime(dates)
        if hasattr(dates_dt, "dt"):
            days_available = [d.strftime(r"%Y-%m-%d") for d in dates_dt.dt.floor("D").unique()]
        else:
            days_available = [d.strftime(r"%Y-%m-%d") for d in dates_dt.floor("D").unique()]

    dates_available = pd.to_datetime(days_available, format=r"%Y-%m-%d", errors="coerce")

    dates_requested = pd.Series(dates).floor("D").drop_duplicates()
    dates_have = dates_requested[dates_requested.isin(dates_available)]

    urls = []
    for date in dates_have:
        sdate = date.strftime(r"%Y-%m-%d")
        if use_s3:
            try:
                files = fs.ls(f"{s3bucket}/{sdate}")
                urls.extend(f"s3://{f}" for f in files)
            except Exception as e:
                logger.warning(f"Failed to list S3 files for date {sdate}: {e}")
                # Fallback to a few standard names if listing fails but we know the day exists?
                # Actually OpenAQ fetches have unpredictable names (timestamps).
                # If S3 ls failed, we probably can't get the filenames easily.
        else:
            # Without S3 listing, we can't easily know the granule names for OpenAQ fetches.
            # But the user might be providing files explicitly.
            pass

    return urls


def read_openaq_json(fn: str, storage_options: dict = None, **kwargs: Any) -> pd.DataFrame:
    """
    Read an OpenAQ JSONL file.

    Parameters
    ----------
    fn : str
        File path or URL.
    storage_options : dict, optional
        Storage options for fsspec, by default None.
    **kwargs : Any
        Additional arguments.

    Returns
    -------
    pd.DataFrame
        The loaded data.
    """
    # Convert HTTP URLs to s3:// if they are in the openaq-fetches bucket
    # to avoid 403 Forbidden issues in some environments.
    if isinstance(fn, str) and "openaq-fetches.s3.amazonaws.com" in fn:
        fn = fn.replace("https://openaq-fetches.s3.amazonaws.com", "s3://openaq-fetches")
        fn = fn.replace("http://openaq-fetches.s3.amazonaws.com", "s3://openaq-fetches")
        if storage_options is None:
            from .drivers import get_default_storage_options

            storage_options = get_default_storage_options(fn)

    try:
        df = pd.read_json(fn, lines=True, storage_options=storage_options)
    except Exception as e:
        logger.debug(f"Failed to read OpenAQ JSON {fn}: {e}")
        raise

    if df.empty:
        return df

    if "attribution" in df.columns:
        df = df.drop(columns="attribution")

    if "coordinates" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["coordinates"])
    if df.empty:
        return df

    to_expand = ["date", "averagingPeriod", "coordinates"]
    to_expand = [c for c in to_expand if c in df.columns]

    # Expand JSON columns
    new = pd.json_normalize(json.loads(df[to_expand].to_json(orient="records")))

    # Process Time
    if "date.utc" in new.columns:
        time = pd.to_datetime(new["date.utc"]).dt.tz_localize(None)
    else:
        time = pd.Series(np.nan, index=new.index, dtype="datetime64[ns]")

    if "date.local" in new.columns:
        try:
            # Handle possible varied offset formats like +0100 or +01:00
            utcoffset_str = new["date.local"].str.slice(-6, None)
            # Replace +0100 with +01:00 if necessary
            utcoffset_str = utcoffset_str.str.replace(r"(\d{2})(\d{2})$", r"\1:\2", regex=True)
            utcoffset = pd.to_timedelta(utcoffset_str)
        except Exception:
            utcoffset = pd.Timedelta(0)
    else:
        utcoffset = pd.Timedelta(0)

    time_local = time + utcoffset

    # Averaging period
    averagingPeriod = pd.Series(np.full(len(new), np.nan, dtype="timedelta64[ns]"))
    if "averagingPeriod.value" in new.columns and "averagingPeriod.unit" in new.columns:
        value = new["averagingPeriod.value"]
        units = new["averagingPeriod.unit"]
        unique_units = units.dropna().unique()
        for unit in unique_units:
            is_unit = units == unit
            try:
                # Map units for pd.to_timedelta
                u = unit
                if u == "hours":
                    u = "h"
                elif u == "minutes":
                    u = "m"
                elif u == "seconds":
                    u = "s"
                averagingPeriod.loc[is_unit] = pd.to_timedelta(value[is_unit], unit=u)
            except Exception:
                pass

    # Reassemble DataFrame
    df = df.drop(columns=to_expand).assign(
        time=time,
        time_local=time_local,
        utcoffset=utcoffset,
        averagingPeriod=averagingPeriod,
    )

    if "coordinates.latitude" in new.columns:
        df["latitude"] = new["coordinates.latitude"]
    if "coordinates.longitude" in new.columns:
        df["longitude"] = new["coordinates.longitude"]

    if "value" in df.columns:
        df["value"] = df["value"].astype(float)

    return df


# -----------------------------------------------------------------------------
# Legacy Compatibility
# -----------------------------------------------------------------------------


def read_json(fp_or_url: str, **kwargs: Any) -> pd.DataFrame:
    """Legacy wrapper for read_openaq_json."""
    return read_openaq_json(fp_or_url, **kwargs)


def read_json2(fp_or_url: str, **kwargs: Any) -> pd.DataFrame:
    """Legacy wrapper for read_openaq_json."""
    # Note: original read_json2 used requests, but read_openaq_json is preferred.
    return read_openaq_json(fp_or_url, **kwargs)


class OPENAQ:
    """Legacy OPENAQ class."""

    NON_MOLEC_PARAMS = ["pm1", "pm25", "pm4", "pm10", "bc"]
    PPM_TO_UGM3 = {
        "o3": 1990,
        "co": 1160,
        "no2": 1900,
        "no": 1240,
        "so2": 2650,
        "ch4": 664,
        "co2": 1820,
    }
    PPM_TO_UGM3["nox"] = PPM_TO_UGM3["no2"]

    def __init__(self, engine: str = "pandas"):
        self.engine = engine

    def build_urls(self, dates: pd.DatetimeIndex | list[datetime] | datetime | str) -> list[str]:
        return build_urls(dates)

    def add_data(
        self,
        dates: pd.DatetimeIndex | list[datetime] | datetime | str,
        *,
        num_workers: int = 1,
        wide_fmt: bool = True,
        lazy: bool = False,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        reader = OpenAQReader()
        # num_workers is ignored in modern reader as it relies on dask config
        return reader.open_dataset(dates=dates, wide_fmt=wide_fmt, lazy=lazy)
