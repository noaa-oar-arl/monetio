"""CEMS Reader"""

from datetime import datetime
from typing import TYPE_CHECKING, Union

import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd


class CEMS:
    """Legacy CEMS class for backward compatibility."""

    pass


@register_reader("cems")
class CEMSReader(PointReader):
    """
    Reader for Continuous Emissions Monitoring System (CEMS) data.
    """

    def open_dataset(
        self,
        files: str | list[str] | None = None,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        dates: pd.DatetimeIndex | list[datetime] | datetime | str | None = None,
        states: str | list[str] = "md",
        n_procs: int = 1,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs: dict,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load CEMS data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File paths or URLs to read. If None, uses `dates` and `states` to discover files.
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
            Dates to retrieve.
        states : Union[str, List[str]], optional
            States to retrieve (e.g., 'md'), by default 'md'.
        n_procs : int, optional
            Number of processors (deprecated, handled by dask), by default 1.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the reader and driver.
            Includes `expand2d`, `pivot`, and `wide_fmt` for Xarray conversion.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded CEMS data.

        Examples
        --------
        >>> from monetio.readers.cems import CEMSReader
        >>> reader = CEMSReader()
        >>> ds = reader.open_dataset(dates="2023-01-01", states="md")
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")

            dates = pd.to_datetime(dates)
            if isinstance(dates, pd.Timestamp):
                dates = pd.DatetimeIndex([dates])

            if isinstance(states, str):
                states = [states]

            # Discovery logic
            files = []
            for dt in dates.to_period("M").to_timestamp().unique():
                for st in states:
                    files.append(build_url(dt, st))

        # Filter out arguments that are not for the reader function
        reader_kwargs = {
            k: v for k, v in kwargs.items() if k not in ["expand2d", "pivot", "wide_fmt"]
        }

        df = self.driver.open(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser=virtualizarr_parser,
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            read_method=read_cems,
            lazy=lazy,
            **reader_kwargs,
        )

        df = self.harmonize(df)

        if as_xarray:
            ds = self.to_xarray(df, **kwargs)

            # Retrieve unit mapping from dataframe attrs
            unit_map = getattr(df, "attrs", {}).get("unit_mapping", {})

            if not unit_map:
                # Eagerly peek at the first file if metadata is missing from attributes
                # This only happens if attrs are lost during merge/concat
                try:
                    file_list = FileUtility.expand_paths(files)
                    if file_list:
                        # We use a limited read to get headers and units
                        meta_df = read_cems(file_list[0], nrows=5)
                        unit_map = meta_df.attrs.get("unit_mapping", {})
                except Exception as e:
                    import warnings

                    warnings.warn(f"CEMS unit mapping recovery failed: {e}")

            # Apply units to variables
            if unit_map:
                for varname, unit in unit_map.items():
                    if varname in ds.data_vars:
                        ds[varname].attrs["units"] = unit

            # Update history
            ds = update_history(ds, "Read CEMS data.")

            return ds

        return df


def build_url(date: datetime, state: str) -> str:
    """
    Build CEMS URL for a given date and state.

    Parameters
    ----------
    date : datetime
        The date to retrieve data for.
    state : str
        The state abbreviation (e.g., 'md').

    Returns
    -------
    str
        The constructed URL.
    """
    url = "ftp://newftp.epa.gov/DmDnLoad/emissions/hourly/monthly/"
    url += date.strftime("%Y") + "/"
    fname = date.strftime("%Y") + state.lower() + date.strftime("%m") + ".zip"
    return url + fname


def get_date_fmt(date: str) -> str:
    """
    Determine the date format based on the first entry.

    Parameters
    ----------
    date : str
        The date string to inspect.

    Returns
    -------
    str
        The identified datetime format string.
    """
    temp = date.split("-")
    if len(temp[0]) == 4:
        fmt = "%Y-%m-%d"
    else:
        fmt = "%m-%d-%Y"
    return fmt


def read_cems(efile: str, **kwargs: dict) -> pd.DataFrame:
    """
    Read a single CEMS file.

    Parameters
    ----------
    efile : str
        The path or URL to the CEMS file.
    **kwargs : dict
        Additional arguments passed to pd.read_csv.

    Returns
    -------
    pd.DataFrame
        The loaded CEMS data in long format.

    Examples
    --------
    >>> df = read_cems("2023md01.zip")
    """
    from ..util import force_object_strings

    # Use FileUtility for protocols and compression
    fs = FileUtility.get_fs(efile)
    storage_options = kwargs.pop("storage_options", {})

    # Identify compression
    compression = "zip" if str(efile).endswith(".zip") else "infer"

    with fs.open(efile, "rb", **storage_options) as f:
        dftemp = pd.read_csv(
            f, sep=",", index_col=False, header=0, compression=compression, **kwargs
        )

    if dftemp.empty:
        return dftemp

    # Standardize column names using a mapping
    rename_map = {
        "facility_name": ["facility", "name"],
        "orispl_code": ["orispl"],
        "fac_id": ["facility", "id"],
        "so2_lbs": ["so2", "lbs"],
        "nox_lbs": ["nox", "lbs"],
        "co2_short_tons": ["co2", "short", "tons"],
        "date": ["date"],
        "hour": ["hour"],
        "latitude": ["lat"],
        "longitude": ["lon"],
        "state_name": ["state"],
    }

    new_columns = []
    for col in dftemp.columns:
        cl = col.lower()
        matched = False
        for target, keywords in rename_map.items():
            if all(k in cl for k in keywords):
                # Special case for SO2/NOx to avoid "rate"
                if target in ["so2_lbs", "nox_lbs"] and "rate" in cl:
                    continue
                new_columns.append(target)
                matched = True
                break
        if not matched:
            new_columns.append(cl.strip())

    dftemp.columns = new_columns

    # Capture units mapping for variables
    unit_map = {
        "so2_lbs": "lbs",
        "nox_lbs": "lbs",
        "co2_short_tons": "short tons",
        "gross_load_mw": "MW",
        "steam_load_1000lb_hr": "1000 lb/hr",
    }
    # Store in attributes to be picked up by open_dataset
    dftemp.attrs["unit_mapping"] = {k: v for k, v in unit_map.items() if k in dftemp.columns}

    # Optimized vectorized time construction
    if "date" in dftemp.columns and "hour" in dftemp.columns:
        # Determine format from first non-null
        first_date = dftemp["date"].dropna().iloc[0] if not dftemp["date"].dropna().empty else None
        if first_date:
            dfmt = get_date_fmt(str(first_date))
            dftemp["time"] = pd.to_datetime(dftemp["date"], format=dfmt) + pd.to_timedelta(
                dftemp["hour"], unit="h"
            )

    dftemp = dftemp.drop(columns=["date", "hour", "year"], errors="ignore")

    # siteid construction
    if "orispl_code" in dftemp.columns:
        dftemp["siteid"] = dftemp["orispl_code"].astype(str)

    return force_object_strings(dftemp)
