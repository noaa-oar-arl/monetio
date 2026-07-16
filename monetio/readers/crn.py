"""CRN Reader"""

import os
from datetime import datetime
from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
import xarray as xr

from ..util import force_object_strings
from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history
from .time_utils import parse_yyyymmdd_hhmm

if TYPE_CHECKING:
    import dask.dataframe as dd

HCOLS = [
    "WBANNO",
    "UTC_DATE",
    "UTC_TIME",
    "LST_DATE",
    "LST_TIME",
    "CRX_VN",
    "LONGITUDE",
    "LATITUDE",
    "T_CALC",
    "T_AVG",
    "T_MAX",
    "T_MIN",
    "P_CALC",
    "SOLARAD",
    "SOLARAD_FLAG",
    "SOLARAD_MAX",
    "SOLARAD_MAX_FLAG",
    "SOLARAD_MIN",
    "SOLARAD_MIN_FLAG",
    "SUR_TEMP_TYPE",
    "SUR_TEMP",
    "SUR_TEMP_FLAG",
    "SUR_TEMP_MAX",
    "SUR_TEMP_MAX_FLAG",
    "SUR_TEMP_MIN",
    "SUR_TEMP_MIN_FLAG",
    "RH_AVG",
    "RH_AVG_FLAG",
    "SOIL_MOISTURE_5",
    "SOIL_MOISTURE_10",
    "SOIL_MOISTURE_20",
    "SOIL_MOISTURE_50",
    "SOIL_MOISTURE_100",
    "SOIL_TEMP_5",
    "SOIL_TEMP_10",
    "SOIL_TEMP_20",
    "SOIL_TEMP_50",
    "SOIL_TEMP_100",
]

DCOLS = [
    "WBANNO",
    "LST_DATE",
    "CRX_VN",
    "LONGITUDE",
    "LATITUDE",
    "T_MAX",
    "T_MIN",
    "T_MEAN",
    "T_AVG",
    "P_CALC",
    "SOLARAD",
    "SUR_TEMP_TYPE",
    "SUR_TEMP_MAX",
    "SUR_TEMP_MAX_2",
    "SUR_TEMP_MIN",
    "SUR_TEMP_AVG",
    "RH_MAX",
    "RH_MIN",
    "RH_AVG",
    "SOIL_MOISTURE_5",
    "SOIL_MOISTURE_10",
    "SOIL_MOISTURE_20",
    "SOIL_MOISTURE_50",
    "SOIL_MOISTURE_100",
    "SOIL_TEMP_5",
    "SOIL_TEMP_10",
    "SOIL_TEMP_20",
    "SOIL_TEMP_50",
    "SOIL_TEMP_100",
]

SHCOLS = [
    "WBANNO",
    "UTC_DATE",
    "UTC_TIME",
    "LST_DATE",
    "LST_TIME",
    "CRX_VN",
    "LONGITUDE",
    "LATITUDE",
    "T_MEAN",
    "P_CALC",
    "SOLARAD",
    "SOLARAD_FLAG",
    "SUR_TEMP_AVG",
    "SUR_TEMP_TYPE",
    "SUR_TEMP_FLAG",
    "RH_AVG",
    "RH_FLAG",
    "SOIL_MOISTURE_5",
    "SOIL_TEMP_5",
    "WETNESS",
    "WET_FLAG",
    "WIND",
    "WIND_FLAG",
]


def read_crn(filename: str, **kwargs: dict) -> pd.DataFrame:
    """
    Read a single CRN (US Climate Reference Network) file.

    Parameters
    ----------
    filename : str
        The path or URL to the CRN file.
    **kwargs : dict
        Additional arguments passed to pd.read_csv.

    Returns
    -------
    pd.DataFrame
        The loaded data.

    Examples
    --------
    >>> df = read_crn("CRNH0203-2023-AL_Fairhope_3_NE.txt")
    """
    nanvals = [-99999, -9999.0]
    if "CRND0103" in filename:
        cols = DCOLS
        is_daily = True
    elif "CRNS0101" in filename:
        cols = SHCOLS
        is_daily = False
    else:
        cols = HCOLS
        is_daily = False

    # Use FileUtility to handle remote files
    fs = FileUtility.get_fs(filename)
    with fs.open(filename, "r") as f:
        df = pd.read_csv(
            f,
            sep=r"\s+",
            names=cols,
            na_values=nanvals,
            index_col=False,
            **kwargs,
        )

    # Vectorized date parsing using parse_yyyymmdd_hhmm
    # We avoid .values to promote backend-agnostic behavior.
    # Note: df is a pd.DataFrame here since it's called per-file in the driver,
    # but the parser logic handles arrays robustly.
    if not is_daily:
        if "UTC_DATE" in df.columns and "UTC_TIME" in df.columns:
            df["time"] = parse_yyyymmdd_hhmm(df["UTC_DATE"], df["UTC_TIME"])
        if "LST_DATE" in df.columns and "LST_TIME" in df.columns:
            df["time_local"] = parse_yyyymmdd_hhmm(df["LST_DATE"], df["LST_TIME"])
    else:
        if "LST_DATE" in df.columns:
            # For daily, time is at midnight
            df["time_local"] = parse_yyyymmdd_hhmm(df["LST_DATE"], 0)

    return df


@register_reader("crn")
class CRNReader(PointReader):
    """
    Reader for US Climate Reference Network (USCRN) data.
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
        dates: datetime | list[datetime] | pd.DatetimeIndex | None = None,
        daily: bool = False,
        sub_hourly: bool = False,
        download: bool = False,
        latlonbox: list[float] | None = None,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs,
    ) -> xr.Dataset | pd.DataFrame:
        """
        Open CRN dataset.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File paths or URLs. If None, uses `dates` to discover files.
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
        dates : Union[datetime, List[datetime], pd.DatetimeIndex], optional
            Dates to retrieve if `files` is None.
        daily : bool, optional
            If True, retrieves daily data, by default False.
        sub_hourly : bool, optional
            If True, retrieves sub-hourly (5-min) data, by default False.
        download : bool, optional
            If True, downloads files locally, by default False.
        latlonbox : List[float], optional
            Bounding box [lat_min, lon_min, lat_max, lon_max].
        as_xarray : bool, optional
            If True, returns an xarray.Dataset, by default True.
        lazy : bool, optional
            If True, returns a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the reader and driver.

        Returns
        -------
        Union[xr.Dataset, pd.DataFrame]
            The loaded dataset.
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files, _ = self.build_urls(
                dates, daily=daily, sub_hourly=sub_hourly, latlonbox=latlonbox
            )

        if download:
            files = self.retrieve(files)

        # We use read_crn as the custom read_method
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
            read_method=read_crn,
            lazy=lazy,
            **kwargs,
        )

        # Post-processing: Merge with monitor info and fix columns
        df = self._postprocess(df, latlonbox=latlonbox)

        # Consistently force object strings
        df = force_object_strings(df)

        if as_xarray:
            ds = self.to_xarray(df, **kwargs)
            # Update history for provenance
            ds = update_history(ds, "Merged with CRN station metadata and harmonized.")
            return ds

        return df

    def _postprocess(
        self, df: Union[pd.DataFrame, "dd.DataFrame"], latlonbox: list[float] | None = None
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Merge with station metadata and harmonize column names.

        Parameters
        ----------
        df : Union[pd.DataFrame, dd.DataFrame]
            Input dataframe.
        latlonbox : List[float], optional
            Bounding box [lat_min, lon_min, lat_max, lon_max].

        Returns
        -------
        Union[pd.DataFrame, dd.DataFrame]
            Post-processed dataframe.
        """
        monitors = self.get_monitor_df(latlonbox=latlonbox)
        # Rename WBAN to WBANNO to match data files
        monitors = monitors.rename(columns={"WBAN": "WBANNO"})
        # Ensure siteid/WBANNO is string and padded to 5 digits
        monitors["WBANNO"] = monitors["WBANNO"].astype(str).str.zfill(5)

        df["WBANNO"] = df["WBANNO"].astype(str).str.zfill(5)
        # Merge (unified logic for both backends)
        df = df.merge(monitors, how="left", on=["WBANNO", "LATITUDE", "LONGITUDE"])

        # Handle time conversion if needed
        if "time" not in df.columns:
            if "time_local" in df.columns and "GMT_OFFSET" in df.columns:
                # Use backend-agnostic arithmetic: astype('timedelta64[h]') works for both
                df["time"] = df["time_local"] + df["GMT_OFFSET"].astype("timedelta64[h]")
            elif "time_local" in df.columns:
                # Fallback for daily data if GMT_OFFSET is missing
                df["time"] = df["time_local"]

        df = df.rename(columns={"WBANNO": "siteid"})
        # Lowercase all columns
        df.columns = [c.lower() for c in df.columns]

        # Update history for provenance
        df = update_history(df, "Merged with CRN station metadata and applied GMT offset.")

        return df

    def get_monitor_df(self, latlonbox: list[float] | None = None) -> pd.DataFrame:
        """
        Load the CRN station metadata.

        Parameters
        ----------
        latlonbox : List[float], optional
            Bounding box [lat_min, lon_min, lat_max, lon_max].

        Returns
        -------
        pd.DataFrame
            Station metadata.

        Examples
        --------
        >>> reader = CRNReader()
        >>> monitors = reader.get_monitor_df(latlonbox=[32.0, -114.0, 35.0, -110.0])
        """
        import monetio

        path = os.path.join(os.path.dirname(monetio.__file__), "data", "stations.tsv")
        try:
            mdf = pd.read_csv(path, delimiter="\t")
            # Filter for USCRN
            mdf = mdf.loc[mdf["NETWORK"] == "USCRN"].copy()
        except Exception:
            # Fallback if file missing
            mdf = pd.DataFrame(
                columns=[
                    "STATE",
                    "LOCATION",
                    "VECTOR",
                    "WBAN",
                    "LATITUDE",
                    "LONGITUDE",
                    "NETWORK",
                ]
            )

        if latlonbox is not None:
            con = (
                (mdf.LATITUDE >= latlonbox[0])
                & (mdf.LATITUDE <= latlonbox[2])
                & (mdf.LONGITUDE >= latlonbox[1])
                & (mdf.LONGITUDE <= latlonbox[3])
            )
            mdf = mdf.loc[con].copy()

        return mdf

    def build_urls(
        self,
        dates: datetime | list[datetime] | pd.DatetimeIndex,
        daily: bool = False,
        sub_hourly: bool = False,
        latlonbox: list[float] | None = None,
    ) -> tuple[list[str], list[str]]:
        """
        Discover available URLs for the given dates and monitors.

        Parameters
        ----------
        dates : Union[datetime, List[datetime], pd.DatetimeIndex]
            Dates to retrieve.
        daily : bool, optional
            If True, retrieves daily data, by default False.
        sub_hourly : bool, optional
            If True, retrieves sub-hourly (5-min) data, by default False.
        latlonbox : List[float], optional
            Bounding box [lat_min, lon_min, lat_max, lon_max].

        Returns
        -------
        Tuple[List[str], List[str]]
            List of URLs and filenames.

        Examples
        --------
        >>> reader = CRNReader()
        >>> urls, fnames = reader.build_urls(dates="2023-01-01", daily=True)
        """
        baseurl = "https://www1.ncdc.noaa.gov/pub/data/uscrn/products/"
        monitors = self.get_monitor_df(latlonbox=latlonbox)
        years = pd.DatetimeIndex(np.atleast_1d(dates)).year.unique().astype(str)

        urls = []
        fnames = []

        # Optimization: group by product and year to use fs.ls
        product = "daily01" if daily else ("subhourly01" if sub_hourly else "hourly02")
        fname_prefix = "CRND0103-" if daily else ("CRNS0101-05-" if sub_hourly else "CRNH0203-")

        for y in years:
            year_url = f"{baseurl}{product}/{y}/"
            fs = FileUtility.get_fs(year_url)
            try:
                # Get all files in the directory for this year/product
                all_files = fs.ls(year_url)
                # Map to basenames for matching
                all_fnames = [os.path.basename(f) for f in all_files]

                for _, row in monitors.iterrows():
                    state = row["STATE"]
                    site = row["LOCATION"].replace(" ", "_")
                    vector = row["VECTOR"].replace(" ", "_")

                    rest = f"{y}-{state}_{site}_{vector}.txt"
                    fname = f"{fname_prefix}{rest}"

                    if fname in all_fnames:
                        urls.append(f"{year_url}{fname}")
                        fnames.append(fname)
            except Exception:
                # Fallback to individual checks if ls fails or is not supported
                for _, row in monitors.iterrows():
                    state = row["STATE"]
                    site = row["LOCATION"].replace(" ", "_")
                    vector = row["VECTOR"].replace(" ", "_")

                    rest = f"{y}-{state}_{site}_{vector}.txt"
                    url = f"{year_url}{fname_prefix}{rest}"
                    fname = f"{fname_prefix}{rest}"

                    if fs.exists(url):
                        urls.append(url)
                        fnames.append(fname)

        return urls, fnames

    def retrieve(self, urls: str | list[str]) -> list[str]:
        """
        Download files locally if they don't exist.

        Parameters
        ----------
        urls : Union[str, List[str]]
            List of URLs to download.

        Returns
        -------
        List[str]
            List of local filenames.
        """
        if isinstance(urls, str):
            urls = [urls]

        local_files = []
        for url in urls:
            fname = os.path.basename(url)
            if not os.path.isfile(fname):
                fs = FileUtility.get_fs(url)
                try:
                    fs.get(url, fname)
                except Exception as e:
                    print(f"Failed to retrieve {url}: {e}")
                    continue
            local_files.append(fname)
        return local_files
