"""ISH Reader"""

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    import dask.dataframe as dd

from ..util import force_object_strings, normalize_pandas_freq
from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

logger = logging.getLogger(__name__)


def add_ish_metadata(
    df: Union[pd.DataFrame, "dd.DataFrame"],
    history: pd.DataFrame,
) -> Union[pd.DataFrame, "dd.DataFrame"]:
    """
    Add ISH station metadata to the dataframe.

    Parameters
    ----------
    df : Union[pd.DataFrame, dd.DataFrame]
        Input dataframe.
    history : pd.DataFrame
        ISH history/metadata dataframe.

    Returns
    -------
    Union[pd.DataFrame, dd.DataFrame]
        Dataframe with metadata merged.
    """
    try:
        import dask.dataframe as dd

        is_dask = isinstance(df, dd.DataFrame)
    except ImportError:
        is_dask = False

    # Ensure siteid is object for reliable merging
    df = df.assign(siteid=df.siteid.astype(object))

    # Prepare metadata for merging
    dfloc = history.rename(columns={"station_id": "siteid", "ctry": "country"})
    dfloc = force_object_strings(dfloc.drop_duplicates(subset=["siteid"]))

    if is_dask:
        dfloc_wrap = dd.from_pandas(dfloc, npartitions=1)
        dfloc_wrap = force_object_strings(dfloc_wrap)
    else:
        dfloc_wrap = dfloc

    # Merge
    df = df.merge(dfloc_wrap, on="siteid", how="left")

    # Update history
    df = update_history(df, "Added ISH station metadata.")

    return df


class ISH:
    """Helper class for ISH data retrieval."""

    VAR_INFO = [
        ("varlength", "i2", 4),
        ("station_id", "S11", 11),
        ("date", "i4", 8),
        ("htime", "i2", 4),
        ("source_flag", "S1", 1),
        ("latitude", "float", 6),
        ("longitude", "float", 7),
        ("code", "S5", 5),
        ("elev", "i2", 5),
        ("call_letters", "S5", 5),
        ("qc_process", "S4", 4),
        ("wdir", "i2", 3),
        ("wdir_quality", "S1", 1),
        ("wdir_type", "S1", 1),
        ("ws", "i2", 4),
        ("ws_quality", "S1", 1),
        ("ceiling", "i4", 5),
        ("ceiling_quality", "S1", 1),
        ("ceiling_code", "S1", 1),
        ("ceiling_cavok", "S1", 1),
        ("vsb", "i4", 6),
        ("vsb_quality", "S1", 1),
        ("vsb_variability", "S1", 1),
        ("vsb_variability_quality", "S1", 1),
        ("t", "i2", 5),
        ("t_quality", "S1", 1),
        ("dpt", "i2", 5),
        ("dpt_quality", "S1", 1),
        ("p", "i4", 5),
        ("p_quality", "S1", 1),
    ]

    DTYPES = [(name, dtype) for name, dtype, _ in VAR_INFO]
    WIDTHS = [width for _, _, width in VAR_INFO]

    def __init__(self):
        self.history_file = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
        self.history = None
        self.dates = None
        self.verbose = False
        self.source = "aws"

    def read_ish_history(self, dates: pd.DatetimeIndex | None = None):
        """
        Read the ISH history file.

        Parameters
        ----------
        dates : pd.DatetimeIndex, optional
            Dates to filter the history, by default None.
        """
        if dates is None:
            dates = self.dates
        fname = self.history_file

        if self.source == "aws":
            fname = "s3://noaa-isd-pds/isd-history.csv"

        fs = FileUtility.get_fs(fname)
        try:
            with fs.open(fname, "r") as f:
                self.history = pd.read_csv(
                    f, parse_dates=["BEGIN", "END"], dtype={"USAF": str, "WBAN": str}
                )
        except Exception:
            alt = fname.replace("www1.ncdc.noaa.gov", "www.ncei.noaa.gov")
            if alt != fname:
                fs_alt = FileUtility.get_fs(alt)
                with fs_alt.open(alt, "r") as f:
                    self.history = pd.read_csv(
                        f, parse_dates=["BEGIN", "END"], dtype={"USAF": str, "WBAN": str}
                    )
                self.history_file = alt
            else:
                raise

        self.history.columns = [i.lower() for i in self.history.columns]
        if dates is not None:
            # Ensure dates is a DatetimeIndex (not a single Timestamp)
            if not hasattr(dates, "__len__"):
                dates = pd.DatetimeIndex([dates])
            elif not isinstance(dates, pd.DatetimeIndex):
                dates = pd.DatetimeIndex(dates)
            index1 = (self.history.end >= dates.min()) & (self.history.begin <= dates.max())
            self.history = self.history.loc[index1, :]
        self.history = self.history.dropna(subset=["lat", "lon"])
        self.history.loc[:, "usaf"] = self.history.usaf.astype("str").str.zfill(6)
        self.history.loc[:, "wban"] = self.history.wban.astype("str").str.zfill(5)
        self.history["station_id"] = self.history.usaf + self.history.wban
        self.history.rename(columns={"lat": "latitude", "lon": "longitude"}, inplace=True)

    def subset_sites(
        self,
        latmin: float = 32.65,
        lonmin: float = -113.3,
        latmax: float = 34.5,
        lonmax: float = -110.4,
    ) -> pd.DataFrame:
        """
        Subset sites by bounding box.
        """
        latindex = (self.history.latitude >= latmin) & (self.history.latitude <= latmax)
        lonindex = (self.history.longitude >= lonmin) & (self.history.longitude <= lonmax)
        dfloc = self.history.loc[latindex & lonindex, :]
        return dfloc

    def read_data_frame(self, url_or_file, **kwargs):
        """
        Legacy method for backward compatibility.
        """
        df = read_ish_file(url_or_file, **kwargs)
        return df

    def build_urls(
        self,
        dates: pd.DatetimeIndex | None = None,
        sites: pd.DataFrame | None = None,
        lite: bool = False,
    ) -> pd.DataFrame:
        """
        Construct ISH URLs.
        """
        if dates is None:
            dates = self.dates
        if sites is None:
            sites = self.history

        unique_years = pd.to_datetime(dates.year.unique(), format="%Y")
        furls = []

        if lite:
            if self.source == "aws":
                url = "s3://noaa-isd-pds/isd-lite/data"
            else:
                url = "https://www.ncei.noaa.gov/pub/data/noaa/isd-lite"
        elif self.source == "aws":
            url = "s3://noaa-isd-pds/data"
        else:
            url = "https://www.ncei.noaa.gov/pub/data/noaa"

        for syear in unique_years.strftime("%Y"):
            # USAF is 6 digits, WBAN is 5 digits
            year_fnames = (
                sites.usaf.astype(str).str.zfill(6)
                + "-"
                + sites.wban.astype(str).str.zfill(5)
                + "-"
                + syear
                + ".gz"
            )
            for fname in year_fnames:
                furls.append(f"{url}/{syear}/{fname}")

        return pd.Series(furls, name="name").to_frame()

    def get_url_file_objs(self, fname):
        import gzip
        import shutil

        objs = []
        for iii in fname:
            try:
                temp = iii.split("/")[-1]
                out_name = "isd." + temp.replace(".gz", "")

                if str(iii).startswith("s3://"):
                    fs = FileUtility.get_fs(iii)
                    with fs.open(iii, "rb") as f_in:
                        with open(out_name, "wb") as f_out:
                            if iii.endswith(".gz"):
                                with gzip.GzipFile(fileobj=f_in) as gz:
                                    shutil.copyfileobj(gz, f_out)
                            else:
                                shutil.copyfileobj(f_in, f_out)
                    objs.append(out_name)
                else:
                    import requests

                    r2 = requests.get(iii, stream=True)
                    if r2.status_code != 404:
                        objs.append(out_name)
                        with open(out_name, "wb") as fid:
                            gzip_file = gzip.GzipFile(fileobj=r2.raw)
                            shutil.copyfileobj(gzip_file, fid)
            except Exception:
                pass
        return objs


def _clean_col(
    series: pd.Series, missing_vals: float | list[float], multiplier: float = 1.0
) -> pd.Series:
    """
    Clean a numeric column by replacing missing values with NaN and applying a multiplier.
    """
    if not isinstance(missing_vals, list | tuple):
        missing_vals = [missing_vals]
    series = series.astype(float)
    for mv in missing_vals:
        series = series.where(series != mv, np.nan)
    return series / multiplier


def read_ish_file(fname: str, **kwargs) -> pd.DataFrame:
    """
    Read a single ISH (Integrated Surface Hourly) file.

    Parameters
    ----------
    fname : str
        File path, URL, or fsspec-compatible path.
    **kwargs : dict
        Additional arguments passed to FileUtility.get_fs and read_csv.
        Includes `request_retries`, `request_timeout`, and `storage_options`.

    Returns
    -------
    pd.DataFrame
        The loaded data in long format.
    """
    # Regression fix: check valid retries
    request_retries = kwargs.get("request_retries", 3)
    if request_retries < 0:
        raise ValueError(f"`request_retries` must be >= 0, got {request_retries!r}")

    request_timeout = kwargs.get("request_timeout", 10)
    storage_options = kwargs.get("storage_options", {})

    compression = "gzip" if str(fname).endswith(".gz") else None

    # Implement a simple retry loop for legacy compatibility in tests
    frame_as_array = None
    tries = 0
    while tries <= request_retries:
        try:
            # Use FileUtility for all protocols (Local, S3, HTTP, FTP)
            fs = FileUtility.get_fs(fname)

            # Ensure timeout is passed for remote filesystems if not already in storage_options
            if (
                str(fname).startswith(("http", "ftp"))
                and "client_kwargs" not in storage_options
                and "timeout" not in storage_options
            ):
                storage_options["timeout"] = request_timeout

            with fs.open(fname, "rb", compression=compression, **storage_options) as f:
                frame_as_array = np.genfromtxt(f, delimiter=ISH.WIDTHS, dtype=ISH.DTYPES)
            break
        except Exception as e:
            tries += 1
            if tries > request_retries:
                if "timeout" in str(e).lower() or "connect" in str(e).lower() or tries > 1:
                    raise RuntimeError(f"Failed to connect to server for URL {fname}.") from e
                logger.warning(f"Could not read {fname}: {e}")
                return pd.DataFrame()

    if frame_as_array is None:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(np.atleast_1d(frame_as_array))

    if df.empty:
        return df

    # Vectorized cleaning
    # Time construction
    dt_str = df["date"].astype(str).str.zfill(8) + df["htime"].astype(str).str.zfill(4)
    df["time"] = pd.to_datetime(dt_str, format="%Y%m%d%H%M", errors="coerce")
    df = df.dropna(subset=["time"])

    # Decode bytes
    for col, dtype in ISH.DTYPES:
        if "S" in str(dtype) and col in df.columns:
            df[col] = df[col].str.decode("utf-8").str.strip()

    # Numeric cleaning
    df["wdir"] = _clean_col(df["wdir"], 999)
    df["ws"] = _clean_col(df["ws"], 9999, multiplier=10.0)
    df["ceiling"] = _clean_col(df["ceiling"], 99999)
    df["vsb"] = _clean_col(df["vsb"], [99999, 999999])
    df["t"] = _clean_col(df["t"], 9999, multiplier=10.0)
    df["dpt"] = _clean_col(df["dpt"], 9999, multiplier=10.0)
    df["p"] = _clean_col(df["p"], 99999, multiplier=10.0)

    df = df.drop(columns=["date", "htime", "latitude", "longitude"], errors="ignore")

    return df


@register_reader("ish")
class ISHReader(PointReader):
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
        box: list[float] | None = None,
        country: str | None = None,
        state: str | None = None,
        site: str | None = None,
        resample: bool = True,
        window: str = "h",
        download: bool = False,
        n_procs: int = 1,
        verbose: bool = False,
        source: str | None = None,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load ISH (Integrated Surface Hourly) data.

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
        box : List[float], optional
            Bounding box [latmin, lonmin, latmax, lonmax].
        country : str, optional
            Country code to filter sites.
        state : str, optional
            State code to filter sites.
        site : str, optional
            Specific station ID to filter.
        resample : bool, optional
            Whether to resample data to a regular window, by default True.
        window : str, optional
            Resampling window (e.g., 'h'), by default 'h'.
        download : bool, optional
            Whether to download files (if source is ncdc), by default False.
        n_procs : int, optional
            Number of processors for dask compute (if not lazy), by default 1.
        verbose : bool, optional
            Whether to print verbose output, by default False.
        source : str, optional
            Data source: 'ncdc' or 'aws', by default 'aws'.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the driver or to_xarray.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded ISH data.

        Examples
        --------
        >>> from monetio.readers.ish import ISHReader
        >>> reader = ISHReader()
        >>> ds = reader.open_dataset(dates='2021-08-01', site='72406093721')
        """
        # Regression fix: check multiple subsets
        if sum([box is not None, country is not None, state is not None, site is not None]) > 1:
            raise ValueError("Only one of `box`, `country`, `state`, or `site` can be used")

        ish = ISH()
        if source is not None:
            ish.source = source

        if files is None and dates is not None:
            dates = pd.to_datetime(dates)
            if ish.history is None:
                ish.read_ish_history(dates=dates)
            dfloc_urls = ish.history.copy()

            if box is not None:
                dfloc_urls = ish.subset_sites(
                    latmin=box[0], lonmin=box[1], latmax=box[2], lonmax=box[3]
                )
            elif country is not None:
                dfloc_urls = dfloc_urls.loc[dfloc_urls.ctry == country, :]
            elif state is not None:
                dfloc_urls = dfloc_urls.loc[dfloc_urls.state == state, :]
            elif site is not None:
                dfloc_urls = dfloc_urls.loc[dfloc_urls.station_id == site, :]

            urls = ish.build_urls(dates=dates, sites=dfloc_urls)
            if urls.empty:
                raise ValueError("No data URLs found")
            files = urls.name.tolist()

        if files is None:
            raise ValueError("Must provide either 'files' or 'dates'.")

        if download:
            files = ish.get_url_file_objs(files)

        # Use driver directly to avoid extra harmonize calls that might clash
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
            read_method=read_ish_file,
            lazy=lazy,
            **kwargs,
        )

        # Filtering by date if requested
        if dates is not None:
            dates = pd.to_datetime(dates)
            df = df.loc[(df.time >= dates.min()) & (df.time <= dates.max())]

        # Construct siteid
        if "station_id" in df.columns:
            df["siteid"] = df["station_id"]

        # Merge with metadata
        if ish.history is None:
            ish.read_ish_history()

        df = add_ish_metadata(df, ish.history)

        df = self.harmonize(df)

        if as_xarray:
            from ..util import ds_to_2d

            # We first convert to 1D
            # Filter out expand2d from kwargs to avoid double-passing it
            to_xr_kwargs = {k: v for k, v in kwargs.items() if k != "expand2d"}
            ds = self.to_xarray(df, expand2d=False, **to_xr_kwargs)

            # Metadata variables to preserve
            meta_coords = [
                "country",
                "state",
                "station name",
                "elev(m)",
                "latitude",
                "longitude",
                "siteid",
                "usaf",
                "wban",
            ]

            if resample:
                # Backend-agnostic resampling in xarray
                # To preserve per-site data, we expand to 2D (time, node) before resampling
                pivot = kwargs.get("wide_fmt", kwargs.get("pivot", True))
                ds = ds_to_2d(ds, pivot=pivot, fixed_location=self.fixed_location)

                # Identify metadata variables to preserve
                metadata = xr.Dataset()
                for c in meta_coords:
                    if c in ds.coords or c in ds.data_vars:
                        val = ds[c]
                        if "time" in val.dims:
                            val = val.isel(time=0, drop=True)
                        metadata[c] = val

                try:
                    ds = (
                        ds.sortby("time")
                        .resample(time=normalize_pandas_freq(window))
                        .mean(numeric_only=True)
                    )
                except Exception:
                    ds = ds.sortby("time").resample(time=normalize_pandas_freq(window)).mean()

                # Restore metadata
                for c in metadata.data_vars:
                    ds[c] = metadata[c]
                for c in metadata.coords:
                    ds.coords[c] = metadata.coords[c]
                if "siteid" not in ds.coords and "siteid" not in ds.data_vars and "node" in ds.dims:
                    ds.coords["siteid"] = (("node",), ds.node.data)

                # Update history for resampling
                ds = update_history(ds, f"Resampled ISH data to {window} window.")

            else:
                # Now expand to 2D if requested (default is True in PointReader)
                expand2d = kwargs.get("expand2d", True)
                if expand2d:
                    pivot = kwargs.get("wide_fmt", kwargs.get("pivot", True))
                    ds = ds_to_2d(ds, pivot=pivot, fixed_location=self.fixed_location)
                    if (
                        "siteid" not in ds.coords
                        and "siteid" not in ds.data_vars
                        and "node" in ds.dims
                    ):
                        ds.coords["siteid"] = (("node",), ds.node.data)

            # Ensure metadata are coordinates
            ds = ds.set_coords([c for c in meta_coords if c in ds.variables])

            # Update history
            ds = update_history(ds, "Read ISH data.")
            return ds

        if resample:
            if not lazy:
                if not df.empty:
                    df = (
                        df.set_index("time")
                        .groupby("siteid")
                        .resample(normalize_pandas_freq(window))
                        .mean(numeric_only=True)
                        .reset_index()
                    )
                    # Re-join metadata
                    df = add_ish_metadata(df, ish.history)
            else:
                import warnings

                warnings.warn(
                    "ISHReader: Resampling is currently not supported for lazy DataFrames. "
                    "Convert to xarray (as_xarray=True) for lazy resampling."
                )

        return df
