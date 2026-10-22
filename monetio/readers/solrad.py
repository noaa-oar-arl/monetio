"""SOLRAD Reader"""

import io
from datetime import datetime
from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
import xarray as xr

from ..util import force_object_strings
from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd

# Base headers for SOLRAD
BASE_HEADERS = (
    "year",
    "jday",
    "month",
    "day",
    "hour",
    "minute",
    "dt",
    "zen",
    "ghi",
    "ghi_flag",
    "dni",
    "dni_flag",
    "dhi",
    "dhi_flag",
    "uvb",
    "uvb_flag",
    "uvb_temp",
    "uvb_temp_flag",
)

# Standard headers for remaining columns
STD_HEADERS = ("std_ghi", "std_dni", "std_dhi", "std_uvb")
HEADERS = BASE_HEADERS + STD_HEADERS

# Headers for Madison, WI site (includes DPIR)
DPIR_HEADERS = ("dpir", "dpir_flag", "dpirc", "dpirc_flag", "dpird", "dpird_flag")
MADISON_HEADERS = BASE_HEADERS + DPIR_HEADERS + STD_HEADERS + ("std_dpir", "std_dpirc", "std_dpird")

# Fixed-width columns (base widths from README_SOLRAD.txt)
# year(4), jday(3), month(2), day(2), hour(2), minute(2), dt(6), zen(6)
# then 5 pairs of (value(7), flag(1)) for ghi, dni, dhi, uvb, uvb_temp
# then 4 values of 9 chars for std_ghi, std_dni, std_dhi, std_uvb
WIDTHS = [4, 3, 2, 2, 2, 2, 6, 6] + 5 * [7, 1] + 4 * [9]
# Madison has 8 pairs of (value, flag) because of DPIR
MADISON_WIDTHS = [4, 3, 2, 2, 2, 2, 6, 6] + 8 * [7, 1] + 7 * [9]


def get_colspecs(widths: list[int]) -> list[tuple]:
    """
    Generate colspecs for pd.read_fwf from widths.

    Parameters
    ----------
    widths : List[int]
        List of column widths.

    Returns
    -------
    List[tuple]
        List of (start, end) tuples.
    """
    colspecs = []
    start = 0
    for w in widths:
        colspecs.append((start, start + w))
        start += w + 1
    return colspecs


COLSPECS = get_colspecs(WIDTHS)
MADISON_COLSPECS = get_colspecs(MADISON_WIDTHS)

# Variable mapping for SOLRAD
VARIABLE_MAP = {
    "zen": "solar_zenith",
}


def read_solrad(filename: str, **kwargs: dict) -> pd.DataFrame:
    """
    Read a single SOLRAD file.

    Parameters
    ----------
    filename : str
        The path or URL to the SOLRAD file.
    **kwargs : dict
        Additional arguments passed to pd.read_fwf.

    Returns
    -------
    pd.DataFrame
        The loaded data in long format.

    Examples
    --------
    >>> df = read_solrad("abq24001.dat")
    """
    if "msn" in filename.lower():
        names = MADISON_HEADERS
        colspecs = MADISON_COLSPECS
    else:
        names = HEADERS
        colspecs = COLSPECS

    # Use FileUtility to handle remote files
    fs = FileUtility.get_fs(filename)
    storage_options = kwargs.get("storage_options", {})

    with fs.open(filename, "r", **storage_options) as f:
        # Read header for metadata
        header_lines = []
        for _ in range(2):
            line = f.readline()
            if not line:
                break
            header_lines.append(line.strip())

        if len(header_lines) < 2:
            return pd.DataFrame()

        station_name = header_lines[0]
        metadata_line = header_lines[1].split()

        try:
            latitude = float(metadata_line[0])
            longitude = float(metadata_line[1])
            elevation = float(metadata_line[2])
        except (ValueError, IndexError):
            latitude = np.nan
            longitude = np.nan
            elevation = np.nan

        data_content = f.read()

    df = pd.read_fwf(
        io.StringIO(data_content),
        colspecs=colspecs,
        header=None,
        names=names,
        na_values=-9999.9,
        **kwargs,
    )

    if df.empty:
        return df

    # Add metadata
    df["latitude"] = latitude
    df["longitude"] = longitude
    df["elevation"] = elevation
    df["siteid"] = station_name

    # Vectorized time construction
    # year(4), month(2), day(2), hour(2), minute(2)
    def to_str(series, n):
        # Handle cases where column might be floating point because of NaNs
        s = pd.to_numeric(series, errors="coerce").fillna(0).astype(int).astype(str)
        return s.str.zfill(n)

    df["time"] = pd.to_datetime(
        to_str(df["year"], 4)
        + to_str(df["month"], 2)
        + to_str(df["day"], 2)
        + to_str(df["hour"], 2)
        + to_str(df["minute"], 2),
        format="%Y%m%d%H%M",
        errors="coerce",
    )

    return df


@register_reader("solrad")
class SOLRADReader(PointReader):
    """
    Reader for NOAA SOLRAD network data.
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
        sites: list[str] | None = None,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs: dict,
    ) -> Union[xr.Dataset, pd.DataFrame, "dd.DataFrame"]:
        """
        Open SOLRAD dataset.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File paths or URLs. If None, uses `dates` and `sites` to discover files.
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
        sites : List[str], optional
            Site abbreviations (e.g. ['abq', 'bis', 'hnx', 'msn', 'slc', 'sea', 'ste']).
        as_xarray : bool, optional
            If True, returns an xarray.Dataset, by default True.
        lazy : bool, optional
            If True, returns a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the reader and driver.

        Returns
        -------
        Union[xr.Dataset, pd.DataFrame, dd.DataFrame]
            The loaded dataset.

        Examples
        --------
        >>> from monetio.readers.solrad import SOLRADReader
        >>> reader = SOLRADReader()
        >>> ds = reader.open_dataset(dates="2024-01-01", sites=["abq"])
        """
        if files is None:
            if dates is None or sites is None:
                raise ValueError("Either 'files' or both 'dates' and 'sites' must be provided.")
            files = self.build_urls(dates, sites)

        # Separate driver kwargs from to_xarray/postprocess kwargs
        driver_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ["expand2d", "wide_fmt", "pivot", "as_xarray", "lazy"]
        }

        # We use read_solrad as the custom read_method
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
            read_method=read_solrad,
            lazy=lazy,
            **driver_kwargs,
        )

        # Post-processing: Harmonize column names
        df = self._postprocess(df)

        # Consistently force object strings
        df = force_object_strings(df)

        if as_xarray:
            ds = self.to_xarray(df, **kwargs)
            # Update history for provenance
            ds = update_history(ds, "Read SOLRAD dataset.")
            return ds

        return df

    def _postprocess(
        self, df: Union[pd.DataFrame, "dd.DataFrame"]
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Harmonize column names.

        Parameters
        ----------
        df : Union[pd.DataFrame, dd.DataFrame]
            Input dataframe.

        Returns
        -------
        Union[pd.DataFrame, dd.DataFrame]
            Post-processed dataframe.
        """
        # Rename according to VARIABLE_MAP
        df = df.rename(columns=VARIABLE_MAP)

        # Update history for provenance
        df = update_history(df, "Applied variable name mapping for SOLRAD.")

        return df

    def build_urls(
        self,
        dates: datetime | list[datetime] | pd.DatetimeIndex,
        sites: list[str],
    ) -> list[str]:
        """
        Discover available URLs for the given dates and sites.

        Parameters
        ----------
        dates : Union[datetime, List[datetime], pd.DatetimeIndex]
            Dates to retrieve.
        sites : List[str]
            Site abbreviations.

        Returns
        -------
        List[str]
            List of URLs.
        """
        baseurl = "https://gml.noaa.gov/aftp/data/radiation/solrad/"

        urls = []
        dates = pd.DatetimeIndex(np.atleast_1d(dates))

        for date in dates:
            for site in sites:
                year = date.year
                # Format: site/year/siteyyjday.dat
                # e.g. abq/2024/abq24001.dat
                fname = f"{site.lower()}{date.strftime('%y%j')}.dat"
                url = f"{baseurl}{site.lower()}/{year}/{fname}"
                urls.append(url)

        return urls
