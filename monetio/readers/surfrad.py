"""SURFRAD Reader"""

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

SURFRAD_COLUMNS = [
    "year",
    "jday",
    "month",
    "day",
    "hour",
    "minute",
    "dt",
    "zen",
    "dw_solar",
    "dw_solar_flag",
    "uw_solar",
    "uw_solar_flag",
    "direct_n",
    "direct_n_flag",
    "diffuse",
    "diffuse_flag",
    "dw_ir",
    "dw_ir_flag",
    "dw_casetemp",
    "dw_casetemp_flag",
    "dw_dometemp",
    "dw_dometemp_flag",
    "uw_ir",
    "uw_ir_flag",
    "uw_casetemp",
    "uw_casetemp_flag",
    "uw_dometemp",
    "uw_dometemp_flag",
    "uvb",
    "uvb_flag",
    "par",
    "par_flag",
    "netsolar",
    "netsolar_flag",
    "netir",
    "netir_flag",
    "totalnet",
    "totalnet_flag",
    "temp",
    "temp_flag",
    "rh",
    "rh_flag",
    "windspd",
    "windspd_flag",
    "winddir",
    "winddir_flag",
    "pressure",
    "pressure_flag",
]

# Dictionary mapping surfrad variables to standard names
VARIABLE_MAP = {
    "temp": "air_temperature",
    "rh": "relative_humidity",
    "windspd": "wind_speed",
    "winddir": "wind_direction",
    "pressure": "surface_pressure",
    "dw_solar": "ghi",
    "direct_n": "dni",
    "diffuse": "dhi",
}


def read_surfrad(filename: str, **kwargs: dict) -> pd.DataFrame:
    """
    Read a single SURFRAD file.

    Parameters
    ----------
    filename : str
        The path or URL to the SURFRAD file.
    **kwargs : dict
        Additional arguments passed to pd.read_csv.

    Returns
    -------
    pd.DataFrame
        The loaded data in long format.

    Examples
    --------
    >>> df = read_surfrad("bon24001.dat")
    """
    # Use FileUtility to handle remote files
    fs = FileUtility.get_fs(filename)
    storage_options = kwargs.get("storage_options", {})

    with fs.open(filename, "r", **storage_options) as f:
        # Read metadata from first two lines
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
            # Fallback for malformed headers
            latitude = np.nan
            longitude = np.nan
            elevation = np.nan

        # Reset pointer or read the rest
        # For remote filesystems, it's often better to read the whole content if small
        # SURFRAD files are daily and small (~100KB)
        data_content = f.read()

    # Data parsing
    df = pd.read_csv(
        io.StringIO(data_content),
        sep=r"\s+",
        names=SURFRAD_COLUMNS,
        header=None,
        na_values=[-9999.9, -999.9, -99.9],
        **kwargs,
    )

    if df.empty:
        return df

    # Add metadata as columns
    df["latitude"] = latitude
    df["longitude"] = longitude
    df["elevation"] = elevation
    df["siteid"] = station_name

    # Vectorized time construction
    # year, jday, hour, minute
    # jday is 1-indexed
    df["time"] = pd.to_datetime(
        df["year"].astype(str)
        + df["jday"].astype(str).str.zfill(3)
        + df["hour"].astype(str).str.zfill(2)
        + df["minute"].astype(str).str.zfill(2),
        format="%Y%j%H%M",
        errors="coerce",
    )

    return df


@register_reader("surfrad")
class SURFRADReader(PointReader):
    """
    Reader for Surface Radiation Budget Network (SURFRAD) data.
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
        Open SURFRAD dataset.

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
            Site abbreviations (e.g. ['tbl', 'bon', 'fpk', 'gwn', 'psu', 'sxf', 'dra']).
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
        >>> from monetio.readers.surfrad import SURFRADReader
        >>> reader = SURFRADReader()
        >>> ds = reader.open_dataset(dates="2024-01-01", sites=["bon"])
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

        # We use read_surfrad as the custom read_method
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
            read_method=read_surfrad,
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
            ds = update_history(ds, "Read SURFRAD dataset.")
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
        df = update_history(df, "Applied variable name mapping for SURFRAD.")

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
        baseurl = "https://gml.noaa.gov/aftp/data/radiation/surfrad/"

        # Site mapping from abbreviation to directory name
        site_map = {
            "tbl": "Table_Mountain_CO",
            "bon": "Bondville_IL",
            "fpk": "Fort_Peck_MT",
            "gwn": "Goodwin_Creek_MS",
            "psu": "Penn_State_PA",
            "sxf": "Sioux_Falls_SD",
            "dra": "Desert_Rock_NV",
        }

        urls = []
        dates = pd.DatetimeIndex(np.atleast_1d(dates))

        for date in dates:
            for site in sites:
                site_dir = site_map.get(site.lower(), site)
                year = date.year
                # Format: site_dir/year/site_yyjday.dat
                # e.g. Bondville_IL/2024/bon24001.dat
                fname = f"{site.lower()}{date.strftime('%y%j')}.dat"
                url = f"{baseurl}{site_dir}/{year}/{fname}"
                urls.append(url)

        return urls
