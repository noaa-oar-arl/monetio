"""AQS Reader"""

import concurrent.futures
import logging
import os
import warnings
from datetime import datetime
from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
import requests
import xarray as xr

from ..util import force_object_strings
from .base import PointReader, register_reader
from .epa_utils import add_monitor_metadata, standardize_epa_units
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd

logger = logging.getLogger(__name__)

#: Mapping from AQS numeric parameter codes to short variable names used
#: throughout monetio (e.g. ``88101 -> "PM2.5"``, ``44201 -> "OZONE"``).
AQS_PARAMETER_CODES: dict[int, str] = {
    88101: "PM2.5",
    88502: "PM2.5",
    44201: "OZONE",
    81102: "PM10",
    42401: "SO2",
    42602: "NO2",
    42101: "CO",
    62101: "TEMP",
    88305: "OC",
    88306: "NO3f",
    88307: "ECf",
    88316: "ECf_optical",
    88403: "SO4f",
    88312: "TCf",
    88104: "Alf",
    88107: "Baf",
    88313: "BCf",
    88109: "Brf",
    88110: "Cdf",
    88111: "Caf",
    88117: "Cef",
    88118: "Csf",
    88203: "Cl-f",
    88115: "Clf",
    88112: "Crf",
    88113: "Cof",
    88114: "Cuf",
    88121: "Euf",
    88143: "Auf",
    88127: "Hff",
    88131: "Inf",
    88126: "Fef",
    88146: "Laf",
    88128: "Pbf",
    88140: "Mgf",
    88132: "Mnf",
    88142: "Hgf",
    88134: "Mof",
    88136: "Nif",
    88147: "Nbf",
    88310: "NO3f",
    88152: "Pf",
    88303: "K+f",
    88176: "Rbf",
    88162: "Smf",
    88163: "Scf",
    88154: "Sef",
    88165: "Sif",
    88166: "Agf",
    88302: "Na+f",
    88184: "Naf",
    88168: "Srf",
    88169: "Sf",
    88170: "Taf",
    88172: "Tbf",
    88160: "Snf",
    88161: "Tif",
    88186: "Wf",
    88314: "C_370nmf",
    88179: "Uf",
    88164: "Vf",
    88183: "Yf",
    88167: "Znf",
    88185: "Zrf",
    88103: "Asf",
    88105: "Bef",
    88124: "Gaf",
    88180: "Kf",
    88301: "NH4+f",
    42600: "NOY",
    42601: "NO",
    42603: "NOX",
    61103: "WS",
    61101: "WS",
    61104: "WD",
    61102: "WD",
    62201: "RH",
    62103: "DP",
}

#: String-keyed version of :data:`AQS_PARAMETER_CODES`
_AQS_PARAMETER_CODES_STR: dict[str, str] = {str(k): v for k, v in AQS_PARAMETER_CODES.items()}

#: Base URL for AQS data retrieval
AQS_BASE_URL = "https://aqs.epa.gov/aqsweb/airdata/"

#: Renamed columns for AQS daily data
AQS_DAILY_COLUMNS = [
    "time",
    "state_code",
    "county_code",
    "site_num",
    "parameter_code",
    "poc",
    "latitude",
    "longitude",
    "datum",
    "parameter_name",
    "sample_duration",
    "pollutant_standard",
    "units",
    "event_type",
    "observation_count",
    "observation_percent",
    "obs",
    "1st_max_value",
    "1st_max_hour",
    "aqi",
    "method_code",
    "method_name",
    "local_site_name",
    "address",
    "state_name",
    "county_name",
    "city_name",
    "msa_name",
    "date_of_last_change",
]


def _get_param_list(param: str | list[str] | None) -> list[str]:
    """Get list of parameters to retrieve."""
    if param is None:
        return [
            "SPEC",
            "PM10",
            "PM2.5",
            "PM2.5_FRM",
            "CO",
            "OZONE",
            "SO2",
            "VOC",
            "NONOXNOY",
            "WIND",
            "TEMP",
            "RHDP",
        ]
    elif isinstance(param, str):
        return [param]
    return param


def columns_rename(columns: list[str]) -> list[str]:
    """
    Rename AQS columns to standard names.

    Parameters
    ----------
    columns : list[str]
        Original column names.

    Returns
    -------
    list[str]
        Standardized column names.
    """
    rcolumn = []
    for ccc in columns:
        ccc_clean = ccc.strip()
        if ccc_clean == "Sample Measurement":
            newc = "obs"
        elif ccc_clean == "Units of Measure":
            newc = "units"
        else:
            newc = ccc_clean.lower().replace(" ", "_")
        rcolumn.append(newc)
    return rcolumn


def get_species(
    df: Union[pd.DataFrame, "dd.DataFrame"], voc: bool = False
) -> Union[pd.DataFrame, "dd.DataFrame"]:
    """
    Map AQS parameter codes to short variable names.

    Parameters
    ----------
    df : Union[pd.DataFrame, dd.DataFrame]
        Input dataframe.
    voc : bool, optional
        Whether the data is VOC data, by default False.

    Returns
    -------
    Union[pd.DataFrame, dd.DataFrame]
        Dataframe with 'variable' column added.
    """
    if voc:
        df["variable"] = df.parameter_name.str.upper()
        return df

    if "variable" not in df.columns:
        df["variable"] = ""

    pcode_as_str = df["parameter_code"].astype(str)
    df["variable"] = pcode_as_str.map(_AQS_PARAMETER_CODES_STR)

    # Handle missing mappings for eager data only to avoid compute
    if not hasattr(df, "compute") and "variable" in df.columns:
        missing = df.loc[
            df["variable"].isna(), ["parameter_name", "parameter_code"]
        ].drop_duplicates()
        if not missing.empty:
            _tbl = missing.to_string(index=False)
            warnings.warn(f"Short names not available for these variables:\n{_tbl}")

    df["variable"] = df["variable"].fillna(df.parameter_name)

    # Update history
    df = update_history(df, "Mapped parameter codes to short variable names.")

    return df


def load_aqs_file(url: str) -> pd.DataFrame:
    """
    Load a single AQS file.

    Parameters
    ----------
    url : str
        URL or local path to the AQS file.

    Returns
    -------
    pd.DataFrame
        The loaded AQS data.
    """
    if "daily" in url:
        df = pd.read_csv(
            url,
            dtype={0: str, 1: str, 2: str},
            encoding="ISO-8859-1",
        )
        if "Date Local" in df.columns:
            df["time_local"] = pd.to_datetime(df["Date Local"])
            df.drop(["Date Local"], axis=1, inplace=True)

        cols = df.columns.tolist()
        if "time_local" in cols:
            cols.insert(0, cols.pop(cols.index("time_local")))
            df = df[cols]

        if len(df.columns) == len(AQS_DAILY_COLUMNS):
            df.columns = AQS_DAILY_COLUMNS
    else:
        df = pd.read_csv(
            url,
            low_memory=False,
            encoding="ISO-8859-1",
        )
        if "Date GMT" in df.columns and "Time GMT" in df.columns:
            df["time"] = pd.to_datetime(df["Date GMT"] + " " + df["Time GMT"])
        if "Date Local" in df.columns and "Time Local" in df.columns:
            df["time_local"] = pd.to_datetime(df["Date Local"] + " " + df["Time Local"])

        df.columns = columns_rename(df.columns.tolist())
        df = df.loc[:, ~df.columns.duplicated()]

    # Vectorized siteid construction
    df["siteid"] = (
        df.state_code.astype(str).str.zfill(2)
        + df.county_code.astype(str).str.zfill(3)
        + df.site_num.astype(str).str.zfill(4)
    )

    df.drop(["state_name", "county_name"], axis=1, inplace=True, errors="ignore")
    df.columns = [i.lower() for i in df.columns]

    if "daily" not in url:
        df.drop(["datum", "qualifier"], axis=1, inplace=True, errors="ignore")

    voc = "VOC" in url
    df = get_species(df, voc=voc)
    df = standardize_epa_units(df)
    df = force_object_strings(df)

    return df.drop(columns="date_of_last_change", errors="ignore")


def build_url(param: str, year: str, daily: bool = False) -> tuple[str, str]:
    """Build URL and filename for AQS data."""
    beginning = f"{AQS_BASE_URL}{'daily_' if daily else 'hourly_'}"
    fname_prefix = "daily_" if daily else "hourly_"

    p = param.upper()
    mapping = {
        "OZONE": "44201_",
        "O3": "44201_",
        "PM2.5": "88101_",
        "PM2.5_FRM": "88502_",
        "PM10": "81102_",
        "SO2": "42401_",
        "NO2": "42602_",
        "CO": "42101_",
        "NONOXNOY": "NONOxNOy_",
        "VOC": "VOCS_",
        "SPEC": "SPEC_",
        "PM10SPEC": "PM10SPEC_",
        "WIND": "WIND_",
        "TEMP": "TEMP_",
        "RHDP": "RH_DP_",
        "WS": "WIND_",
        "WDIR": "WIND_",
    }
    code = mapping.get(p, p + "_")

    fname = f"{fname_prefix}{code}{year}.zip"
    url = f"{AQS_BASE_URL}{fname}"
    return url, fname


def build_urls(
    params: list[str],
    dates: pd.DatetimeIndex | list[datetime] | datetime | str,
    daily: bool = False,
) -> tuple[list[str], list[str]]:
    """Build and validate URLs for AQS data."""
    years = pd.DatetimeIndex(np.atleast_1d(pd.to_datetime(dates))).year.unique().astype(str)
    urls = []
    fnames = []

    def check_url(p_y):
        p, y = p_y
        url, fname = build_url(p, y, daily=daily)
        try:
            with requests.get(url, stream=True, timeout=10) as r:
                if r.status_code == 200:
                    content_length = int(r.headers.get("Content-Length", 0))
                    if content_length > 500:
                        return url, fname
        except Exception:
            pass
        return None

    to_check = [(p, y) for p in params for y in years]

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(check_url, to_check))

    for res in results:
        if res:
            urls.append(res[0])
            fnames.append(res[1])

    return urls, fnames


class AQS:
    """Deprecated helper class for AQS data retrieval."""

    pass


@register_reader("aqs")
class AQSReader(PointReader):
    """Reader for EPA AQS data."""

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
        param: str | list[str] | None = None,
        daily: bool = False,
        network: str | None = None,
        download: bool = False,
        local: bool = False,
        wide_fmt: bool = True,
        meta: bool = False,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load AQS (Air Quality System) data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File path, list of paths, or glob pattern.
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr, by default False.
        virtualizarr_file : str or None, optional
            Path to VirtualiZarr reference file, by default None.
        virtualizarr_parser : str or None, optional
            The VirtualiZarr parser to use.
        virtualizarr_backend : str, optional
            Backend for VirtualiZarr references, by default "kerchunk".
        icechunk_repo : str or None, optional
            Path to Icechunk repository.
        use_icechunk : bool, optional
            Whether to use Icechunk, by default False.
        icechunk_url : str or None, optional
            Path to Icechunk repository.
        use_dask : bool, optional
            Whether to use Dask for lazy loading, by default False.
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str], optional
            Dates to retrieve if files are not provided.
        param : Union[str, List[str]], optional
            Parameter(s) to retrieve, by default None.
        daily : bool, optional
            Whether to load daily data, by default False.
        network : str, optional
            Network to filter sites, by default None.
        download : bool, optional
            Whether to download files, by default False.
        local : bool, optional
            Whether to load from local files, by default False.
        wide_fmt : bool, optional
            Whether to return data in wide format, by default True.
        meta : bool, optional
            Whether to add site metadata, by default False.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the reader.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded AQS data.

        Examples
        --------
        >>> from monetio.readers.aqs import AQSReader
        >>> ds = AQSReader().open_dataset(dates='2023-06-01', param='OZONE')
        """
        if files is None:
            if dates is None:
                raise ValueError("Must provide either 'files' or 'dates'.")

            params = _get_param_list(param)
            urls, fnames = build_urls(params, dates, daily=daily)

            if not urls:
                return pd.DataFrame()

            if download:
                for url, fname in zip(urls, fnames):
                    if not os.path.isfile(fname):
                        r = requests.get(url)
                        with open(fname, "wb") as f:
                            f.write(r.content)
                files = fnames
            elif local:
                files = fnames
            else:
                files = urls

        # Clean kwargs for driver
        driver_kwargs = kwargs.copy()
        driver_kwargs.pop("n_procs", None)

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
            read_method=load_aqs_file,
            as_xarray=False,
            lazy=lazy,
            **driver_kwargs,
        )

        # Handle empty case
        try:
            import dask.dataframe as dd

            is_dask = isinstance(df, dd.DataFrame)
        except ImportError:
            is_dask = False

        if is_dask:
            if df.npartitions == 0:
                return df
        else:
            if df.empty:
                return df

        # Filter dates backend-agnostically
        if dates is not None:
            dates_idx = pd.DatetimeIndex(np.atleast_1d(pd.to_datetime(dates)))
            df = df[df.time.between(dates_idx.min(), dates_idx.max())]

        if meta:
            df = add_monitor_metadata(df, daily=daily, network=network)

        if as_xarray:
            # Filter out expand2d from kwargs if present to avoid double-passing
            to_xr_kwargs = {k: v for k, v in kwargs.items() if k != "expand2d"}
            ds = self.to_xarray(df, expand2d=wide_fmt, wide_fmt=wide_fmt, **to_xr_kwargs)
            ds = update_history(ds, "Read AQS data.")
            return ds

        return df
