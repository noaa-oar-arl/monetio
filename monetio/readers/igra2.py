"""IGRA2 Reader"""

import logging
import zipfile
from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd

logger = logging.getLogger(__name__)


def read_igra2(fname: str, **kwargs) -> pd.DataFrame:
    """
    Read a single IGRA2 (Integrated Global Radiosonde Archive) file.
    """
    storage_options = kwargs.get("storage_options", {})
    fs = FileUtility.get_fs(fname)

    try:
        with fs.open(fname, "rb", **storage_options) as f:
            content = f.read()
            if fname.endswith(".zip"):
                with zipfile.ZipFile(BytesIO(content)) as z:
                    internal_fname = z.namelist()[0]
                    with z.open(internal_fname) as internal_f:
                        lines = internal_f.readlines()
            elif fname.endswith(".gz"):
                import gzip

                with gzip.open(BytesIO(content), "rb") as gz:
                    lines = gz.readlines()
            else:
                lines = content.splitlines()
    except Exception as e:
        logger.warning(f"Could not read {fname}: {e}")
        return pd.DataFrame()

    data_list = []
    current_header = None
    for line in lines:
        line_str = line.decode("utf-8")
        if line_str.startswith("#"):
            try:
                sid = line_str[1:12].strip()
                year = int(line_str[13:17])
                month = int(line_str[18:20])
                day = int(line_str[21:23])
                hour = int(line_str[24:26])
                reltime = line_str[27:31].strip()
                # numlev = int(line_str[32:36])
                lat_str = line_str[55:62].strip()
                lon_str = line_str[63:71].strip()
                lat = int(lat_str) / 10000.0 if lat_str else np.nan
                lon = int(lon_str) / 10000.0 if lon_str else np.nan

                if hour == 99:
                    hour = 0
                try:
                    time = pd.Timestamp(year=year, month=month, day=day, hour=hour)
                except ValueError:
                    time = pd.NaT

                current_header = {
                    "siteid": sid,
                    "time": time,
                    "reltime": reltime,
                    "latitude": lat,
                    "longitude": lon,
                }
            except Exception as e:
                logger.debug(f"Failed to parse header line: {line_str}. Error: {e}")
                current_header = None
        else:
            if current_header is None:
                continue
            try:
                # 1-based indexing from doc
                line_str = line_str.ljust(51)

                lvl1 = int(line_str[0:1])
                lvl2 = int(line_str[1:2])
                etime_str = line_str[3:8].strip()
                press_str = line_str[9:15].strip()
                gph_str = line_str[16:21].strip()
                temp_str = line_str[22:27].strip()
                rh_str = line_str[28:33].strip()
                dpdp_str = line_str[34:39].strip()
                wdir_str = line_str[40:45].strip()
                wspd_str = line_str[46:51].strip()

                etime = int(etime_str) if etime_str else -9999
                press = int(press_str) if press_str else -9999
                gph = int(gph_str) if gph_str else -9999
                temp = int(temp_str) if temp_str else -9999
                rh = int(rh_str) if rh_str else -9999
                dpdp = int(dpdp_str) if dpdp_str else -9999
                wdir = int(wdir_str) if wdir_str else -9999
                wspd = int(wspd_str) if wspd_str else -9999

                data_row = current_header.copy()
                data_row.update(
                    {
                        "lvltyp1": lvl1,
                        "lvltyp2": lvl2,
                        "etime": etime,
                        "press": press,
                        "gph": gph,
                        "temp": temp,
                        "rh": rh,
                        "dpdp": dpdp,
                        "wdir": wdir,
                        "wspd": wspd,
                    }
                )
                data_list.append(data_row)
            except Exception as e:
                logger.debug(f"Failed to parse data line: {line_str}. Error: {e}")

    df = pd.DataFrame(data_list)
    if df.empty:
        return df

    def clean(series, scale=1.0):
        series = series.where(~series.isin([-9999, -8888]), np.nan)
        return series / scale

    df["press"] = clean(df["press"])
    df["gph"] = clean(df["gph"])
    df["temp"] = clean(df["temp"], 10.0)
    df["rh"] = clean(df["rh"], 10.0)
    df["dpdp"] = clean(df["dpdp"], 10.0)
    df["wdir"] = clean(df["wdir"])
    df["wspd"] = clean(df["wspd"], 10.0)
    df["etime"] = clean(df["etime"])

    return df


def read_igra2_derived(fname: str, **kwargs) -> pd.DataFrame:
    """
    Read a single IGRA2 derived parameters file.
    """
    storage_options = kwargs.get("storage_options", {})
    fs = FileUtility.get_fs(fname)

    try:
        with fs.open(fname, "rb", **storage_options) as f:
            content = f.read()
            if fname.endswith(".zip"):
                with zipfile.ZipFile(BytesIO(content)) as z:
                    internal_fname = z.namelist()[0]
                    with z.open(internal_fname) as internal_f:
                        lines = internal_f.readlines()
            elif fname.endswith(".gz"):
                import gzip

                with gzip.open(BytesIO(content), "rb") as gz:
                    lines = gz.readlines()
            else:
                lines = content.splitlines()
    except Exception as e:
        logger.warning(f"Could not read {fname}: {e}")
        return pd.DataFrame()

    data_list = []
    current_header = None
    for line in lines:
        line_str = line.decode("utf-8")
        if line_str.startswith("#"):
            try:
                sid = line_str[1:12].strip()
                year = int(line_str[13:17])
                month = int(line_str[18:20])
                day = int(line_str[21:23])
                hour = int(line_str[24:26])
                reltime = line_str[27:31].strip()
                # numlev = int(line_str[31:36])

                if hour == 99:
                    hour = 0
                try:
                    time = pd.Timestamp(year=year, month=month, day=day, hour=hour)
                except ValueError:
                    time = pd.NaT

                pw_str = line_str[37:43].strip()
                pw = int(pw_str) if pw_str else -99999

                cape, cin = -99999, -99999
                if len(line_str) >= 151:
                    cape_str = line_str[145:151].strip()
                    cape = int(cape_str) if cape_str else -99999
                if len(line_str) >= 157:
                    cin_str = line_str[151:157].strip()
                    cin = int(cin_str) if cin_str else -99999

                current_header = {
                    "siteid": sid,
                    "time": time,
                    "reltime": reltime,
                    "pw": pw,
                    "cape": cape,
                    "cin": cin,
                }
            except Exception as e:
                logger.debug(f"Failed to parse derived header: {line_str}. Error: {e}")
                current_header = None
        else:
            if current_header is None:
                continue
            try:
                line_str = line_str.ljust(151)
                press = int(line_str[0:7].strip()) if line_str[0:7].strip() else -99999
                repgph = int(line_str[8:15].strip()) if line_str[8:15].strip() else -99999
                calcgph = int(line_str[16:23].strip()) if line_str[16:23].strip() else -99999
                temp = int(line_str[24:31].strip()) if line_str[24:31].strip() else -99999
                uwnd = int(line_str[112:119].strip()) if line_str[112:119].strip() else -99999
                vwnd = int(line_str[128:135].strip()) if line_str[128:135].strip() else -99999

                data_row = current_header.copy()
                data_row.update(
                    {
                        "press": press,
                        "gph": repgph,
                        "calcgph": calcgph,
                        "temp": temp,
                        "uwnd": uwnd,
                        "vwnd": vwnd,
                    }
                )
                data_list.append(data_row)
            except Exception as e:
                logger.debug(f"Failed to parse derived data line: {line_str}. Error: {e}")

    df = pd.DataFrame(data_list)
    if df.empty:
        return df

    def clean(series, scale=1.0):
        series = series.where(series != -99999, np.nan)
        return series / scale

    for col in ["pw", "cape", "cin", "press", "gph", "calcgph", "uwnd", "vwnd"]:
        if col in df.columns:
            s = 100.0 if col == "pw" else (10.0 if col in ["uwnd", "vwnd"] else 1.0)
            df[col] = clean(df[col], s)
    if "temp" in df.columns:
        # Derived temp is K*10. Convert to Celsius for consistency.
        df["temp"] = clean(df["temp"], 10.0) - 273.15

    return df


@register_reader("igra2")
class IGRA2Reader(PointReader):
    """
    Reader for IGRA2 (Integrated Global Radiosonde Archive) data.
    """

    fixed_location = True

    def __init__(self):
        super().__init__()
        self.station_list_url = "https://www.ncei.noaa.gov/pub/data/igra/igra2-station-list.txt"
        self.stations = None

    def read_station_list(self):
        """Read the IGRA2 station list."""
        if self.stations is not None:
            return self.stations

        fs = FileUtility.get_fs(self.station_list_url)
        try:
            with fs.open(self.station_list_url, "r") as f:
                colspecs = [
                    (0, 11),
                    (12, 20),
                    (21, 30),
                    (31, 37),
                    (38, 40),
                    (41, 71),
                    (72, 76),
                    (77, 81),
                    (82, 88),
                ]
                names = [
                    "siteid",
                    "latitude",
                    "longitude",
                    "elevation",
                    "state",
                    "name",
                    "fstyear",
                    "lstyear",
                    "nobs",
                ]
                self.stations = pd.read_fwf(f, colspecs=colspecs, names=names)
        except Exception as e:
            logger.warning(f"Could not read station list: {e}")
            self.stations = pd.DataFrame()
        return self.stations

    def build_urls(
        self,
        dates: pd.DatetimeIndex | None = None,
        sites: str | list[str] | None = None,
        source: str = "ncei",
        derived: bool = False,
    ) -> list[str]:
        """
        Construct IGRA2 URLs.
        """
        if source != "ncei":
            raise ValueError("Only 'ncei' source is currently supported for IGRA2.")

        if derived:
            base_url = "https://www.ncei.noaa.gov/pub/data/igra/derived/derived-por"
            suffix = "-drvd.txt.zip"
        else:
            base_url = "https://www.ncei.noaa.gov/pub/data/igra/data/data-por"
            suffix = "-data.txt.zip"

        if sites is None:
            raise ValueError("Must specify 'sites' (station IDs) to build URLs.")

        if isinstance(sites, str):
            sites = [sites]

        urls = []
        for s in sites:
            urls.append(f"{base_url}/{s}{suffix}")

        return urls

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
        site: str | list[str] | None = None,
        derived: bool = False,
        add_metadata: bool = True,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load IGRA2 data.
        """
        if files is None:
            if site is None:
                raise ValueError("Must provide either 'files' or 'site'.")
            files = self.build_urls(sites=site, derived=derived)

        read_method = read_igra2_derived if derived else read_igra2
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
            read_method=read_method,
            lazy=lazy,
            **kwargs,
        )

        if add_metadata:
            stations = self.read_station_list()
            if not stations.empty:
                stations = stations.assign(siteid=stations.siteid.astype(object))
                if lazy:
                    import dask.dataframe as dd

                    df = df.assign(siteid=df.siteid.astype(object))
                    stations_dask = dd.from_pandas(stations, npartitions=1)
                    stations_dask = stations_dask.assign(siteid=stations_dask.siteid.astype(object))
                    df = df.merge(
                        stations_dask.drop(columns=["latitude", "longitude"]),
                        on="siteid",
                        how="left",
                    )
                else:
                    df = df.assign(siteid=df.siteid.astype(object))
                    df = df.merge(
                        stations.drop(columns=["latitude", "longitude"]), on="siteid", how="left"
                    )

        if dates is not None:
            dates = pd.to_datetime(dates)
            if isinstance(dates, pd.Timestamp):
                dates = pd.DatetimeIndex([dates])
            if lazy:
                df = df[df.time.dt.normalize().isin(dates.normalize())]
            else:
                df = df.loc[df.time.dt.normalize().isin(dates.normalize())]

        df = self.harmonize(df)

        if as_xarray:
            ds = self.to_xarray(df, **kwargs)

            # Add unit attributes
            units = {
                "press": "Pa",
                "temp": "degC",
                "rh": "%",
                "gph": "m",
                "calcgph": "m",
                "uwnd": "m/s",
                "vwnd": "m/s",
                "wdir": "deg",
                "wspd": "m/s",
                "etime": "s",
                "pw": "mm",
                "cape": "J/kg",
                "cin": "J/kg",
            }
            for var, unit in units.items():
                if var in ds.data_vars:
                    ds[var].attrs["units"] = unit

            ds = update_history(ds, f"Read IGRA2 {'derived ' if derived else ''}data.")
            return ds

        return df
