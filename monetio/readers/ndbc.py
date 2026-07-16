"""NDBC Buoy Reader"""

from functools import lru_cache, partial
from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd


@register_reader("ndbc")
class NDBCReader(PointReader):
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
        stations: str | list[str] = None,
        years: int | list[int] = None,
        realtime: bool = True,
        wide_fmt: bool = True,
        n_procs: int = 1,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load NOAA National Data Buoy Center (NDBC) data.

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
        stations : Union[str, List[str]], optional
            Station IDs to retrieve if files are not provided.
        years : Union[int, List[int]], optional
            Years to retrieve for historical data.
        realtime : bool, optional
            Whether to retrieve real-time data (last 45 days), by default True.
        wide_fmt : bool, optional
            Whether to return data in wide format, by default True.
        n_procs : int, optional
            Number of processors for dask compute (if not lazy), by default 1.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the driver.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded NDBC data.
        """
        if files is None:
            if stations is None:
                raise ValueError("Must provide either 'files' or 'stations'.")
            files = build_urls(stations, years=years, realtime=realtime)

        if not files:
            raise ValueError("No files found or URLs built.")

        # Define per-file preprocessing
        read_func = partial(read_ndbc)

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
            read_method=read_func,
            as_xarray=False,
            lazy=lazy,
            **kwargs,
        )

        # Post-processing
        df = self._post_process(df)
        df = self.harmonize(df)

        if not lazy and hasattr(df, "compute") and not isinstance(df, pd.DataFrame):
            df = df.compute(num_workers=n_procs)

        if as_xarray:
            # NDBC is already "wide" (one row per time/station)
            # expand2d=True will use ds_to_2d which works fine.
            # Filter out expand2d from kwargs if present to avoid double-passing
            to_xr_kwargs = {k: v for k, v in kwargs.items() if k != "expand2d"}
            ds = self.to_xarray(df, expand2d=wide_fmt, **to_xr_kwargs)
            ds = update_history(ds, "Read NDBC buoy data.")
            return ds

        return df

    def harmonize(self, df):
        """
        Harmonize NDBC data to standard names and units.
        """
        # Backend-agnostic check for empty
        if hasattr(df, "columns") and len(df.columns) == 0:
            return df

        rename_map = {
            "WDIR": "wind_direction",
            "WSPD": "wind_speed",
            "GST": "wind_gust",
            "WVHT": "wave_height",
            "DPD": "dominant_wave_period",
            "APD": "average_wave_period",
            "MWD": "mean_wave_direction",
            "PRES": "air_pressure",
            "ATMP": "air_temperature",
            "WTMP": "sea_surface_temperature",
            "DEWP": "dew_point_temperature",
            "VIS": "visibility",
            "PTDY": "pressure_tendency",
            "TIDE": "tide_height",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # Add metadata (lat/lon)
        df = add_station_metadata(df)

        return super().harmonize(df)

    def _post_process(
        self,
        df: Union[pd.DataFrame, "dd.DataFrame"],
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Internal post-processing logic.
        """
        # Backend-agnostic check for empty
        if hasattr(df, "columns") and len(df.columns) == 0:
            return df

        # Determine backend
        try:
            import dask.dataframe as dd

            is_dask = isinstance(df, dd.DataFrame)
        except ImportError:
            is_dask = False

        # Convert time
        # Year column can be YY or YYYY
        year_col = "YYYY" if "YYYY" in df.columns else "YY"

        def _to_datetime_ndbc(df_chunk):
            if len(df_chunk.columns) == 0:
                return pd.Series(dtype="datetime64[ns]")

            years = df_chunk[year_col].astype(int)
            # Handle 2-digit years if present (NDBC uses 90s and 00s/10s)
            years = years.apply(
                lambda x: x + 1900 if x >= 70 and x < 100 else (x + 2000 if x < 70 else x)
            )

            months = df_chunk["MM"].astype(int)
            days = df_chunk["DD"].astype(int)
            hours = df_chunk["hh"].astype(int)
            if "mm" in df_chunk.columns:
                minutes = df_chunk["mm"].astype(int)
            else:
                minutes = 0

            return pd.to_datetime(
                {
                    "year": years,
                    "month": months,
                    "day": days,
                    "hour": hours,
                    "minute": minutes,
                }
            )

        if is_dask:
            df["time"] = df.map_partitions(_to_datetime_ndbc)
        else:
            df["time"] = _to_datetime_ndbc(df)

        # Drop internal time columns
        time_cols = ["YY", "MM", "DD", "hh", "mm", "YYYY"]
        df = df.drop(columns=[c for c in time_cols if c in df.columns], errors="ignore")

        df = df.dropna(subset=["time"])

        return df


def build_urls(
    stations: str | list[str],
    years: int | list[int] = None,
    realtime: bool = True,
) -> list[str]:
    """
    Construct NDBC URLs.
    """
    if isinstance(stations, str):
        stations = [stations]

    urls = []
    if realtime:
        base_url = "https://www.ndbc.noaa.gov/data/realtime2/"
        for s in stations:
            urls.append(f"{base_url}{s.upper()}.txt")
    else:
        if years is None:
            # If no years provided, we might want to default to some, but better to error
            raise ValueError("Years must be provided for historical data.")
        if isinstance(years, int | str):
            years = [years]
        base_url = "https://www.ndbc.noaa.gov/data/historical/stdmet/"
        for s in stations:
            for y in years:
                urls.append(f"{base_url}{s.lower()}h{y}.txt.gz")
    return urls


def read_ndbc(fn: str, **kwargs) -> pd.DataFrame:
    """
    Read a single NDBC standard meteorological data file.
    """
    try:
        # Read header rows to get column names
        header_df = pd.read_csv(fn, sep=r"\s+", nrows=1, header=None)
        if len(header_df.columns) == 0:
            return pd.DataFrame()

        cols = header_df.iloc[0].tolist()
        if str(cols[0]).startswith("#"):
            cols[0] = str(cols[0]).lstrip("#")
            skip = 2  # Usually second row is units with # too
        else:
            skip = 1

        df = pd.read_csv(
            fn,
            sep=r"\s+",
            skiprows=skip,
            header=None,
            names=cols,
            na_values=["MM", "99", "999", "99.0", "999.0"],
            on_bad_lines="warn",
        )
    except Exception:
        return pd.DataFrame()

    # Extract siteid from filename
    import os

    basename = os.path.basename(fn)
    # 41002.txt -> 41002
    # 41002h2020.txt.gz -> 41002
    siteid = basename.split(".")[0].split("h")[0].upper()
    df["siteid"] = siteid

    return df


@lru_cache(maxsize=1)
def get_station_table() -> pd.DataFrame:
    """
    Download and parse the NDBC station table for metadata.
    """
    url = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
    try:
        df = pd.read_csv(url, sep="|", skiprows=2, header=None, on_bad_lines="skip")
        df.columns = [
            "siteid",
            "owner",
            "ttype",
            "hull",
            "name",
            "payload",
            "location",
            "timezone",
            "forecast",
            "note",
        ]
        df["siteid"] = df["siteid"].astype(str).str.strip().str.upper()

        def parse_loc(loc_str):
            try:
                # 44.794 N 87.313 W (44&#176;47'39" N 87&#176;18'48" W)
                parts = str(loc_str).split()
                lat = float(parts[0])
                if parts[1].upper() == "S":
                    lat = -lat
                lon = float(parts[2])
                if parts[3].upper() == "W":
                    lon = -lon
                return lat, lon
            except (ValueError, IndexError):
                return np.nan, np.nan

        locs = df["location"].apply(parse_loc)
        df["latitude"] = locs.apply(lambda x: x[0])
        df["longitude"] = locs.apply(lambda x: x[1])

        return df[["siteid", "name", "latitude", "longitude"]]
    except Exception:
        return pd.DataFrame()


def add_station_metadata(
    df: Union[pd.DataFrame, "dd.DataFrame"],
) -> Union[pd.DataFrame, "dd.DataFrame"]:
    """
    Merge station metadata (lat/lon) into the dataframe.
    """
    if hasattr(df, "columns") and len(df.columns) == 0:
        return df

    meta = get_station_table()
    if meta.empty:
        return df

    # Remove lat/lon from df if already present to avoid merge suffix
    cols_to_drop = [c for c in meta.columns if c in df.columns and c != "siteid"]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    try:
        import dask.dataframe as dd

        is_dask = isinstance(df, dd.DataFrame)
    except ImportError:
        is_dask = False

    if is_dask:
        meta_wrap = dd.from_pandas(meta, npartitions=1)
        df = df.merge(meta_wrap, on="siteid", how="left")
    else:
        df = df.merge(meta, on="siteid", how="left")

    return df
