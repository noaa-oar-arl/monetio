"""OpenAQ V2 REST API Reader"""

import functools
import json
import logging
import os
import warnings
from datetime import datetime
from typing import TYPE_CHECKING, Any, Union

import numpy as np
import pandas as pd
import requests
import xarray as xr

from ..util import force_object_strings
from .base import PointReader, register_reader
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("OPENAQ_API_KEY", None)
if API_KEY is not None:
    API_KEY = API_KEY.strip()
    if len(API_KEY) != 64:
        warnings.warn(f"API key length is {len(API_KEY)}, expected 64")

_PPM_TO_UGM3 = {
    "o3": 1990,
    "co": 1160,
    "no2": 1900,
    "no": 1240,
    "so2": 2650,
    "ch4": 664,
    "co2": 1820,
}
_PPM_TO_UGM3["nox"] = _PPM_TO_UGM3["no2"]

_NON_MOLEC_PARAMS = [
    "pm1",
    "pm25",
    "pm4",
    "pm10",
    "bc",
]

_BASE_URL = "https://api.openaq.org"
_ENDPOINTS = {
    "locations": "/v2/locations",
    "parameters": "/v2/parameters",
    "measurements": "/v2/measurements",
}


def _api_key_warning(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if API_KEY is None:
            warnings.warn(
                "Non-cached requests to the OpenAQ v2 web API will be slow without an API key "
                "or requests will fail (HTTP error 401). "
                "Obtain one (https://docs.openaq.org/docs/getting-started#api-key) "
                "and set your OPENAQ_API_KEY environment variable.",
                stacklevel=2,
            )
        return func(*args, **kwargs)

    return wrapper


def _consume(endpoint, *, params=None, timeout=10, retry=5, limit=500, npages=None):
    """Consume a paginated OpenAQ API endpoint."""
    import time
    from random import random as rand

    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    if not endpoint.startswith("/v2"):
        endpoint = "/v2" + endpoint
    url = _BASE_URL + endpoint

    if params is None:
        params = {}

    if npages is None:
        npages = min(100_000 // limit, 6_000)

    params["limit"] = limit

    headers = {
        "Accept": "application/json",
        "X-API-Key": API_KEY,
        "User-Agent": "monetio",
    }

    data = []
    for page in range(1, npages + 1):
        params["page"] = page

        tries = 0
        while tries < retry:
            logger.debug(f"GET {url} params={params}")
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            tries += 1
            if r.status_code == 408:
                logger.info(f"request timed out (try {tries}/{retry})")
                time.sleep(tries + 0.1 * rand())
            elif r.status_code == 429:
                logger.info(f"rate limited (try {tries}/{retry})")
                time.sleep(tries * 5 + 0.2 * rand())
            else:
                break
        r.raise_for_status()

        this_data = r.json()
        found = this_data["meta"]["found"]
        n = len(this_data["results"])
        logger.info(f"page={page} found={found!r} n={n}")
        if n == 0:
            break
        data.extend(this_data["results"])

    return data


@_api_key_warning
def get_locations(**kwargs) -> pd.DataFrame:
    """
    Get available site info (including site IDs) from OpenAQ v2 API.

    Parameters
    ----------
    **kwargs : Any
        Arguments passed to _consume (e.g. limit, npages).

    Returns
    -------
    pd.DataFrame
        Available site info.

    Examples
    --------
    >>> from monetio.readers.openaq_v2 import get_locations
    >>> sites = get_locations(limit=10)
    """
    data = _consume(_ENDPOINTS["locations"], **kwargs)

    some_scalars = [
        "id",
        "name",
        "city",
        "country",
        "isMobile",
        "firstUpdated",
        "lastUpdated",
    ]

    data2 = []
    for d in data:
        lat = d["coordinates"]["latitude"]
        lon = d["coordinates"]["longitude"]
        parameters = [p["parameter"] for p in d["parameters"]]
        mfs = d["manufacturers"]
        manufacturer = mfs[0]["manufacturerName"] if mfs else None
        d2 = {k: d[k] for k in some_scalars}
        d2.update(
            latitude=lat,
            longitude=lon,
            parameters=parameters,
            manufacturer=manufacturer,
        )
        data2.append(d2)

    df = pd.DataFrame(data2)
    if df.empty:
        return df

    df["firstUpdated"] = pd.to_datetime(df.firstUpdated.str.slice(0, 19))
    df["lastUpdated"] = pd.to_datetime(df.lastUpdated.str.slice(0, 19))

    df = df.rename(columns={"id": "siteid"})
    df["siteid"] = df.siteid.astype(str)
    df = df.drop_duplicates("siteid", keep="first").reset_index(drop=True)

    return df


def get_parameters(**kwargs) -> pd.DataFrame:
    """
    Get supported parameter info from OpenAQ v2 API.

    Parameters
    ----------
    **kwargs : Any
        Arguments passed to _consume (e.g. limit, npages).

    Returns
    -------
    pd.DataFrame
        Supported parameters.

    Examples
    --------
    >>> from monetio.readers.openaq_v2 import get_parameters
    >>> params = get_parameters()
    """
    data = _consume(_ENDPOINTS["parameters"], **kwargs)
    return pd.DataFrame(data)


@register_reader("openaq_v2")
class OpenAQV2Reader(PointReader):
    """
    Reader for OpenAQ V2 REST API data.
    """

    @_api_key_warning
    def open_dataset(
        self,
        dates: pd.DatetimeIndex | list[datetime] | datetime | str = None,
        parameters: list[str] = None,
        country: str | list[str] = None,
        sites: list[str] = None,
        wide_fmt: bool = True,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieves OpenAQ data via the REST API.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        parameters : List[str], optional
            Species to retrieve, by default ['pm25', 'o3'].
        country : Union[str, List[str]], optional
            Country code(s).
        sites : List[str], optional
            Site ID(s).
        wide_fmt : bool, optional
            Whether to return data in wide format, by default True.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the API.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded data.

        Examples
        --------
        >>> from monetio.readers.openaq_v2 import OpenAQV2Reader
        >>> reader = OpenAQV2Reader()
        >>> df = reader.open_dataset(dates='2023-01-01', as_xarray=False)
        """
        if dates is None:
            raise ValueError("must provide at least one datetime-like via 'dates'")

        dates = pd.to_datetime(dates)
        if pd.api.types.is_scalar(dates):
            dates = pd.DatetimeIndex([dates])
        dates = dates.dropna()
        if dates.empty:
            raise ValueError("must provide at least one datetime-like")

        if parameters is None:
            parameters = ["pm25", "o3"]
        elif isinstance(parameters, str):
            parameters = [parameters]

        if lazy:
            import dask.dataframe as dd
            from dask import delayed

            query_time_split = kwargs.get("query_time_split", "1D")
            query_dt = pd.to_timedelta(query_time_split)
            date_min, date_max = dates.min(), dates.max()

            def iter_time_slices():
                one_sec = pd.Timedelta(seconds=1)
                if date_min < date_max:
                    t = date_min
                    while t < date_max:
                        t_next = min(t + query_dt, date_max)
                        yield t - one_sec, t_next
                        t = t_next
                else:
                    yield date_min - one_sec, date_max

            delayed_dfs = []
            for t_from, t_to in iter_time_slices():
                for p in parameters:
                    part_kwargs = kwargs.copy()
                    part_kwargs.update(
                        dates=pd.DatetimeIndex([t_from + pd.Timedelta(seconds=1), t_to]),
                        parameters=[p],
                        country=country,
                        sites=sites,
                        query_time_split=None,
                    )
                    delayed_dfs.append(delayed(self._fetch_data)(**part_kwargs))

            if not delayed_dfs:
                df = dd.from_pandas(pd.DataFrame(), npartitions=1)
            else:
                meta = self._get_meta()
                df = dd.from_delayed(delayed_dfs, meta=meta)
        else:
            df = self._fetch_data(
                dates=dates, parameters=parameters, country=country, sites=sites, **kwargs
            )

        df = self.harmonize(df)

        df = force_object_strings(df)

        if as_xarray:
            return self.to_xarray(df, wide_fmt=wide_fmt, **kwargs)

        if wide_fmt:
            from ..util import long_to_wide

            df = long_to_wide(df)

        return df

    def _get_meta(self) -> pd.DataFrame:
        """Returns an empty DataFrame with the expected columns for Dask metadata."""
        cols = {
            "siteid": str,
            "location": str,
            "variable": str,
            "obs": float,
            "units": str,
            "country": str,
            "city": str,
            "is_mobile": bool,
            "is_analysis": object,
            "entity": str,
            "sensor_type": str,
            "time": "datetime64[us]",
            "time_local": "datetime64[us]",
            "utcoffset": "timedelta64[us]",
            "latitude": float,
            "longitude": float,
        }
        df = pd.DataFrame({k: pd.Series(dtype=v) for k, v in cols.items()})
        return df

    def _fetch_data(self, **kwargs) -> pd.DataFrame:
        """Internal fetch logic (Eager)."""
        dates = kwargs.get("dates")
        parameters = kwargs.get("parameters")
        country = kwargs.get("country")
        sites = kwargs.get("sites")
        entity = kwargs.get("entity")
        sensor_type = kwargs.get("sensor_type")
        query_time_split = kwargs.get("query_time_split", "1h")
        search_radius = kwargs.get("search_radius")

        query_dt = (
            pd.to_timedelta(query_time_split) if query_time_split and len(dates) > 1 else None
        )
        date_min, date_max = dates.min(), dates.max()

        def iter_time_slices():
            one_sec = pd.Timedelta(seconds=1)
            if query_dt is not None and date_min < date_max:
                t = date_min
                while t < date_max:
                    t_next = min(t + query_dt, date_max)
                    yield t - one_sec, t_next
                    t = t_next
            else:
                yield date_min - one_sec, date_max

        base_params = {}
        if country is not None:
            base_params.update(country=country)
        if sites is not None:
            base_params.update(location_id=sites)
        if entity is not None:
            base_params.update(entity=entity)
        if sensor_type is not None:
            base_params.update(sensor_type=sensor_type)

        def iter_queries():
            for parameter in parameters:
                for t_from, t_to in iter_time_slices():
                    if search_radius is not None:
                        for coords, radius in search_radius.items():
                            lat, lon = coords
                            yield {
                                **base_params,
                                "parameter": parameter,
                                "date_from": t_from,
                                "date_to": t_to,
                                "coordinates": f"{lat:.8f},{lon:.8f}",
                                "radius": radius,
                            }
                    else:
                        yield {
                            **base_params,
                            "parameter": parameter,
                            "date_from": t_from,
                            "date_to": t_to,
                        }

        # Clean kwargs for _consume
        consume_kwargs = {
            k: v for k, v in kwargs.items() if k in ["timeout", "retry", "limit", "npages"]
        }

        threads = kwargs.get("threads", None)
        if threads is not None:
            import concurrent.futures
            from itertools import chain

            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                data = chain.from_iterable(
                    executor.map(
                        lambda p: _consume(_ENDPOINTS["measurements"], params=p, **consume_kwargs),
                        iter_queries(),
                    )
                )
        else:
            data = []
            for p in iter_queries():
                this_data = _consume(_ENDPOINTS["measurements"], params=p, **consume_kwargs)
                data.extend(this_data)

        df = pd.DataFrame(data)
        if df.empty:
            return self._get_meta()

        to_expand = ["date", "coordinates"]
        to_expand = [c for c in to_expand if c in df.columns]
        new = pd.json_normalize(json.loads(df[to_expand].to_json(orient="records")))

        if "date.utc" in new.columns:
            time = pd.to_datetime(new["date.utc"]).dt.tz_localize(None)
        else:
            time = pd.Series(np.nan, index=new.index, dtype="datetime64[ns]")

        if "date.local" in new.columns:
            time_local = pd.to_datetime(new["date.local"].str.slice(0, 19))
        else:
            time_local = time.copy()

        utcoffset = time_local - time

        lat = new["coordinates.latitude"] if "coordinates.latitude" in new.columns else np.nan
        lon = new["coordinates.longitude"] if "coordinates.longitude" in new.columns else np.nan

        df = df.drop(columns=to_expand).assign(
            time=time,
            time_local=time_local,
            utcoffset=utcoffset,
            latitude=lat,
            longitude=lon,
        )

        df = df.rename(
            columns={
                "locationId": "siteid",
                "isMobile": "is_mobile",
                "isAnalysis": "is_analysis",
                "sensorType": "sensor_type",
                "parameter": "variable",
                "unit": "units",
                "value": "obs",
            },
        )
        df["siteid"] = df.siteid.astype(str)

        meta = self._get_meta()
        for col in meta.columns:
            if col not in df.columns:
                df[col] = pd.Series(dtype=meta[col].dtype)

        # Force exact dtypes from meta to avoid dask mismatch
        for col in meta.columns:
            df[col] = df[col].astype(meta[col].dtype)

        return df[meta.columns]

    def harmonize(
        self, df: Union[pd.DataFrame, "dd.DataFrame"]
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Harmonize the dataset (standard naming, dropping NaNs, unit conversion).

        Parameters
        ----------
        df : Union[pd.DataFrame, dd.DataFrame]
            Input dataframe.

        Returns
        -------
        Union[pd.DataFrame, dd.DataFrame]
            Harmonized dataframe.
        """
        df = super().harmonize(df)

        non_neg_units = [
            "particles/cm³",
            "ppm",
            "ppb",
            "umol/mol",
            "µg/m³",
            "ugm3",
            "ng/m3",
            "iaq",
            "%",
            "m/s",
            "hpa",
            "mb",
        ]

        def _clean_values(df_part):
            if df_part.empty:
                return df_part
            if "units" in df_part.columns and "obs" in df_part.columns:
                mask = df_part.units.isin(non_neg_units) & (df_part.obs < 0)
                df_part.loc[mask, "obs"] = np.nan
            return df_part

        def _convert_units(df_part):
            if df_part.empty:
                return df_part
            for vn, f in _PPM_TO_UGM3.items():
                if "variable" in df_part.columns and "units" in df_part.columns:
                    is_ug = (df_part.variable == vn) & (df_part.units == "µg/m³")
                    df_part.loc[is_ug, "obs"] /= f
                    df_part.loc[is_ug, "units"] = "ppm"
            return df_part

        try:
            import dask.dataframe as dd

            is_dask = isinstance(df, dd.DataFrame)
        except ImportError:
            is_dask = False

        if is_dask:
            df = df.map_partitions(_clean_values)
            df = df.map_partitions(_convert_units)
            df = df.drop_duplicates(subset=["time", "siteid", "variable"])
        else:
            df = _clean_values(df)
            df = _convert_units(df)
            df = df.drop_duplicates(subset=["time", "siteid", "variable"])

        df = update_history(df, "Cleaned negative values, converted units, and dropped duplicates.")
        return df

    def to_xarray(
        self,
        df: Union[pd.DataFrame, "dd.DataFrame"],
        expand2d: bool = True,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Convert to xarray and rename variables for consistency.

        Parameters
        ----------
        df : Union[pd.DataFrame, dd.DataFrame]
            Input dataframe.
        wide_fmt : bool, optional
            Whether to expand to wide format, by default True.
        **kwargs : Any
            Additional arguments.

        Returns
        -------
        xr.Dataset
            The loaded dataset.
        """
        # OpenAQ v2 uses wide_fmt as an alias for expand2d
        wide_fmt = kwargs.get("wide_fmt", expand2d)
        ds = super().to_xarray(df, expand2d=wide_fmt, **kwargs)

        if wide_fmt:
            rename_dict = {}
            for v in _PPM_TO_UGM3:
                if v in ds.data_vars:
                    ds[v].attrs["units"] = "ppm"
                    rename_dict[v] = f"{v}_ppm"
            for v in _NON_MOLEC_PARAMS:
                if v in ds.data_vars:
                    ds[v].attrs["units"] = "ug/m3"
                    rename_dict[v] = f"{v}_ugm3"

            if rename_dict:
                ds = ds.rename(rename_dict)
                from .base import _format_units

                ds = _format_units(ds)
                ds = update_history(ds, f"Renamed variables: {list(rename_dict.values())}")

        return ds
