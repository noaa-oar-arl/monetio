"""OpenAQ archive data on AWS.

https://openaq.org/

https://registry.opendata.aws/openaq/

https://docs.openaq.org/aws/about
"""

import logging
import warnings
from pathlib import Path
from time import perf_counter

import pandas as pd

HERE = Path(__file__).parent

logger = logging.getLogger(__name__)


def read(fp):
    """Read OpenAQ archive data from a file-like object.

    Parameters
    ----------
    fp : str or path-like or file-like
        OpenAQ archive data, suitable for passing to ``pd.read_csv``.

    Returns
    -------
    pd.DataFrame
        OpenAQ archive data.
    """

    df = pd.read_csv(
        fp,
        dtype={
            0: str,  # location_id
            1: str,  # sensor_id or sensors_id ??
            2: str,  # location
            3: str,  # datetime
            4: float,  # lat
            5: float,  # lon
            6: str,  # parameter
            7: str,  # unit or units ??
            8: float,  # value
        },
        parse_dates=["datetime"],
    )

    # Normalize to web API column names
    if "sensors_id" in df.columns:
        df = df.rename(columns={"sensors_id": "sensor_id"})
    if "unit" in df.columns:
        df = df.rename(columns={"unit": "units"})
    df = df.rename(
        columns={
            "location_id": "siteid",
            "datetime": "time",
            "lat": "latitude",
            "lon": "longitude",
        }
    )

    # Convert to UTC, non-localized
    df["time"] = df["time"].dt.tz_convert("UTC").dt.tz_localize(None)

    return df


def _maybe_to_list(x, *, not_none=False):
    """Convert non-None scalar to singleton list, or return original."""
    if not_none:
        assert x is not None
    if x is not None and pd.api.types.is_scalar(x):
        return [x]
    else:
        return x


def _to_datetime_index(dates, **kwargs):
    """Convert `dates` to a pandas DatetimeIndex
    by calling ``pd.to_datetime(dates, **kwargs)`` and then
    converting to a singleton DatetimeIndex if necessary.

    Notes
    -----
    If `dates` is already a DatetimeIndex, the same object is returned.
    """
    dates = pd.to_datetime(dates, **kwargs)
    if pd.api.types.is_scalar(dates):
        dates = pd.DatetimeIndex([dates])

    return dates


def _cache_site_days():
    """Discover all available site-days and save to CSV.

    Returns
    -------
    None
    """
    import s3fs

    fs = s3fs.S3FileSystem(anon=True)

    # TODO: could try multi-thread by month (12 threads) and start/ending digit of location ID
    glb = "openaq-data-archive/records/csv.gz/locationid=*/year=*/month=*/location-*-*.csv.gz"
    tic = perf_counter()
    paths = fs.glob(glb)
    print(f"found {len(paths)} site-day(s) in {pd.Timedelta(seconds=perf_counter() - tic)}")

    df = pd.DataFrame({"path": paths})
    df["filename"] = df["path"].str.rsplit("/", n=1, expand=True)[1]

    ext = ".csv.gz"
    assert df["filename"].str.endswith(ext).all()
    df[["siteid", "date"]] = (
        df["filename"].str.slice(None, -len(ext)).str.rsplit("-", expand=True)[[1, 2]]
    )

    df[["siteid", "date"]].to_csv(
        HERE / "openaq-data-archive_site-days.csv.gz",
        index=False,
    )


def get_paths(dates, *, siteid=None, country=None, provider=None):
    """Get site-day paths, searching independently by location ID, country, and provider.

    Parameters
    ----------
    dates : datetime-like or array-like of datetime-like
    """
    import s3fs

    fs = s3fs.S3FileSystem(anon=True)

    dates = _to_datetime_index(dates)
    location_ids = _maybe_to_list(siteid)
    providers = _maybe_to_list(provider)
    countries = _maybe_to_list(country)

    if location_ids is None and providers is None and countries is None:
        warnings.warn(
            "location ID(s) not provided; using all locations, which may be quite slow",
            stacklevel=2,
        )
        location_ids = ["*"]

    unique_dates = dates.floor("D").unique()

    print("discovering paths...")
    paths = []

    if location_ids is not None:
        tpl = (
            "openaq-data-archive/records/csv.gz/"
            "locationid={loc}/year={date:%Y}/month={date:%m}/"
            "location-{loc}-{date:%Y%m%d}.csv.gz"
        )
        for date in unique_dates:
            for loc in location_ids:
                glb = tpl.format(loc=loc, date=date)
                if "*" in glb:
                    loc_date_paths = fs.glob(glb)
                    logger.debug(f"found {len(loc_date_paths)} path(s) for glob='{glb}'")
                    paths.extend(loc_date_paths)
                else:
                    if fs.exists(glb):
                        logger.debug(f"path exists: {glb}")
                        paths.append(glb)
                    else:
                        logger.debug(f"path does not exist: {glb}")

    if providers is not None:
        tpl = (
            "openaq-data-archive/records/csv.gz/"
            "provider={prvdr}/country=*/locationid={loc}/"
            "year={date:%Y}/month={date:%m}/"
            "location-{loc}-{date:%Y%m%d}.csv.gz"
        )
        for date in unique_dates:
            for prvdr in providers:
                glb = tpl.format(prvdr=prvdr.lower(), loc="*", date=date)
                prvdr_date_paths = fs.glob(glb)
                logger.debug(f"found {len(prvdr_date_paths)} path(s) for glob='{glb}'")
                paths.extend(prvdr_date_paths)

    if countries is not None:
        tpl = (
            "openaq-data-archive/records/csv.gz/"
            "provider=*/country={cntry}/locationid={loc}/"
            "year={date:%Y}/month={date:%m}/"
            "location-{loc}-{date:%Y%m%d}.csv.gz"
        )
        for date in unique_dates:
            for cntry in countries:
                glb = tpl.format(cntry=cntry.lower(), loc="*", date=date)
                print(glb)
                cntry_date_paths = fs.glob(glb)
                logger.debug(f"found {len(cntry_date_paths)} path(s) for glob='{glb}'")
                paths.extend(cntry_date_paths)

    return sorted(set(paths))


def get_providers():
    """Get OpenAQ data providers by searching the bucket paths.

    As such, these are just lowercase short names.

    Returns
    -------
    list of str
    """
    import s3fs

    fs = s3fs.S3FileSystem(anon=True)

    paths = fs.glob("openaq-data-archive/records/csv.gz/provider=*", maxdepth=1)
    logger.debug(f"found {len(paths)} path(s)")

    providers = []
    for p in paths:
        _, provider = p.split("=")
        providers.append(provider)

    logger.debug(f"found {len(providers)} provider(s)")
    if not providers:
        warnings.warn("no providers found", stacklevel=2)

    return providers


def get_provider_countries(provider):
    """Get countries for a given provider by searching the bucket paths.

    Parameters
    ----------
    provider : str
        OpenAQ data provider lowercase short name, e.g. 'airnow', 'aqdc'.

    Returns
    -------
    list of str
    """
    import s3fs

    fs = s3fs.S3FileSystem(anon=True)

    glb = f"openaq-data-archive/records/csv.gz/provider={provider.lower()}/country=*"
    paths = fs.glob(glb, maxdepth=1)
    logger.debug(f"found {len(paths)} path(s) for provider={provider!r}")

    countries = []
    for p in paths:
        _, _, country = p.split("=")
        countries.append(country)

    logger.debug(f"found {len(countries)} country(ies) for provider={provider!r}")
    if not countries:
        warnings.warn(f"no countries found for provider={provider!r}", stacklevel=2)

    return countries


def get_all_locations():
    """Get all location IDs by searching for /records/csv.gz/locationid=* bucket paths.

    .. note::
       This returns a significantly greater number of location IDs
       than using :func:`get_locations` with ``provider=None`` and ``country=None``.
       The latter's count is more consistent with results from the web API.

    Returns
    -------
    list of str
    """
    import re

    import s3fs

    fs = s3fs.S3FileSystem(anon=True)

    # Note: ~ 2x faster than using fs.glob
    paths = fs.find("openaq-data-archive/records/csv.gz/", withdirs=True, maxdepth=1)
    logger.debug(f"found {len(paths)} path(s)")

    locs = []
    for p in paths:
        m = re.fullmatch(r"openaq-data-archive/records/csv\.gz/locationid=([0-9]+)", p)
        if m is not None:
            locs.append(m.group(1))

    logger.debug(f"found {len(locs)} location(s)")
    if not locs:
        warnings.warn("no locations found", stacklevel=2)

    return sorted(locs)


def get_locations(*, provider=None, country=None):
    """Get location IDs corresponding to provider(s) and/or country(ies)
    by searching for /records/csv.gz/provider=*/country=*/locationid=* bucket paths.

    Default: all locations (search all providers/countries;
    should take less than 10 seconds).

    Returns
    -------
    pd.DataFrame
    """
    import re

    import s3fs

    if country is None and provider is None:
        warnings.warn(
            "get_all_locations() is a faster way to get all locations, "
            "though the result is different",
            stacklevel=2,
        )

    print("discovering locations...")
    fs = s3fs.S3FileSystem(anon=True)

    country = _maybe_to_list(country)
    if provider is None:
        providers = get_providers()
    else:
        providers = _maybe_to_list(provider, not_none=True)

    paths = []
    for prvdr in providers:
        if country is None:
            countries = get_provider_countries(prvdr)
        else:
            countries = country

        for cntry in countries:
            glb = (
                "openaq-data-archive/records/csv.gz/"
                f"provider={prvdr.lower()}/country={cntry.lower()}/"
            )
            prvdr_cntry_paths = fs.find(
                glb,
                withdirs=True,
                maxdepth=1,
            )
            paths.extend(prvdr_cntry_paths)

    logger.debug(f"found {len(paths)} path(s)")

    rows = []
    for p in paths:
        m = re.fullmatch(
            r"openaq-data-archive/records/csv\.gz/"
            r"provider=([a-z0-9\-]+)/country=([a-z]{2}|\-\-|99|mobile)/"
            r"locationid=([0-9]+)",
            p,
        )
        if m is not None:
            rows.append(m.groups())

    df = pd.DataFrame(rows, columns=["provider", "country", "siteid"])

    logger.debug(f"found {len(df)} location(s)")
    if df.empty:
        warnings.warn(
            f"no locations found for country={country!r} provider={provider!r}",
            stacklevel=2,
        )

    return df


def _build_urls(dates, sites, *, protocol="s3"):
    """Naively build URLs for OpenAQ archive data on AWS
    for the given `sites` (location IDs) and dates.

    "Naively" meaning not checking if the files actually exist.

    Parameters
    ----------
    dates : datetime-like or array-like of datetime-like
        Desired dates.
    sites : str or list of str
        Sites (OpenAQ location IDs).
    protocol : {'s3', 'http', 'https'}
        URL protocol to use.

    Returns
    -------
    list of str
    """
    dates = _to_datetime_index(dates)
    sites = _maybe_to_list(sites, not_none=True)

    if protocol.lower() == "s3":
        pref = "s3://openaq-data-archive"
    elif protocol.lower() in {"http", "https"}:
        pref = f"{protocol.lower()}://openaq-data-archive.s3.amazonaws.com"
    else:
        raise ValueError(f"protocol: {protocol!r}")

    urls = []
    for site in sites:
        for date in dates.floor("D").unique():
            urls.append(
                f"{pref}/records/csv.gz/"
                f"locationid={site}/year={date:%Y}/month={date:%m}/"
                f"location-{site}-{date:%Y%m%d}.csv.gz"
            )

    return urls


def _maybe_read(fp):
    """Try to :func:`read` a file, returning an empty DataFrame if it doesn't exist."""
    try:
        return read(fp)
    except FileNotFoundError:
        logger.info(f"file not found: {fp}")
        return pd.DataFrame(
            columns=[
                "siteid",
                "sensor_id",
                "location",
                "time",
                "latitude",
                "longitude",
                "parameter",
                "units",
                "value",
            ]
        )


def add_data(
    dates,
    *,
    siteid=None,
    country=None,
    provider=None,
    find_paths=True,
    n_procs=1,
):
    """Add OpenAQ data from AWS Open Data (https://registry.opendata.aws/openaq/).

    Parameters
    ----------
    dates : datetime-like or array-like of datetime-like
        Desired dates (the archive data is stored in daily files, per location).
    siteid : str or int or list, optional
        OpenAQ location ID(s) to include.
        For example, from :func:`get_locations`.
    country : str or list of str, optional
        Country or countries to include. 2-character ISO country codes.
        'mobile' is also an option.
        Other special values are '99' and '--'.
    provider : str or list of str, optional
        Data provider(s) to include.
    find_paths : bool
        Search for paths in the bucket using :func:`get_paths` (default).
        If false, you must provide `siteid` and paths will be constructed naively.
        Use ``find_paths=False`` if you know in advance the sites you want
        and are confident they have data for most of the given dates.
    n_procs : int
        Number of Dask workers to use.

    Returns
    -------
    pd.DataFrame
        OpenAQ archive data.
    """
    import dask.dataframe as dd

    dates = _to_datetime_index(dates).dropna()
    if dates.empty:
        raise ValueError("must provide at least one datetime-like")

    if find_paths:
        paths = get_paths(dates, siteid=siteid, country=country, provider=provider)
        print(f"found {len(paths)} path(s)")
        urls = [f"s3://{p}" for p in paths]
        func = read
    else:
        if siteid is None:
            raise ValueError("must provide `siteid` when `find_paths` is false")
        urls = _build_urls(dates, siteid)
        print(f"built {len(urls)} path(s)")
        func = _maybe_read

    meta = [
        ("siteid", str),
        ("sensor_id", str),
        ("location", str),
        ("time", "datetime64[ns]"),
        ("latitude", float),
        ("longitude", float),
        ("parameter", str),
        ("units", str),
        ("value", float),
    ]
    print("reading...")
    df = dd.from_map(func, urls, meta=meta).compute(num_workers=n_procs)

    return df.reset_index(drop=True)
