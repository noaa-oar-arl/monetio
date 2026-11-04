"""Reader for EMC-referenced GHRSST Level 4 SST analyses."""

from __future__ import annotations

import json
from datetime import date, datetime
from html.parser import HTMLParser
from urllib.parse import unquote, urljoin, urlparse

import pandas as pd
import requests
import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import standardize_satellite_coords, update_history

PRODUCTS = {
    "ghrsst_ncei_avhrr_anl": {
        "provider": "NCEI",
        "marker": "avhrr_oi",
        "root": "https://www.ncei.noaa.gov/data/oceans/ghrsst/L4/GLOB/NCEI/AVHRR_OI/",
        "offset": 86400,
    },
    "ghrsst_ospo_geopolar_anl": {
        "provider": "OSPO",
        "marker": "geo_polar_blended",
        "root": "https://www.ncei.noaa.gov/data/oceans/ghrsst/L4/GLOB/OSPO/Geo_Polar_Blended/",
        "offset": 43200,
    },
}


class _Links(HTMLParser):
    """Collect href values from a simple HTTP directory index."""

    def __init__(self):
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self.hrefs.append(href)


def _identify_product(ds: xr.Dataset, requested: str | None) -> str:
    """Identify a supported product from metadata, optionally checking selection."""
    if requested is not None and requested not in PRODUCTS:
        raise ValueError(
            f"Unsupported GHRSST product {requested!r}; choose one of {', '.join(PRODUCTS)}"
        )

    metadata = " ".join(str(value) for value in ds.attrs.values()).lower()
    if "ghrsst_ncei_avhrr_anl" in metadata or ("ncei" in metadata and "avhrr_oi" in metadata):
        found = "ghrsst_ncei_avhrr_anl"
    elif "ghrsst_ospo_geopolar_anl" in metadata or (
        "ospo" in metadata and "geo_polar_blended" in metadata
    ):
        found = "ghrsst_ospo_geopolar_anl"
    else:
        found = None

    if found and requested and found != requested:
        source = ds.encoding.get("source", "input")
        raise ValueError(
            f"GHRSST product selection {requested!r} conflicts with metadata in {source!r} ({found})"
        )
    if found:
        return found
    if requested:
        return requested
    source = ds.encoding.get("source", "input")
    raise ValueError(
        f"Cannot identify GHRSST product from metadata in {source!r}; pass product explicitly"
    )


def ghrsst_preprocess(ds: xr.Dataset, product: str | None = None) -> xr.Dataset:
    """Validate and standardize one GHRSST dataset without materializing science arrays."""
    source = ds.encoding.get("source", "input")
    identified = _identify_product(ds, product)
    ds = standardize_satellite_coords(
        ds,
        lat_name="lat",
        lon_name="lon",
        y_dim=["lat", "latitude"],
        x_dim=["lon", "longitude"],
        time_name="time",
    )
    # standardize_satellite_coords renames a one-dimensional lat/lon axis to
    # y/x before it inspects variable names, so restore canonical geographic
    # coordinate aliases without copying the underlying arrays.
    if "latitude" not in ds.variables and "y" in ds.coords:
        ds = ds.assign_coords(latitude=ds["y"])
    if "longitude" not in ds.variables and "x" in ds.coords:
        ds = ds.assign_coords(longitude=ds["x"])
    missing = [
        name
        for name in ("analysed_sst", "time", "latitude", "longitude")
        if name not in ds.variables
    ]
    if missing:
        raise ValueError(
            f"GHRSST input {source!r} is missing required field(s): {', '.join(missing)}"
        )
    if "latitude" not in ds.coords or "longitude" not in ds.coords:
        raise ValueError(f"GHRSST input {source!r} must provide latitude and longitude coordinates")
    ds.attrs["ghrsst_product"] = identified
    ds.attrs["ghrsst_provider"] = PRODUCTS[identified]["provider"]
    return ds


def _date_token(value) -> str:
    try:
        target = pd.Timestamp(value)
        if pd.isna(target):
            raise ValueError(f"Invalid GHRSST date {value!r}")
        return target.strftime("%Y%m%d")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid GHRSST date {value!r}") from exc


def _relevant_directory(url: str, root: str, target: pd.Timestamp) -> bool:
    """Limit index traversal to directories whose path can contain this date."""
    root_path = urlparse(root).path.rstrip("/") + "/"
    path = urlparse(url).path
    if not path.startswith(root_path):
        return False
    relative = path[len(root_path) :].strip("/")
    if not relative:
        return True
    tokens = {
        target.strftime("%Y"),
        target.strftime("%Y%m"),
        target.strftime("%Y%m%d"),
        target.strftime("%j"),
        target.strftime("%m"),
        target.strftime("%d"),
    }
    parts = relative.split("/")
    return len(parts) <= 4 and all(part in tokens for part in parts)


def _archive_candidates(root: str, target: pd.Timestamp, product: str) -> list[str]:
    """Find matching daily files by traversing date-relevant HTML index links."""
    root = root.rstrip("/") + "/"
    marker = PRODUCTS[product]["marker"]
    day = target.strftime("%Y%m%d")
    queue = [root]
    visited: set[str] = set()
    candidates: set[str] = set()

    while queue:
        index_url = queue.pop(0)
        if index_url in visited:
            continue
        visited.add(index_url)
        response = requests.get(index_url, timeout=30)
        response.raise_for_status()
        links = _Links()
        links.feed(response.text)
        for href in links.hrefs:
            if href.startswith(("../", "./", "#", "?")):
                continue
            url = urljoin(index_url, href)
            if not url.startswith(root) or url in visited:
                continue
            parsed = urlparse(url)
            name = unquote(parsed.path.rsplit("/", 1)[-1]).lower()
            if not parsed.path.endswith("/") and day in name and marker in name:
                candidates.add(url)
            elif (
                parsed.path.endswith("/")
                and _relevant_directory(url, root, target)
                and len(visited) < 500
            ):
                queue.append(url)
    return sorted(candidates)


@register_reader("ghrsst")
class GHRSSTReader(GriddedReader):
    """Read NCEI AVHRR OI and OSPO Geo-Polar Blended GHRSST L4 products."""

    def open_dataset(
        self,
        files: str | list[str] | None = None,
        *,
        dates=None,
        product: str | None = None,
        base_url: str | None = None,
        time_mode: str = "native",
        use_dask: bool = False,
        defer: bool = False,
        **kwargs,
    ) -> xr.Dataset:
        """Open local/HTTPS files or resolve archive URLs from product and date."""
        if time_mode not in {"native", "emc"}:
            raise ValueError("time_mode must be either 'native' or 'emc'")
        if files is None:
            if dates is None or product is None:
                raise ValueError("Provide either files or both dates and product")
            files = self.build_urls(dates, product=product, base_url=base_url)
        elif product is not None and product not in PRODUCTS:
            raise ValueError(
                f"Unsupported GHRSST product {product!r}; choose one of {', '.join(PRODUCTS)}"
            )

        inputs = files if isinstance(files, list | tuple) else [files]
        if not inputs:
            raise ValueError("GHRSST files must not be empty")
        seen_products: set[str] = set()
        user_preprocess = kwargs.pop("preprocess", None)

        def preprocess(ds):
            result = ghrsst_preprocess(ds, product=product)
            seen_products.add(result.attrs["ghrsst_product"])
            if user_preprocess is not None:
                result = user_preprocess(result)
            return result

        if len(inputs) > 1:
            # Preserve every member, including duplicate-time members, so the
            # reader can diagnose conflicts explicitly after opening them.
            kwargs.setdefault("combine", "nested")
            kwargs.setdefault("concat_dim", "time")
            kwargs.setdefault("join", "exact")
        ds = super().open_dataset(
            files,
            use_dask=use_dask,
            defer=defer,
            preprocess=preprocess,
            **kwargs,
        )
        if len(seen_products) != 1:
            ds.close()
            raise ValueError(
                f"GHRSST inputs contain mixed or unidentified products: {sorted(seen_products)}"
            )
        identified = next(iter(seen_products))
        if product and identified != product:
            ds.close()
            raise ValueError(
                f"Requested GHRSST product {product!r} does not match the input product {identified!r}"
            )

        times = pd.Index(ds.time.values)
        if times.has_duplicates:
            ds.close()
            raise ValueError(
                f"GHRSST inputs contain duplicate observation times: {times[times.duplicated()].tolist()}"
            )
        if len(inputs) > 1:
            ds = ds.sortby("time")
        offsets = 0
        if time_mode == "emc":
            offsets = PRODUCTS[identified]["offset"]
            ds = ds.assign_coords(time=ds.time + pd.to_timedelta(offsets, unit="s"))
        ds.attrs["ghrsst_product"] = identified
        ds.attrs["ghrsst_provider"] = PRODUCTS[identified]["provider"]
        ds.attrs["ghrsst_time_mode"] = time_mode
        ds.attrs["ghrsst_time_offset_seconds"] = offsets
        ds.attrs["source_files"] = json.dumps([str(path) for path in inputs])
        ds = update_history(
            ds, f"Read {identified} GHRSST Level 4 analysis ({time_mode} time mode)."
        )
        return ds

    def build_urls(self, dates, product: str, base_url: str | None = None) -> list[str]:
        """Resolve one current NCEI archive file for each requested date."""
        if product not in PRODUCTS:
            raise ValueError(
                f"Unsupported GHRSST product {product!r}; choose one of {', '.join(PRODUCTS)}"
            )
        if isinstance(dates, str | date | datetime | pd.Timestamp) or not hasattr(
            dates, "__iter__"
        ):
            dates = [dates]
        date_values = [_date_token(value) for value in dates]
        if not date_values:
            raise ValueError("At least one GHRSST date is required")
        root = (base_url or PRODUCTS[product]["root"]).rstrip("/") + "/"
        urls = []
        for value in date_values:
            target = pd.Timestamp(value)
            candidates = _archive_candidates(root, target, product)
            if not candidates:
                raise FileNotFoundError(f"No {product} archive file found for {value} under {root}")
            if len(candidates) > 1:
                raise ValueError(
                    f"Multiple {product} archive files found for {value}: {candidates}"
                )
            urls.append(candidates[0])
        return urls
