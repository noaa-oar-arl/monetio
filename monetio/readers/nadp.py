"""NADP Reader."""

from __future__ import annotations

import functools
from datetime import datetime
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import xarray as xr

from ..util import force_object_strings
from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd

# Meta URLs
META_URLS = {
    "ntn": "https://bit.ly/2sPMvaO",
    "mdn": "https://bit.ly/2Lq6kgq",
    "airmon": "https://bit.ly/2xMlgTW",
    "amon": "https://bit.ly/2sJmkCg",
    "amnet": "https://bit.ly/2sJmkCg",
}

# Network Specifications
NADP_SPECS = {
    "ntn": {
        "parse_dates": [2, 3],
        "rename_cols": {"dateon": "time", "dateoff": "time_off"},
        "cleaning_cols": ["mg", "br", "so4", "cl", "no3", "nh4", "k", "na", "ca"],
        "flag_col_prefix": "flag",
        "flag_values": ["<"],
        "mask_negative": True,
    },
    "mdn": {
        "parse_dates": [1, 2],
        "rename_cols": {"dateon": "time", "dateoff": "time_off"},
        "cleaning_cols": ["rgppt", "svol", "subppt", "hgconc", "hgdep"],
        "flag_col": "qr",
        "flag_contains": "C",
    },
    "airmon": {
        "parse_dates": [2, 3],
        "rename_cols": {"dateon": "time", "dateoff": "time_off"},
        "cleaning_cols": [
            "subppt",
            "pptnws",
            "pptbel",
            "svol",
            "ca",
            "mg",
            "k",
            "na",
            "nh4",
            "no3",
            "cl",
            "so4",
            "po4",
            "phlab",
            "phfield",
            "conduclab",
            "conducfield",
        ],
        "flag_col": "qrcode",
        "flag_contains": "C",
    },
    "amon": {
        "parse_dates": [2, 3],
        "rename_cols": {"startdate": "time", "enddate": "time_off"},
        "cleaning_cols": ["airvol", "conc"],
        "flag_col": "qr",
        "flag_contains": "C",
    },
    "amnet": {
        "parse_dates": [2, 3],
        "rename_cols": {"startdate": "time", "enddate": "time_off"},
        "cleaning_cols": ["airvol", "conc"],
        "flag_col": "qr",
        "flag_contains": "C",
    },
}


def read_nadp(filename: str, network: str = "ntn", **kwargs: dict) -> pd.DataFrame:
    """
    Read a single NADP file.

    Parameters
    ----------
    filename : str
        The path or URL to the NADP file.
    network : str, optional
        The NADP network (ntn, mdn, amon, airmon, amnet), by default "ntn".
    **kwargs : dict
        Additional arguments passed to pd.read_csv.

    Returns
    -------
    pd.DataFrame
        The loaded data.

    Examples
    --------
    >>> df = read_nadp("NTN-All-w.csv", network="ntn")
    """
    network = network.lower()
    spec = NADP_SPECS.get(network, {})

    parse_dates = spec.get("parse_dates", [])
    rename_cols = spec.get("rename_cols", {})

    # Use FileUtility to handle remote files
    fs = FileUtility.get_fs(filename)
    with fs.open(filename, "r") as f:
        df = pd.read_csv(f, **kwargs)

    df.columns = [i.lower() for i in df.columns]

    # Handle date parsing
    if parse_dates:
        for col_idx in parse_dates:
            if col_idx < len(df.columns):
                col_name = df.columns[col_idx]
                df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
    df = df.rename(columns=rename_cols)

    # Ensure flag/status columns are strings for later .str.contains usage
    for col in ["qr", "qrcode"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper()

    # Update history for I/O
    df = update_history(df, f"Read NADP {network} data.")

    # Apply network-specific cleaning from spec
    cleaning_cols = spec.get("cleaning_cols", [])
    available_cols = [c for c in cleaning_cols if c in df.columns]

    if available_cols:
        # Convert to numeric
        df[available_cols] = df[available_cols].apply(pd.to_numeric, errors="coerce")

        # 1. Prefix-based flags (e.g. NTN flagmg for mg)
        prefix = spec.get("flag_col_prefix")
        flag_vals = spec.get("flag_values", [])
        mask_neg = spec.get("mask_negative", False)

        if prefix or mask_neg:
            for col in available_cols:
                mask = pd.Series(False, index=df.index)
                if prefix:
                    flag_col = prefix + col
                    if flag_col in df.columns:
                        for fv in flag_vals:
                            mask |= df[flag_col] == fv
                if mask_neg:
                    mask |= df[col] < 0
                df[col] = df[col].mask(mask)

        # 2. Global flag column (e.g. MDN 'qr' contains 'C')
        flag_col = spec.get("flag_col")
        flag_contains = spec.get("flag_contains")
        if flag_col and flag_contains and flag_col in df.columns:
            mask = df[flag_col].str.contains(flag_contains, na=False)
            for col in available_cols:
                df[col] = df[col].mask(mask)

    return df


@register_reader("nadp")
class NADPReader(PointReader):
    """
    Reader for National Atmospheric Deposition Program (NADP) data.
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
        network: str = "NTN",
        siteid: str | None = None,
        weekly: bool = True,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs: Any,
    ) -> xr.Dataset | pd.DataFrame | dd.DataFrame:
        """
        Retrieve and load NADP (National Atmospheric Deposition Program) data.

        Parameters
        ----------
        files : str or list of str, optional
            File paths or URLs. If None, uses `network`, `siteid`, and `weekly` to build URL.
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
        dates : datetime, list of datetime, or pd.DatetimeIndex, optional
            Dates to filter data.
        network : str, optional
            NADP network (NTN, MDN, AMON, AIRMON, AMNET), by default "NTN".
        siteid : str, optional
            Specific site ID to retrieve.
        weekly : bool, optional
            Whether to retrieve weekly data (if applicable), by default True.
        as_xarray : bool, optional
            If True, returns an xarray.Dataset, by default True.
        lazy : bool, optional
            If True, returns a dask-backed object, by default False.
        **kwargs : Any
            Additional arguments passed to the reader and driver.

        Returns
        -------
        xr.Dataset, pd.DataFrame, or dd.DataFrame
            The loaded dataset.

        Examples
        --------
        >>> reader = NADPReader()
        >>> ds = reader.open_dataset(network="NTN", siteid="TX01")
        """
        if files is None:
            files = self.build_url(network=network, siteid=siteid, weekly=weekly)

        # We use read_nadp as the custom read_method
        def _reader(f: str, **inner_kwargs: Any) -> pd.DataFrame:
            return read_nadp(f, network=network, **inner_kwargs)

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
            read_method=_reader,
            lazy=lazy,
            **kwargs,
        )

        # Filter by dates if provided
        if dates is not None:
            dates = pd.DatetimeIndex(np.atleast_1d(dates))
            df = df.loc[(df.time >= dates.min()) & (df.time_off <= dates.max())]

        # Post-processing: Merge with monitor info
        df = self._postprocess(df, network=network)

        # Consistently force object strings
        df = force_object_strings(df)

        if as_xarray:
            ds = self.to_xarray(df, **kwargs)
            return ds

        return df

    def _postprocess(
        self, df: pd.DataFrame | dd.DataFrame, network: str
    ) -> pd.DataFrame | dd.DataFrame:
        """
        Merge with station metadata and harmonize column names.

        Parameters
        ----------
        df : pd.DataFrame or dd.DataFrame
            Input dataframe.
        network : str
            NADP network name.

        Returns
        -------
        pd.DataFrame or dd.DataFrame
            Post-processed dataframe.

        Examples
        --------
        >>> reader = NADPReader()
        >>> df = reader._postprocess(df, network="ntn")
        """
        try:
            import dask.dataframe as dd

            is_dask = isinstance(df, dd.DataFrame)
        except ImportError:
            is_dask = False

        meta = self.get_monitor_df(network=network)

        if is_dask:
            meta = dd.from_pandas(meta, npartitions=1)

        # Ensure siteid is consistent for merge
        df = force_object_strings(df)
        meta = force_object_strings(meta)

        # Merge (unified logic for both backends)
        df = df.merge(meta, on="siteid", how="left")

        # Original code dropped NaNs in latitude/longitude in NADP.read_*
        # PointReader.harmonize also does this.
        df = self.harmonize(df)

        # Update history
        df = update_history(df, f"Merged with NADP ({network}) station metadata.")

        return df

    @functools.lru_cache(maxsize=8)
    def get_monitor_df(self, network: str) -> pd.DataFrame:
        """
        Load the NADP station metadata for a specific network.

        Parameters
        ----------
        network : str
            NADP network name.

        Returns
        -------
        pd.DataFrame
            Station metadata.

        Examples
        --------
        >>> reader = NADPReader()
        >>> meta = reader.get_monitor_df(network="ntn")
        """
        network = network.lower()
        url = META_URLS.get(network)
        if url is None:
            return pd.DataFrame(columns=["siteid", "latitude", "longitude"])

        try:
            meta = pd.read_csv(url)
        except Exception:
            return pd.DataFrame(columns=["siteid", "latitude", "longitude"])

        meta.columns = [i.lower() for i in meta.columns]
        if "startdate" in meta.columns:
            meta = meta.drop(["startdate", "stopdate"], axis=1, errors="ignore")

        return meta

    def build_url(
        self, network: str = "NTN", siteid: str | None = None, weekly: bool = True
    ) -> str:
        """
        Build URL for NADP data.

        Parameters
        ----------
        network : str, optional
            NADP network (NTN, MDN, AMON, AIRMON, AMNET), by default "NTN".
        siteid : str, optional
            Specific site ID to retrieve.
        weekly : bool, optional
            Whether to retrieve weekly data, by default True.

        Returns
        -------
        str
            The URL to the data file.

        Examples
        --------
        >>> reader = NADPReader()
        >>> url = reader.build_url(network="NTN", siteid="TX01")
        """
        baseurl = "http://nadp.slh.wisc.edu/datalib/"
        site_part = (siteid.upper() + "-") if siteid is not None else ""
        network = network.lower()

        if network == "amnet":
            return "http://nadp.slh.wisc.edu/datalib/AMNet/AMNet-All.zip"
        elif network == "amon":
            return "http://nadp.slh.wisc.edu/dataLib/AMoN/csv/all-ave.csv"
        elif network == "airmon":
            return "http://nadp.slh.wisc.edu/datalib/AIRMoN/AIRMoN-ALL.csv"
        else:
            if weekly:
                return f"{baseurl}{network}/weekly/{site_part}{network.upper()}-All-w.csv"
            else:
                return f"{baseurl}{network}/annual/{site_part}{network.upper()}-All-a.csv"
