"""ISH Lite Reader"""

import os
from datetime import datetime
from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    import dask.dataframe as dd

from ..util import normalize_pandas_freq
from .base import PointReader, register_reader
from .drivers import FileUtility
from .ish import ISH, add_ish_metadata
from .sat_utils import update_history


def read_ish_lite_file(fname: str, **kwargs) -> pd.DataFrame:
    """
    Read a single ISH (Integrated Surface Hourly) Lite file.

    Parameters
    ----------
    fname : str
        File path, URL, or fsspec-compatible path.
    **kwargs : dict
        Additional arguments passed to fsspec.open.

    Returns
    -------
    pd.DataFrame
        The loaded data in long format.

    Examples
    --------
    >>> df = read_ish_lite_file('012345-67890-2023.gz')
    """
    columns = [
        "year",
        "month",
        "day",
        "hour",
        "temp",
        "dew_pt_temp",
        "press",
        "wdir",
        "ws",
        "sky_condition",
        "precip_1hr",
        "precip_6hr",
    ]

    # Use FileUtility
    fs = FileUtility.get_fs(fname)
    compression = "gzip" if str(fname).endswith(".gz") else None
    storage_options = kwargs.get("storage_options", {})

    with fs.open(fname, "rb", compression=compression, **storage_options) as f:
        df = pd.read_csv(
            f,
            sep=r"\s+",
            header=None,
            names=columns,
        )

    if df.empty:
        return df

    # Vectorized time construction
    df["time"] = pd.to_datetime(df[["year", "month", "day", "hour"]])
    df = df.drop(columns=["year", "month", "day", "hour"])

    # Extract siteid from filename (USAF + WBAN)
    filename = os.path.basename(fname).split("-")
    if len(filename) >= 2:
        siteid = filename[0] + filename[1]
    else:
        siteid = "unknown"

    # Scale values
    scale_cols = ["temp", "dew_pt_temp", "press", "ws", "precip_1hr", "precip_6hr"]
    for col in scale_cols:
        if col in df.columns:
            df[col] = df[col] / 10.0

    df["siteid"] = siteid

    # Clean missing values
    df = df.replace(-999.9, np.nan).replace(-9999, np.nan)

    return df


@register_reader("ish_lite")
class ISHLiteReader(PointReader):
    """
    Reader for ISH (Integrated Surface Hourly) Lite data.
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
        dates: pd.DatetimeIndex | list[datetime] | datetime | str | None = None,
        box: list[float] | None = None,
        country: str | None = None,
        state: str | None = None,
        site: str | None = None,
        resample: bool = False,
        window: str = "h",
        n_procs: int = 1,
        verbose: bool = False,
        source: str | None = None,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load ISH (Integrated Surface Hourly) Lite data.

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
            Whether to resample data to a regular window, by default False.
        window : str, optional
            Resampling window (e.g., 'h'), by default 'h'.
        n_procs : int, optional
            Number of processors (deprecated, handled by dask), by default 1.
        verbose : bool, optional
            Whether to print verbose output, by default False.
        source : str, optional
            Data source: 'ncdc' or 'aws', by default 'aws'.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object (dask.dataframe or xarray with dask),
            by default False.
        **kwargs : dict
            Additional arguments passed to the driver or to_xarray.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded ISH Lite data.

        Examples
        --------
        >>> from monetio.readers.ish_lite import ISHLiteReader
        >>> reader = ISHLiteReader()
        >>> ds = reader.open_dataset(dates='2021-08-01', site='72406093721')
        """
        ish = ISH()
        if source is not None:
            ish.source = source

        if files is None and dates is not None:
            dates = pd.to_datetime(dates)
            # Ensure dates is always a DatetimeIndex, not a single Timestamp
            if not hasattr(dates, "__len__"):
                dates = pd.DatetimeIndex([dates])
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

            urls = ish.build_urls(dates=dates, sites=dfloc_urls, lite=True)
            if urls.empty:
                raise ValueError("No data URLs found")
            files = urls.name.tolist()

        if not files:
            raise ValueError("Must provide either 'files' or 'dates'.")

        # Use driver directly
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
            read_method=read_ish_lite_file,
            lazy=lazy,
            **kwargs,
        )

        # Filtering by date if requested
        if dates is not None:
            dates = pd.to_datetime(dates)
            # Use exclusive upper bound to match unit test expectations in legacy
            # For single-date queries, include the full day (min == max otherwise)
            _end = dates.max() + pd.Timedelta(days=1) if len(dates) == 1 else dates.max()
            df = df.loc[(df.time >= dates.min()) & (df.time < _end)]

        # Merge with metadata
        if ish.history is None:
            ish.read_ish_history()

        df = add_ish_metadata(df, ish.history)

        df = self.harmonize(df)

        # Rename temperature to standard name for model-obs pairing
        if "temp" in df.columns:
            df = df.rename(columns={"temp": "t2m"})

        if as_xarray:
            from ..util import ds_to_2d

            # We first convert to 1D UGRID
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
                            if val.sizes["time"] == 0:
                                continue
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
                ds = update_history(ds, f"Resampled ISH Lite data to {window} window.")

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
            ds = update_history(ds, "Read ISH Lite data.")
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
                    "ISHLiteReader: Resampling is currently not supported for lazy DataFrames. "
                    "Convert to xarray (as_xarray=True) for lazy resampling."
                )

        return df


def add_data(
    dates: pd.DatetimeIndex | list[datetime] | datetime | str,
    box: list[float] | None = None,
    country: str | None = None,
    state: str | None = None,
    site: str | None = None,
    resample: bool = False,
    window: str = "h",
    n_procs: int = 1,
    verbose: bool = False,
    source: str | None = None,
    **kwargs,
) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
    """
    Retrieve and load ISH Lite data (backward-compatible wrapper).

    Parameters
    ----------
    dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
        Dates to retrieve.
    box : List[float], optional
        Bounding box [latmin, lonmin, latmax, lonmax].
    country : str, optional
        Country code.
    state : str, optional
        State code.
    site : str, optional
        Station ID.
    resample : bool, optional
        Whether to resample, by default False.
    window : str, optional
        Resampling window, by default 'h'.
    n_procs : int, optional
        Number of processors, by default 1.
    verbose : bool, optional
        Verbose output, by default False.
    source : str, optional
        Data source: 'ncdc' or 'aws', by default 'aws'.
    **kwargs : dict
        Additional arguments.

    Returns
    -------
    Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
        The loaded ISH Lite data.

    Examples
    --------
    >>> from monetio.readers.ish_lite import add_data
    >>> ds = add_data(dates='2021-08-01', site='72406093721')
    """
    return ISHLiteReader().open_dataset(
        dates=dates,
        box=box,
        country=country,
        state=state,
        site=site,
        resample=resample,
        window=window,
        n_procs=n_procs,
        verbose=verbose,
        source=source,
        **kwargs,
    )
