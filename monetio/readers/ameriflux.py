"""
AmeriFlux / FLUXNET Reader.
"""

import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history


def _prepare_ameriflux_frame(df):
    """Add the timestamp column without evaluating a lazy DataFrame."""
    if "time" in df.columns:
        return df

    try:
        import dask.dataframe as dd

        is_dask = isinstance(df, dd.DataFrame)
    except ImportError:
        is_dask = False

    if "TIMESTAMP_START" in df.columns:
        if is_dask:
            df["time"] = df["TIMESTAMP_START"].map_partitions(
                pd.to_datetime,
                format="%Y%m%d%H%M",
                meta=("time", "datetime64[ns]"),
            )
        else:
            df["time"] = pd.to_datetime(df["TIMESTAMP_START"], format="%Y%m%d%H%M")
    elif "TIMESTAMP" in df.columns:
        ts_str = df["TIMESTAMP"].astype(str)
        if is_dask:
            df["time"] = ts_str.map_partitions(pd.to_datetime, meta=("time", "datetime64[ns]"))
        elif len(ts_str.iloc[0]) == 4:
            df["time"] = pd.to_datetime(ts_str, format="%Y")
        elif len(ts_str.iloc[0]) == 6:
            df["time"] = pd.to_datetime(ts_str, format="%Y%m")
        else:
            df["time"] = pd.to_datetime(ts_str)
    return df


@register_reader("ameriflux")
class AmeriFluxReader(PointReader):
    """
    Reader for AmeriFlux BASE data (CSV).
    """

    def open_dataset(
        self,
        files: str | list[str],
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        expand2d: bool = False,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads AmeriFlux data.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s) or URL(s).
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
        expand2d : bool, optional
            Whether to expand to a 2D (time, node) layout, by default False.
            AmeriFlux BASE files carry no site identity — station coordinates
            live in the separate BADM metadata files — so there is no ``siteid``
            axis to expand along and the result stays 1D in ``time``.
        **kwargs : dict
            Additional arguments passed to PointReader.open_dataset.

        Returns
        -------
        xr.Dataset
            The AmeriFlux dataset.
        """
        # AmeriFlux BASE files are CSV with a header and -9999 as missing value.
        kwargs.setdefault("na_values", -9999)
        kwargs.setdefault("skiprows", 0)  # Adjust if there are comment lines

        # Dask's from_delayed otherwise reads a full first partition to infer
        # metadata. A single row is sufficient for this CSV parser and keeps
        # open_dataset bounded while preserving its raw, pre-harmonization
        # column schema.
        meta = kwargs.pop("meta", None)
        if use_dask or kwargs.get("lazy", False) or kwargs.get("defer", False):
            # Perform timestamp normalization inside each delayed pandas
            # partition. Supplying its post-preprocess meta keeps Dask from
            # evaluating the dataframe to infer the added time column.
            kwargs.setdefault("preprocess", _prepare_ameriflux_frame)
            if meta is None:
                paths = FileUtility.expand_paths(files)
                if paths:
                    probe_kwargs = {
                        key: value
                        for key, value in kwargs.items()
                        if key
                        not in {"as_xarray", "lazy", "defer", "read_method", "meta", "preprocess"}
                    }
                    sample = pd.read_csv(paths[0], nrows=1, **probe_kwargs)
                    meta = _prepare_ameriflux_frame(sample).iloc[:0]

        # Use PointReader's open_dataset which handles CSV and to_xarray
        return super().open_dataset(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser=virtualizarr_parser,
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            expand2d=expand2d,
            meta=meta,
            **kwargs,
        )

    def harmonize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmonize AmeriFlux DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            Input dataframe.

        Returns
        -------
        pd.DataFrame
            Harmonized dataframe.
        """
        # Partition-local conversion keeps both eager and Dask results in the
        # same scientific representation; lazy input is never passed through
        # pandas coercion.
        df = _prepare_ameriflux_frame(df)

        # 2. Rename coordinates if present (often in a separate metadata file,
        # but sometimes included or can be passed)
        # Note: BASE files themselves often DON'T have Lat/Lon in every row.
        # They are usually in the BADM (metadata) files.

        df = update_history(df, "Harmonized AmeriFlux data.")

        return super().harmonize(df)
