"""IMPROVE Reader"""

from functools import partial
from typing import Any, Union

import pandas as pd
import xarray as xr

from ..util import force_object_strings
from .base import PointReader, register_reader
from .epa_utils import add_monitor_metadata
from .sat_utils import update_history

try:
    import dask.dataframe as dd
except ImportError:
    dd = None


@register_reader("improve")
class IMPROVEReader(PointReader):
    """
    Reader for IMPROVE (Interagency Monitoring of Protected Visual Environments) data.
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
        add_meta: bool = False,
        delimiter: str = "\t",
        as_xarray: bool = True,
        lazy: bool = False,
        pivot: bool = True,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load IMPROVE data.

        Parameters
        ----------
        files : Union[str, List[str]]
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
        add_meta : bool, optional
            Whether to add site metadata, by default False.
        delimiter : str, optional
            Delimiter used in the IMPROVE file, by default "\\t".
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        pivot : bool, optional
            Whether to pivot the data to wide format, by default True.
        **kwargs : Any
            Additional arguments passed to the reader and driver.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded IMPROVE data.

        Examples
        --------
        >>> reader = IMPROVEReader()
        >>> ds = reader.open_dataset("improve_data.txt")
        """
        # Use PandasDriver via base class
        read_func = partial(read_improve_file, delimiter=delimiter)

        driver_kwargs = kwargs.copy()
        for k in ["expand2d", "pivot", "add_meta", "as_xarray", "lazy"]:
            driver_kwargs.pop(k, None)

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
            **driver_kwargs,
        )

        # Check for empty (Backend-agnostic)
        if dd is not None and isinstance(df, dd.DataFrame):
            is_empty = df.npartitions == 0
        else:
            is_empty = df.empty

        if is_empty:
            if as_xarray:
                return xr.Dataset()
            return df

        if add_meta:
            df = self.add_metadata(df)

        # Re-harmonize to drop sites without metadata (NaN lat/lon)
        df = self.harmonize(df)

        if as_xarray:
            ds = self.to_xarray(df, pivot=pivot, **kwargs)
            # Update history
            ds = update_history(ds, "Read IMPROVE data.")
            return ds

        return df

    def add_metadata(
        self, df: Union[pd.DataFrame, "dd.DataFrame"]
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Add site metadata from the IMPROVE monitor file.

        Parameters
        ----------
        df : Union[pd.DataFrame, dd.DataFrame]
            Input dataframe.

        Returns
        -------
        Union[pd.DataFrame, dd.DataFrame]
            Dataframe with metadata merged.

        Examples
        --------
        >>> df = reader.add_metadata(df)
        """
        df = add_monitor_metadata(
            df,
            network="IMPROVE",
            left_on="epaid",
            history_msg="Merged with IMPROVE station metadata.",
        )

        # Handle IMPROVE-specific column name cleanup
        if "state_name_y" in df.columns:
            df = df.drop(columns=["state_name_y"], errors="ignore").rename(
                columns={"state_name_x": "state_name"}
            )

        return df


def read_improve_file(fname: str, delimiter: str = "\t", **kwargs: Any) -> pd.DataFrame:
    """
    Read a single IMPROVE data file.

    Parameters
    ----------
    fname : str
        File path or URL.
    delimiter : str, optional
        Delimiter used in the file, by default "\\t".
    **kwargs : Any
        Additional arguments passed to pd.read_csv.

    Returns
    -------
    pd.DataFrame
        The loaded data.

    Examples
    --------
    >>> df = read_improve_file("site_data.txt")
    """
    # Determine storage options if S3
    storage_options = kwargs.get("storage_options")
    if fname.startswith("s3://") and storage_options is None:
        from .drivers import get_default_storage_options

        storage_options = get_default_storage_options(fname)

    # Find the data section
    skiprows = 0
    try:
        import fsspec

        with fsspec.open(fname, "r", **(storage_options or {})) as f:
            for i, line in enumerate(f):
                if line.strip() == "Data":
                    skiprows = i + 1
                    break
    except Exception:
        pass

    # Read the CSV
    read_kwargs = kwargs.copy()
    for k in ["pivot", "add_meta", "as_xarray", "lazy", "expand2d", "storage_options"]:
        read_kwargs.pop(k, None)

    df = pd.read_csv(
        fname,
        delimiter=delimiter,
        parse_dates=["Date"],
        dtype={"EPACode": str},
        skiprows=skiprows,
        storage_options=storage_options,
        **read_kwargs,
    )

    # Standardize columns
    df = df.rename(
        columns={
            "EPACode": "epaid",
            "Val": "obs",
            "State": "state_name",
            "ParamCode": "variable",
            "SiteCode": "siteid",
            "Unit": "units",
            "Date": "time",
        }
    )

    if "Dataset" in df.columns:
        df = df.drop(columns="Dataset")

    df.columns = [i.lower() for i in df.columns]

    if "epaid" in df.columns:
        df["epaid"] = df["epaid"].astype(str).str.zfill(9)

    return force_object_strings(df)
