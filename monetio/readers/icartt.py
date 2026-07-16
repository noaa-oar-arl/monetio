"""ICARTT Reader."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd


def parse_icartt_header(filename: str) -> dict[str, Any]:
    """
    Parse ICARTT file header metadata.

    Parameters
    ----------
    filename : str
        Path to the ICARTT file.

    Returns
    -------
    dict[str, Any]
        Dictionary containing header metadata.

    Examples
    --------
    >>> header = parse_icartt_header("test.ict")
    >>> print(header["PI"])
    """
    fs = FileUtility.get_fs(filename)
    header = {}
    with fs.open(filename, "r") as f:
        line1 = f.readline()
        if not line1:
            return {}
        try:
            n_header = int(line1.split(",")[0])
        except (ValueError, IndexError):
            return {}

        header["n_header"] = n_header
        header["PI"] = f.readline().strip()
        header["organization"] = f.readline().strip()
        header["source"] = f.readline().strip()
        header["mission"] = f.readline().strip()
        f.readline()  # Line 6: volume

        # Line 7: Dates (index 6)
        date_line = f.readline().split(",")
        try:
            header["date_valid"] = datetime.datetime(
                int(date_line[0]), int(date_line[1]), int(date_line[2])
            )
        except (ValueError, IndexError):
            header["date_valid"] = datetime.datetime(1970, 1, 1)

        f.readline()  # Line 8: interval

        # Line 9: IVAR name and units
        ivar_line = f.readline().split(",")
        header["ivar_name"] = ivar_line[0].strip()
        header["ivar_units"] = ivar_line[1].strip() if len(ivar_line) > 1 else ""

        # Line 10: Number of DVARs
        try:
            n_vars = int(f.readline().strip())
        except ValueError:
            n_vars = 0
        header["n_vars"] = n_vars

        # Line 11: Scales
        header["scales"] = [float(x) for x in f.readline().split(",")]

        # Line 12: Missing values
        header["missing_values"] = [x.strip() for x in f.readline().split(",")]

        # Line 13+: DVAR names
        var_names = [header["ivar_name"]]
        for i in range(n_vars):
            vname = f.readline().split(",")[0].strip()
            var_names.append(vname)
        header["var_names"] = var_names

    return header


def read_icartt(filename: str, **kwargs: Any) -> pd.DataFrame:
    """
    Reads a single ICARTT file into a pandas DataFrame.
    Data is returned unscaled and with raw missing values to allow lazy processing.

    Parameters
    ----------
    filename : str
        Path to the ICARTT file.
    **kwargs : dict
        Additional arguments.

    Returns
    -------
    pandas.DataFrame
        Data from the ICARTT file.

    Examples
    --------
    >>> df = read_icartt("test.ict")
    """
    header = parse_icartt_header(filename)
    if not header:
        return pd.DataFrame()

    df = pd.read_csv(
        filename,
        skiprows=header["n_header"],
        names=header["var_names"],
        sep=",",
        skipinitialspace=True,
    )

    # Convert IVAR to time if possible
    if "time" in header["ivar_name"].lower() or "sec" in header["ivar_units"].lower():
        df["time"] = header["date_valid"] + pd.to_timedelta(df[header["ivar_name"]], unit="s")

    return df


@register_reader("icartt")
class ICARTTReader(PointReader):
    """
    ICARTT Data Reader following standard conventions.
    """

    fixed_location = False

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
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs: Any,
    ) -> xr.Dataset | pd.DataFrame | dd.DataFrame:
        """
        Retrieve and load ICARTT data with lazy scaling and missing value handling.

        Parameters
        ----------
        files : str or list of str
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
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the reader and driver.

        Returns
        -------
        xarray.Dataset or pandas.DataFrame or dask.dataframe.DataFrame
            The loaded ICARTT data.

        Examples
        --------
        >>> reader = ICARTTReader()
        >>> ds = reader.open_dataset("*.ict", lazy=True)
        """
        # We need metadata from the first file to setup lazy processing
        file_list = FileUtility.expand_paths(files)
        if not file_list:
            raise FileNotFoundError(f"No files found matching {files}")

        header = parse_icartt_header(file_list[0])

        # Open data
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
            read_method=read_icartt,
            lazy=lazy,
            **kwargs,
        )

        if lazy:
            # For Dask, we apply scaling and missing values via map_partitions
            # or we do it after converting to Xarray (preferable for backend-agnostic lazy processing)
            pass

        df = self.harmonize(df)

        if as_xarray:
            # Default to expand2d=False for ICARTT to keep it simple and match expected results
            # if not explicitly requested otherwise.
            if "expand2d" not in kwargs:
                kwargs["expand2d"] = False

            ds = self.to_xarray(df, **kwargs)

            # Apply scaling and missing values lazily in Xarray
            ds = icartt_preprocess(ds, header)

            # Add global metadata
            ds.attrs.update(
                {
                    k: v
                    for k, v in header.items()
                    if k in ["PI", "organization", "source", "mission"]
                }
            )

            ds = update_history(ds, "Read ICARTT data using standardized preprocessing.")
            return ds

        return df

    def harmonize(self, df: pd.DataFrame | dd.DataFrame) -> pd.DataFrame | dd.DataFrame:
        """
        Standardize coordinate column names.

        Parameters
        ----------
        df : pandas.DataFrame or dask.dataframe.DataFrame
            Input dataframe.

        Returns
        -------
        pandas.DataFrame or dask.dataframe.DataFrame
            Harmonized dataframe.
        """
        rename_dict = {}
        for col in df.columns:
            lcol = col.lower()
            if "latitude" in lcol and col != "latitude":
                rename_dict[col] = "latitude"
            if "longitude" in lcol and col != "longitude":
                rename_dict[col] = "longitude"
            if "altitude" in lcol and col != "altitude":
                rename_dict[col] = "altitude"
            if "siteid" in lcol and col != "siteid":
                rename_dict[col] = "siteid"

        if rename_dict:
            df = df.rename(columns=rename_dict)

        return super().harmonize(df)


def icartt_preprocess(ds: xr.Dataset, header: dict[str, Any]) -> xr.Dataset:
    """
    Apply scaling and missing value handling lazily to ICARTT dataset.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.
    header : dict[str, Any]
        Header metadata including scales and missing values.

    Returns
    -------
    xarray.Dataset
        Processed dataset with scaling and missing values applied.

    Examples
    --------
    >>> header = parse_icartt_header("file.ict")
    >>> ds = icartt_preprocess(ds, header)
    """
    scales = header.get("scales", [])
    missing_values = header.get("missing_values", [])
    var_names = header.get("var_names", [])

    # Create a mapping of original names to current dataset names (handles harmonization)
    col_map = {}
    for orig_name in var_names:
        if orig_name in ds.variables:
            col_map[orig_name] = orig_name
        else:
            for c in ds.variables:
                if c.lower() == orig_name.lower():
                    col_map[orig_name] = c
                    break

    # We use a dictionary to collect updates and apply them in one go
    updates = {}

    # The ICARTT spec says scales/missing are for [DVAR1, DVAR2, ...]
    # IVAR (index 0 in var_names) is NOT scaled or masked in the same way.
    for i, (scale, miss) in enumerate(zip(scales, missing_values)):
        if i + 1 >= len(var_names):
            break

        orig_col = var_names[i + 1]
        col = col_map.get(orig_col)

        if col is None:
            continue

        da = ds[col]

        # 1. Handle Missing Values
        try:
            # Numeric missing value
            miss_val = float(miss)
            # We cast to float to support NaNs after masking
            da = da.astype(float)
            # Backend-agnostic lazy masking
            mask = np.abs(da - miss_val) < 1e-5
            da = da.where(~mask)
        except (ValueError, TypeError):
            # String/categorical missing value
            # Avoid .str accessor on DataArray
            mask = xr.apply_ufunc(
                lambda x: np.char.strip(x.astype(str)) == str(miss).strip(),
                da,
                dask="parallelized",
                output_dtypes=[bool],
            )
            da = da.where(~mask).astype(float)

        # 2. Apply Scaling
        if scale != 1.0:
            # If not already float, cast it
            if not np.issubdtype(da.dtype, np.floating):
                da = da.astype(float)
            da = da * scale

        updates[col] = da

    # If IVAR was scaled and time was already created, we may need to adjust time.
    # However, conventionally IVAR (time) scale is 1.0.
    # If a reader needs specific IVAR handling, it should be done here.

    ds = ds.assign(updates)

    # If we adjusted IVAR and it is 'time' or used for 'time', and 'time' is already a coord,
    # it might be tricky. But since ICARTTReader.open_dataset calls this AFTER to_xarray,
    # 'time' is already a coordinate if it was in the dataframe.

    ds = update_history(ds, "Applied ICARTT scaling and missing value masking.")

    return ds
