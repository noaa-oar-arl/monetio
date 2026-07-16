"""SKYNET Sun Photometer Reader."""

import warnings
from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
import xarray as xr

from ..util import force_object_strings
from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd


@register_reader("skynet")
class SKYNETReader(PointReader):
    """
    Reader for SKYNET sun photometer data.

    This reader supports retrieving and loading SKYNET data from local or remote files,
    standardizing it into a common format, and optionally converting it to an
    xarray Dataset.
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
        siteid: str | None = None,
        product: str = "AOT",
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs: dict,
    ) -> pd.DataFrame | xr.Dataset:
        """
        Retrieve and load SKYNET data.

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
        siteid : str, optional
            Specific SKYNET site ID.
        product : str, optional
            SKYNET product (e.g., 'AOT', 'SSA'), by default "AOT".
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the driver.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset]
            The loaded SKYNET data.

        Examples
        --------
        >>> reader = SKYNETReader()
        >>> ds = reader.open_dataset(siteid="POC", dates="2023-01-01")
        """
        if files is None:
            if dates is None:
                raise ValueError("Must provide either 'files' or 'dates'.")
            files = self.build_urls(dates, siteid=siteid, product=product, **kwargs)

        if not files:
            if as_xarray:
                return xr.Dataset()
            return pd.DataFrame()

        # Define per-file preprocessing
        read_func = read_skynet_csv

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

        if as_xarray:
            ds = self.to_xarray(df, **kwargs)
            ds = update_history(ds, "Read SKYNET data.")
            return ds

        return df

    def harmonize(
        self, df: Union[pd.DataFrame, "dd.DataFrame"]
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Standardize column names and types.

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

        # Force string columns to object for Pandas 3.0 compatibility
        df = force_object_strings(df)
        return df

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime] | datetime | str,
        siteid: str | None = None,
        product: str = "AOT",
        **kwargs: dict,
    ) -> list[str]:
        """
        Construct SKYNET URLs.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        siteid : str, optional
            SKYNET site ID.
        product : str, optional
            SKYNET product, by default "AOT".
        **kwargs : dict
            Additional arguments.

        Returns
        -------
        List[str]
            List of constructed URLs.

        Examples
        --------
        >>> urls = reader.build_urls("2023-01-01", siteid="POC")
        """
        dates = pd.DatetimeIndex(np.atleast_1d(pd.to_datetime(dates)))
        if dates.empty or siteid is None:
            return []

        # Placeholder for SKYNET ISDC URL structure
        # Example: https://www.skynet-isdc.org/data/L2/AOT/SITE/YYYY/SITE_YYYYMMDD.AOT
        base_url = "https://www.skynet-isdc.org/data/L2"
        urls = []
        for date in dates.normalize().unique():
            fname = f"{siteid}_{date.strftime('%Y%m%d')}.{product.upper()}"
            url = f"{base_url}/{product.upper()}/{siteid}/{date.year}/{fname}"
            urls.append(url)

        return urls


def read_skynet_csv(fn: str, **kwargs: dict) -> pd.DataFrame:
    """
    Read a single SKYNET ASCII file.

    Parameters
    ----------
    fn : str
        Path to the SKYNET file.
    **kwargs : dict
        Additional arguments.

    Returns
    -------
    pd.DataFrame
        Data from the SKYNET file.

    Examples
    --------
    >>> df = read_skynet_csv("site_20230101.AOT")
    """
    fs = FileUtility.get_fs(fn)
    try:
        with fs.open(fn, mode="rb") as f:
            content = f.read()
            if isinstance(content, bytes):
                content = content.decode("utf-8", errors="ignore")
    except Exception as e:
        warnings.warn(f"Failed to read {fn}: {e}")
        return pd.DataFrame()

    lines = content.splitlines()
    if not lines:
        return pd.DataFrame()

    # Generic SKYNET ASCII parsing logic
    # Assume metadata in header lines starting with '#' or some keyword
    # and a CSV-like structure for the rest.
    metadata = {}
    data_start = 0
    for i, line in enumerate(lines):
        if line.startswith("#"):
            # Try to parse key: value
            if ":" in line:
                parts = line[1:].split(":", 1)
                metadata[parts[0].strip().lower()] = parts[1].strip()
            data_start = i + 1
        elif not line.strip():
            data_start = i + 1
            continue
        else:
            # First non-empty, non-comment line is assumed to be the header or data
            data_start = i
            break

    try:
        df = pd.read_csv(
            BytesIO(content.encode("utf-8")),
            skiprows=data_start,
            sep=r"\s+|,",
            engine="python",
            na_values=["-999", "-999.9", "-9.999"],
        )
    except Exception as e:
        warnings.warn(f"Error parsing SKYNET file {fn}: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    df.columns = [c.lower() for c in df.columns]

    # Handle Time
    # Common SKYNET formats might have 'date' and 'time' or 'year', 'month', 'day', 'hour' etc.
    if "date" in df.columns and "time" in df.columns:
        df["time"] = pd.to_datetime(df["date"] + " " + df["time"], errors="coerce")
    elif all(c in df.columns for c in ["year", "month", "day", "hour", "minute"]):
        df["time"] = pd.to_datetime(df[["year", "month", "day", "hour", "minute"]], errors="coerce")

    # Standard names for coordinates and variables
    rename_dict = {
        "lat": "latitude",
        "lon": "longitude",
        "alt": "elevation",
        "site": "siteid",
        "aot": "aerosol_optical_thickness",
        "ssa": "single_scattering_albedo",
        "ae": "angstrom_exponent",
        "ri": "refractive_index",
    }
    # For AOT at specific wavelengths, we want to map them to 'aod_XXXnm'
    for col in df.columns:
        if col.startswith("aot_"):
            rename_dict[col] = col.replace("aot_", "aod_")
        elif col.endswith("aot"):
            # Some files might have 500aot
            import re

            match = re.match(r"(\d+)aot", col)
            if match:
                rename_dict[col] = f"aod_{match.group(1)}nm"

    df = df.rename(columns=rename_dict)

    # If metadata contains location, add it if not in DF
    if "latitude" not in df.columns:
        for k in ["latitude", "lat"]:
            if k in metadata:
                df["latitude"] = float(metadata[k])
                break
    if "longitude" not in df.columns:
        for k in ["longitude", "lon"]:
            if k in metadata:
                df["longitude"] = float(metadata[k])
                break
    if "elevation" not in df.columns:
        for k in ["elevation", "alt", "altitude"]:
            if k in metadata:
                df["elevation"] = float(metadata[k])
                break
    if "siteid" not in df.columns:
        for k in ["siteid", "site"]:
            if k in metadata:
                df["siteid"] = metadata[k]
                break

    return df
