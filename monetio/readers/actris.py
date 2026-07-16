"""ACTRIS/EBAS Reader."""

from __future__ import annotations

import datetime
import functools
import re
import warnings
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd


@functools.lru_cache(maxsize=1)
def get_ebas_catalog() -> str:
    """
    Fetch and cache the EBAS THREDDS catalog XML.

    Returns
    -------
    str
        The catalog XML content.
    """
    catalog_url = "https://thredds.nilu.no/thredds/catalog/ebas/catalog.xml"
    try:
        import requests

        response = requests.get(catalog_url, timeout=60)
        response.raise_for_status()
        return response.text
    except Exception as e:
        warnings.warn(f"Failed to fetch EBAS catalog: {e}")
        return ""


def parse_ebas_header(filename: str) -> dict[str, Any]:
    """
    Parse EBAS NASA-Ames 1001 file header metadata.

    Parameters
    ----------
    filename : str
        Path to the EBAS file.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing header metadata.
    """
    fs = FileUtility.get_fs(filename)
    header = {}
    with fs.open(filename, "r") as f:
        line1 = f.readline().split()
        if not line1:
            return {}
        try:
            n_header = int(line1[0])
            format_code = int(line1[1])
        except (ValueError, IndexError):
            return {}

        if format_code != 1001:
            # We only support 1001 for now
            return {}

        header["n_header"] = n_header
        header["PI"] = f.readline().strip()
        header["organization"] = f.readline().strip()
        header["submitter"] = f.readline().strip()
        header["project"] = f.readline().strip()
        header["volume"] = f.readline().strip()

        # Dates
        date_line = f.readline().split()
        try:
            header["date_valid"] = datetime.datetime(
                int(date_line[0]), int(date_line[1]), int(date_line[2])
            )
        except (ValueError, IndexError):
            header["date_valid"] = datetime.datetime(1970, 1, 1)

        header["dt"] = f.readline().strip()
        header["ivar_name"] = f.readline().strip()

        # Number of DVARs
        try:
            n_vars = int(f.readline().strip())
        except ValueError:
            n_vars = 0
        header["n_vars"] = n_vars

        # Scales
        header["scales"] = [float(x) for x in f.readline().split()]

        # Missing values
        header["missing_values"] = [float(x) for x in f.readline().split()]

        # DVAR names and units
        var_names = [header["ivar_name"]]
        for _ in range(n_vars):
            vname = f.readline().strip()
            var_names.append(vname)
        header["var_names"] = var_names

        # The rest are metadata lines (e.g. Station latitude, etc.)
        metadata = []
        current_line = 13 + n_vars
        while current_line <= n_header:
            line = f.readline().strip()
            metadata.append(line)
            current_line += 1

        header["metadata"] = metadata

        # Try to extract site metadata from EBAS-style metadata lines
        for line in metadata:
            if "Station latitude:" in line:
                try:
                    header["latitude"] = float(line.split(":")[1].split()[0])
                except (ValueError, IndexError):
                    pass
            elif "Station longitude:" in line:
                try:
                    header["longitude"] = float(line.split(":")[1].split()[0])
                except (ValueError, IndexError):
                    pass
            elif "Station altitude:" in line:
                try:
                    header["elevation"] = float(line.split(":")[1].split()[0])
                except (ValueError, IndexError):
                    pass
            elif "Station name:" in line:
                header["siteid"] = line.split(":")[1].strip()
            elif "Station code:" in line:
                header["site_code"] = line.split(":")[1].strip()
                if "siteid" not in header:
                    header["siteid"] = header["site_code"]

    return header


def read_actris(filename: str, **kwargs) -> pd.DataFrame:
    """
    Reads a single ACTRIS/EBAS NASA-Ames 1001 file into a pandas DataFrame.
    Data is returned unscaled and with raw missing values to allow lazy processing.

    Parameters
    ----------
    filename : str
        Path to the file.
    **kwargs : dict
        Additional arguments.

    Returns
    -------
    pd.DataFrame
        Data from the file.
    """
    header = parse_ebas_header(filename)
    if not header:
        return pd.DataFrame()

    df = pd.read_csv(
        filename,
        skiprows=header["n_header"],
        names=header["var_names"],
        sep=r"\s+",
        skipinitialspace=True,
    )

    # Convert IVAR (usually start_time in days from reference date) to time
    # EBAS standard: IVAR is usually days since reference date
    ref_date = header.get("date_valid", datetime.datetime(1970, 1, 1))
    ivar_name = header.get("ivar_name", "")
    if "time" in ivar_name.lower() or "start" in ivar_name.lower():
        # Check if it's already a datetime or needs conversion from days
        try:
            df["time"] = pd.to_datetime(ref_date) + pd.to_timedelta(df[ivar_name], unit="D")
        except Exception:
            pass

    # Add site metadata to each row
    if "latitude" in header:
        df["latitude"] = header["latitude"]
    if "longitude" in header:
        df["longitude"] = header["longitude"]
    if "elevation" in header:
        df["elevation"] = header["elevation"]
    if "siteid" in header:
        df["siteid"] = header["siteid"]

    return df


@register_reader("actris")
class ACTRISReader(PointReader):
    """
    ACTRIS/EBAS Data Reader following standard conventions.
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
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str | None = None,
        siteid: str | None = None,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs,
    ) -> xr.Dataset | pd.DataFrame | dd.DataFrame:
        """
        Retrieve and load ACTRIS/EBAS data.

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
            Specific ACTRIS/EBAS site ID (e.g. 'NO0042G').
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : dict
            Additional arguments passed to the reader and driver.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded ACTRIS data.
        """
        if files is None:
            if dates is None:
                raise ValueError("Must provide either 'files' or 'dates'.")
            files = self.build_urls(dates, siteid=siteid, **kwargs)

        if not files:
            if as_xarray:
                return xr.Dataset()
            return pd.DataFrame()

        # Handle list of files/URLs
        files_list = np.atleast_1d(files).tolist()
        first_file = files_list[0]

        # Check if files are NetCDF (.nc)
        if str(first_file).endswith(".nc"):
            # NetCDF format from THREDDS
            # OPeNDAP URLs work better with Xarray/NetCDF4
            ds = xr.open_mfdataset(files, **kwargs)
            # Coordinate standardization is already somewhat standard in EBAS NetCDF
            # but we may need to rename to match PointReader conventions
            rename_dict = {}
            if "lat" in ds.coords:
                rename_dict["lat"] = "latitude"
            if "lon" in ds.coords:
                rename_dict["lon"] = "longitude"
            if "alt" in ds.coords:
                rename_dict["alt"] = "elevation"
            if rename_dict:
                ds = ds.rename(rename_dict)

            ds = update_history(ds, "Read ACTRIS/EBAS NetCDF data.")
            if not as_xarray:
                return ds.to_dataframe().reset_index()
            return ds

        # Default to NASA-Ames 1001 format
        # We need metadata from the first file to setup lazy processing
        expanded_files = FileUtility.expand_paths(files_list)
        if not expanded_files:
            raise FileNotFoundError(f"No files found matching {files}")

        header = parse_ebas_header(expanded_files[0])

        # EBAS NASA-Ames files are space-separated
        kwargs.setdefault("read_method", read_actris)

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
            as_xarray=False,
            lazy=lazy,
            **kwargs,
        )

        df = self.harmonize(df)

        if as_xarray:
            # Default to expand2d=False for ACTRIS as it's often irregular
            if "expand2d" not in kwargs:
                kwargs["expand2d"] = False

            ds = self.to_xarray(df, **kwargs)

            # Apply scaling and missing values lazily in Xarray
            ds = actris_preprocess(ds, header)

            # Add global metadata
            ds.attrs.update(
                {
                    k: v
                    for k, v in header.items()
                    if k in ["PI", "organization", "project", "volume"]
                }
            )

            ds = update_history(ds, "Read ACTRIS/EBAS data using standardized preprocessing.")
            return ds

        return df

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        siteid: str | None = None,
        **kwargs,
    ) -> list[str]:
        """
        Construct EBAS THREDDS URLs.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to build URLs for.
        siteid : str, optional
            Specific site ID (e.g. 'NO0042G').

        Returns
        -------
        List[str]
            List of matching EBAS NetCDF OPeNDAP URLs.
        """
        dates = pd.DatetimeIndex(np.atleast_1d(pd.to_datetime(dates)))
        if dates.empty:
            return []

        catalog_xml = get_ebas_catalog()
        if not catalog_xml:
            return []

        # Find datasets in the catalog
        # Example dataset entry:
        # <dataset name="NO0042G.20230101000000.20241021102319.uv_abs.ozone.air.1y.1h.NO01L_uv_abs_02.NO01L_uv_abs..nc" ... urlPath="ebas/NO0042G..." />
        pattern = r'<dataset name="([^"]+)" ID="([^"]+)" urlPath="([^"]+)"'
        matches = re.findall(pattern, catalog_xml)

        urls = []
        # Use OPeNDAP for better integration with Xarray
        base_url = "https://thredds.nilu.no/thredds/dodsC/"

        requested_min = dates.min()
        requested_max = dates.max()

        for name, dataset_id, url_path in matches:
            # Exclude EARLINET as requested by user
            if "earlinet" in name.lower():
                continue

            # Check siteid
            if siteid and siteid not in name:
                continue

            # Check dates
            # EBAS filename format: site.start_date.revision_date.component...nc
            # start_date is YYYYMMDDHHMMSS
            parts = name.split(".")
            if len(parts) < 7:
                continue

            try:
                start_date = pd.to_datetime(parts[1], format="%Y%m%d%H%M%S")
            except Exception:
                continue

            # Duration heuristic from filename (e.g., 1y, 1d, 1mo)
            duration_str = parts[6]
            duration = pd.Timedelta(days=365)  # Default to 1 year
            if "y" in duration_str:
                try:
                    duration = pd.Timedelta(days=int(duration_str.replace("y", "")) * 365)
                except ValueError:
                    pass
            elif "mo" in duration_str:
                try:
                    duration = pd.Timedelta(days=int(duration_str.replace("mo", "")) * 30)
                except ValueError:
                    pass
            elif "d" in duration_str:
                try:
                    duration = pd.Timedelta(days=int(duration_str.replace("d", "")))
                except ValueError:
                    pass

            end_date = start_date + duration

            # Check for overlap: [start_date, end_date] overlaps [requested_min, requested_max]
            if start_date <= requested_max and end_date >= requested_min:
                urls.append(f"{base_url}{url_path}")

        return urls

    def harmonize(self, df: pd.DataFrame | dd.DataFrame) -> pd.DataFrame | dd.DataFrame:
        """Standardize coordinate column names."""
        # PointReader.harmonize already handles dropping NaNs for lat/lon
        return super().harmonize(df)


def actris_preprocess(ds: xr.Dataset, header: dict[str, Any]) -> xr.Dataset:
    """
    Apply scaling and missing value handling lazily to ACTRIS dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    header : Dict[str, Any]
        Header metadata including scales and missing values.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    scales = header.get("scales", [])
    missing_values = header.get("missing_values", [])
    var_names = header.get("var_names", [])

    # DVARs start from index 1 (0 is IVAR)
    for i, (scale, miss) in enumerate(zip(scales, missing_values)):
        if i + 1 >= len(var_names):
            break
        col = var_names[i + 1]

        if col not in ds.variables:
            continue

        # Handle missing values
        miss_val = float(miss)
        ds[col] = ds[col].where(np.abs(ds[col].astype(float) - miss_val) > 1e-4)

        # Scaling
        if scale != 1.0:
            ds[col] = ds[col].astype(float) * scale

    return ds
