"""PAMS Reader"""

import json
from typing import Any, Union

import pandas as pd
import xarray as xr

from ..util import force_object_strings
from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

try:
    import dask.dataframe as dd
except ImportError:
    dd = None


@register_reader("pams")
class PAMSReader(PointReader):
    """
    Reader for PAMS (Photochemical Assessment Monitoring Stations) data.
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
        as_xarray: bool = True,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load PAMS data.

        Parameters
        ----------
        files : Union[str, List[str]]
            File paths or URLs to read.
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
        **kwargs : Any
            Additional arguments passed to the reader and driver.
            Includes `expand2d`, `pivot`, `wide_fmt`, and `lazy`.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded PAMS data. If `as_xarray=True`, units are preserved
            on individual data variables via an internal `unit_mapping`.

        Examples
        --------
        >>> from monetio.readers.pams import PAMSReader
        >>> reader = PAMSReader()
        >>> ds = reader.open_dataset("pams_data*.json")
        """
        # Filter out arguments that are not for the reader function
        reader_kwargs = {
            k: v for k, v in kwargs.items() if k not in ["expand2d", "pivot", "wide_fmt"]
        }

        # Use PandasDriver to open files
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
            read_method=read_pams,
            **reader_kwargs,
        )

        df = self.harmonize(df)

        # Consistently force object strings
        df = force_object_strings(df)

        if as_xarray:
            # Retrieve unit mapping from dataframe attrs before it's possibly lost
            # in to_xarray or further transformations.
            unit_map = getattr(df, "attrs", {}).get("unit_mapping", {})

            if not unit_map:
                # For Dask-backed DataFrames, attributes from the delayed read_pams
                # are lost. We peek at the first file's metadata to recover them.
                try:
                    file_list = FileUtility.expand_paths(files)
                    if file_list:
                        # Only read the first file's metadata
                        meta_df = read_pams(file_list[0])
                        unit_map = meta_df.attrs.get("unit_mapping", {})
                except Exception:
                    pass

            # We must handle unit transfer BEFORE or AFTER expansion.
            # If we expand (pivot), the variables change names.
            ds = self.to_xarray(df, **kwargs)

            if unit_map:
                for varname, unit in unit_map.items():
                    if varname in ds.data_vars:
                        ds[varname].attrs["units"] = unit
                    if varname == "obs" and "obs" in ds.data_vars:
                        ds["obs"].attrs["units"] = unit

            if "obs" in ds.data_vars and "units" not in ds["obs"].attrs:
                # Fallback for non-pivoted or if mapping failed
                if "units" in ds.variables:
                    try:
                        # Extract units from the first element of 'units' coordinate/variable
                        # This works if 'units' is a string array/coordinate
                        if hasattr(ds["units"].data, "dask"):
                            # Use delayed or just skip if dask to avoid compute
                            pass
                        else:
                            unit_val = str(ds["units"].to_numpy()[0])
                            ds["obs"].attrs["units"] = unit_val
                    except (IndexError, AttributeError, KeyError):
                        pass

            # Update history
            ds = update_history(ds, "Read PAMS data.")

            return ds

        return df


def read_pams(filename: str, **kwargs: Any) -> pd.DataFrame:
    """
    Read a single PAMS JSON file.

    Parameters
    ----------
    filename : str
        File path or URL.
    **kwargs : Any
        Additional arguments.

    Returns
    -------
    pd.DataFrame
        The loaded PAMS data with standardized columns (siteid, time, obs, units).

    Examples
    --------
    >>> from monetio.readers.pams import read_pams
    >>> df = read_pams("site_data.json")
    """
    # Determine storage options if S3
    storage_options = kwargs.get("storage_options")
    if filename.startswith("s3://") and storage_options is None:
        storage_options = {"anon": True}

    fs = FileUtility.get_fs(filename)
    # Note: json.load requires the full file in memory.
    with fs.open(filename, "r") as f:
        jsonf = json.load(f)

    dataf = jsonf.get("Data", [])
    data = pd.DataFrame.from_dict(dataf)

    if data.empty:
        # Return empty with standard columns
        return pd.DataFrame(columns=["siteid", "time", "latitude", "longitude", "obs", "units"])

    # siteid construction
    data["siteid"] = (
        data.state_code.astype(str).str.zfill(2)
        + data.county_code.astype(str).str.zfill(3)
        + data.site_number.astype(str).str.zfill(4)
    )

    # Standardize column naming for PointReader
    if "parameter" in data.columns:
        data = data.rename(columns={"parameter": "variable"})

    # Vectorized time construction
    data["time"] = pd.to_datetime(data["date_gmt"] + " " + data["time_gmt"])

    data = data.rename(
        columns={
            "sample_measurement": "obs",
            "units_of_measure": "units",
            "units_of_measure_code": "unit_code",
        }
    )

    cols_to_drop = [
        "state_code",
        "county_code",
        "site_number",
        "datum",
        "qualifier",
        "uncertainty",
        "county",
        "state",
        "date_of_last_change",
        "date_local",
        "time_local",
        "date_gmt",
        "time_gmt",
        "poc",
        "unit_code",
        "sample_duration_code",
        "method_code",
    ]
    data = data.drop(columns=[c for c in cols_to_drop if c in data.columns])

    # Standardize units
    repl = {
        "Parts per billion Carbon": "ppbC",
        "Parts per billion": "ppb",
        "Parts per million": "ppm",
    }
    data["units"] = data["units"].replace(repl)

    # Set metadata for variables if they exist
    # To be picked up by open_dataset
    if "units" in data.columns and not data.empty:
        # PAMS JSONs typically contain one parameter per file or mixed.
        # We'll store a mapping of parameter -> unit in attrs.
        if "variable" in data.columns:
            mapping = data.groupby("variable")["units"].first().to_dict()
        else:
            # Fallback if not pivoted or missing variable col
            mapping = {"obs": data["units"].iloc[0]}
        data.attrs["unit_mapping"] = mapping

    return data
