"""AMDAR/ACARS (Aircraft Meteorological Data Relay) Reader"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    import dask.dataframe as dd

from .base import PointReader, register_reader
from .sat_utils import update_history


@register_reader("amdar")
class AMDARReader(PointReader):
    """
    Reader for AMDAR/ACARS (Aircraft Meteorological Data Relay) observations.
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
        expand2d: bool = True,
        **kwargs,
    ) -> xr.Dataset | pd.DataFrame | dd.DataFrame:
        """
        Reads AMDAR/ACARS data. Files are typically NetCDF (from MADIS or BUFR-converted).

        Parameters
        ----------
        files : str or list[str]
            File path(s) or glob pattern.
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
            Whether to return an xarray Dataset, by default True.
        expand2d : bool, optional
            Whether to expand to 2D (time, node) structure, by default True.
        **kwargs : dict
            Additional arguments passed to Xarray.

        Returns
        -------
        xr.Dataset or pd.DataFrame
            UGRID xarray dataset by default, or DataFrame if ``as_xarray=False``.
        """
        if isinstance(files, str):
            import glob

            files = sorted(glob.glob(files)) if "*" in files else [files]

        if use_dask:
            kwargs.setdefault("chunks", "auto")

        datasets = []
        for f in files:
            ds = xr.open_dataset(f, **kwargs)
            # Standardize dimension to node
            if "recNum" in ds.dims:
                ds = ds.rename({"recNum": "node"})
            elif "observation" in ds.dims:
                ds = ds.rename({"observation": "node"})
            datasets.append(ds)

        if len(datasets) > 1:
            ds = xr.concat(datasets, dim="node")
        else:
            ds = datasets[0]

        ds = self.harmonize(ds)

        if as_xarray:
            # We already have a Dataset. We can directly call to_xarray to apply UGRID/2D logic.
            ds_out = self.to_xarray(ds, expand2d=expand2d)
            ds_out = update_history(ds_out, "Converted AMDAR data to UGRID xarray format.")
            return ds_out

        # Handle Lazy vs Eager DataFrame conversion
        if use_dask or (hasattr(ds, "chunks") and ds.chunks):
            import dask.dataframe as dd

            # Construct dask dataframe from xarray dataset to preserve laziness
            # All variables are 1D on the 'node' dimension.
            var_list = [v for v in ds.variables if v != "node"]

            dfs = []
            for v in var_list:
                data = ds[v].data
                if not hasattr(data, "dask"):
                    import dask.array as da

                    data = da.from_array(
                        data, chunks=ds.chunks.get("node", "auto") if ds.chunks else "auto"
                    )
                dfs.append(dd.from_dask_array(data, columns=[v]))

            if not dfs:
                df = dd.from_pandas(pd.DataFrame(columns=var_list), npartitions=1)
            else:
                df = dd.concat(dfs, axis=1).reset_index()
        else:
            df = ds.to_dataframe().reset_index()

        df.attrs = dict(ds.attrs)

        return df

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Harmonize AMDAR/ACARS dataset.

        Parameters
        ----------
        ds : xr.Dataset
            Input dataset.

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        # Mapping for aircraft data
        mapping = {
            "latitude": "latitude",
            "longitude": "longitude",
            "observationTime": "time",
            "altitude": "altitude",
            "pressure": "pressure",
            "temperature": "temperature",
            "windDir": "wind_dir",
            "windSpeed": "wind_speed",
            "tailNumber": "siteid",
            "flightNumber": "flight_number",
            "phase": "flight_phase",
        }

        rename_dict = {
            old: new
            for old, new in mapping.items()
            if old in ds.variables and new not in ds.variables
        }
        if rename_dict:
            ds = ds.rename(rename_dict)

        # Handle time if needed
        if "time" in ds.variables:
            if ds["time"].attrs.get("units") == "seconds since 1970-01-01 00:00:00.0 +0000":
                # Convert to datetime64[ns] lazily by using the epoch offset
                # 1970-01-01 is the default epoch for datetime64
                ds["time"] = (ds["time"] * 1e9).astype("datetime64[ns]")

        # Set coordinates
        coords = ["time", "siteid", "latitude", "longitude", "altitude", "pressure"]
        ds = ds.set_coords([c for c in coords if c in ds.variables])

        # Update history
        ds = update_history(ds, "Harmonized AMDAR/ACARS data.")

        return ds
