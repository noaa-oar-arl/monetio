"""RAQMS Reader"""

import os
from functools import partial
from glob import glob
from typing import Any

import numpy as np
import xarray as xr

from .base import GriddedReader, _ensure_time_dimension, register_reader
from .sat_utils import update_history
from .time_utils import parse_wrf_times


@register_reader("raqms")
class RAQMSReader(GriddedReader):
    """
    Reader for RAQMS (Real-time Air Quality Modeling System) model output files.
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
        convert_to_ppb: bool = True,
        var_list: list[str] | None = None,
        surf_only: bool = False,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Reads RAQMS netCDF files.

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
        convert_to_ppb : bool, optional
            Convert gas species from ppv to ppbv, by default True.
        var_list : list of str, optional
            List of variables to keep, by default None.
        surf_only : bool, optional
            Whether to only return the surface layer, by default False.
        **kwargs : Any
            Additional arguments passed to xarray.open_mfdataset or the driver.

        Returns
        -------
        xarray.Dataset
            The processed RAQMS dataset.

        Examples
        --------
        >>> from monetio.readers.raqms import RAQMSReader
        >>> reader = RAQMSReader()
        >>> ds = reader.open_dataset("uwhyb_*.nc")
        """
        # RAQMS check file format
        if isinstance(files, str):
            fpaths = sorted(glob(files))
        else:
            fpaths = sorted(files)

        if not fpaths or not all(
            fp.endswith(".nc") and "uwhyb" in os.path.basename(fp) for fp in fpaths
        ):
            raise ValueError(
                "File format not supported. Note that files should be preprocessed to netCDF."
            )

        # 1. Setup preprocessing
        if "preprocess" not in kwargs:
            kwargs["preprocess"] = partial(
                raqms_preprocess,
                convert_to_ppb=convert_to_ppb,
                var_list=var_list,
                surf_only=surf_only,
            )

        # Prepare kwargs
        if "concat_dim" not in kwargs:
            kwargs["concat_dim"] = "time"
        if "combine" not in kwargs:
            kwargs["combine"] = "nested"

        # RAQMS specific drop
        if "drop_variables" not in kwargs:
            kwargs["drop_variables"] = ["theta"]
        elif "theta" not in kwargs["drop_variables"]:
            if isinstance(kwargs["drop_variables"], list):
                kwargs["drop_variables"].append("theta")

        # 2. Open the dataset using standard xarray (via XarrayDriver)
        # Use fpaths instead of files to ensure consistent set of files
        ds = self.driver.open(fpaths, virtualizarr_parser="hdf5", **kwargs)

        # Update history
        ds = update_history(ds, "Read RAQMS data.")
        ds = _ensure_time_dimension(ds)

        return ds


def raqms_preprocess(
    ds: xr.Dataset,
    *,
    convert_to_ppb: bool = True,
    var_list: list[str] | None = None,
    surf_only: bool = False,
) -> xr.Dataset:
    """
    Preprocess function for a single RAQMS file.

    Parameters
    ----------
    ds : xarray.Dataset
        Input RAQMS dataset.
    convert_to_ppb : bool, optional
        Convert gas species to ppbv, by default True.
    var_list : list of str, optional
        List of variables to keep, by default None.
    surf_only : bool, optional
        Whether to keep only the surface layer, by default False.

    Returns
    -------
    xarray.Dataset
        Processed dataset.
    """
    # 1. Variable selection
    if var_list is not None:
        required = [
            "lat",
            "lon",
            "IDATE",
            "Times",
            "psfc",
            "delp",
            "pdash",
            "ttheta",
        ]
        vars_to_keep = list(set(var_list + required))
        vars_to_keep = [v for v in vars_to_keep if v in ds.variables]
        ds = ds[vars_to_keep]

    # 2. Grid and Coordinates
    ds = _fix_grid(ds)

    # 3. Time
    ds = _fix_time(ds)

    # 4. Pressure
    ds = _fix_pres(ds)

    # 5. Surface only
    if surf_only:
        # Check if 'z' dimension exists before slicing
        if "z" in ds.dims:
            ds = ds.isel(z=0).expand_dims("z")

    # 6. Unit conversion
    if convert_to_ppb:
        to_convert = [v for v in ds.data_vars if ds[v].attrs.get("units") == "ppv"]
        if to_convert:
            for v in to_convert:
                with xr.set_options(keep_attrs=True):
                    ds[v] = ds[v] * 1e9
                ds[v].attrs["units"] = "ppbv"
            ds = update_history(ds, f"Converted {', '.join(to_convert)} from ppv to ppbv.")

    # 7. Temperature
    if "ttheta" in ds.data_vars and "pres_pa_mid" in ds.data_vars:
        k = 0.28571428571428564  # R/cp = kappa
        with xr.set_options(keep_attrs=True):
            ds["temperature_k"] = ds["ttheta"] * (ds["pres_pa_mid"] / 100000) ** k
        ds["temperature_k"].attrs.update({"units": "K", "long_name": "Temperature"})
        ds = update_history(ds, "Calculated temperature_k from ttheta and pres_pa_mid.")

    # 8. Transpose
    dims = [d for d in ["time", "z", "y", "x"] if d in ds.dims]
    ds = ds.transpose(*dims)

    # Update history
    ds = update_history(ds, "Preprocessed RAQMS data.")

    return ds


def _fix_grid(ds: xr.Dataset) -> xr.Dataset:
    """
    Fix grid and coordinates for RAQMS.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with 'latitude' and 'longitude' coordinates.
    """
    # Handle coordinates lazily BEFORE renaming dims
    lat_name = "lat" if "lat" in ds.dims else "y"
    lon_name = "lon" if "lon" in ds.dims else "x"

    if lat_name in ds.variables and lon_name in ds.variables:
        lat_orig = ds[lat_name]
        lon_orig = ds[lon_name]
        lon_adj = xr.where(lon_orig >= 180, lon_orig - 360, lon_orig)

        # Broadcast to 2D
        # xr.broadcast will handle both NumPy and Dask lazily
        lon2d, lat2d = xr.broadcast(lon_adj, lat_orig)

        # Rename dims of the dataset FIRST
        rename_dims = {}
        if "lat" in ds.dims:
            rename_dims["lat"] = "y"
        if "lon" in ds.dims:
            rename_dims["lon"] = "x"
        if "lev" in ds.dims:
            rename_dims["lev"] = "z"

        if rename_dims:
            ds = ds.rename_dims(rename_dims)

        # Ensure dimension order and rename to standard y, x for coordinates
        lon2d = lon2d.transpose(lat_name, lon_name).rename({lat_name: "y", lon_name: "x"})
        lat2d = lat2d.transpose(lat_name, lon_name).rename({lat_name: "y", lon_name: "x"})

        ds["longitude"] = lon2d.assign_attrs(
            {
                "long_name": "Longitude",
                "units": "degree_east",
                "standard_name": "longitude",
            }
        )
        ds["latitude"] = lat2d.assign_attrs(
            {
                "long_name": "Latitude",
                "units": "degree_north",
                "standard_name": "latitude",
            }
        )

        ds = ds.drop_vars(["lat", "lon"], errors="ignore")
        ds = ds.set_coords(["latitude", "longitude"])

    if "lev" in ds.variables:
        ds["lev"].attrs.update(
            long_name="Nominal potential temperature of model level",
            units="K",
            description=(
                "In the stratosphere (beginning at lev=492), the model levels are on potential temperature surfaces. "
                "Below lev=492, the model levels are a blend of potential temperature and sigma (terrain-following) coordinates."
            ),
        )

    # Invert in z so that index 0 is closest to surface
    if "z" in ds.dims:
        ds = ds.isel(z=slice(None, None, -1))

    return ds


def _fix_time(ds: xr.Dataset) -> xr.Dataset:
    """
    Fix time coordinate for RAQMS.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with 'time' coordinate.
    """
    if "Times" in ds.variables:
        # Times is usually a character array (time, char_len)
        times_var = ds.Times
        string_dim = [d for d in times_var.dims if d != "time"]
        if not string_dim:
            if times_var.ndim == 1:
                # Assuming it's already a string array
                input_core_dims = [[]]
            else:
                return ds
        else:
            string_dim = string_dim[-1]
            input_core_dims = [[string_dim]]

        # Use vectorized parser from time_utils
        time_values = xr.apply_ufunc(
            parse_wrf_times,
            times_var,
            input_core_dims=input_core_dims,
            output_core_dims=[[]],
            vectorize=False,
            dask="parallelized",
            output_dtypes=[np.dtype("datetime64[ns]")],
        )

        ds = ds.assign_coords(time=time_values)
        ds = ds.drop_vars(["IDATE", "Times"], errors="ignore")

        # Update history
        ds = update_history(ds, "Optimized time parsing.")
    return ds


def _fix_pres(ds: xr.Dataset) -> xr.Dataset:
    """
    Fix pressure variables for RAQMS.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with renamed and scaled pressure variables.
    """
    rename0 = {
        "psfc": "surfpres_pa",
        "delp": "dp_pa",
        "pdash": "pres_pa_mid",
    }
    rename = {k: v for k, v in rename0.items() if k in ds.variables}

    if rename:
        ds = ds.rename_vars(rename)
        # Update history
        ds = update_history(ds, f"Renamed pressure variables: {rename}.")

    to_scale = [vn for vn in rename.values() if ds[vn].attrs.get("units") in {"mb", "hPa"}]
    if to_scale:
        for vn in to_scale:
            with xr.set_options(keep_attrs=True):
                ds[vn] = ds[vn] * 100
            ds[vn].attrs.update(units="Pa")
        # Update history
        ds = update_history(ds, f"Scaled {', '.join(to_scale)} from mb/hPa to Pa.")

    return ds
