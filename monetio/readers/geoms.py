"""GEOMS Reader"""

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from .base import GriddedReader, _ensure_time_dimension, register_reader
from .drivers import FileUtility
from .sat_utils import update_history


@register_reader("geoms")
class GEOMSReader(GriddedReader):
    """
    Reader for GEOMS format files (HDF4/HDF5).
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
        rename_all: bool = True,
        squeeze: bool = True,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Reads GEOMS format files.

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
        rename_all : bool, optional
            Whether to rename all variables to lowercase/standard names, by default True.
        squeeze : bool, optional
            Whether to squeeze dimensions of size 1, by default True.
        **kwargs : Any
            Additional arguments passed to the driver.

        Returns
        -------
        xr.Dataset
            The processed GEOMS dataset.

        Examples
        --------
        >>> reader = GEOMSReader()
        >>> ds = reader.open_dataset("groundbased_uvvis.doas.directsun.no2*.h5")
        """
        # Note: GEOMS files (especially HDF4) often require custom logic that
        # open_mfdataset might not handle natively without a complex engine.
        # We maintain a loop but ensure laziness where possible.

        file_list = FileUtility.expand_paths(files)

        dsets = []
        for f in file_list:
            ds = open_dataset_geoms(f, rename_all=rename_all, squeeze=squeeze)
            dsets.append(ds)

        if not dsets:
            return xr.Dataset()

        if len(dsets) == 1:
            ds = dsets[0]
        else:
            ds = xr.concat(dsets, dim="time")

        # Update history
        ds = update_history(ds, "Read GEOMS data.")
        ds = _ensure_time_dimension(ds)

        return ds


def open_dataset_geoms(fp: str, *, rename_all: bool = True, squeeze: bool = True) -> xr.Dataset:
    """
    Internal function to open a single GEOMS file.

    Parameters
    ----------
    fp : str
        File path.
    rename_all : bool, optional
        Whether to rename all variables, by default True.
    squeeze : bool, optional
        Whether to squeeze, by default True.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    from monetio.util import _import_required

    ext = Path(fp).suffix.lower()
    fs = FileUtility.get_fs(fp)

    if ext in {".h4", ".hdf4", ".hdf"}:
        pyhdf_SD = _import_required("pyhdf.SD")
        if fp.startswith("s3://"):
            import tempfile

            with tempfile.NamedTemporaryFile(suffix=ext, delete=True) as tmp:
                fs.get(fp, tmp.name)
                sd = pyhdf_SD.SD(tmp.name)
                data_vars, attrs = _read_hdf4(sd)
                sd.end()
        else:
            sd = pyhdf_SD.SD(str(fp))
            data_vars, attrs = _read_hdf4(sd)
            sd.end()

        # For HDF4, we have NumPy arrays now. Wrap in Dataset.
        ds = xr.Dataset(data_vars=data_vars, attrs=attrs)

    elif ext in {".h5", ".he5", ".hdf5"}:
        # For HDF5, we try to use xarray's native lazy loading if possible,
        # but GEOMS HDF5 structure (using dimension labels) often needs manual intervention.
        from monetio.util import _import_required

        h5py = _import_required("h5py")
        da_array = _import_required("dask.array")

        f_obj = fs.open(fp, "rb")
        f = h5py.File(f_obj, "r")

        data_vars = {}
        for k, v in f.items():
            dims = tuple(_rename_h5_dim(str(d)) for d in v.dims)
            # We wrap in DataArray with lazy loading using dask.array.from_array.
            # This allows backend-agnostic lazy evaluation without immediate v[...] compute.
            data_lazy = da_array.from_array(v, chunks="auto")
            data_vars[k] = (dims, data_lazy, dict(v.attrs))

        attrs = dict(f.attrs)
        # Note: We don't close the file yet because dask needs the handle for lazy reads.
        # This is a trade-off for GEOMS HDF5 files which don't fit standard xr.open_dataset engines.
        ds = xr.Dataset(data_vars=data_vars, attrs=attrs)
    else:
        raise ValueError(f"unrecognized file extension: {ext!r}")

    ds = geoms_preprocess(ds, rename_all=rename_all, squeeze=squeeze)

    return ds


def geoms_preprocess(
    ds: xr.Dataset, *, rename_all: bool = True, squeeze: bool = True
) -> xr.Dataset:
    """
    Standardizes GEOMS dataset coordinates and dimensions.

    Parameters
    ----------
    ds : xr.Dataset
        Input GEOMS dataset.
    rename_all : bool, optional
        Whether to rename all variables, by default True.
    squeeze : bool, optional
        Whether to squeeze, by default True.

    Returns
    -------
    xr.Dataset
        Preprocessed dataset.
    """
    # 1. Handle Instrument Coordinates
    instru_coords = [
        "LATITUDE.INSTRUMENT",
        "LONGITUDE.INSTRUMENT",
        "ALTITUDE.INSTRUMENT",
    ]
    for vn in instru_coords:
        if vn in ds:
            da = ds[vn]
            if da.ndim == 0:
                ds = ds.set_coords(vn)
                continue
            (dim_name0,) = da.dims
            dim_name = _rename_var(vn)
            ds = ds.set_coords(vn).rename_dims({dim_name0: dim_name})

    # 2. Main Dimension Renaming
    rename_main_dims = {"DATETIME": "time", "ALTITUDE": "altitude"}
    actual_dim_renames = {}
    for ref, new_dim in rename_main_dims.items():
        if ref not in ds:
            continue
        n = ds[ref].size
        time_dims = [
            dim_name
            for dim_name, dim_size in ds.sizes.items()
            if dim_name.startswith("fakeDim") and dim_size == n
        ]
        for td in time_dims:
            actual_dim_renames[td] = new_dim

    if actual_dim_renames:
        ds = ds.rename_dims(actual_dim_renames)
        ds = update_history(ds, f"Renamed dimensions: {actual_dim_renames}")

    # 3. Squeeze singleton dimensions if they look like placeholders
    for vn, da in ds.variables.items():
        if da.ndim >= 1 and da.dims[-1].startswith("fakeDim") and da.dtype.kind == "f":
            n = da.sizes[da.dims[-1]]
            if n == 1:
                ds[vn] = da.squeeze(dim=da.dims[-1])

    # 4. Handle String Variables (Backend-agnostic)
    ds = _handle_strings(ds)

    # 5. Type and Byte Order Cleanup
    for vn, da in ds.variables.items():
        if da.dtype.kind == "f":
            if da.dtype.byteorder not in {"=", "|"}:
                ds[vn] = da.astype(da.dtype.newbyteorder("="))

    # 6. Time Conversion (Lazy)
    ds = _convert_times_lazy(ds)
    ds = update_history(ds, "Converted MJD2000 times to datetime64[ns] lazily.")

    # 7. Final Renaming and Squeezing
    rename_vars = {k: v for k, v in rename_main_dims.items() if k in ds.variables}
    rename_vars.update({old: _rename_var(old) for old in instru_coords if old in ds})
    if rename_vars:
        ds = ds.rename_vars(rename_vars)

    if rename_all:
        ds = ds.rename_vars({old: _rename_var(old) for old in ds.data_vars})

    if "latitude_instrument" in ds.coords and "latitude" not in ds.coords:
        ds = ds.rename(
            {
                "latitude_instrument": "latitude",
                "longitude_instrument": "longitude",
            }
        )

    if squeeze:
        ds = ds.squeeze()

    # Update history
    ds = update_history(ds, "Preprocessed GEOMS data.")

    return ds


def _handle_strings(ds: xr.Dataset) -> xr.Dataset:
    """
    Decodes byte strings and handles object-type strings lazily.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Dataset with decoded strings.
    """
    for vn, da in ds.variables.items():
        if da.dtype.kind == "S":
            ds[vn] = da.astype(str)
        elif da.dtype.kind == "O":
            # GEOMS often has object arrays containing bytes or strings
            # We can use xr.apply_ufunc for a robust backend-agnostic conversion
            # Use 'object' output_dtypes to avoid truncation during parallel processing
            decoded = xr.apply_ufunc(
                _decode_obj,
                da,
                vectorize=True,
                dask="parallelized",
                output_dtypes=[object],
            )
            ds[vn] = decoded.astype(str)
    return ds


def _decode_obj(x: Any) -> str:
    """
    Helper to decode object that might be bytes.

    Parameters
    ----------
    x : Any
        Object to decode.

    Returns
    -------
    str
        Decoded string.
    """
    if isinstance(x, bytes):
        return x.decode("utf-8")
    return str(x)


def _convert_times_lazy(ds: xr.Dataset) -> xr.Dataset:
    """
    Convert GEOMS MJD2000 times lazily.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Dataset with converted times.
    """
    time_vars = ["DATETIME", "DATETIME.START", "DATETIME.STOP"]
    for vn in time_vars:
        if vn in ds:
            # GEOMS MJD2000: days since 2000-01-01 00:00:00 (Julian?)
            # Actually MJD2000 in GEOMS is often relative to 2000-01-01.
            # We use the vectorized parser.
            ds[vn] = xr.apply_ufunc(
                _mjd2000_to_datetime,
                ds[vn],
                dask="parallelized",
                output_dtypes=[np.dtype("datetime64[ns]")],
            )
    return ds


def _mjd2000_to_datetime(x: np.ndarray) -> np.ndarray:
    """
    Vectorized conversion from MJD2000 to datetime64[ns].

    Parameters
    ----------
    x : np.ndarray
        MJD2000 days.

    Returns
    -------
    np.ndarray
        datetime64[ns] array.
    """
    # MJD2K in GEOMS: Days since 2000-01-01 00:00:00 UTC
    # MJD is JD - 2400000.5
    # JD for 2000-01-01 00:00:00 is 2451544.5
    # So MJD for 2000-01-01 is 51544.0
    # geoms utility used: x + 2400000.5 + 51544 (This seems wrong if it was already MJD2000)
    # Re-evaluating: np.asarray(x) + 2400000.5 + 51544 = x + 2451544.5 (This is JD)
    # pd.to_datetime(..., unit='D', origin='julian') converts JD to datetime.

    # We maintain the legacy logic but vectorized.
    jd = np.asarray(x) + 2451544.5
    # Use astype('datetime64[ns]') on the pandas series/index to avoid .values compute
    # when possible, though apply_ufunc will pass numpy arrays here anyway.
    return pd.to_datetime(jd, unit="D", origin="julian").to_numpy().astype("datetime64[ns]")


def _read_hdf4(
    sd: Any,
) -> tuple[dict[str, tuple[tuple[str, ...], np.ndarray, dict[str, Any]]], dict[str, Any]]:
    """
    Reads HDF4 datasets using pyhdf.

    Parameters
    ----------
    sd : Any
        pyhdf SD object.

    Returns
    -------
    Tuple[Dict, Dict]
        data_vars and global attributes.
    """
    data_vars = {}
    for name, _ in sd.datasets().items():
        sds = sd.select(name)
        data = sds.get()
        dims = tuple(sds.dimensions())
        attrs = sds.attributes()
        data_vars[name] = (dims, data, attrs)
        sds.endaccess()
    attrs = sd.attributes()
    return data_vars, attrs


def _rename_h5_dim(s: str) -> str:
    """
    Parses HDF5 dimension label.

    Parameters
    ----------
    s : str
        Dimension label string.

    Returns
    -------
    str
        Renamed dimension.
    """
    import re

    s_re = r'<"(.*)" dimension (\d+) of HDF5 dataset at (\d+)>'
    m = re.fullmatch(s_re, s)
    if m is None:
        # Fallback if it's already renamed or different format
        return s
    label, num, _ = m.groups()
    return f"fakeDim{num}{label}"


def _rename_var(vn: str, *, under: str = "_", dot: str = "_") -> str:
    """
    Standardizes variable names.

    Parameters
    ----------
    vn : str
        Original variable name.
    under : str, optional
        Replacement for underscore, by default "_".
    dot : str, optional
        Replacement for dot, by default "_".

    Returns
    -------
    str
        Standardized name.
    """
    return vn.lower().replace("_", under).replace(".", dot)


def _dti_from_mjd2000(x: Any) -> pd.DatetimeIndex:
    """
    Convert MJD2000 values to DatetimeIndex.

    The input must have a ``VAR_UNITS`` attribute containing ``"MJD2K"``
    (case-insensitive), otherwise an ``AttributeError`` is raised.

    Parameters
    ----------
    x : Any
        DataArray or array-like with MJD2000 values.

    Returns
    -------
    pd.DatetimeIndex
        DatetimeIndex.

    Raises
    ------
    AttributeError
        If ``x`` does not have a ``VAR_UNITS`` attribute containing ``"MJD2K"``.
    """
    units = getattr(x, "attrs", {}).get("VAR_UNITS")
    if units is None:
        raise AttributeError("VAR_UNITS attribute not found")
    if "MJD2K" not in units.upper():
        raise AttributeError(f"VAR_UNITS='{units}' does not contain 'MJD2K'")
    return pd.to_datetime(np.asarray(x) + 2451544.5, unit="D", origin="julian")
