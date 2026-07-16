import abc
from typing import TYPE_CHECKING, NamedTuple, Union

import numpy as np
import pandas as pd
import xarray as xr

from ..util import ds_to_2d, force_object_strings
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd

from .drivers import PandasDriver, XarrayDriver


# 1. Diagnostic Specification
class DiagnosticSpec(NamedTuple):
    """Specification for a derived diagnostic variable."""

    variables: list[str]
    weights: list[float] | None = None
    units: str = "unknown"
    long_name: str = "unknown"
    name: str = "unknown"


# 2. The Registry
READER_REGISTRY = {}


def register_reader(name):
    """Decorator to register a reader class."""

    def _register(cls):
        READER_REGISTRY[name] = cls
        return cls

    return _register


# 2. The Abstract Base Class
class BaseReader(abc.ABC):
    """
    The interface that ALL readers must implement.
    """

    @abc.abstractmethod
    def open_dataset(self, files: str | list[str], **kwargs) -> xr.Dataset | pd.DataFrame:
        """
        Main entry point to read data.

        Args:
            files: File path, list of paths, or glob pattern.
            **kwargs: Reader-specific arguments.

        Returns:
            xarray.Dataset (for models/sat) or pandas.DataFrame (for point obs).
        """
        pass

    def harmonize(self, ds):
        """
        Optional: Apply standard naming conventions (middleware).
        Can be overridden by specific readers.
        """
        return ds


def _ensure_time_dimension(ds: xr.Dataset) -> xr.Dataset:
    """Ensure ``time`` is represented as a dimension when possible."""
    if not isinstance(ds, xr.Dataset):
        return ds

    if "time" in ds.dims:
        return ds

    if "time" in ds.coords:
        time_dims = ds["time"].dims

        # Promote scalar time to a singleton dimension.
        if len(time_dims) == 0:
            ds = ds.expand_dims("time")
            return ds

        # If time is a 1D coordinate attached to another dimension, swap dimensions.
        if len(time_dims) == 1 and time_dims[0] in ds.dims:
            try:
                ds = ds.swap_dims({time_dims[0]: "time"})
            except Exception:
                pass
            return ds

    if "time" in ds.variables and "time" not in ds.coords:
        ds = ds.set_coords("time")
        return _ensure_time_dimension(ds)

    return ds


class GriddedReader(BaseReader):
    """
    Base class for gridded data (Models, Satellites) that utilizes XarrayDriver.
    """

    def __init__(self):
        self.driver = XarrayDriver()

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
        **kwargs,
    ) -> xr.Dataset:
        """
        Uses XarrayDriver to open files. VirtualiZarr options are forwarded to the driver.

        Parameters
        ----------
        files : str or list[str]
            File path(s), URL(s), or glob pattern.
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
            Whether to use Icechunk for VirtualiZarr references, by default False.
        icechunk_url : str or None, optional
            Path to the Icechunk repository, by default None.
        use_dask : bool, optional
            Whether to use Dask for lazy loading, by default False.
        **kwargs : dict
            Additional arguments passed to the driver.

        Returns
        -------
        xr.Dataset
            The loaded dataset.
        """
        ds = self.driver.open(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser=virtualizarr_parser,
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            **kwargs,
        )
        ds = self.harmonize(ds)
        ds = _ensure_time_dimension(ds)
        return ds

    def to_kerchunk(self, files: str | list[str], virtualizarr_file: str | None = None, **kwargs):
        """Generate Kerchunk references for the given files."""
        return self.driver.to_kerchunk(files, virtualizarr_file=virtualizarr_file, **kwargs)

    def to_icechunk(self, files: str | list[str], icechunk_url: str, **kwargs):
        """Generate Icechunk references for the given files."""
        return self.driver.to_icechunk(files, icechunk_url=icechunk_url, **kwargs)


class PointReader(BaseReader):
    """
    Base class for point/tabular data (Observations) that utilizes PandasDriver.
    """

    fixed_location = True

    def __init__(self):
        self.driver = PandasDriver()

    def open_dataset(
        self,
        files: str | list[str],
        # VirtualiZarr kwargs accepted but silently ignored for PointReaders
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        # Standard PointReader kwargs
        read_method: str = "read_csv",
        as_xarray: bool = True,
        lazy: bool = False,
        use_dask: bool = False,
        meta: pd.DataFrame | pd.Series | dict | tuple | None = None,
        expand2d: bool = True,
        **kwargs,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load point data.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path, list of paths, or glob pattern.
        use_virtualizarr : bool, optional
            Accepted but ignored for PointReaders (VirtualiZarr only applies to gridded data).
        virtualizarr_file : str or None, optional
            Accepted but ignored for PointReaders.
        virtualizarr_parser : str or None, optional
            Accepted but ignored for PointReaders.
        virtualizarr_backend : str, optional
            Accepted but ignored for PointReaders.
        icechunk_repo : str or None, optional
            Accepted but ignored for PointReaders.
        use_icechunk : bool, optional
            Accepted but ignored for PointReaders.
        icechunk_url : str or None, optional
            Accepted but ignored for PointReaders.
        read_method : str, optional
            The pandas/dask reading method to use, by default "read_csv".
        as_xarray : bool, optional
            If True, return an xarray.Dataset, by default True.
        lazy : bool, optional
            If True, return a dask-backed object, by default False.
        use_dask : bool, optional
            Alias for `lazy`, by default False.
        meta : pd.DataFrame, pd.Series, dict, or tuple, optional
            Dask metadata to use for lazy loading, by default None.
        **kwargs : dict
            Additional arguments passed to the reader and driver.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded dataset.
        """
        # Handle 'use_dask' as an alias for 'lazy'
        if use_dask:
            lazy = True

        # VirtualiZarr kwargs (use_virtualizarr, virtualizarr_file, virtualizarr_parser,
        # virtualizarr_backend, icechunk_repo, use_icechunk, icechunk_url)
        # are silently discarded here and NOT forwarded to PandasDriver.
        df = self.driver.open(files, read_method=read_method, lazy=lazy, meta=meta, **kwargs)

        df = self.harmonize(df)

        # Consistently force object strings to avoid nullable string issues in Pandas/Dask
        df = force_object_strings(df)

        if as_xarray:
            return self.to_xarray(df, expand2d=expand2d, **kwargs)

        return df

    def harmonize(
        self, df: Union[pd.DataFrame, "dd.DataFrame"]
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Harmonize the dataset (standard naming, dropping NaNs).

        Parameters
        ----------
        df : Union[pd.DataFrame, "dd.DataFrame"]
            Input dataframe.

        Returns
        -------
        Union[pd.DataFrame, "dd.DataFrame"]
            Harmonized dataframe.
        """
        if "latitude" in df.columns and "longitude" in df.columns:
            df = df.dropna(subset=["latitude", "longitude"])

        # Update history if attributes exist (backend-agnostic)
        df = update_history(df, "Harmonized and dropped NaN locations.")

        return super().harmonize(df)

    def to_xarray(
        self,
        obj: Union[pd.DataFrame, "dd.DataFrame", xr.Dataset],
        expand2d: bool = True,
        **kwargs,
    ) -> xr.Dataset:
        """
        Convert a DataFrame or Dataset to an xarray Dataset in UGRID convention.
        By default, returns a 2D dataset (time, node) if expand2d=True.

        Parameters
        ----------
        obj : Union[pd.DataFrame, dd.DataFrame, xr.Dataset]
            Input dataframe or unstructured dataset.
        expand2d : bool, optional
            Whether to expand to 2D (time, node) structure, by default True.
        **kwargs : dict
            Additional arguments passed to ds_to_2d (e.g. pivot).

        Returns
        -------
        xr.Dataset
            The dataset in UGRID convention.
        """
        # 1. Handle Dataset Input (Optimization to avoid round-trip)
        if isinstance(obj, xr.Dataset):
            ds = obj
            # Ensure dimension is 'node'
            if "node" not in ds.dims:
                # Find the primary dimension (should be 1D)
                potential_dims = [d for d in ds.dims if d not in ["time", "siteid"]]
                if potential_dims:
                    ds = ds.rename({potential_dims[0]: "node"})
        else:
            df = obj
            # 2. Identify backend
            try:
                import dask.dataframe as dd

                is_dask = isinstance(df, dd.DataFrame)
            except ImportError:
                is_dask = False

            # 3. Prepare DataFrame (ensure time and siteid are columns)
            if is_dask:
                temp_df = df
            else:
                temp_df = df.copy()

            # Drop 'index' if it exists to avoid conflicts during conversion
            if "index" in temp_df.columns:
                temp_df = temp_df.drop(columns="index")

            for name in ["time", "siteid"]:
                try:
                    names = temp_df.index.names
                except AttributeError:
                    names = [temp_df.index.name]

                if name in names:
                    temp_df = temp_df.reset_index()

            # 4. Handle Backends
            # Consistently force object strings for both backends to avoid nullable string issues.
            temp_df = force_object_strings(temp_df)

            if is_dask:
                # 4a. Lazy Path
                # Exception to "No Hidden Computes": lengths=True is often required by Xarray
                # to determine dimension sizes for the Dataset structure.
                # For dask-expr collections, this may trigger a small compute.
                ds = xr.Dataset()
                for col in temp_df.columns:
                    if col == "node" and "node" in ds.dims:
                        continue
                    ds[col] = (("node",), temp_df[col].to_dask_array(lengths=True))
            else:
                # 4b. Eager Path
                # Consistently use 1D for both Eager and Lazy by default.
                ds = temp_df.reset_index(drop=True).to_xarray()
                if "index" in ds.dims:
                    if "node" in ds.variables or "node" in ds.dims:
                        ds = ds.drop_vars("node", errors="ignore")
                    ds = ds.rename({"index": "node"})

        # Set standard coordinates
        coords = [
            c for c in ["time", "siteid", "latitude", "longitude", "elevation"] if c in ds.data_vars
        ]
        ds = ds.set_coords(coords)

        # Ensure node coordinate is a simple integer range for both
        if "node" in ds.dims:
            ds.coords["node"] = (("node",), np.arange(ds.sizes["node"]))

        # 4. Standard Path (Consistently try 2D expansion by default)
        # The user requested 2D UGRID as default.
        if expand2d:
            # We pass kwargs to allow control over pivoting (wide_fmt or pivot)
            pivot = kwargs.get("wide_fmt", kwargs.get("pivot", True))
            ds = ds_to_2d(ds, pivot=pivot, fixed_location=self.fixed_location)

        # Add UGRID metadata
        if "node" in ds.dims:
            node_coords = []
            for c in ["longitude", "latitude", "elevation"]:
                if c in ds.coords:
                    node_coords.append(c)

            if node_coords:
                ds["mesh"] = xr.DataArray(
                    data=np.int32(0),
                    attrs={
                        "cf_role": "mesh_topology",
                        "topology_dimension": 0,
                        "node_coordinates": " ".join(node_coords),
                    },
                )

            if "latitude" in ds.coords:
                ds.coords["latitude"].attrs.update(
                    {"units": "degrees_north", "standard_name": "latitude"}
                )
            if "longitude" in ds.coords:
                ds.coords["longitude"].attrs.update(
                    {"units": "degrees_east", "standard_name": "longitude"}
                )
            if "elevation" in ds.coords:
                ds.coords["elevation"].attrs.update(
                    {"units": "m", "standard_name": "height_above_mean_sea_level"}
                )

            for var in ds.data_vars:
                if "node" in ds[var].dims:
                    ds[var].attrs.update({"mesh": "mesh", "location": "node"})

        # Copy attributes from input object if they exist (e.g. history).
        # Dask DataFrames don't support .attrs the same way as pandas, so
        # we guard with getattr to avoid AttributeError.
        df_attrs = getattr(obj, "attrs", {}) or {}
        for k, v in df_attrs.items():
            if k not in ds.attrs:
                ds.attrs[k] = v
            elif k == "history":
                ds.attrs[k] = f"{v}\n{ds.attrs[k]}"

        # Add Global Attributes
        if "Conventions" not in ds.attrs:
            ds.attrs["Conventions"] = "CF-1.8 UGRID-1.0"
        elif "UGRID-1.0" not in ds.attrs["Conventions"]:
            ds.attrs["Conventions"] += " UGRID-1.0"

        # Update history
        ds = update_history(ds, "Converted to xarray Dataset with UGRID convention.")

        ds = _ensure_time_dimension(ds)

        return ds


def add_lazy_diagnostic(
    ds: xr.Dataset,
    name: str,
    spec: DiagnosticSpec,
    aliases: dict[str, list[str]] | None = None,
) -> xr.Dataset:
    """
    Adds a lazy diagnostic variable to the dataset if constituent variables exist.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.
    name : str
        Name of the diagnostic variable.
    spec : DiagnosticSpec
        Specification for the diagnostic.
    aliases : Dict[str, List[str]], optional
        Mapping of diagnostic names to potential existing variables in the file
        to use instead of calculating from constituents.

    Returns
    -------
    xarray.Dataset
        Dataset with diagnostic added if possible.
    """
    # 1. Check if name already exists as a data variable
    if name in ds.data_vars:
        return ds

    # 2. Check for pre-calculated summary variables to prevent regressions
    # Comprehensive default aliases for common model outputs
    default_aliases = {
        "PM25": ["PM25_TOT", "PM2_5", "PM2_5_DRY"],
        "PM10": ["PMC_TOT", "PM10", "PM_TOT", "PM10_DRY", "PM10_TOT"],
        "NOx": ["NOX"],
        "NOy": ["NOY"],
        "O3": ["OZONE"],
    }
    if aliases is not None:
        # Merge user aliases with defaults
        for k, v in aliases.items():
            if k in default_aliases:
                default_aliases[k] = list(set(default_aliases[k] + v))
            else:
                default_aliases[k] = v

    for alias in default_aliases.get(name, []):
        if alias in ds.data_vars:
            ds[name] = ds[alias].copy()
            ds[name].attrs.update(
                {"units": spec.units, "name": spec.name, "long_name": spec.long_name}
            )
            # Update history
            ds = update_history(ds, f"Added lazy diagnostic: {name} (using alias {alias}).")
            return ds

    # 3. Identify constituent variables available in the dataset
    available_vars = [v for v in spec.variables if v in ds.data_vars]
    if not available_vars:
        return ds

    # If weights are provided, they must match the full variable list in spec
    if spec.weights is not None:
        weights_map = dict(zip(spec.variables, spec.weights))
        weights = [weights_map[v] for v in available_vars]
    else:
        weights = [1.0] * len(available_vars)

    # 4. Compute lazy sum with unit synchronization
    with xr.set_options(keep_attrs=True):
        # Use first variable as base
        v0 = available_vars[0]
        new_var = ds[v0] * weights[0]
        base_units = ds[v0].attrs.get("units", "").lower()

        for i in range(1, len(available_vars)):
            v = available_vars[i]
            v_var = ds[v]
            v_units = v_var.attrs.get("units", "").lower()

            # Unit synchronization (e.g. ppmV vs ppbV)
            if v_units != base_units:
                if "ppm" in v_units and "ppb" in base_units:
                    v_var = v_var * 1000.0
                elif "ppb" in v_units and "ppm" in base_units:
                    v_var = v_var / 1000.0

            new_var = new_var + v_var * weights[i]

    # Inherit units from constituent variables if available, otherwise use spec
    units = ds[v0].attrs.get("units", spec.units)

    ds[name] = new_var.assign_attrs(
        {"units": units, "name": spec.name, "long_name": spec.long_name}
    )

    # Update history
    ds = update_history(ds, f"Added lazy diagnostic: {name} (sum of {', '.join(available_vars)}).")

    return ds


def _convert_to_ppb(ds: xr.Dataset) -> xr.Dataset:
    """
    Converts gas species units from ppmV to ppbV lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with converted units.
    """
    to_convert = [
        v for v in ds.data_vars if "units" in ds[v].attrs and "ppm" in ds[v].attrs["units"].lower()
    ]

    if not to_convert:
        return ds

    for v in to_convert:
        ds[v] = ds[v] * 1000.0
        ds[v].attrs["units"] = "ppbV"

    # Update history
    ds = update_history(ds, f"Converted {', '.join(to_convert)} from ppmV to ppbV.")

    return ds


def _format_units(ds: xr.Dataset) -> xr.Dataset:
    """
    Formats unit strings for particulate matter lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with formatted unit strings.
    """
    to_format = [
        v
        for v in ds.data_vars
        if "units" in ds[v].attrs
        and ("micrograms" in ds[v].attrs["units"].lower() or "ug" in ds[v].attrs["units"].lower())
    ]

    if not to_format:
        return ds

    for v in to_format:
        ds[v].attrs["units"] = r"$\mu g m^{-3}$"

    # Update history
    ds = update_history(ds, rf"Formatted units for {', '.join(to_format)} to $\mu g m^{{-3}}$.")

    return ds


def _convert_ugkg_to_ugm3(
    ds: xr.Dataset,
    *,
    alt_name: str = "ALT",
    pres_name: str = "pres_pa_mid",
    temp_name: str = "temperature_k",
    R: float = 287.05,
) -> xr.Dataset:
    """
    Converts mass mixing ratio (ug/kg) to mass concentration (ug/m3) lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.
    alt_name : str, optional
        Name of the specific volume variable, by default "ALT".
    pres_name : str, optional
        Name of the pressure variable (in Pa), by default "pres_pa_mid".
    temp_name : str, optional
        Name of the temperature variable (in K), by default "temperature_k".
    R : float, optional
        Gas constant for dry air in J/(kg·K), by default 287.05.

    Returns
    -------
    xarray.Dataset
        Dataset with converted units.
    """
    to_convert = [
        v
        for v in ds.data_vars
        if "units" in ds[v].attrs and "ug/kg" in ds[v].attrs["units"].lower()
    ]

    if not to_convert:
        return ds

    method = None
    if alt_name in ds.variables:
        # rho = 1 / ALT
        rho = 1.0 / ds[alt_name]
        method = f"using {alt_name} (specific volume)"
    elif pres_name in ds.variables and temp_name in ds.variables:
        # rho = P / (R * T)
        rho = ds[pres_name] / (R * ds[temp_name])
        method = f"using air density calculated from {pres_name} and {temp_name}"
    elif "P" in ds.variables and "PB" in ds.variables and "T" in ds.variables:
        # WRF-specific fallback if not already handled by pres_name/temp_name
        P_tot = ds["P"] + ds["PB"]
        T_actual = (ds["T"] + 300.0) * (P_tot / 100000.0) ** (287.05 / 1004.5)
        rho = P_tot / (R * T_actual)
        method = "using air density calculated from P, PB, T"
    else:
        return ds

    for v in to_convert:
        ds[v] = ds[v] * rho
        ds[v].attrs["units"] = r"$\mu g m^{-3}$"

    # Update history
    ds = update_history(ds, f"Converted {', '.join(to_convert)} from ug/kg to ug/m3 {method}.")

    return ds


def _add_ioapi_latlon(ds: xr.Dataset, proj4_srs: str) -> xr.Dataset:
    """
    Assigns latitude and longitude coordinates lazily for IOAPI-compliant grids.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset with IOAPI grid metadata (XORIG, YORIG, XCELL, YCELL, NCOLS, NROWS).
    proj4_srs : str
        The PROJ4 projection string.

    Returns
    -------
    xarray.Dataset
        Dataset with 'latitude' and 'longitude' coordinates.

    Examples
    --------
    >>> ds = _add_ioapi_latlon(ds, "+proj=lcc +lat_1=33 +lat_2=45 ...")
    """
    # 1. Generate 1D x and y values
    # NCOLS/NROWS might be attributes or dimensions
    ncols = ds.attrs.get("NCOLS", ds.sizes.get("x", ds.sizes.get("COL")))
    nrows = ds.attrs.get("NROWS", ds.sizes.get("y", ds.sizes.get("ROW")))

    xorig = ds.attrs.get("XORIG")
    yorig = ds.attrs.get("YORIG")
    xcell = ds.attrs.get("XCELL")
    ycell = ds.attrs.get("YCELL")

    if any(v is None for v in [ncols, nrows, xorig, yorig, xcell, ycell]):
        return ds

    # 2. Coordinate and Dimension Names
    x_dim = "COL" if "COL" in ds.dims else "x"
    y_dim = "ROW" if "ROW" in ds.dims else "y"

    # Generate 1D arrays
    x = np.linspace(xorig + xcell * 0.5, xorig + (ncols - 0.5) * xcell, ncols)
    y = np.linspace(yorig + ycell * 0.5, yorig + (nrows - 0.5) * ycell, nrows)

    xda = xr.DataArray(x, dims=x_dim)
    yda = xr.DataArray(y, dims=y_dim)

    # 3. Backend-Agnostic Chunking
    if ds.chunks:
        # Match coordinate chunking to data variables to maintain laziness.
        # We avoid hardcoded 'auto' and instead respect existing dataset chunks.
        x_chunks = {d: ds.chunks[d] for d in xda.dims if d in ds.chunks}
        y_chunks = {d: ds.chunks[d] for d in yda.dims if d in ds.chunks}
        if x_chunks:
            xda = xda.chunk(x_chunks)
        if y_chunks:
            yda = yda.chunk(y_chunks)

    # Broadcast to 2D
    yv, xv = xr.broadcast(yda, xda)

    # 4. Apply projection lazily
    def _proj_inv(
        x_val: np.ndarray, y_val: np.ndarray, p_srs: str | np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Kernel for lazy projection inversion using pyproj.Transformer.

        Parameters
        ----------
        x_val : np.ndarray
            X-coordinates in the projection.
        y_val : np.ndarray
            Y-coordinates in the projection.
        p_srs : Union[str, np.ndarray]
            PROJ projection string.

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
            Longitude and latitude arrays.
        """
        from pyproj import Transformer

        # Handle scalar/array inputs from apply_ufunc
        if isinstance(p_srs, np.ndarray | np.generic):
            p_srs = p_srs.item()
        if isinstance(p_srs, bytes):
            p_srs = p_srs.decode()

        # Transformer is generally faster than Proj for repeated use
        # and better handles modern PROJ versions.
        # We use a transformer from the input SRS to WGS84 (EPSG:4326).
        transformer = Transformer.from_crs(p_srs, "EPSG:4326", always_xy=True)
        return transformer.transform(x_val, y_val)

    lon, lat = xr.apply_ufunc(
        _proj_inv,
        xv,
        yv,
        proj4_srs,
        dask="parallelized",
        output_dtypes=[float, float],
        output_core_dims=[(), ()],
        keep_attrs=True,
    )

    # Assign coordinates and standard metadata
    ds = ds.assign_coords(
        longitude=lon.assign_attrs(
            {"long_name": "Longitude", "units": "degree_east", "standard_name": "longitude"}
        ),
        latitude=lat.assign_attrs(
            {"long_name": "Latitude", "units": "degree_north", "standard_name": "latitude"}
        ),
    )

    # Update history
    ds = update_history(ds, "Generated Latitude/Longitude coordinates via PROJ inversion.")

    return ds


def _get_ioapi_times(ds: xr.Dataset) -> xr.Dataset:
    """
    Extracts and assigns time coordinate from IOAPI TFLAG lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset with TFLAG variable.

    Returns
    -------
    xarray.Dataset
        Dataset with 'time' coordinate.

    Examples
    --------
    >>> ds = _get_ioapi_times(ds)
    """
    from .time_utils import parse_ioapi_times

    tflag = ds.TFLAG
    # TFLAG can be (TSTEP, DATE_TIME) or (TSTEP, VAR, DATE_TIME)
    if tflag.ndim == 3:
        tflag = tflag.isel(VAR=0, drop=True)

    # Handle dimension names (COL is often used for DATE_TIME in pseudonetcdf)
    dt_dims = [d for d in tflag.dims if "DATE" in str(d).upper() and "TIME" in str(d).upper()]
    if not dt_dims:
        dt_dim = tflag.dims[-1]
    else:
        dt_dim = dt_dims[0]

    # Use apply_ufunc to construct dates lazily using vectorized parser
    dates = xr.apply_ufunc(
        parse_ioapi_times,
        tflag.isel(**{dt_dim: 0}),
        tflag.isel(**{dt_dim: 1}),
        vectorize=False,
        dask="parallelized",
        output_dtypes=[np.dtype("datetime64[ns]")],
    )

    # If 'TSTEP' is the time dimension, we replace its values
    if "TSTEP" in ds.dims:
        ds = ds.assign_coords(TSTEP=dates)
        ds = ds.rename({"TSTEP": "time"})
    else:
        # Fallback: assume first dimension is time
        time_dim = tflag.dims[0]
        ds = ds.assign_coords({time_dim: dates})
        ds = ds.rename({time_dim: "time"})

    # Update history
    ds = update_history(ds, "Optimized IOAPI time parsing.")

    return ds


def _harmonize_ioapi_dims(ds: xr.Dataset) -> xr.Dataset:
    """
    Standardize IOAPI dimension names (COL, ROW, LAY) to (x, y, z) lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with standardized dimensions.
    """
    rename_dict = {}
    if "COL" in ds.dims:
        rename_dict["COL"] = "x"
    if "ROW" in ds.dims:
        rename_dict["ROW"] = "y"
    if "LAY" in ds.dims:
        rename_dict["LAY"] = "z"

    if rename_dict:
        ds = ds.rename(rename_dict)
        ds = update_history(ds, f"Renamed dimensions: {rename_dict}")

    return ds


def _harmonize_ioapi_vars(ds: xr.Dataset) -> xr.Dataset:
    """
    Standardize IOAPI variable names and remove redundant data variables.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with standardized variables.
    """
    # 1. Standardize variable names to lowercase
    mapping = {v: v.lower() for v in ds.data_vars}
    # Avoid collisions with existing coordinates
    mapping = {k: v for k, v in mapping.items() if v not in ds.coords}
    ds = ds.rename(mapping)

    # 2. Drop redundant variables that are now coordinates
    # This prevents cluttering the data_vars when they have been promoted
    to_drop = [v for v in ds.data_vars if v in ds.coords]

    # Specific common ones in IOAPI that might be present but promoted/renamed
    # e.g. 'lat', 'lon', 'tflag'
    for redundant in ["lat", "lon", "tflag"]:
        if redundant in ds.data_vars:
            to_drop.append(redundant)

    if to_drop:
        ds = ds.drop_vars(list(set(to_drop)))
        ds = update_history(ds, f"Dropped redundant data variables: {list(set(to_drop))}")

    return ds


def _scientific_hygiene(ds: xr.Dataset) -> xr.Dataset:
    """
    Apply standard scientific hygiene to the dataset.

    1. Identifies standard coordinates (latitude, longitude, time) and ensures they are set.
    2. Strips whitespace from all string attributes.
    3. Updates the history attribute.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Processed dataset.
    """
    # 1. Coordinate Handling - Ensure standard coordinates are set
    # We do NOT use reset_coords() to avoid demoting non-standard coords.
    coords = [
        c for c in ["latitude", "longitude", "time"] if c in ds.variables and c not in ds.coords
    ]
    if coords:
        ds = ds.set_coords(coords)

    # 2. Attribute Cleaning - Strip whitespace from string attributes
    for var in ds.variables:
        for attr, val in ds[var].attrs.items():
            if isinstance(val, str):
                ds[var].attrs[attr] = val.strip()

    # Also for global attributes
    for attr, val in ds.attrs.items():
        if isinstance(val, str):
            ds.attrs[attr] = val.strip()

    # 3. Provenance tracking
    ds = update_history(
        ds, "Applied scientific hygiene (standard coordinates and attribute cleaning)."
    )

    return ds
