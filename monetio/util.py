import datetime

import numpy as np
import xarray as xr


def normalize_pandas_freq(freq: str) -> str:
    """
    Normalize pandas frequency codes for compatibility with pandas 3.0+.

    'h' instead of 'H' (mandatory in 3.0)
    'D' instead of 'd' (recommended in 3.0)
    'ME' instead of 'M' or 'm'
    'QE' instead of 'Q' or 'q'
    'YE' instead of 'A' or 'a' or 'ye' or 'Y' or 'y'

    Parameters
    ----------
    freq : str
        Input frequency code.

    Returns
    -------
    str
        Normalized frequency code.
    """
    if not freq or not isinstance(freq, str):
        return freq

    # Extract numeric part and alias part
    import re

    match = re.match(r"(\d*)(.*)", freq)
    num, alias = match.groups()

    mapping = {
        "h": "h",
        "H": "h",
        "d": "D",
        "D": "D",
        "m": "ME",
        "M": "ME",
        "me": "ME",
        "ME": "ME",
        "q": "QE",
        "Q": "QE",
        "qe": "QE",
        "QE": "QE",
        "a": "YE",
        "A": "YE",
        "y": "YE",
        "Y": "YE",
        "ye": "YE",
        "YE": "YE",
    }

    if alias in mapping:
        return f"{num}{mapping[alias]}"

    return freq


def nearest(items, pivot):
    return min(items, key=lambda x: abs(x - pivot))


def search_listinlist(array1, array2):
    import numpy as np

    # Find common elements and their indices
    # This vectorizes the search using np.isin which is significantly faster
    # than iterating over the intersection set and calling np.where in a loop.
    # It also correctly handles multidimensional arrays by flattening logic
    # implicit in np.isin for the check, but np.where acts on original shape.
    # We return the indices along the first dimension, matching original behavior.

    mask1 = np.isin(array1, array2)
    index1 = np.where(mask1)[0]

    mask2 = np.isin(array2, array1)
    index2 = np.where(mask2)[0]

    return np.sort(np.int32(index1)), np.sort(np.int32(index2))


def linregress(x, y):
    import numpy as np
    import statsmodels.api as sm

    xx = sm.add_constant(x)
    model = sm.OLS(y, xx)
    fit = model.fit()
    b, a = fit.params[0], fit.params[1]
    rsquared = fit.rsquared
    std_err = np.sqrt(fit.mse_resid)
    return a, b, rsquared, std_err


def findclosest(list, value):
    a = min((abs(x - value), x, i) for i, x in enumerate(list))
    return a[2], a[1]


def _force_forder(x):
    """
    Converts arrays x to fortran order. Returns
    a tuple in the form (x, is_transposed).
    """
    if x.flags.c_contiguous:
        return (x.T, True)
    else:
        return (x, False)


def kolmogorov_zurbenko_filter(df, window, iterations):
    """KZ filter implementation
    series is a pandas series
    window is the filter window m in the units of the data (m = 2q+1)
    iterations is the number of times the moving average is evaluated
    """
    z = df.copy()
    for i in range(iterations):
        z = z.rolling(window=window, min_periods=1, center=True).mean()
    return z


def wsdir2uv(ws, wdir):
    from numpy import cos, pi, sin

    u = -ws * sin(wdir * pi / 180.0)
    v = -ws * cos(wdir * pi / 180.0)
    return u, v


def is_dask_dataframe(obj) -> bool:
    """True if ``obj`` is a ``dask.dataframe.DataFrame``/``Series``.

    Shared by the deferred wide path and the drivers. Imports
    ``dask.dataframe`` lazily so this helper never forces it on the eager path;
    returns ``False`` when dask is not installed at all.
    """
    try:
        import dask.dataframe as dd
    except ImportError:
        return False
    return isinstance(obj, dd.DataFrame | dd.Series)


def long_to_wide(df, defer=False):
    """
    Convert a long-format DataFrame (or Dask DataFrame) to wide format.

    Parameters
    ----------
    df : Union[pd.DataFrame, dd.DataFrame]
        The input DataFrame in long format, containing 'time', 'siteid', 'variable', 'obs', and 'units'.
    defer : bool, optional
        When True **and** ``df`` is a Dask DataFrame, return it unchanged (still
        long) instead of computing the pivot — the reshape is deferred to
        :func:`ds_to_2d` via ``to_xarray``. The eager (pandas) result is unaffected.
        Default False
        reproduces today's behavior byte-for-byte, including the ``UserWarning``.

    Returns
    -------
    Union[pd.DataFrame, dd.DataFrame]
        The DataFrame in wide format, or the original long Dask frame when
        ``defer=True``.
    """
    try:
        import dask.dataframe as dd

        is_dask = isinstance(df, dd.DataFrame)
    except ImportError:
        is_dask = False

    if is_dask and defer:
        # dask cannot express this multi-index pivot lazily
        # (no MultiIndex, no lazy pivot_table on a composite key without
        # collapsing partitions). Stay long; ds_to_2d reshapes lazily.
        return df

    if is_dask:
        # Dask doesn't support multi-index pivot_table well and requires categories.
        # To remain lazy, we should avoid computing here if possible.
        # However, many parts of MONETIO expect a wide DataFrame before Xarray conversion.
        # For now, we keep the compute but make it explicit that it's a bottleneck.
        import warnings

        warnings.warn(
            "long_to_wide: Computing dask dataframe to perform pivot_table. "
            "Consider using as_xarray=True with lazy=True to avoid this.",
            UserWarning,
        )
        df = df.compute()

    # Pivot the data
    w = df.pivot_table(values="obs", index=["time", "siteid"], columns="variable").reset_index()

    # Add units (columns)
    # We do this in a vectorized way to be faster
    if not w.empty:
        # Get unique variable/unit pairs
        units_map = df[["variable", "units"]].drop_duplicates()
        # If there are multiple units for one variable, we take the first
        units_map = units_map.drop_duplicates(subset=["variable"])
        for _, row in units_map.iterrows():
            w[f"{row.variable}_unit"] = row.units

    # Get site info to add, allowing for possible time variation
    # We drop 'variable', 'obs', 'units' which are handled by the pivot/units_map
    site_info = df.drop(columns=["variable", "obs", "units"], errors="ignore").drop_duplicates()

    return w.merge(site_info, on=["time", "siteid"], how="left")


def calc_8hr_rolling_max(df, col=None, window=None):
    df.index = df.time_local
    df_rolling = (
        df.groupby("siteid")[col]
        .rolling(window, center=True, win_type="boxcar")
        .mean()
        .reset_index()
        .dropna()
    )
    df_rolling_max = (
        df_rolling.groupby("siteid")
        .resample(normalize_pandas_freq("d"), on="time_local")
        .max()
        .reset_index(drop=True)
    )
    df = df.reset_index(drop=True)
    return df.merge(df_rolling_max, on=["siteid", "time_local"])


def calc_24hr_ave(df, col=None):
    df.index = df.time_local
    df_24hr_ave = (
        df.groupby("siteid")[col].resample(normalize_pandas_freq("D")).mean().reset_index()
    )
    df = df.reset_index(drop=True)
    return df.merge(df_24hr_ave, on=["siteid", "time_local"])


def calc_3hr_ave(df, col=None):
    df.index = df.time_local
    df_3hr_ave = (
        df.groupby("siteid")[col].resample(normalize_pandas_freq("3h")).mean().reset_index()
    )
    df = df.reset_index(drop=True)
    return df.merge(df_3hr_ave, on=["siteid", "time_local"])


def calc_annual_ave(df, col=None):
    df.index = df.time_local
    df_annual_ave = (
        df.groupby("siteid")[col].resample(normalize_pandas_freq("YE")).mean().reset_index()
    )
    df = df.reset_index(drop=True)
    return df.merge(df_annual_ave, on=["siteid", "time_local"])


def get_giorgi_region_bounds(index=None, acronym=None):
    import pandas as pd

    i = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
    acro = [
        "NAU",
        "SAU",
        "AMZ",
        "SSA",
        "CAM",
        "WNA",
        "CNA",
        "ENA",
        "ALA",
        "GRL",
        "MED",
        "NEU",
        "WAF",
        "EAF",
        "SAF",
        "SAH",
        "SEA",
        "EAS",
        "SAS",
        "CAS",
        "TIB",
        "NAS",
    ]
    lonmax = [
        155,
        155,
        -34,
        -40,
        -83,
        -103,
        -85,
        -60,
        -103,
        -10,
        40,
        40,
        22,
        52,
        52,
        65,
        155,
        145,
        100,
        75,
        100,
        180,
    ]
    lonmin = [
        110,
        110,
        -82,
        -76,
        -116,
        -130,
        -103,
        -85,
        -170,
        -103,
        -10,
        -10,
        -20,
        22,
        -10,
        -20,
        95,
        100,
        65,
        40,
        75,
        40,
    ]
    latmax = [
        -11,
        -28,
        12,
        -20,
        30,
        60,
        50,
        50,
        72,
        85,
        48,
        75,
        18,
        18,
        -12,
        30,
        20,
        50,
        30,
        50,
        50,
        70,
    ]
    latmin = [
        -28,
        -45,
        -20,
        -56,
        10,
        30,
        30,
        25,
        60,
        50,
        30,
        48,
        -12,
        -12,
        -35,
        18,
        -11,
        20,
        5,
        30,
        30,
        50,
    ]
    df = pd.DataFrame(
        {
            "latmin": latmin,
            "lonmin": lonmin,
            "latmax": latmax,
            "lonmax": lonmax,
            "acronym": acro,
        },
        index=i,
    )
    try:
        if index is None and acronym is None:
            print("either index or acronym needs to be supplied")
            print("look here https://web.northeastern.edu/sds/web/demsos/images_002/subregions.jpg")
            raise ValueError
        elif index is not None:
            return df.loc[df.index == index].values.flatten()
        else:
            return df.loc[df.acronym == acronym.upper()].values.flatten()
    except ValueError:
        exit


def get_giorgi_region_df(df):
    df.loc[:, "GIORGI_INDEX"] = None
    df.loc[:, "GIORGI_ACRO"] = None
    for i in range(22):
        latmin, lonmin, latmax, lonmax, acro = get_giorgi_region_bounds(index=int(i + 1))
        con = (
            (df.longitude <= lonmax)
            & (df.longitude >= lonmin)
            & (df.latitude <= latmax)
            & (df.latitude >= latmin)
        )
        df.loc[con, "GIORGI_INDEX"] = i + 1
        df.loc[con, "GIORGI_ACRO"] = acro
    return df


def calc_13_category_usda_soil_type(
    clay: xr.DataArray | np.ndarray,
    sand: xr.DataArray | np.ndarray,
    silt: xr.DataArray | np.ndarray,
) -> xr.DataArray | np.ndarray:
    """Calculate the 13 category USDA soil type from clay, sand, and silt percentages.

    The categories are:
    0  -- WATER
    1  -- SAND
    2  -- LOAMY SAND
    3  -- SANDY LOAM
    4  -- SILT LOAM
    5  -- SILT
    6  -- LOAM
    7  -- SANDY CLAY LOAM
    8  -- SILTY CLAY LOAM
    9  -- CLAY LOAM
    10 -- SANDY CLAY
    11 -- SILTY CLAY
    12 -- CLAY

    Parameters
    ----------
    clay : xarray.DataArray or numpy.ndarray
        Percentage of clay (0-100).
    sand : xarray.DataArray or numpy.ndarray
        Percentage of sand (0-100).
    silt : xarray.DataArray or numpy.ndarray
        Percentage of silt (0-100).

    Returns
    -------
    xarray.DataArray or numpy.ndarray
        The 13-category USDA soil type.
    """

    def _logic(c, sa, si):
        # We use the reverse order of the original assignments to ensure correct priority
        # in np.select (first matching condition wins).
        condlist = [
            (c >= 40) & (sa <= 45) & (si < 40) & (c != 255),  # 12: CLAY
            (c >= 40) & (si >= 40) & (c != 255),  # 11: SILTY CLAY
            (c >= 35) & (sa > 45) & (c != 255),  # 10: SANDY CLAY
            (c >= 27) & (c < 40.0) & (sa > 20) & (sa <= 45) & (c != 255),  # 9: CLAY LOAM
            (c >= 27) & (c < 40.0) & (sa > 40) & (c != 255),  # 8: SILTY CLAY LOAM
            (c >= 20) & (c < 35) & (si < 28) & (sa > 45) & (c != 255),  # 7: SANDY CLAY LOAM
            (c >= 7) & (c < 27) & (si >= 28) & (si < 50) & (sa <= 52) & (c != 255),  # 6: LOAM
            (si >= 80) & (c < 12) & (c != 255),  # 5: SILT
            ((si >= 50) & (c >= 12) & (c < 27) & (c != 255))
            | ((si >= 50) & (si < 80) & (c < 12) & (c != 255)),  # 4: SILT LOAM
            ((c >= 7.0) & (c < 20) & (sa > 52) & (si + 2 * c >= 30) & (c != 255))
            | ((c < 7) & (si < 50) & (si + 2 * c >= 30) & (c != 255)),  # 3: SANDY LOAM
            (si + 1.5 * c >= 15.0) & (si + 1.5 * c < 30) & (c != 255),  # 2: LOAMY SAND
            (si + c * 1.5 < 15.0) & (c != 255),  # 1: SAND
        ]
        choicelist = [12.0, 11.0, 10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
        return np.select(condlist, choicelist, default=0.0)

    result = xr.apply_ufunc(
        _logic,
        clay,
        sand,
        silt,
        dask="parallelized",
        output_dtypes=[float],
    )

    if isinstance(result, xr.DataArray):
        history = (
            f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: "
            "Calculated USDA soil type backend-agnostic."
        )
        if "history" in result.attrs:
            result.attrs["history"] = f"{result.attrs['history']}\n{history}"
        else:
            result.attrs["history"] = history

    return result


_module_install_names = {
    # module: GH, PyPI, conda-forge
    "pyhdf": ("fhs/pyhdf", "pyhdf", "pyhdf"),
}


def _install_message(mod_name):
    if mod_name not in _module_install_names:
        return ""

    gh, pypi_name, cf_name = _module_install_names[mod_name]
    cf_ = f"Try installing from conda-forge using `conda install -c conda-forge {cf_name}`."

    return f"{cf_}"


def _import_required(mod_name: str):
    from importlib import import_module

    try:
        return import_module(mod_name)
    except ImportError as e:
        raise RuntimeError(
            f"importing required module '{mod_name}' failed. {_install_message(mod_name)}"
        ) from e


def get_nc_attrs(nc_obj):
    """Safe retrieval of attributes from both netCDF4 and h5netcdf."""
    if hasattr(nc_obj, "ncattrs"):
        return {a: nc_obj.getncattr(a) for a in nc_obj.ncattrs()}
    elif hasattr(nc_obj, "attrs"):
        return dict(nc_obj.attrs)
    return getattr(nc_obj, "__dict__", {})


def get_nc_var(dso, group_path, varname):
    """Safe retrieval of a variable from nested groups in both netCDF4 and h5netcdf."""
    # Handle list-like group_path
    if isinstance(group_path, list):
        group_path = group_path[0]

    if not group_path or group_path == "/":
        return dso.variables[varname]

    # Try direct access if supported (h5netcdf)
    # h5netcdf.legacyapi.Dataset and Group support this.
    full_path = f"/{group_path.strip('/')}/{varname}"
    try:
        # Check if we are dealing with h5netcdf
        if hasattr(dso, "_h5group") or "h5netcdf" in str(type(dso)):
            return dso[full_path]
    except (KeyError, TypeError, AttributeError):
        pass

    # Fallback to nested navigation (netCDF4)
    obj = dso
    for part in group_path.strip("/").split("/"):
        if part:
            if hasattr(obj, "groups") and part in obj.groups:
                obj = obj.groups[part]
            else:
                # Try accessing as an item (works for some objects)
                try:
                    obj = obj[part]
                except Exception:
                    # Last resort, try variables if part is the varname but it shouldn't be
                    if part == varname:
                        return obj.variables[varname]
                    raise
    return obj.variables[varname]


def get_nc_values(nc_var):
    """Safe retrieval of masked and scaled values from both netCDF4 and h5netcdf."""
    import numpy as np

    values = nc_var[:].squeeze()
    if not isinstance(values, np.ma.MaskedArray):
        # Handle manual masking/scaling for h5netcdf
        attrs = get_nc_attrs(nc_var)
        fill_value = attrs.get("_FillValue", attrs.get("missing_value"))
        if fill_value is not None:
            # Handle possible array-like fill_value
            if hasattr(fill_value, "__iter__") and not isinstance(fill_value, str | bytes):
                fill_value = fill_value[0]
            # Use masked_values for float precision tolerance
            values = np.ma.masked_values(values, fill_value, atol=1e-5, copy=False)

        scale_factor = attrs.get("scale_factor")
        add_offset = attrs.get("add_offset")
        if scale_factor is not None or add_offset is not None:
            sf = float(scale_factor) if scale_factor is not None else 1.0
            ao = float(add_offset) if add_offset is not None else 0.0
            values = values * sf + ao
    else:
        # NetCDF4 already masked
        # Sometimes netCDF4 doesn't mask INF values if they were meant to be FillValue
        # but the attribute didn't match perfectly.
        pass
    return values


def force_object_strings(df):
    """
    Force string columns to 'object' dtype to avoid nullable string issues in Pandas/Dask.

    Parameters
    ----------
    df : Union[pd.DataFrame, dd.DataFrame]
        Input dataframe.

    Returns
    -------
    Union[pd.DataFrame, dd.DataFrame]
        Dataframe with string columns cast to object.
    """
    import pandas as pd

    try:
        import dask.dataframe as dd

        is_dask = isinstance(df, dd.DataFrame)
    except ImportError:
        is_dask = False

    # Preserve attributes (scientific hygiene)
    attrs = getattr(df, "attrs", {}).copy()

    if is_dask:
        # For Dask, we use assign to ensure metadata is updated
        # and we explicitly cast to object.
        for col in df.columns:
            # We use .dtype to avoid triggering is_all_strings(df[col])
            # which would call len(df[col]) and trigger a compute.
            if pd.api.types.is_string_dtype(df[col].dtype):
                df = df.assign(**{col: df[col].astype(object)})
    else:
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col].dtype):
                df[col] = df[col].astype(object)

    if attrs:
        try:
            df.attrs.update(attrs)
        except Exception:  # noqa: BLE001 - attrs are best-effort metadata (malformed); per-key fallback below
            # Fallback for backends where update() might fail
            for k, v in attrs.items():
                try:
                    df.attrs[k] = v
                except Exception:  # noqa: BLE001 - single-key best-effort (malformed); metadata must never fail a load
                    pass
    return df


def ds_to_2d(ds, pivot=True, fixed_location=False):
    """
    Lazily transform a 1D UGRID dataset into a 2D (time, node) dataset.
    If 'variable' is present in coordinates and pivot=True, it also pivots the data variables.

    Parameters
    ----------
    ds : xarray.Dataset
        Input 1D dataset with 'time' and 'siteid' coordinates.
    pivot : bool, optional
        Whether to pivot by 'variable' column if present, by default True.
    fixed_location : bool, optional
        Whether to enforce fixed latitude/longitude/elevation per node, by default False.

    Returns
    -------
    xarray.Dataset
        2D expanded dataset with dimensions (time, node).
    """
    import datetime

    if "node" not in ds.dims:
        return ds

    # A query that legitimately returned no records has nothing to lay out, so
    # an empty frame is passed through unchanged; deciding whether an empty
    # result is acceptable is the caller's job, not this transformation's. The
    # zero-length check is on the node dimension itself because an empty result
    # still carries the time/siteid/variable coordinate names, so a later
    # "missing coordinate" test would not catch it and the pivot path would
    # instead fail on a size-zero dimension.
    if ds.sizes.get("node", 0) == 0:
        return ds

    # Ensure time, siteid, and variable are coords for set_index
    for col in ["time", "siteid", "variable"]:
        if col in ds.data_vars and col not in ds.coords:
            ds = ds.set_coords(col)

    if "time" not in ds.coords or "siteid" not in ds.coords:
        # The stated job of this transformation is to produce a (time, node)
        # layout; without both axes there is nothing to expand, and returning
        # the unexpanded input would let the caller believe it had been
        # expanded.
        from .readers.errors import TransformNotAppliedError

        missing = [name for name in ("time", "siteid") if name not in ds.coords]
        raise TransformNotAppliedError(
            "ds_to_2d",
            f"cannot expand to 2D (time, node): missing coordinate(s) {missing}",
        )

    # Handle history
    history = (
        f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: "
        "Expanded 1D UGRID to 2D (time, node) backend-agnostic."
    )

    try:
        if "variable" in ds.coords and pivot:
            # Full pivot path (long-to-wide)
            # 1. Expand obs and units to 2D (time, siteid, variable)
            # To handle multiple data vars consistently, we set MultiIndex and unstack
            # We use drop=True to avoid keeping the old 'node' coordinate which is now a MultiIndex
            ds_idx = ds.set_index(node=["time", "siteid", "variable"])

            # Ensure entries are unique before unstacking
            if "node" in ds_idx.coords:
                ds_idx = ds_idx.drop_duplicates("node")

            ds_unstacked = ds_idx.unstack("node")

            # 2. Extract 'obs' and pivot it by 'variable'
            if "obs" in ds_unstacked.data_vars:
                obs_wide = ds_unstacked["obs"].to_dataset(dim="variable")
            else:
                obs_wide = xr.Dataset()

            # 3. Handle units
            if "units" in ds_unstacked.data_vars:
                # Get one unit per variable per site (usually constant)
                units_wide = ds_unstacked["units"].to_dataset(dim="variable")

                # Rename columns to match MONET convention (e.g. OZONE_unit)
                units_wide = units_wide.rename({v: f"{v}_unit" for v in units_wide.data_vars})
            else:
                units_wide = xr.Dataset()

            # 4. Handle other metadata (constant over variable)
            # Unstacking node=[time, siteid, variable] makes everything 3D (time, siteid, variable)
            # We want to keep metadata as 2D (time, siteid)
            meta_vars = [v for v in ds_unstacked.data_vars if v not in ["obs", "units"]]

            ds_meta = xr.Dataset()
            for v in meta_vars:
                # Metadata should be constant over 'variable' dimension, so we take the first
                ds_meta[v] = ds_unstacked[v].isel(variable=0, drop=True)

            # Explicitly set compat to avoid warnings
            ds2d = xr.merge([obs_wide, units_wide, ds_meta], compat="no_conflicts")

            # Handle coordinates that might still have 'variable' dimension
            # and drop 'variable' coordinate
            for c in list(ds2d.coords):
                if "variable" in ds2d[c].dims:
                    ds2d.coords[c] = ds2d[c].isel(variable=0, drop=True)
            if "variable" in ds2d.coords:
                ds2d = ds2d.drop_vars("variable")

        else:
            # Simple expansion path
            ds_idx = ds.set_index(node=["time", "siteid"])

            # Ensure entries are unique before unstacking
            if "node" in ds_idx.coords:
                ds_idx = ds_idx.drop_duplicates("node")

            ds2d = ds_idx.unstack("node")

        # In MONET 2D convention, 'siteid' becomes the second dimension,
        # but we rename it to 'node' for UGRID compliance.
        if "siteid" in ds2d.dims:
            # Preserve siteid as a coordinate while renaming dimension to node for UGRID compliance
            ds2d = ds2d.rename({"siteid": "node"})
            # Now 'node' has siteid values.
            # We want 'node' to be 0...N-1 and 'siteid' to be a coordinate of 'node'.
            siteids = ds2d.node.values
            ds2d.coords["siteid"] = (("node",), siteids)
            ds2d.coords["node"] = (("node",), np.arange(ds2d.sizes["node"]))

        if fixed_location:
            # Enforce fixed locations by averaging over time (ignoring NaNs)
            # This fixes issues where missing time steps result in NaN lat/lon
            for coord in ["latitude", "longitude", "elevation"]:
                if coord in ds2d.coords or coord in ds2d.data_vars:
                    if "time" in ds2d[coord].dims and "node" in ds2d[coord].dims:
                        try:
                            # Use mean to reduce time dimension, ignoring NaNs
                            fixed_coord = ds2d[coord].mean(dim="time", skipna=True)
                            if coord in ds2d.coords:
                                ds2d.coords[coord] = fixed_coord
                            else:
                                # Convert data var to coordinate after fixing
                                ds2d = ds2d.set_coords(coord)
                                ds2d.coords[coord] = fixed_coord
                        except Exception:  # noqa: BLE001 - per-coordinate best-effort fix (malformed); the un-fixed coordinate is still valid
                            pass

        # Copy attributes from original dataset (scientific hygiene)
        for k, v in ds.attrs.items():
            if k not in ds2d.attrs:
                ds2d.attrs[k] = v
            elif k == "history":
                if v not in ds2d.attrs["history"]:
                    ds2d.attrs["history"] = f"{v}\n{ds2d.attrs['history']}"

        if "history" not in ds2d.attrs:
            ds2d.attrs["history"] = history
        return ds2d
    except Exception as e:
        # A caller who asked for the 2D layout must not receive the 1D input
        # with only a warning: downstream code written against (time, node)
        # then fails later with an error that does not mention this
        # transformation at all. categories: malformed, content_invalid.
        from .readers.errors import TransformNotAppliedError

        if isinstance(e, TransformNotAppliedError):
            raise
        raise TransformNotAppliedError(
            "ds_to_2d", f"expansion to 2D (time, node) failed: {e}"
        ) from e


def _try_merge_exact(left, right, *, right_name=None):
    """For two ``xr.Dataset``s, try ``left.merge(right, compat="equals", join="exact")``.
    If it fails, print informative debugging messages and re-raise.
    Otherwise, return the result.
    """
    import warnings

    if right_name is None:
        right_name = " "
    else:
        right_name = f" {right_name.strip()} "

    try:
        left = left.merge(right, compat="equals", join="exact")
    except ValueError as e:
        # Try to print more debug info
        import re

        name = r"(?P<name>[a-zA-Z0-9_]*)"

        m = None
        for regex in [
            rf"not equal along these coordinates \(dimensions\): '{name}'",
            # Older message, used up to at least 0.21.1:
            rf"indexes along dimension '{name}' are not equal",
        ]:
            m = re.search(regex, str(e))
            if m is not None:
                break
        if m is None:
            warnings.warn(
                f"Unexpected Exception message (expected to match {regex!r}): {e}",
                stacklevel=2,
            )
            raise
        else:
            vn = m.groupdict()["name"]
            print(f"self {vn!r}: dtype={left[vn].dtype}")
            print(left[vn])
            print(f"other {vn!r}: dtype={right[vn].dtype}")
            print(right[vn])
            raise ValueError(
                f"Unable to merge{right_name}due to issue matching coordinates. "
                "See debug messages above the traceback."
            ) from e
    else:
        return left
