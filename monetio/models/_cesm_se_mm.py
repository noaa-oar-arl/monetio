"""CESM File Reader"""

import xarray as xr
import warnings
import numpy as np
from xregrid import Regridder, create_global_grid
from numpy import meshgrid

def open_mfdataset(
    fname,
    earth_radius=6370000,
    convert_to_ppb=True,
    var_list=["O3", "NO", "NO2", "lat", "lon"],
    scrip_file="",
    mesh_file="",
    regrid_method ='nearest_s2d',
    lat_resolution = 0.1,
    lon_resolution = 0.1,
    surf_only=False,
    **kwargs,
):
    """Method to open multiple (or single) CESM SE netcdf files.
       This method extends the xarray.open_mfdataset functionality
       It is the main method called by the driver. Other functions defined
       in this file are internally called by open_mfdataset and are proceeded
       by an underscore (e.g. _get_latlon).

    Parameters
    ----------
    fname : string or list
        fname is the path to the file or files.  It will accept wildcards in
        strings as well.
    earth_radius : float
        The earth radius used for map projections
    convert_to_ppb : boolean
        If true the units of the gas species will be converted to ppbV
        and units of aerosols to ug m^-3
    var_list : string or list
        List of variables to load from the CESM file. Default is to load ozone (O3) and PM2.5 (PM25).
    scrip_file: string
        Scrip file path for unstructured grid output
    mesh_file: string
        Mesh file path for unstructured grid output
    regrid_method: string
        Regrid method for Xregrid to regrid unstructured data into regular lat/lon grid
    lat_resolution: float
        Latitude resolution for regrided output 
    lon_resolution: float
        Longitude resolution for regrided output
    
    Returns
    -------
    xarray.DataSet


    """

    ## copied from -cesm-fv_mm.py ##
    ## trying to add dz_m for cesm_se for vertial interpolation ##
    
    # ==========================================================
    # Open dataset
    # ==========================================================
    
    names, netcdf = _ensure_mfdataset_filenames(fname)
    
    try:
        if netcdf:
    
            dset_load = xr.open_mfdataset(fname, **kwargs)
    
            if scrip_file:
                scrip_dset_load = xr.open_dataset(scrip_file)
    
            if mesh_file:
                mesh_dset_load = xr.open_dataset(mesh_file)
    
        else:
            raise ValueError
    
    except ValueError:
        print(
            """File format not recognized. Note that files should be in netcdf
               format. Do not mix and match file types."""
        )
    
    # ==========================================================
    # CASE 1: surface only
    # Keep original unstructured dataset
    # ==========================================================
    
    if surf_only:
    
        # keep coordinates
        for coord in ["lat", "lon", "lev"]:
    
            if coord not in var_list:
                var_list.append(coord)
    
        # extract variables
        dset = dset_load[var_list].copy()
    
        # rename vertical coordinate
        dset = dset.rename(
            {
                "lev": "z"
            }
        )
    
        # surface first
        dset = dset.sortby(
            "z",
            ascending=False
        )
    
    # ==========================================================
    # CASE 2: 3D processing
    # Regrid to structured grid
    # ==========================================================
    
    else:
        
        warnings.warn(
            "3D data processing is still experimental in CESM-SE (CAM-Chem), "
            + "and has not been properly tested. Use at own risk."
        )
        
        # ------------------------------------------------------
        # Convert MUSICA/CESM-SE to MPAS-like format
        # ------------------------------------------------------
    
        dset_mpas_like = _create_mpas_like_ds(
            dset_load,
            mesh_dset_load
        )
    
        # ------------------------------------------------------
        # Regrid
        # ------------------------------------------------------
    
        warnings.warn(
            "Regriding to structured Dataset is still experimental in CESM-SE "
            + "(CAM-Chem), currently using nearest_s2d at 0.1 degree."
        )

        # ======================================================
        # Keep only necessary variables BEFORE regridding
        # ======================================================
        
        required_vars = set(var_list)
        
        # variables needed later for vertical calculations
        required_vars.update([
            "T",
            "Z3",
            "PMID",
            "PS",
            "PHIS",
            "hyai",
            "hybi",
            "PDELDRY",
        ])
        
        # only keep variables that actually exist
        required_vars = [
            v for v in required_vars
            if v in dset_mpas_like.variables
        ]
        
        # subset BEFORE regridding
        dset_mpas_like = dset_mpas_like[required_vars]
    
        dset_structured = _regrid_to_structured(
            dset_mpas_like,
            lat_resolution,
            lon_resolution,
            regrid_method,
        )
    
        # ======================================================
        # Restore global attrs
        # ======================================================
        dset_structured.attrs = dset_load.attrs.copy()
        
        # ======================================================
        # Restore variable attrs + encoding
        # ======================================================
        for var in dset_structured.data_vars:
        
            if var in dset_load.data_vars:
        
                dset_structured[var].attrs = (
                    dset_load[var].attrs.copy()
                )
        
                dset_structured[var].encoding = (
                    dset_load[var].encoding.copy()
                )
        
        # ======================================================
        # Restore coordinate attrs
        # ======================================================
        for coord in dset_structured.coords:
        
            if coord in dset_load.coords:
        
                dset_structured[coord].attrs = (
                    dset_load[coord].attrs.copy()
                )
        
                dset_structured[coord].encoding = (
                    dset_load[coord].encoding.copy()
                )
    
        # ------------------------------------------------------
        # Vertical processing
        # ------------------------------------------------------
    
        if "PMID" not in dset_load.keys():
            dset_structured["PMID"] = _calc_pressure(dset_structured)
    
        if "Z3" not in dset_load.keys():
    
            warnings.warn(
                "Geopotential height Z3 not found. "
                + "Assuming hydrostatic runs."
            )
    
            dset_structured["Z3"] = (
                _calc_hydrostatic_height(dset_structured)
            )
    
        if "PS" in dset_load.keys():
            dset_structured["PS"] = (
                dset_structured["PS"].rename("surfpres_pa")
            )
        else:
            warnings.warn(
                "Surface pressure (PS) not found."
            )
    
        # ------------------------------------------------------
        # Height above ground
        # ------------------------------------------------------
    
        if "PHIS" in dset_load.keys():
    
            dset_structured["alt_agl_m_mid"] = (
                dset_structured["Z3"]
                - dset_structured["PHIS"] / 9.80665
            )
    
            dset_structured["alt_agl_m_mid"].attrs = {
                "description": (
                    "geopotential height above ground level"
                ),
                "units": "m",
            }
    
        else:
            warnings.warn("PHIS not found.")
    
        # ------------------------------------------------------
        # Layer thickness
        # ------------------------------------------------------
    
        if {"hyai", "hybi", "PHIS"} <= dset_load.keys():
    
            dset_structured["pres_pa_int"] = (
                _calc_pressure_i(dset_structured)
            )
    
            dset_structured["dz_m"] = (
                _calc_layer_thickness_i(dset_structured)
            )
    
            var_list.append("dz_m")
    
        elif {"PDELDRY"} <= dset_load.keys():
    
            dset_structured["dz_m"] = (
                _calc_layer_thickness_mid(dset_structured)
            )
    
            var_list.append("dz_m")
    
        else:
    
            print(
                "Cannot calculate dz_m. Missing hyai/hybi "
                + "or PDELDRY."
            )
    
        # ------------------------------------------------------
        # Rename variables
        # ------------------------------------------------------
    
        dset_structured = dset_structured.rename(
            {
                "T": "temperature_k",
                "Z3": "alt_msl_m_mid",
                "PMID": "pres_pa_mid",
            }
        )
    
        var_list = var_list + [
            "temperature_k",
            "alt_msl_m_mid",
            "pres_pa_mid",
        ]
    
        # ------------------------------------------------------
        # Keep coordinates
        # ------------------------------------------------------
    
        for coord in ["lat", "lon", "lev"]:
    
            if coord not in var_list:
                var_list.append(coord)
    
        # ------------------------------------------------------
        # Extract variables
        # ------------------------------------------------------
    
        dset = dset_structured[var_list]
    
        # ------------------------------------------------------
        # Rename dimensions for MONET
        # ------------------------------------------------------
    
        dset = dset.rename_dims(
            {
                "lon": "x",
                "lat": "y",
                "lev": "z",
            }
        )
    
        # ------------------------------------------------------
        # Convert longitude
        # ------------------------------------------------------
    
        if np.any(dset["lon"] > 180):
    
            dset["lon"] = (
                (dset["lon"] + 180) % 360
            ) - 180
    
            dset = dset.sortby("lon")
    
        # ------------------------------------------------------
        # Create 2D lat/lon
        # ------------------------------------------------------
    
        lon = dset["lon"]
        lat = dset["lat"]
    
        lats, lons = xr.broadcast(lat, lon)
    
        dset["longitude"] = (("y", "x"), lons.data)
        dset["latitude"] = (("y", "x"), lats.data)
    
        dset = dset.reset_coords()
    
        dset = dset.set_coords(
            ["latitude", "longitude"]
        )
    
        # ------------------------------------------------------
        # Surface-first ordering
        # ------------------------------------------------------
    
        dset = dset.sortby("z", ascending=False)
    
        # ------------------------------------------------------
        # Remove original 1D coords
        # ------------------------------------------------------
    
        dset = dset.drop_vars(["lat", "lon","nEdgesOnCell", "lonCell", "latCell"])
    
    # ==========================================================
    # Shared processing
    # ==========================================================
    
    dset.attrs["mio_has_unstructured_grid"] = True

    dset.attrs["mio_scrip_file"] = scrip_file
    dset.attrs["mio_mesh_file"] = mesh_file
    
    # ==========================================================
    # Unit conversion
    # ==========================================================
    
    if convert_to_ppb:
    
        for i in dset.variables:
    
            if "units" in dset[i].attrs:
    
                # mol/mol -> ppbv
                if "mol/mol" in dset[i].attrs["units"]:
    
                    dset[i] *= 1e09
                    dset[i].attrs["units"] = "ppbV"
    
                # kg/m3 -> ug/m3
                elif "kg/m3" in dset[i].attrs["units"]:
    
                    dset[i] *= 1e09
                    dset[i].attrs["units"] = r"$\mu g m^{-3}$"
    
    return dset

# -----------------------------------------
# Below are internal functions to this file
# -----------------------------------------


def _ensure_mfdataset_filenames(fname):
    """Checks if dataset in netcdf format
    Parameters
    ----------
    fname : string or list of strings
    Returns
    -------
    type
    """
    from glob import glob

    from numpy import sort

    if isinstance(fname, str):
        names = sort(glob(fname))
    else:
        names = sort(fname)
    netcdfs = [True for i in names if "nc" in i]
    netcdf = False
    if len(netcdfs) >= 1:
        netcdf = True
    return names, netcdf

def unwrap_cell(lon):
    lon = lon.copy()
    ref = lon[0]
    for i in range(1, len(lon)):
        diff = lon[i] - ref
        if diff > 180:
            lon[i] -= 360
        elif diff < -180:
            lon[i] += 360
    return lon

def _create_mpas_like_ds(ds, mesh_ds):
    """
    Convert a given CESM-SE or MUSICA-like Dataset `ds` into an MPAS-like Dataset suitable for xregrid.
    Automatically handles polygon unwrap, unique vertex creation, and vertices mapping, 
    while keeping all original variables. Uses SCRIP file's grid center coordinates.

    Parameters
    ----------
    ds : xarray.Dataset
        Original dataset, dimensions may include ncol, lev, time, etc.
    mesh_ds : xarray.Dataset
        Mesh dataset, must contain grid connectivity.
        
    Returns
    -------
    ds_mpas_like : xarray.Dataset
        MPAS-like dataset containing latCell/lonCell, latVertex/lonVertex, verticesOnCell, nEdgesOnCell,
        and all original variables from `ds`.
    """
    data_vars = {
    varname: ds[varname]
    for varname in ds.data_vars
}
    
    # -----------------------------
    # making new coords
    # -----------------------------
    coords = {
        "time": ds["time"],
        "lev": ds["lev"],
    
        "latCell": (
            ["ncol"],
            np.deg2rad(mesh_ds.centerCoords[:,1].values),
            {"units": "radians"}
        ),
    
        "lonCell": (
            ["ncol"],
            np.deg2rad(mesh_ds.centerCoords[:,0].values),
            {"units": "radians"}
        ),
    
        "latVertex": (
            ["nVertices"],
            np.deg2rad(mesh_ds.nodeCoords[:,1].values),
            {"units": "radians"}
        ),
    
        "lonVertex": (
            ["nVertices"],
            np.deg2rad(mesh_ds.nodeCoords[:,0].values),
            {"units": "radians"}
        ),
    
        "verticesOnCell": (
            ["ncol", "maxNodes"],
            mesh_ds.elementConn.values
        ),
    
        "nEdgesOnCell": (
            ["ncol"],
            mesh_ds.numElementConn.values.astype(int)
        ),
    }
    
    # -----------------------------
    # making new Dataset
    # -----------------------------
    ds_mpas_like = xr.Dataset(
        data_vars={
            v: ds[v]
            for v in ds.data_vars
        },
        coords=coords,
    )

    return ds_mpas_like

def _regrid_to_structured(ds_mpas_like, lat_resolution, lon_resolution, regrid_method):
    """
    Convert a MPAS-like Dataset `ds` into an structured grid dataset by xregrid.
    Currently using predefined resolution of 0.5 degree.

    Parameters
    ----------
    ds_mpas_like : xarray.Dataset
        MPAS-like dataset, dimensions like containing latCell/lonCell, latVertex/lonVertex, verticesOnCell, nEdgesOnCell.
    
    Returns
    -------
    ds_structured : xarray.Dataset
        Structured grid dataset containing with lat and lon in predefined resolution,
        and all original variables from `ds`.
    """
    # Define a rectilinear target grid
    target_grid = create_global_grid(lat_resolution, lon_resolution)
    
    # Create the regridder using the 'conservative' method
    # XRegrid will detect the MPAS connectivity and use ESMF Mesh
    regridder = Regridder(ds_mpas_like, target_grid, method=regrid_method, periodic=True)
    
    # Apply regridding
    ds_structured = regridder(ds_mpas_like)
    
    return ds_structured


def _calc_pressure(dset):
    """Vectorized hybrid pressure (memory + speed optimized, preserves lev dim)"""

    presvars = ["PS", "hyam", "hybm"]
    if not all(pvar in dset.keys() for pvar in presvars):
        raise KeyError(
            "The model does not have the variables to calculate "
            "the pressure. This can be done either with PMID or with "
            "P0, PS, hyam and hybm. "
            "If the vertical coordinate is not needed, set surface_only=True"
        )

    PS = dset["PS"]
    hyam = dset["hyam"]
    hybm = dset["hybm"]

    # -------------------------
    # P0 handling
    # -------------------------
    if "P0" not in dset:
        warnings.warn("P0 not in netcdf keys, assuming 100_000 Pa")
        p0 = 100000.0
    else:
        p0 = dset["P0"]

    # -------------------------
    # ensure vertical coordinate consistency
    # -------------------------
    vert = hyam.lev.values
    time = PS.time.values
    lat = PS.lat.values
    lon = PS.lon.values

    n_vert = len(vert)

    # -------------------------
    # fix time-dependent hybrid coords
    # -------------------------
    if "time" in hyam.dims:
        hyam = hyam.isel(time=0)
        hybm = hybm.isel(time=0)

    # -------------------------
    # vectorized computation (NO LOOP)
    # -------------------------

    # shape:
    # hyam: (lev,)
    # PS: (time, lat, lon)

    pressure = (
        hyam.values[:, None, None] * p0
        + hybm.values[:, None, None] * PS.values[:, None, :, :]
    )

    # reorder to (time, lev, lat, lon)
    pressure = np.transpose(pressure, (0, 1, 2, 3))

    # -------------------------
    # keep exact CESM dimension structure
    # -------------------------
    P = xr.DataArray(
        data=pressure,
        dims=["time", "lev", "lat", "lon"],
        coords={
            "time": time,
            "lev": vert,   # ✔ MUST remain 32
            "lat": lat,
            "lon": lon,
        },
        attrs={
            "description": "Mid layer pressure",
            "units": "Pa",
        },
    )

    return P


def _calc_pressure_i(dset):
    """Vectorized interface pressure (fast + memory safe, preserves ilev)"""

    presvars = ["PS", "hyai", "hybi"]
    if not all(pvar in dset.keys() for pvar in presvars):
        raise KeyError(
            "The model does not have the variables to calculate "
            "the interface pressure. Need PS, hyai, hybi."
        )

    PS = dset["PS"]
    hyai = dset["hyai"]
    hybi = dset["hybi"]

    # -------------------------
    # P0 handling
    # -------------------------
    if "P0" not in dset:
        warnings.warn("P0 not in netcdf keys, assuming 100_000 Pa")
        p0 = 100000.0
    else:
        p0 = dset["P0"]

    # -------------------------
    # coordinates
    # -------------------------
    vert = hyai.ilev.values
    time = PS.time.values
    lat = PS.lat.values
    lon = PS.lon.values

    # -------------------------
    # fix hybrid time dimension if exists
    # -------------------------
    if "time" in hyai.dims:
        hyai = hyai.isel(time=0)
        hybi = hybi.isel(time=0)

    # -------------------------
    # vectorized computation
    # -------------------------
    # shapes:
    # hyai: (ilev,)
    # hybi: (ilev,)
    # PS:   (time, lat, lon)

    pressure_i = (
        hyai.values[:, None, None, None] * p0
        + hybi.values[:, None, None, None] * PS.values[None, :, :, :]
    )

    # -------------------------
    # build DataArray (NO transpose)
    # -------------------------
    P_int = xr.DataArray(
        data=pressure_i,
        dims=["ilev", "time", "lat", "lon"],
        coords={
            "ilev": vert,
            "time": time,
            "lat": lat,
            "lon": lon,
        },
        attrs={
            "description": "Interface layer pressure",
            "units": "Pa",
        },
    )

    # -------------------------
    # reorder to standard CESM layout
    # -------------------------
    P_int = P_int.transpose("time", "ilev", "lat", "lon")

    return P_int


def _calc_hydrostatic_height(dset):
    """Vectorized + stable hydrostatic height (keeps CESM structure)"""

    R = 8.314
    M_AIR = 0.028
    GRAVITY = 9.80665

    PMID = dset["PMID"]
    T = dset["T"]

    time = PMID.time.values
    vert = PMID.lev.values
    lat = PMID.lat.values
    lon = PMID.lon.values

    n_vert = len(vert)

    # -------------------------
    # check vertical ordering
    # -------------------------
    if not np.all(vert[:-1] < vert[1:]):
        raise ValueError(
            "Expected CESM ordering: pressure increasing with level index"
        )

    # -------------------------
    # preload arrays (avoid repeated xarray access)
    # -------------------------
    pmid = PMID.values
    temp = T.values
    phis = dset["PHIS"].values

    # -------------------------
    # allocate output
    # -------------------------
    height = np.empty_like(pmid)

    # bottom boundary (surface geopotential height)
    height[:, -1, :, :] = phis / GRAVITY

    # -------------------------
    # vertical recursion (only loop we cannot avoid physically)
    # -------------------------
    for k in range(n_vert - 2, -1, -1):

        height[:, k, :, :] = (
            height[:, k + 1, :, :]
            - (R / (GRAVITY * M_AIR))
            * temp[:, k, :, :]
            * np.log(pmid[:, k, :, :] / pmid[:, k + 1, :, :])
        )

    # -------------------------
    # build DataArray
    # -------------------------
    z = xr.DataArray(
        data=height,
        dims=["time", "lev", "lat", "lon"],
        coords={
            "time": time,
            "lev": vert,
            "lat": lat,
            "lon": lon,
        },
        attrs={
            "description": "Mid layer (hydrostatic) height",
            "units": "m",
        },
    )

    return z


def _calc_hydrostatic_height_i(dset):
    """Vectorized + stable interface hydrostatic height (ilev preserved)"""

    R = 8.314
    M_AIR = 0.029
    GRAVITY = 9.80665

    # -------------------------
    # coordinates
    # -------------------------
    time = dset.time.values
    ilev = dset.ilev.values
    lat = dset.lat.values
    lon = dset.lon.values

    n_ilev = len(ilev)

    # -------------------------
    # sanity check (CESM ordering)
    # -------------------------
    if not np.all(ilev[:-1] < ilev[1:]):
        raise ValueError(
            "Expected CESM interface ordering: increasing pressure with level index"
        )

    # -------------------------
    # preload arrays (CRITICAL optimization)
    # -------------------------
    t = dset["T"].values                     # (time, lev)
    p_int = dset["pres_pa_int"].values       # (time, ilev)
    phis = dset["PHIS"].values

    # -------------------------
    # output
    # -------------------------
    height = np.empty_like(p_int)

    # bottom boundary (surface geopotential height)
    height[:, -1, :, :] = phis / GRAVITY

    # -------------------------
    # vertical recursion (physics required)
    # -------------------------
    for k in range(n_ilev - 2, -1, -1):

        # vectorized horizontal computation
        height[:, k, :, :] = (
            height[:, k + 1, :, :]
            - (R / (GRAVITY * M_AIR))
            * t[:, k, :, :]
            * np.log(
                p_int[:, k, :, :] / p_int[:, k + 1, :, :]
            )
        )

    # -------------------------
    # wrap result
    # -------------------------
    z = xr.DataArray(
        data=height,
        dims=["time", "ilev", "lat", "lon"],
        coords={
            "time": time,
            "ilev": ilev,   # ✔ MUST remain 33
            "lat": lat,
            "lon": lon,
        },
        attrs={
            "description": "Interface Layer (hydrostatic) Height",
            "units": "m",
        },
    )

    return z


def _calc_layer_thickness_i(dset):
    """
    Fast vectorized layer thickness calculation
    using interface heights.

    Returns
    -------
    xr.DataArray
        dz_m(time, lev, lat, lon)
    """

    # -----------------------------------
    # interface heights
    # shape:
    # (time, ilev=33, lat, lon)
    # -----------------------------------
    z_int = _calc_hydrostatic_height_i(dset)

    # -----------------------------------
    # vectorized layer thickness
    #
    # dz[k] = z[k] - z[k+1]
    #
    # result:
    # (time, lev=32, lat, lon)
    # -----------------------------------

    dz = (
        z_int.data[:, :-1, :, :]
        -
        z_int.data[:, 1:, :, :]
    )

    # -----------------------------------
    # build xarray
    # -----------------------------------

    dz_m = xr.DataArray(
        data=dz,
        dims=["time", "lev", "lat", "lon"],
        coords={
            "time": dset.time,
            "lev": dset.lev,
            "lat": dset.lat,
            "lon": dset.lon,
        },
        attrs={
            "description": "Layer Thickness (based on interface pressure)",
            "units": "m",
        },
    )

    return dz_m


def _calc_layer_thickness_mid(dset):
    """
    Midpoint-based layer thickness (fully vectorized, preserves lev=32)
    """

    GRAVITY = 9.80665
    RGAS = 287.04

    # -------------------------
    # pull numpy arrays once
    # -------------------------
    dp = dset["PDELDRY"].values   # (time, lev, lat, lon)
    temp = dset["T"].values       # (time, lev, lat, lon)
    pmid = dset["PMID"].values    # (time, lev, lat, lon)

    # -------------------------
    # density
    # ρ = p / (R T)
    # -------------------------
    rho = pmid / (RGAS * temp)

    # -------------------------
    # hydrostatic thickness
    # dz = dp / (ρ g)
    # -------------------------
    dz = dp / (rho * GRAVITY)

    # -------------------------
    # IMPORTANT: enforce full lev dimension (32)
    # -------------------------
    lev = dset.lev.values

    dz_m = xr.DataArray(
        data=dz,
        dims=["time", "lev", "lat", "lon"],
        coords={"time": dset.time, "lev": dset.lev, "lat": dset.lat, "lon": dset.lon},
        attrs={"description": "Layer Thickness (based on interface pressure)", "units": "m"},
    )
    return dz_m