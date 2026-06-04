"""

Generic unstructured model grid reader. Takes the CESM_SE reader and generalizes for the ability to read
MPAS 

"""

import xarray as xr
import uxarray as ux
# integrate uxarray here
# conda install -c conda-forge uxarray

def open_mfdataset(
    fname,
    earth_radius=6370000,
    convert_to_ppb=True,
    var_list=["O3", "NO", "NO2", "lat", "lon"],
    scrip_file=None,
    mesh_file=None,
    **kwargs):
    """

    Generic unstrucutred CAM reader - supports cesm-se and mpas

    uxarray auto detects the grid geometry either from a scrip file (CESM-SE) of the native MESH file (MPAS)
    
    Method to open multiple (or single) CESM SE netcdf files.
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


    Returns
    -------
    xarray.DataSet


    """
    # check that the files are netcdf format
    names, netcdf = _ensure_mfdataset_filenames(fname)

    # Grid geometry: SCRIP (CESM-SE) or MPAS mesh/init file. Either is fine.
    ux_grid_path = scrip_file or mesh_file
    if ux_grid_path is None:
        raise ValueError(
            "Unstructured reader requires a grid file: set 'scrip_file:' "
            "(SCRIP, e.g. CESM-SE) or 'mesh_file:' (MPAS init) in your YAML."
        )

    try:
        print(f"Opening unstructured grid with UXArray: {ux_grid_path}")
        dset_load = ux.open_mfdataset(ux_grid_path, fname, **kwargs)
    except Exception as e:
        print("ERROR while opening dataset:")
        print(repr(e))
        raise

    for _c in ("lat", "lon", "lev"):
        if _c not in var_list:
            var_list.append(_c)

    # variables for cesm-se specific derivations
    _src_vars = []
    
    # Always request the source variables needed for standardized derivations
    # Filtered against what's actually present, so a missing one just
    # skips the corresponding derivation
    
    for _v in ("hyam", "hybm", "PS", "P0", "T", "PDELDRY", "PMID"):
        if _v not in var_list:
            var_list.append(_v)
            _src_vars.append(_v)

    # filter to vars present and then warn about vars missing rather than generic rename error
    _present = [v for v in var_list if v in dset_load.variables]
    _missing = [v for v in var_list if v not in dset_load.variables]
    if _missing:
        print(
            f"unstructured reader: requested vars not in {names[0]!r}: "
            f"{_missing}. Continuing with: {_present}."
        )
    dset = dset_load[_present]

    # vertical: detect height (MPAS zeta, m) v. pressure hybrid in CESM

    ####### NOTE for MPAS height / zeta in m increases upwards

    _lev_attrs = dset["lev"].attrs if "lev" in dset.variables else {}
    _lev_units = str(_lev_attrs.get("units", "")).strip().lower()
    _lev_long = str(_lev_attrs.get("long_name", "")).lower()
    _is_height = (
        _lev_units in ("m", "meter", "meters")
        or "zeta" in _lev_long
        or "height" in _lev_long)
        
    dset = dset.rename({"lev": "z"})
    
    # re-order so surface is associated with the first vertical index
    dset = dset.sortby("z", ascending=_is_height)

    # longitude/latitude can come out broadcast across time when multiple
    # files are concatenated via xr.open_mfdataset. Each time slice has
    # identical values; collapse to 1-D so downstream consumers (esp.
    # back_to_modgrid sampling) get a proper coord.
    for _c in ("longitude", "latitude", "lat", "lon"):
        if _c in dset.variables and dset[_c].ndim > 1:
            _col_dims = [d for d in dset[_c].dims if d in ("ncol", "n_face", "n_node")]
            _col_dim = _col_dims[0] if _col_dims else dset[_c].dims[-1]
            dset[_c] = dset[_c].isel({d: 0 for d in dset[_c].dims if d != _col_dim})
    # ===========================

    # Derive MM-standardized variables from CESM-SE native fields.
    # Source vars are optional: missing inputs then derivation skipped,
    
    # pres_pa_mid: hybrid sigma-pressure midpoint (Pa)
    #   P = hyam*P0 + hybm*PS    (standard CAM hybrid coords)
    if "pres_pa_mid" not in dset.variables:
        if "PMID" in dset.variables:
            dset["pres_pa_mid"] = dset["PMID"]
            dset["pres_pa_mid"].attrs.update(
                {"units": "Pa", "long_name": "Pressure at mid-level",
                 "description": "PMID (provided)"})
        elif {"hyam", "hybm", "PS"} <= set(dset.variables):
            _P0 = float(dset["P0"].values) if "P0" in dset.variables else 100000.0
            dset["pres_pa_mid"] = dset["hyam"] * _P0 + dset["hybm"] * dset["PS"]
            dset["pres_pa_mid"].attrs.update(
                {"units": "Pa", "long_name": "Pressure at mid-level",
                 "description": "hyam*P0 + hybm*PS"})
    
    # temperature_k
    if "temperature_k" not in dset.variables and "T" in dset.variables:
        dset["temperature_k"] = dset["T"]
        dset["temperature_k"].attrs.setdefault("units", "K")
    
    # dz_m: layer thickness (m) via hydrostatic + ideal gas
    #   dz = PDELDRY * Rd * T / (P_mid * g)
    if (
        "dz_m" not in dset.variables
        and {"PDELDRY", "pres_pa_mid", "temperature_k"} <= set(dset.variables)
    ):
        _Rd, _g = 287.04, 9.80665
        dset["dz_m"] = (
            dset["PDELDRY"] * _Rd * dset["temperature_k"]
            / (dset["pres_pa_mid"] * _g)
        )
        dset["dz_m"].attrs.update(
            {"units": "m", "long_name": "Layer thickness",
             "description": "PDELDRY * Rd * T / (P_mid * g)"})

    # once derivations are complete, dont keep in returned dataset
    for _v in _src_vars:
        if _v in dset.variables:
            dset = dset.drop_vars(_v)

    # Make sure this dataset has unstructured grid
    dset.attrs["mio_has_unstructured_grid"] = True
    if scrip_file:
        dset.attrs["mio_scrip_file"] = scrip_file
    if mesh_file:
        dset.attrs["mio_mesh_file"] = mesh_file

    # convert units
    if convert_to_ppb:
        for i in dset.variables:
            if "units" in dset[i].attrs:
                # convert all gas species from mol/mol to ppbv
                if "mol/mol" in dset[i].attrs["units"]:
                    dset[i] *= 1e09
                    dset[i].attrs["units"] = "ppbV"
                # convert 'kg/m3 to \mu g/m3 '
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
