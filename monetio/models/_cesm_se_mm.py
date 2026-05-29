"""CESM File Reader"""

import xarray as xr
import uxarray as ux
# integrate uxarray here
# conda install -c conda-forge uxarray

def open_mfdataset(
    fname,
    earth_radius=6370000,
    convert_to_ppb=True,
    var_list=["O3", "NO", "NO2", "lat", "lon"],
    scrip_file= None,
    **kwargs):
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


    Returns
    -------
    xarray.DataSet


    """
    # check that the files are netcdf format
    names, netcdf = _ensure_mfdataset_filenames(fname)

    if scrip_file is None:
        raise ValueError(
            "CESM-SE requires a scrip_file (set 'scrip_file:' in your YAML).")
    ux_grid_path = scrip_file
        
    # open the dataset using xarray
    # try:
    #     if ux_grid_path:
    #         print(f"Opening unstructured grid with UXArray: {ux_grid_path}")
    #         dset_load = ux.open_mfdataset(ux_grid_path, fname, **kwargs)
    #     elif netcdf:
    #         print("Opening Xarray...")
    #         dset_load = xr.open_mfdataset(fname, **kwargs)
    #     else:
    #         raise ValueError(
    #             "File format not recognized. Files should be in netcdf format; "
    #             "do not mix file types."
    #         )
    # except Exception as e:
    #     print("ERROR while opening dataset:")
    #     print(repr(e))
    #     raise

    try:
        print(f"Opening unstructured grid with UXArray: {ux_grid_path}")
        dset_load = ux.open_mfdataset(ux_grid_path, fname, **kwargs)
    except Exception as e:
        print("ERROR while opening dataset:")
        print(repr(e))
        raise

    # To keep lat & lon variables in the dataset
    if "lat" not in var_list:
        var_list.append("lat")
    if "lon" not in var_list:
        var_list.append("lon")
    if "lev" not in var_list:
        var_list.append("lev")

    # variables for cesm-se specific derivations
    _cesm_se_var = []
    
    # Always request the source variables needed for standardized derivations
    # Filtered against what's actually present, so a missing one just
    # skips the corresponding derivation
    
    for _v in ("hyam", "hybm", "PS", "P0", "T", "PDELDRY"):
        if _v not in var_list:
            var_list.append(_v)
            _cesm_se_var.append(_v)

    # filter to vars present and then warn about vars missing rather than generic rename error
    _requested = list(var_list)
    _present = [v for v in _requested if v in dset_load.variables]
    _missing = [v for v in _requested if v not in dset_load.variables]

    if _missing:
        print(
            f"CESM-SE: requested variables not found in {names[0]!r}: "
            f"{_missing}. Continuing with what's available: {_present}."
        )
    dset = dset_load[_present]

    # ===========================
    # Process the loaded data
    # extract variables of choice
    #dset = dset_load.get(var_list)
    # rename altitude variable to z for monet use
    dset = dset.rename({"lev": "z"})
    
    # re-order so surface is associated with the first vertical index
    dset = dset.sortby("z", ascending=False)
    # ===========================

    # Derive MM-standardized variables from CESM-SE native fields.
    # Source vars are optional: missing inputs then derivation skipped,
    
    # pres_pa_mid: hybrid sigma-pressure midpoint (Pa)
    #   P = hyam*P0 + hybm*PS    (standard CAM hybrid coords)
    if "pres_pa_mid" not in dset.variables and {"hyam", "hybm", "PS"} <= set(dset.variables):
        _P0 = float(dset["P0"].values) if "P0" in dset.variables else 100000.0
        dset["pres_pa_mid"] = dset["hyam"] * _P0 + dset["hybm"] * dset["PS"]
        dset["pres_pa_mid"].attrs.update({
            "units": "Pa",
            "long_name": "Pressure at mid-level",
            "description": "hyam*P0 + hybm*PS",
        })
    
    # temperature_k
    if "temperature_k" not in dset.variables and "T" in dset.variables:
        dset["temperature_k"] = dset["T"]
        dset["temperature_k"].attrs.setdefault("units", "K")
    
    # dz_m: layer thickness (m) via hydrostatic + ideal gas
    #   dz = PDELDRY * Rd * T / (P_mid * g)
    if (
        "dz_m" not in dset.variables
        and "PDELDRY" in dset.variables
        and "pres_pa_mid" in dset.variables
        and "temperature_k" in dset.variables
    ):
        _Rd = 287.04   # J/(kg*K), dry air
        _g = 9.80665   # m/s^2
        dset["dz_m"] = (
            dset["PDELDRY"] * _Rd * dset["temperature_k"]
            / (dset["pres_pa_mid"] * _g)
        )
        dset["dz_m"].attrs.update({
            "units": "m",
            "long_name": "Layer thickness",
            "description": "PDELDRY * Rd * T / (P_mid * g)",
        })

    # once derivations are complete, dont keep in returned dataset
    for _v in _cesm_se_var:
        if _v in dset.variables:
            dset = dset.drop_vars(_v)

    # Make sure this dataset has unstructured grid
    dset.attrs["mio_has_unstructured_grid"] = True
    if scrip_file:
        dset.attrs["mio_scrip_file"] = scrip_file

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

    # dset_scrip = xr.open_dataset( scrip_file )
    # return dset, dset_scrip
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
