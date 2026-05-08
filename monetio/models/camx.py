"""CAMx File Reader"""

import warnings

# from numpy import array, concatenate
import numpy as np
import xarray as xr
from pandas import Series, to_datetime

from ..grids import get_ioapi_pyresample_area_def, grid_from_dataset


def can_do(index):
    if index.max():
        return True
    else:
        return False


def open_mfdataset(
    fname,
    fname_met_3D=None,
    fname_met_2D=None,
    landuse_file=None,
    earth_radius=6370000,
    convert_to_ppb=True,
    drop_duplicates=False,
    var_list=["O3"],
    surf_only=True,
    **kwargs,
):
    """Method to open CAMx IOAPI netcdf files.

    Parameters
    ----------
    fname : string or list
        fname is the path to the file or files.  It will accept hot keys in
        strings as well.
    fname_met: string, list or None
        If string or list, fname_met is used for the meteorological variables
    earth_radius : float
        The earth radius used for the map projection
    convert_to_ppb : boolean
        If true the units of the gas species will be converted to ppbV
    var_list: list
        List of variables to include in output. MELODIES-MONET only reads in
        variables need to plot in order to save on memory and simulation cost
        especially for vertical data
    surf_only: boolean
        Whether to save only surface data to save on memory and computational
        cost (True) or not (False)

    Returns
    -------
    xarray.DataSet
        CAM-X model dataset in standard format for use in MELODIES-MONET
    """

    file_keywords = _choose_xarray_engine_and_keywords(fname)
    dset = xr.open_mfdataset(**file_keywords)
    if surf_only:
        dset = dset.isel(LAY=[0])

    if not surf_only:
        if fname_met_3D is not None:
            file_keywords = _choose_xarray_engine_and_keywords(fname_met_3D)
            with xr.open_mfdataset(**file_keywords) as dset_met:
                dset = add_met_data_3D(dset, dset_met)
            if "alt_agl_m_mid" in dset.variables:
                var_list = var_list + ["alt_agl_m_mid"]
            if "dz_m" in dset.variables:
                var_list = var_list + ["dz_m"]
            if "pres_pa_mid" in dset.variables:
                var_list = var_list + ["pres_pa_mid"]
            if "temperature_k" in dset.variables:
                var_list = var_list + ["temperature_k"]
        else:
            warnings.warn("Filename for meteorological input not provided. Adding only altitude.")
        if (landuse_file is not None) and ("alt_agl_m_mid" in dset.variables):
            file_keywords = _choose_xarray_engine_and_keywords(landuse_file)
            with xr.open_dataset(**file_keywords) as dset_lu:
                if ("topo" in dset_lu.variables) or ("TOPO_M" in dset_lu.variables):
                    dset["alt_msl_m_mid"] = _calc_midlayer_height_msl(dset, dset_lu)

    # get the grid information
    grid = grid_from_dataset(dset, earth_radius=earth_radius)
    area_def = get_ioapi_pyresample_area_def(dset, grid)
    # assign attributes for dataset and all DataArrays
    dset = dset.assign_attrs({"proj4_srs": grid})
    for i in dset.variables:
        dset[i] = dset[i].assign_attrs({"proj4_srs": grid})
        for j in dset[i].attrs:
            dset[i].attrs[j] = dset[i].attrs[j].strip()
        dset[i] = dset[i].assign_attrs({"area": area_def})
    dset = dset.assign_attrs(area=area_def)

    # add lazy diagnostic variables
    if "PM25" in var_list:
        dset = add_lazy_pm25(dset)
    if "PM10" in var_list:
        dset = add_lazy_pm10(dset)
    if "PM_COURSE" in var_list:
        dset = add_lazy_pm_course(dset)
    if "NOy" in var_list:
        dset = add_lazy_noy(dset)
    if "NOx" in var_list:
        dset = add_lazy_nox(dset)

    # get the times
    dset = _get_times(dset)

    # get the lat lon
    dset = _get_latlon(dset)

    # get Predefined mapping tables for observations
    dset = _predefined_mapping_tables(dset)

    # rename dimensions
    dset = dset.rename({"COL": "x", "ROW": "y", "LAY": "z"})

    dset = dset[var_list]

    if convert_to_ppb:
        for varname in dset.variables:
            if "units" in dset[varname].attrs:
                if "mol/mol" in dset[varname].attrs["units"]:
                    dset[varname][:] *= 1e09
                    dset[varname].attrs["units"] = "ppbv"
                elif "ppm" in dset[varname].attrs["units"]:
                    dset[varname][:] *= 1e03
                    dset[varname].attrs["units"] = "ppbv"

    return dset


def _get_times(d):
    idims = len(d.TFLAG.dims)
    if idims == 2:
        tflag1 = Series(d["TFLAG"][:, 0]).astype(str).str.zfill(7)
        tflag2 = Series(d["TFLAG"][:, 1]).astype(str).str.zfill(6)
    else:
        tflag1 = Series(d["TFLAG"][:, 0, 0]).astype(str).str.zfill(7)
        tflag2 = Series(d["TFLAG"][:, 0, 1]).astype(str).str.zfill(6)
    date = to_datetime([i + j for i, j in zip(tflag1, tflag2)], format="%Y%j%H%M%S")
    indexdates = Series(date).drop_duplicates(keep="last").index.values
    d = d.isel(TSTEP=indexdates)
    d["TSTEP"] = date[indexdates]
    return d.rename({"TSTEP": "time"})


def _get_latlon(dset):
    """gets the lat and lons from the pyreample.geometry.AreaDefinition

    Parameters
    ----------
    dset : xarray.Dataset
        Description of parameter `dset`.

    Returns
    -------
    xarray.Dataset
        Description of returned object.

    """
    lon, lat = dset.area.get_lonlats()
    dset["longitude"] = xr.DataArray(lon[::-1, :], dims=["ROW", "COL"])
    dset["latitude"] = xr.DataArray(lat[::-1, :], dims=["ROW", "COL"])
    dset = dset.assign_coords(longitude=dset.longitude, latitude=dset.latitude)
    return dset


def add_met_data_3D(d_chem, d_met):
    """Adds 3D meteorological data

    Parameters
    ----------
    d_chem: xarray.Dataset
        Dataset with the CAM-X output
    d_met: xarrray.Dataset
        Dataset with the CAM-X 3D meteorological input

    Returns
    -------
    xarray.Dataset
        Dataset containing all of the added parameters
    """
    if d_chem.sizes["LAY"] != d_met.sizes["LAY"]:
        raise IndexError(
            "Different layer number in meteorological and chemical datasets."
            + " Maybe one of the is 2D?"
        )

    # d_met has a final TSTEP not present in d_chem
    d_met = d_met.isel(TSTEP=slice(0, len(d_met.TSTEP) - 1))
    if "pressure" in d_met.variables:
        d_chem["pres_pa_mid"] = d_met["pressure"] * 100
    elif "PRESS_MB" in d_met.variables:
        d_chem["pres_pa_mid"] = d_met["PRESS_MB"] * 100
    else:
        warnings.warn("No pressure variable found. PRESS_MB and pressure were tested.")

    if "press_pa_mid" in d_chem.variables:
        d_chem["pres_pa_mid"].attrs = {
            "units": "Pa",
            "long_name": "pressure",
            "var_desc": "pressure",
        }
    if ("z" in d_met.variables) or ("ZGRID_M" in d_met.variables):
        d_chem["alt_agl_m_mid"], d_chem["dz_m"] = _calc_midlayer_height_agl(d_met)
    else:
        warnings.warn("No altitude AGL was found.")

    if "temperature" in d_met.variables:
        d_chem["temperature_k"] = d_met["temperature"]
    elif "TEMP_K" in d_met.variables:
        d_chem["temperature_k"] = d_met["TEMP_K"]
    else:
        warnings.warn("No temperature variable found. TEMP_K and temperature were tested.")
    if "temperature_k" in d_chem.variables:
        d_chem["temperature_k"].attrs["var_desc"] = "Temperature of layer in K."

    return d_chem


# TODO: Add the possibility of adding just 2D meteorological variables
#       Not done right now because of missing toy data to test
#
# def add_met_data_2D(d_chem, d_met, surfpres_only=False):
#     """Adds 2D meteorological data
#
#     Parameters
#     ----------
#     d_chem: xarray.Dataset
#         Dataset with the CAM-X output.
#     d_met: xarray.Dataset
#         Dataset with the CAM-X 2D meteorological input.
#     surfpres_only: boolean
#         Whether to only return the surface pressure.
#         Useful when the temperature is included in the 3D data.
#
#     Returns
#     -------
#     xarray.Dataset
#         Dataset containing all of the added parameters.
#
#     """


def add_lazy_pm25(d):
    """Short summary.

    Parameters
    ----------
    d : xarray.Dataset


    Returns
    -------
    d: xarray
        including PM2.5

    """
    keys = Series([i for i in d.variables])
    allvars = Series(fine)
    if "PM25_TOT" in keys:
        d["PM25"] = d["PM25_TOT"].chunk()
    else:
        index = allvars.isin(keys)
        newkeys = allvars.loc[index]
        d["PM25"] = add_multiple_lazy(d, newkeys)
        d["PM25"].assign_attrs({"name": "PM2.5", "long_name": "PM2.5"})
    return d


def add_lazy_pm10(d):
    """Short summary.

    Parameters
    ----------
    d : xarray.Dataset


    Returns
    -------
    d: xarray
        including PM10

    """
    keys = Series([i for i in d.variables])
    allvars = Series(np.concatenate([fine, coarse]))
    if "PM_TOT" in keys:
        d["PM10"] = d["PM_TOT"].chunk()
    else:
        index = allvars.isin(keys)
        if can_do(index):
            newkeys = allvars.loc[index]
            d["PM10"] = add_multiple_lazy(d, newkeys)
            d["PM10"] = d["PM10"].assign_attrs(
                {"name": "PM10", "long_name": "Particulate Matter < 10 microns"}
            )
    return d


def add_lazy_pm_course(d):
    """Short summary.

    Parameters
    ----------
    d : xarray.Dataset


    Returns
    -------
    d: xarray
        including Course Mode Partilate Matter

    """
    keys = Series([i for i in d.variables])
    allvars = Series(coarse)
    index = allvars.isin(keys)
    if can_do(index):
        newkeys = allvars.loc[index]
        d["PM_COURSE"] = add_multiple_lazy(d, newkeys)
        d["PM_COURSE"] = d["PM_COURSE"].assign_attrs(
            {"name": "PM_COURSE", "long_name": "Course Mode Particulate Matter"}
        )
    return d


def add_lazy_clf(d):
    """Short summary.

    Parameters
    ----------
    d : xarray.Dataset


    Returns
    -------
    d: xarray
        including CLF

    """
    keys = Series([i for i in d.variables])
    allvars = Series(["ACLI", "ACLJ", "ACLK"])
    weights = Series([1, 1, 0.2])
    index = allvars.isin(keys)
    if can_do(index):
        newkeys = allvars.loc[index]
        neww = weights.loc[index]
        d["CLf"] = add_multiple_lazy(d, newkeys, weights=neww)
        d["CLf"] = d["CLf"].assign_attrs({"name": "CLf", "long_name": "Fine Mode particulate Cl"})
    return d


def add_lazy_noy(d):
    """Short summary.

    Parameters
    ----------
    d : xarray.Dataset


    Returns
    -------
    d: xarray
        including NOy

    """
    keys = Series([i for i in d.variables])
    allvars = Series(noy_gas)
    index = allvars.isin(keys)
    if can_do(index):
        newkeys = allvars.loc[index]
        d["NOy"] = add_multiple_lazy(d, newkeys)
        d["NOy"] = d["NOy"].assign_attrs({"name": "NOy", "long_name": "NOy"})
    return d


def add_lazy_nox(d):
    """Short summary.

    Parameters
    ----------
    d : xarray.Dataset


    Returns
    -------
    d: xarray
        including NOx

    """
    keys = Series([i for i in d.variables])
    allvars = Series(["NO", "NOX"])
    index = allvars.isin(keys)
    if can_do(index):
        newkeys = allvars.loc[index]
        d["NOx"] = add_multiple_lazy(d, newkeys)
        d["NOx"] = d["NOx"].assign_attrs({"name": "NOx", "long_name": "NOx"})
    return d


def add_multiple_lazy(dset, variables, weights=None):
    """Short summary.

    Parameters
    ----------
    d : xarray.Dataset


    Returns
    -------
    xarray.Dataset
        including multiple variables

    """
    from numpy import ones

    if weights is None:
        weights = ones(len(variables))
    new = dset[variables[0]].copy() * weights[0]
    for i, j in zip(variables[1:], weights[1:]):
        new = new + dset[i].chunk() * j
    return new


def _calc_midlayer_height_agl(dset):
    """Calculates the midlayer height

    Parameters
    ----------
    dset : xarray.Dataset
        Should include variables 'z' with dims [TSTEP, LAY, ROW, COL]
        and topo with dims [ROW, COL]

    Returns
    ------
    xarray.DataArray
        DataArray with the midlayer height above ground level
    """

    if "z" in dset.variables:
        height = "z"
    elif "ZGRID_M" in dset.variables:
        height = "ZGRID_M"
    else:
        raise "No height variable found, but _calc_midlayer_height_agl was called."
    mid_layer_height = np.array(dset[height])  # height in the layer upper interface of each layer
    layer_height_agl = dset[height]
    layer_height_agl.attrs["long_name"] = "Height AGL at top"
    layer_height_agl.attrs["var_desc"] = "Layer height above ground level at top"
    mid_layer_height[:, 1:, :, :] = (
        mid_layer_height[:, :-1, :, :] + mid_layer_height[:, 1:, :, :]
    ) / 2
    mid_layer_height[0, 0, :, :] = mid_layer_height[0, 0, :, :] / 2
    alt_agl_m_mid = xr.zeros_like(dset[height])
    alt_agl_m_mid[:, :, :, :] = mid_layer_height
    alt_agl_m_mid.attrs["var_desc"] = "Layer height above ground level at midpoint"
    alt_agl_m_mid.attrs["long_name"] = "Height AGL at midpoint"

    dz_m = xr.zeros_like(layer_height_agl)
    dz_m[:, 0, :, :] = layer_height_agl[:, 0, :, :].values
    dz_m[:, 1:, :, :] = layer_height_agl[:, 1:, :, :].values - layer_height_agl[:, :-1, :, :].values
    dz_m.attrs["long_name"] = "dz in meters"
    dz_m.attrs["var_desc"] = "Layer thickness in meters"
    return alt_agl_m_mid, dz_m


def _calc_midlayer_height_msl(dset, dset_lu):
    """Calculates the midlayer height

    Parameters
    ----------
    dset : xarray.Dataset
        Should include variables 'z' with dims [TSTEP, LAY, ROW, COL]
        and topo with dims [ROW, COL]

    Returns
    ------
    xarray.DataArray
        DataArray with the midlayer height above sea level
    """

    nlayers = len(dset["LAY"])
    ntsteps = len(dset["TSTEP"])
    if "alt_agl_m_mid" in dset.keys():
        alt_agl_m_mid = dset["alt_agl_m_mid"]
    else:
        alt_agl_m_mid, _ = _calc_midlayer_height_agl(dset)
    if "topo" in dset_lu:
        topo = "topo"
    else:
        topo = "TOPO_M"
    alt_msl_m_mid = dset["alt_agl_m_mid"] + np.tile(dset[topo].values, (ntsteps, nlayers, 1, 1))
    alt_msl_m_mid.attrs = alt_agl_m_mid.attrs
    alt_msl_m_mid.attrs["var_desc"] = "Layer height above sea level"
    return alt_msl_m_mid


def _predefined_mapping_tables(dset):
    """Predefined mapping tables for different observational parings used when
        combining data.

    Returns
    -------
    dictionary
        A dictionary of to map to.

    """
    to_improve = {}
    to_nadp = {}
    to_aqs = {
        "OZONE": ["O3"],
        "PM2.5": ["PM25"],
        "CO": ["CO"],
        "NOY": [
            "NO",
            "NO2",
            "NO3",
            "N2O5",
            "HONO",
            "HNO3",
            "PAN",
            "PANX",
            "PNA",
            "NTR",
            "CRON",
            "CRN2",
            "CRNO",
            "CRPX",
            "OPAN",
        ],
        "NOX": ["NO", "NO2"],
        "SO2": ["SO2"],
        "NO": ["NO"],
        "NO2": ["NO2"],
        "SO4f": ["PSO4"],
        "PM10": ["PM10"],
        "NO3f": ["PNO3"],
        "ECf": ["PEC"],
        "OCf": ["OC"],
        "ETHANE": ["ETHA"],
        "BENZENE": ["BENZENE"],
        "TOLUENE": ["TOL"],
        "ISOPRENE": ["ISOP"],
        "O-XYLENE": ["XYL"],
        "WS": ["WSPD10"],
        "TEMP": ["TEMP2"],
        "WD": ["WDIR10"],
        "NAf": ["NA"],
        "NH4f": ["PNH4"],
    }
    to_airnow = {
        "OZONE": ["O3"],
        "PM2.5": ["PM25"],
        "CO": ["CO"],
        "NOY": [
            "NO",
            "NO2",
            "NO3",
            "N2O5",
            "HONO",
            "HNO3",
            "PAN",
            "PANX",
            "PNA",
            "NTR",
            "CRON",
            "CRN2",
            "CRNO",
            "CRPX",
            "OPAN",
        ],
        "NOX": ["NO", "NO2"],
        "SO2": ["SO2"],
        "NO": ["NO"],
        "NO2": ["NO2"],
        "SO4f": ["PSO4"],
        "PM10": ["PM10"],
        "NO3f": ["PNO3"],
        "ECf": ["PEC"],
        "OCf": ["OC"],
        "ETHANE": ["ETHA"],
        "BENZENE": ["BENZENE"],
        "TOLUENE": ["TOL"],
        "ISOPRENE": ["ISOP"],
        "O-XYLENE": ["XYL"],
        "WS": ["WSPD10"],
        "TEMP": ["TEMP2"],
        "WD": ["WDIR10"],
        "NAf": ["NA"],
        "NH4f": ["PNH4"],
    }
    to_crn = {}
    to_aeronet = {}
    to_cems = {}
    mapping_tables = {
        "improve": to_improve,
        "aqs": to_aqs,
        "airnow": to_airnow,
        "crn": to_crn,
        "cems": to_cems,
        "nadp": to_nadp,
        "aeronet": to_aeronet,
    }
    dset = dset.assign_attrs({"mapping_tables": mapping_tables})
    return dset


def _choose_xarray_engine_and_keywords(fname):
    """Chooses xarray engine and keywords to open
    model data
    fname: str or list
        List of files that need to be opened
    """
    netcdf_file_extensions = ("nc", "nc4", "nc3", "cdf", "cdf5", "ncf")
    # open the dataset using xarray

    if isinstance(fname, np.ndarray) or isinstance(fname, list):
        check_extension = fname[0]
    else:
        check_extension = fname

    if check_extension.split(".")[-1] in netcdf_file_extensions:
        keywords = {"paths": fname, "engine": "netcdf4"}
    else:
        keywords = {
            "paths": fname,
            "engine": "pseudonetcdf",
            "backend_kwargs": {"format": "uamiv"},
        }
    keywords["combine"] = "nested"
    keywords["concat_dim"] = "TSTEP"
    return keywords


# Arrays for different gasses and pm groupings
coarse = np.array(["CPRM", "CCRS"])
fine = np.array(
    [
        "NA",
        "PSO4",
        "PNO3",
        "PNH4",
        "PCL",
        "PEC",
        "FPRM",
        "FCRS",
        "SOA1",
        "SOA2",
        "SOA3",
        "SOA4",
    ]
)
noy_gas = np.array(
    [
        "NO",
        "NO2",
        "NO3",
        "N2O5",
        "HONO",
        "HNO3",
        "PAN",
        "PANX",
        "PNA",
        "NTR",
        "CRON",
        "CRN2",
        "CRNO",
        "CRPX",
        "OPAN",
    ]
)
poc = np.array(["SOA1", "SOA2", "SOA3", "SOA4"])
