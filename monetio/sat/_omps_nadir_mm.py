def read_OMPS_nm(files):
    """Loop to open OMPS nadir mapper L2 files.
    Files can be pre-downloaded or streamed from NASA opendap server with list of urls.

    Parameters:
    -----------
    files: string or list of strings

    returns xarray Dataset.
    """
    from glob import glob

    import xarray as xr

    count = 0
    print(files)
    # Check if files are url
    if "https" in files[0]:

        filelist = sorted(files)
        for filename in filelist:
            data = extract_OMPS_nm_opendap(filename)
            # print(data)
            if count == 0:
                data_array = data
                count += 1
            else:
                data_array = xr.concat([data_array, data], "x")
    else:
        filelist = sorted(glob(files, recursive=False))

        for filename in filelist:
            data = extract_OMPS_nm(filename)

            if count == 0:
                data_array = data
                count += 1
            else:
                data_array = xr.concat([data_array, data], "x")

    return data_array


def extract_OMPS_nm_opendap(fname):
    """Read SNPP OMPS Nadir Mapper Total Column Ozone L2 data from NASA GES DISC
    OPeNDAP site. Requires Earthdata account to use. Must also have set up .netrc
    with Earthdata login information.

    Parameters
    ----------
    fname: string
        fname is url location of h5 file

    Returns
    -------
    ds: xarray dataset
    """

    from datetime import datetime, timedelta

    import numpy as np
    import xarray as xr
    from netCDF4 import Dataset

    with Dataset(fname, "r") as f:
        time = f["GeolocationData_Time"][:]
        to3 = f["ScienceData_ColumnAmountO3"][:]
        lat = f["GeolocationData_Latitude"][:]
        lon = f["GeolocationData_Longitude"][:]
        aprior = f["AncillaryData_APrioriLayerO3"][:]
        plevs = f["DimPressureLevel"][:]
        layere = f["ScienceData_LayerEfficiency"][:]
        flags = f["ScienceData_QualityFlags"][:]
        cloud_fraction = f["ScienceData_RadiativeCloudFraction"][:]

    # Apply Quality Control filters and convert time to usable version
    to3[((to3 < 50.0) | (to3 > 700.0))] = np.nan
    layere[((layere < 0.0) | (layere > 10.0))] = 0
    time[((time < -5e9) | (time > 1e10))] = np.nan
    ssday = datetime(year=1993, day=1, month=1, hour=0)
    time = np.asarray([ssday + timedelta(seconds=i) for i in time])

    to3[(cloud_fraction > 0.3)] = np.nan
    layere[(cloud_fraction > 0.3), :] = 0
    to3[(flags >= 138)] = np.nan

    ds = xr.Dataset(
        {
            "ozone_column": (["x", "y"], to3),
            "apriori": (["x", "y", "z"], aprior),
            "layer_efficiency": (["x", "y", "z"], layere),
        },
        coords={
            "longitude": (["x", "y"], lon),
            "latitude": (["x", "y"], lat),
            "time": (["x"], time),
            "pressure": (["z"], plevs),
        },
        attrs={"missing_value": -999},
    )
    return ds


def extract_OMPS_nm(fname):
    """Read locally stored NASA Suomi NPP OMPS Level 2 Nadir Mapper Total Ozone Column Files.
    Parameters
    ----------
    fname: string
        fname is local path to h5 file

    Returns
    -------
    ds: xarray dataset

    """

    from datetime import datetime, timedelta

    import h5py
    import numpy as np
    import xarray as xr

    with h5py.File(fname, "r") as f:
        time = f["GeolocationData"]["Time"][:]
        to3 = f["ScienceData"]["ColumnAmountO3"][:]
        lat = f["GeolocationData"]["Latitude"][:]
        lon = f["GeolocationData"]["Longitude"][:]
        aprior = f["AncillaryData"]["APrioriLayerO3"][:]
        plevs = f["DimPressureLevel"][:]
        layere = f["ScienceData"]["LayerEfficiency"][:]
        flags = f["ScienceData"]["QualityFlags"][:]
        cloud_fraction = f["ScienceData"]["RadiativeCloudFraction"][:]

    to3[((to3 < 50.0) | (to3 > 700.0))] = np.nan
    layere[((layere < 0.0) | (layere > 10.0))] = 0
    time[((time < -5e9) | (time > 1e10))] = np.nan
    ssday = datetime(year=1993, day=1, month=1, hour=0)
    time = np.asarray([ssday + timedelta(seconds=i) for i in time])

    to3[(cloud_fraction > 0.3)] = np.nan
    layere[(cloud_fraction > 0.3), :] = 0
    to3[(flags >= 138)] = np.nan

    ds = xr.Dataset(
        {
            "ozone_column": (["x", "y"], to3),
            "apriori": (["x", "y", "z"], aprior),
            "layer_efficiency": (["x", "y", "z"], layere),
        },
        coords={
            "longitude": (["x", "y"], lon),
            "latitude": (["x", "y"], lat),
            "time": (["x"], time),
            "pressure": (["z"], plevs),
        },
        attrs={"missing_value": -999},
    )
    return ds
