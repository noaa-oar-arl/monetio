# HRRR-Smoke READER
import xarray as xr

def open_dataset(fname):
    ''' Open a single dataset from HRRR-Smoke output, following conversion
        to netCDF from grib2.
        Parameters
        ----------
        fname: string
            Filename to be opened
        Returns
        -------
        xarray.Dataset
    '''
    f = xr.open_dataset(fname)
    return f

def open_mfdataset(fnames):
    '''Open a multiple file dataset from hrrr-smoke output files converted
       from grib3 to netcdf.
    Parameters
    ----------
    fname : string
        Filenames to be opened
    Returns
    -------
    xarray.Dataset
    '''

    f = xr.open_mfdataset(fnames, concat_dim='time')
    return f
