def read_OMPS_l3(files):
    """Loop to open OMPS nadir mapper Total Column Ozone L3 files.
    Parameters:
    -----------
    files: string or list of strings

    returns xarray Dataset.
    """
    
    from glob import glob
    import xarray as xr
    import numpy as np
    from matplotlib import pyplot as plt
    
    start_dataset = True
    times = []
    filelist = sorted(glob(files,recursive=False))
    
    for filename in filelist:
        data = extract_OMPS_l3(filename)
        times.append(data.attrs['time'])
        del data.attrs['time']
        if start_dataset:
            data_array = data
            start_dataset = False
        else:
            data_array = xr.concat([data_array,data],'time')
    data_array['time'] = (('time'),np.array(times))
    data_array = data_array.reset_coords().set_coords(['latitude','longitude','time'])
    return data_array

def extract_OMPS_l3(fname):
    '''Read locally stored NASA Suomi NPP OMPS Level 3 Nadir Mapper TO3 files
    Parameters
    ----------
    fname: string
        fname is local path to h5 file

    Returns
    -------
    ds: xarray dataset

    '''
    
    import h5py
    import numpy as np
    import xarray as xr
    import pandas as pd
    from matplotlib import pyplot as plt
    
    with h5py.File(fname,'r') as f:
        lat = f['Latitude'][:]
        lon = f['Longitude'][:]
        column = f['ColumnAmountO3'][:]
        cloud_fraction = f['RadiativeCloudFraction'][:]
        time = pd.to_datetime(f.attrs.__getitem__('Date').decode('UTF-8'),format='%Y-%m-%d')

    # remove cloudy scenes and points with no data (eg. polar dark zone)
    column[(column < 0)] = np.nan
    column[(cloud_fraction > 0.3)] = np.nan
    lon_2d,lat_2d = np.meshgrid(lon,lat)
    
    ds = xr.Dataset({'ozone_column':(['time','x','y'],column[None,:,:]),
                    },
                    coords={'longitude':(['x','y'],lon_2d),'latitude':(['x','y'],lat_2d),},
                   attrs={'time':time})
    
    return ds