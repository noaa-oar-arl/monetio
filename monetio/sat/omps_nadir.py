def read_OMPS_nm(files):
    ''' Loop to open OMPS nadir mapper L2 files. 
    
    Parameters:
    -----------
    - files: string or list of strings
    
    returns xarray Dataset.
    '''
    from glob import glob
    import xarray as xr
    
    count = 0
    filelist = sorted(glob(files, recursive=False))
    
    for filename in filelist:
        print(filename)
        data = extract_OMPS_nm(filename)
        
        if count == 0:
            data_array = data
            count += 1
        else:
            data_array = xr.concat([data_array, data], 'x')
            
    return data_array
    
    return
def extract_OMPS_nm(fname):
    '''Method to read OMPS nadir mapper Ozone level 2 hdf5 files from NASA repository
        (https://ozoneaq.gsfc.nasa.gov/data/omps/#)
    Parameters
    __________
    filename : string
        filename is the path to the file
    
    Returns
    _______
    xarray dataset
    
    '''
    import xarray as xr
    import h5py
    from datetime import datetime,timedelta
    import numpy as np
    
    with h5py.File(fname,'r') as f:
        time = f['GeolocationData']['Time'][:]
        to3 = f['ScienceData']['ColumnAmountO3'][:]
        lat = f['GeolocationData']['Latitude'][:]
        lon = f['GeolocationData']['Longitude'][:]
        aprior = f['AncillaryData']['APrioriLayerO3'][:]
        plevs = f['DimPressureLevel'][:]
        layere = f['ScienceData']['LayerEfficiency'][:]
        flags = (f['ScienceData']['QualityFlags'][:])
        cloud_fraction = f['ScienceData']['RadiativeCloudFraction'][:]
        
    to3[((to3 < 50.0)|(to3 > 700.0))] = np.nan
    layere[((layere < 0.)|(layere > 10.))] = 0
    time[((time < -5e9)|(time > 1e10))] = np.nan
    ssday = datetime(year=1993,day=1,month=1,hour=0)
    time = np.asarray([ssday + timedelta(seconds = i) for i in time])
   
    to3[(cloud_fraction > .3)] = np.nan
    layere[(cloud_fraction > .3),:] = 0
    to3[(flags >= 138 )] = np.nan
    
    ds = xr.Dataset(
            {
            'ozone_column': (['x','y'],to3),
            'apriori': (['x','y','z'],aprior),
            'layer_efficiency': (['x','y','z'],layere),
            },
            coords={
                'longitude':(['x','y'],lon),
                'latitude':(['x','y'],lat),
                'time':(['x'],time),
                'pressure':(['z'],plevs),
            },
            attrs={'missing_value':-999}
        )         
    return ds