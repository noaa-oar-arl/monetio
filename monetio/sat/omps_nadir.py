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

def omps_nm_pairing(model_data,obs_data):
    'Pairs UFS-RAQMS ozone mixing ratio with OMPS nadir mapper retrievals. Calculates column without applying apriori'
    import xesmf as xe
    import xarray as xr
    import numpy as np
    import pandas as pd
    
    print('pairing without applying averaging kernel')
    from matplotlib import pyplot as plt
    
    du_fac = 1.0e4*6.023e23/28.97/9.8/2.687e19 # conversion factor; moves model from ppv to dobson
    
    # define satellite lat/lon grid as dataset
    ds_out = xr.Dataset({'lat': (['x','y'], obs_data['latitude'].values),
                         'lon': (['x','y'], obs_data['longitude'].values),
                        }
                       )
    # regrid spatially (model lat/lon to satellite swath lat/lon)
    # dimensions of new variables will be (time, z, satellite_x, satellite_y)
    regridr = xe.Regridder(model_data,ds_out,'bilinear') # standard bilinear spatial regrid. 
    regrid_oz = regridr(model_data['o3vmr'])
    regrid_dp = regridr(model_data['dpm'])
    
    # calculate ozone column, no averaging kernel or apriori applied.
    col = np.nansum(du_fac*regrid_dp*regrid_oz,axis=1) # new dimensions will be (time, satellite_x, satellite_y)
    plt.pcolormesh(np.mean(col,axis=0))
    plt.colorbar()
    plt.show()
    # Interpolate model data in time. Final dataset will be (satellite_x, satellite_y) dimensions at satellite times.
    nf,nx,ny = col.shape
    
    ## initialize dataset holder for final dataset
    oz = np.zeros_like(obs_data.ozone_column.values)
    
    sum_tf = np.zeros((nx),'float32')
    ## loop over model time steps
    for f in range(nf):
        tindex = np.where(np.abs(obs_data.time - model_data.time[f]) <= (model_data.time[1]-model_data.time[0]))[0]
        #tfac1 = 1-np.abs(model_data.time[f] - obs_data.time[tindex])/(model_data.time[1]-model_data.time[0])

        #sum_tf[tindex] += tfac1
        # fixes for observations before/after model time range.
        if f == (nf-1):
            print('last')
            tindex = np.where((obs_data.time >= model_data.time[f]))[0]
            oz[tindex,:] = col[f][tindex,:]#.values
            sum_tf[tindex] += 1
            
            tind_2 = np.where((obs_data.time < model_data.time[f]) & 
                              (np.abs(obs_data.time - model_data.time[f]) <= (model_data.time[1]-model_data.time[0])))[0]
            tfac1 = 1-(np.abs(model_data.time[f] - obs_data.time[tind_2])/(model_data.time[1]-model_data.time[0]))
            
            oz[tind_2,:] += np.expand_dims(tfac1.values,axis=1)*col[f][tind_2,:]
            sum_tf[tind_2] += tfac1
        elif f == (0):
            print('first')
            tindex = np.where((obs_data.time <= model_data.time[f]))[0]
            oz[tindex,:] = col[f][tindex,:]#.values
            sum_tf[tindex] += 1
            tind_2 = np.where((obs_data.time > model_data.time[f]) & 
                              (np.abs(obs_data.time - model_data.time[f]) <= (model_data.time[1]-model_data.time[0])))[0]
            tfac1 = 1-(np.abs(model_data.time[f] - obs_data.time[tind_2])/(model_data.time[1]-model_data.time[0]))
            
            oz[tind_2,:] += np.expand_dims(tfac1.values,axis=1)*col[f][tind_2,:]
            sum_tf[tind_2] += tfac1
        else:
            print('not 1 or last')
            tindex = np.where(np.abs(obs_data.time - model_data.time[f]) <= (model_data.time[1]-model_data.time[0]))[0]
            
            tfac1 = 1-(np.abs(model_data.time[f] - obs_data.time[tindex])/(model_data.time[1]-model_data.time[0]))
            sum_tf[tindex] += tfac1
            oz[tindex,:] += np.expand_dims(tfac1.values,axis=1)*col[f][tindex,:]#.values

    ds = xr.Dataset({'o3vmr': (['x','y'],oz),
                     'ozone_column':(['x','y'],obs_data.ozone_column.values)
                               },
                    coords={
                        'longitude':(['x','y'],obs_data['longitude'].values),
                        'latitude':(['x','y'],obs_data['latitude'].values),
                        'time':(['x'],obs_data.time.values),
                    })    
    plt.plot(sum_tf)
    plt.show()
    return ds

def omps_nm_pairing_apriori(model_data,obs_data):
    'Pairs UFS-RAQMS data with OMPS nm. Applies satellite apriori column to model observations.'
    import xesmf as xe
    import xarray as xr
    import numpy as np
    import pandas as pd
    from matplotlib import pyplot as plt
    
    du_fac = 1.0e4*6.023e23/28.97/9.8/2.687e19 # conversion factor; moves model from ppv to dobson
    
    print('pairing with averaging kernel application')
    # define satellite lat/lon grid as dataset
    ds_out = xr.Dataset({'lat': (['x','y'], obs_data['latitude'].values),
                         'lon': (['x','y'], obs_data['longitude'].values),
                        }
                       )
    # regrid spatially (model lat/lon to satellite swath lat/lon)
    # dimensions of new variables will be (time, z, satellite_x, satellite_y)
    regridr = xe.Regridder(model_data,ds_out,'bilinear') # standard bilinear spatial regrid. 
    regrid_oz = regridr(model_data['o3vmr'])
    regrid_p = regridr(model_data['pdash']) # this one should be pressure variable (for the interpolation).
    
    # Interpolate model data in time. Final dataset will be (satellite_x, satellite_y) dimensions at satellite times.
    nf,nz_m,nx,ny = regrid_oz.shape

    ## initialize intermediates for use in calcluating column
    pressure_temp = np.zeros((nz_m,nx,ny))
    ozone_temp = np.zeros((nz_m,nx,ny))
    
    ## loop over model time steps
    for f in range(nf):
        tindex = np.where(np.abs(obs_data.time - model_data.time[f]) <= (model_data.time[1]-model_data.time[0]))[0]
        # fixes for observations before/after model time range.
        if f == (nf-1):
            tindex = np.where((obs_data.time >= model_data.time[f]))[0]
            ozone_temp[:,tindex,:] = regrid_oz[f,:,tindex,:].values
            pressure_temp[:,tindex,:] = regrid_p[f,:,tindex,:].values
            
            tind_2 = np.where((obs_data.time < model_data.time[f]) & 
                              (np.abs(obs_data.time - model_data.time[f]) <= (model_data.time[1]-model_data.time[0])))[0]
            tfac1 = 1-(np.abs(model_data.time[f] - obs_data.time[tind_2])/(model_data.time[1]-model_data.time[0]))
            
            ozone_temp[:,tind_2,:] += np.expand_dims(tfac1.values,axis=1)*regrid_oz[f,:,tind_2,:].values
            pressure_temp[:,tind_2,:] += np.expand_dims(tfac1.values,axis=1)*regrid_p[f,:,tind_2,:].values
        elif f == 0:
            tindex = np.where((obs_data.time <= model_data.time[f]))[0]
            ozone_temp[:,tindex,:] = regrid_oz[f,:,tindex,:].values
            pressure_temp[:,tindex,:] = regrid_p[f,:,tindex,:].values
            
            tind_2 = np.where((obs_data.time > model_data.time[f]) & 
                              (np.abs(obs_data.time - model_data.time[f]) <= (model_data.time[1]-model_data.time[0])))[0]
            tfac1 = 1-(np.abs(model_data.time[f] - obs_data.time[tind_2])/(model_data.time[1]-model_data.time[0]))
            ozone_temp[:,tind_2,:] += np.expand_dims(tfac1.values,axis=1)*regrid_oz[f,:,tind_2,:].values
            pressure_temp[:,tind_2,:] += np.expand_dims(tfac1.values,axis=1)*regrid_p[f,:,tind_2,:].values
        else:
            tfac1 = 1-(np.abs(model_data.time[f] - obs_data.time[tindex])/(model_data.time[1]-model_data.time[0]))
            ozone_temp[:,tindex,:] += np.expand_dims(tfac1.values,axis=1)*regrid_oz[f,:,tindex,:].values
            pressure_temp[:,tindex,:] += np.expand_dims(tfac1.values,axis=1)*regrid_p[f,:,tindex,:].values
    
    # Interpolate model data to satellite pressure levels
    from wrf import interplevel
    ozone_satp = interplevel(ozone_temp,pressure_temp/100.,obs_data.pressure[::-1],missing=0)
    ozone_satp = ozone_satp.values
    
    ozone_satp[np.isnan(ozone_satp)] = 0
    oz = np.zeros_like(obs_data.ozone_column.values)
    # flip things
    ozone_satp = ozone_satp[::-1]
    plt.plot(ozone_satp[:,8,8],obs_data.pressure.values,'.')
    plt.plot(ozone_temp[:,8,8],pressure_temp[:,8,8]/100.,'.')
    plt.show()
    nl,n1,n2 = ozone_satp.shape
    part_sum = np.zeros((nl),dtype='float32')
    for i in range(11):
        if i != 0:
            dp = np.abs(obs_data.pressure[i-1].values - obs_data.pressure[i].values)
            
        else:
            dp = np.abs(pressure_temp[0]/100. - obs_data.pressure[i].values)
            print(dp)
        print()
        add = du_fac*dp*ozone_satp[i]
        eff = obs_data.layer_efficiency[:,:,i].values
        ap = obs_data.apriori[:,:,i].values
        oz = oz + ap*(1-eff) + (eff)*(add)
        part_sum[i] = oz[8,8]
    plt.pcolormesh(oz,vmin=150,vmax=350)
    plt.title('AK applied')
    plt.colorbar()
    plt.show()
    plt.plot(part_sum[::-1],np.cumsum(obs_data.pressure.values)[::-1])
    plt.xlabel('summed column')
    plt.ylabel('summed pressure')
    plt.show()
    ds = xr.Dataset({'o3vmr': (['x','y'],oz),
                     'ozone_column':(['x','y'],obs_data.ozone_column.values)
                               },
                    coords={
                        'longitude':(['x','y'],obs_data['longitude'].values),
                        'latitude':(['x','y'],obs_data['latitude'].values),
                        'time':(['x'],obs_data.time.values),
                    })
    return ds