"""@package hysplit_cf
Functions for making HYSPLIT output files CF compliant.

This module contains functions for converting HYSPLIT output datasets
to be compliant with the CF (Climate and Forecast) Metadata Conventions.
"""

import warnings
import numpy as np
import pandas as pd
import xarray as xr
import datetime
import json

def write_with_compression(data, fname):
    """@brief Write dataset to netCDF with compression.
    @param data xarray.Dataset or DataArray to write
    @param fname str: Output filename
    @return xarray.Dataset Written dataset
    @details Applies compression to all variables and handles encoding of bounds
    """
    VALID_NETCDF4_ENCODINGS = {
    'endian', 'significant_digits', 'least_significant_digit', '_FillValue',
    'compression', 'szip_pixels_per_block', 'complevel', 'dtype', 'fletcher32',
    'blosc_shuffle', 'shuffle', 'quantize_mode', 'szip_coding', 'zlib',
    'contiguous', 'chunksizes'
    }

    # Convert DataArray to Dataset if needed
    if isinstance(data, xr.DataArray):
        dset = data.to_dataset(name="HYSPLIT_DATA")
    else:
        dset = data.copy()
        
    # Process attributes for each variable
    for var in dset.data_vars:
        atthash = _check_attributes(dset[var].attrs)
        dset[var].attrs = atthash
        
    # Remove calendar attribute from time_bounds to avoid encoding conflict
    if 'time_bounds' in dset.data_vars:
        if 'calendar' in dset.time_bounds.attrs:
            del dset.time_bounds.attrs['calendar']
            
    # Set up compression encoding
    ehash = {"zlib": True, "complevel": 9}
    vhash = {}
    
    # Handle data variables with proper chunking
    for var in dset.data_vars:
        existing_enc = dset[var].encoding.copy()
        existing_enc.update(ehash)
        
        # Generate appropriate chunksizes for this variable
        if 'chunksizes' in existing_enc:
            # Check if chunksizes match variable dimensions
            if len(existing_enc['chunksizes']) != len(dset[var].dims):
                # Create appropriate chunksizes based on variable dimensions
                chunks = []
                for dim in dset[var].dims:
                    # Use smaller of dimension size or 100 as chunk size
                    chunks.append(min(dset[dim].size, 100))
                existing_enc['chunksizes'] = chunks
                
        # Filter to valid encodings
        vhash[var] = {k: v for k, v in existing_enc.items() if k in VALID_NETCDF4_ENCODINGS}
        
    # Handle coordinates and bounds
    for coord in ['time', 'time_bounds']:
        if coord in dset.coords:
            existing_enc = dset[coord].encoding.copy()
            existing_enc.update(ehash)
            # Handle chunksizes for coordinates too
            if 'chunksizes' in existing_enc and len(existing_enc['chunksizes']) != len(dset[coord].dims):
                chunks = []
                for dim in dset[coord].dims:
                    chunks.append(min(dset[dim].size, 100))
                if chunks:  # Only set if there are dimensions
                    existing_enc['chunksizes'] = chunks
            vhash[coord] = {k: v for k, v in existing_enc.items() if k in VALID_NETCDF4_ENCODINGS}
            
    # Add _FillValue encoding for bounds variables
    bounds_vars = ['z_bounds', 'latitude_bounds', 'longitude_bounds', 'time_bounds']
    for var in bounds_vars:
        if var in dset.data_vars:  # Changed from checking both coords and data_vars
            existing_enc = dset[var].encoding.copy()
            existing_enc.update(ehash)
            existing_enc['_FillValue'] = None
            # Handle chunksizes for bounds variables
            if 'chunksizes' in existing_enc and len(existing_enc['chunksizes']) != len(dset[var].dims):
                chunks = []
                for dim in dset[var].dims:
                    chunks.append(min(dset[dim].size, 100))
                if chunks:
                    existing_enc['chunksizes'] = chunks
            vhash[var] = {k: v for k, v in existing_enc.items() if k in VALID_NETCDF4_ENCODINGS}
    
    dset.to_netcdf(fname, encoding=vhash)
    dset.close()


def calculate_bounds(dset):
    """@brief Calculate lat/lon cell bounds from centers for 1D coordinates.
    @param dset xarray.Dataset HYSPLIT dataset with lat/lon coordinates
    @return xarray.Dataset Dataset with added boundary coordinates
    @details Calculates cell boundaries for regular lat/lon grid from cell centers.
             Handles ensemble and source coordinates if present.
    """
    # First create 'bnds' dimension by adding a dummy variable
    b32 = np.array([0,1], dtype='int32')
    dset = xr.merge([dset, 
                     xr.Dataset({'bnds': ('bnds', b32)})])
    dset.bnds.attrs['long_name'] = 'index of bounds dimension'

    # Calculate latitude bounds using dimensions already in dataset
    lat = dset.latitude.values
    dlat = np.abs(lat[1] - lat[0])
    lat_bounds = np.zeros((len(lat), 2))
    lat_bounds[:,0] = lat - dlat/2.0
    lat_bounds[:,1] = lat + dlat/2.0

    # Add latitude bounds using existing dimensions
    dset['latitude_bounds'] = (('y', 'bnds'), lat_bounds)
    dset.latitude_bounds.attrs.update({
        #'units': 'degrees_north',
        #'axis': 'Y',
        #'long_name': 'latitude cell boundaries'
        'comment' : 'Bounds'
    })

    # Calculate longitude bounds
    lon = dset.longitude.values  
    dlon = np.abs(lon[1] - lon[0])
    lon_bounds = np.zeros((len(lon), 2))
    lon_bounds[:,0] = lon - dlon/2.0
    lon_bounds[:,1] = lon + dlon/2.0

    # Add longitude bounds
    dset['longitude_bounds'] = (('x', 'bnds'), lon_bounds)
    dset.longitude_bounds.attrs.update({
        #'units': 'degrees_east',
        #'axis': 'X', 
        #'long_name': 'longitude cell boundaries'
        'comment' : 'Bounds'
    })

    return dset

def make_z_coordinate_cf_compliant(dset, height_reference='sea_level'):
    """@brief Make z coordinate CF compliant using level_heights attribute.
    @param dset xarray.Dataset Dataset to modify
    @param height_reference str: Either 'sea_level' or 'ground_level'
    @return xarray.Dataset Modified dataset with z_bounds coordinate
    @details Uses level_heights attribute to determine layer boundaries.
             z values represent center of each level.
             Handles cases where z coordinate is subset of level_heights.
    @throws ValueError if height_reference invalid or level_heights missing
    """
    if height_reference not in ['sea_level', 'ground_level']:
        raise ValueError("height_reference must be either 'sea_level' or 'ground_level'")
        
    if 'level_heights' not in dset.attrs:
        raise ValueError("Dataset missing required 'level_heights' attribute")
        
    all_tops = dset.attrs['level_heights']  # All possible level heights
    if isinstance(all_tops, np.ndarray):
        all_tops = all_tops.tolist()
    
    # Get current z values and find their indices in level_heights
    z_vals = dset.z.values
    z_indices = []
    for z in z_vals:
        try:
            idx = all_tops.index(z)
            z_indices.append(idx)
        except ValueError:
            raise ValueError(f"Z coordinate value {z} not found in level_heights attribute")
            
    z_bounds = np.zeros((len(z_vals), 2))
    z_centers = np.zeros(len(z_vals))
    
    # Calculate bounds for each z value based on its position in level_heights
    for i, (z_idx, z) in enumerate(zip(z_indices, z_vals)):
        if z_idx == 0:
            # First level
            z_bounds[i,0] = 0
            z_bounds[i,1] = all_tops[0]
        else:
            # Use previous level height as bottom bound
            z_bounds[i,0] = all_tops[z_idx-1]
            z_bounds[i,1] = all_tops[z_idx]
        z_centers[i] = (z_bounds[i,0] + z_bounds[i,1]) / 2

    # Add z coordinate standard name based on reference
    standard_name = ('height_above_mean_sea_level' if height_reference == 'sea_level' 
                    else 'height_above_ground')
    long_name = ('height above mean sea level' if height_reference == 'sea_level'
                 else 'height above ground level')

    # Update z coordinate to use centers
    dset = dset.assign_coords(z=z_centers)

    # Add z_bounds coordinate 
    dset['z_bounds'] = xr.DataArray(
        z_bounds,
        dims=['z', 'bnds'],
        coords={'z': dset.z, 'bnds': np.array([0,1], dtype='int32')},
        attrs={
             'comment' : 'Bounds'
        #    'units': 'm',
        #    'standard_name': standard_name,
        #    'long_name': 'cell boundaries of vertical levels'
        }
    )
    
    # Update z coordinate attributes
    dset.z.attrs['bounds'] = 'z_bounds'
    dset.z.attrs['standard_name'] = standard_name
    dset.z.attrs['long_name'] = long_name
    dset.z.attrs['units'] = 'm'
    dset.z.attrs['positive'] = 'up'
    dset.z.attrs['axis'] = 'Z'
    return dset

def add_crs(dset):
    dset.attrs['Conventions'] = 'CF-1.9'
    val = np.array(0,dtype='int32')
    dset['crs'] = xr.DataArray(val, attrs={'grid_mapping_name': 'latitude_longitude', 
                                           'earth_radius' : 6371200.0,
                                           'long_name' : 'Spherical earth with radius 6371.2 km',
                                           'comment' : 'This grid uses spherical Earth approximation. No EPSG code applies'
                                        #'epsg_code': "EPSG:4326",
                                        #'semi_major_axis': 6378137.0, 
                                        #'inverse_flattening': 298.257223563
                                        })
    return dset


def make_coordinates_cf_compliant(dset):
    """@brief Make coordinates CF compliant.
    @param dset xarray.Dataset Dataset to modify
    @return xarray.Dataset Modified dataset with CF compliant coordinates
    @details Adds CF standard names, units and bounds for coordinates.
             Creates CRS definition and adds grid mapping variable.
             Handles ensemble and source coordinates if present.
    """
    dset = add_crs(dset)

    # Handle ensemble and source coordinates if present
    if 'ens' in dset.coords:
        dset['ens'].attrs.update({
            'long_name': 'ensemble member',
            'standard_name': 'realization',
            'units': '1'
        })

    if 'source' in dset.coords:
        dset['source'].attrs.update({
            'long_name': 'meteorological data source',
            'standard_name': 'source_identifier',
            'units': '1'
        })

    # Add attributes to x/y coordinates
    dset['x'].attrs.update({
        'long_name': 'x coordinate of projection',
        'units' : '1'
    })
    
    dset['y'].attrs.update({
        'long_name': 'y coordinate of projection',
        'units' : '1'
    })

    # Extract unique latitude and longitude values
    # lat_unique = np.unique(dset.latitude.values[:,0])  # Take first column for each y
    # lon_unique = np.unique(dset.longitude.values[0,:])  # Take first row for each x
    
    # Create 1D coordinates with updated attributes
    #dset['latitude'] = xr.DataArray(
    #    lat_unique,
    #    dims=['y'],
    dset['latitude'].attrs={
            'standard_name': 'latitude',
            'long_name': 'latitude',
            'units': 'degrees_north',
            'axis': 'Y'
        }
    #)
    
    #dset['longitude'] = xr.DataArray(
    #    lon_unique,
    #    dims=['x'],
    dset.longitude.attrs={
            'standard_name': 'longitude',
            'long_name': 'longitude',
            'units': 'degrees_east',
            'axis': 'X'
        }
    #)
    
    return dset



def time2cf(dset,ref_time=None):
    """
    @brief convert time coordinate to CF compliant format
    @param dset xarray.Dataset Dataset to modify
    @param ref_time pd.Timestamp, datetime.datetime, or str reference time for time coordinate
    @return xarray.Dataset Modified dataset with CF compliant time coordinate   
    @details input time coordinate is assumed to be mid point of averaging period
             time_bounds assumed to exist 
    """
    time_attrs = dset.time.attrs if 'time' in dset.coords else {}
    if all(attr in time_attrs for attr in ['standard_name', 'units', 'calendar']):
        if time_attrs['calendar'] == 'standard' and 'time_bounds' in dset.coords:
            return dset

    time_vals = dset.time.values
    time_bounds = dset.time_bounds.values
    if not isinstance(ref_time,(pd.Timestamp,datetime.datetime,str)):
        ref_time = pd.Timestamp(time_bounds[0][0])
    if isinstance(ref_time,str):
        ref_time = pd.Timestamp(ref_time)
    if isinstance(ref_time, datetime.datetime): 
        ref_time = pd.Timestamp(ref_time)

    hours_since = []
    bounds = []
    for t, tb in zip(time_vals, time_bounds):
        t_stamp = pd.Timestamp(t)
        tb_stamp = [pd.Timestamp(tb[0]), pd.Timestamp(tb[1])]
        delta = (t_stamp - ref_time).total_seconds()/3600
        delta1 = (tb_stamp[0] - ref_time).total_seconds()/3600
        delta2 = (tb_stamp[1] - ref_time).total_seconds()/3600
        
        hours_since.append(delta)
        bounds.append([delta1, delta2])

    # Add time bounds
    dset['time_bounds'] = (('time', 'bnds'), bounds)
    dset['time_bounds'].attrs.update({
        'long_name': 'start and end times of sampling period',
        'units': f'hours since {ref_time.strftime("%Y-%m-%d %H:%M:%S")}Z',
        'calendar': 'standard',
    })
    
    # Update time coordinate
    dset.coords['time'] = ('time', hours_since)
    dset['time'].attrs.update({
        'standard_name': 'time',
        'long_name': 'time at middle of sampling period',
        'units': f'hours since {ref_time.strftime("%Y-%m-%d %H:%M:%S")}Z',
        'calendar': 'standard',
        'axis': 'T',
        'bounds': 'time_bounds',
    })
    
    return dset

def make_time_cf_compliant(dset):
    """@brief Make time coordinate CF compliant using dataset attributes.
    @param dset xarray.Dataset Dataset to modify 
    @return xarray.Dataset Modified dataset with CF compliant time coordinate
    @details Uses sampling_period_hours and Coordinate time description attributes
             to determine time bounds. If sampling_period_hours=0, treats time as
             instantaneous with no bounds. Falls back to make_time_cf_compliant if
             required attributes are not present.
    """
    # Check if we can use attributes
    if 'sampling_period_hours' not in dset.attrs:
        return make_time_cf_compliant_alternate(dset)

    sampling_hours = float(dset.attrs['sampling_period_hours'])
    time_desc = dset.attrs.get('Coordinate time description', '')
    time_vals = dset.time.values
    ref_time = pd.Timestamp(time_vals[0])
    
    # Handle instantaneous time data (no bounds needed)
    if sampling_hours == 0:
        hours_since = [(pd.Timestamp(t) - ref_time).total_seconds()/3600 
                      for t in time_vals]
        
        dset.coords['time'] = ('time', hours_since)
        dset['time'].attrs.update({
            'standard_name': 'time',
            'long_name': 'instantaneous measurement time',
            'units': f'hours since {ref_time.strftime("%Y-%m-%d %H:%M:%S")}Z',
            'calendar': 'standard',
            'axis': 'T'
        })
        return dset

    # Handle time periods with bounds
    #if not time_desc:
    #    return make_time_cf_compliant(dset)

    # Use sampling period and time description to determine bounds
    is_start = time_desc.lower().startswith('begin')
    hours_since = []
    bounds = []

 
    for t in time_vals:
        t_stamp = pd.Timestamp(t)
        if is_start:
            start = t_stamp
            end = start + pd.Timedelta(hours=sampling_hours)
            t_mid = start + pd.Timedelta(hours=sampling_hours/2)
        else:
            end = t_stamp
            start = end - pd.Timedelta(hours=sampling_hours)
            t_mid = end - pd.Timedelta(hours=sampling_hours/2)
        
        delta = (t_mid - ref_time).total_seconds()/3600
        delta1 = (start - ref_time).total_seconds()/3600
        delta2 = (end - ref_time).total_seconds()/3600
        
        hours_since.append(delta)
        bounds.append([delta1, delta2])

    # Add time bounds
    dset.coords['time_bounds'] = (('time', 'bnds'), bounds)
    dset['time_bounds'].attrs.update({
        'long_name': 'start and end times of sampling period',
        'units': f'hours since {ref_time.strftime("%Y-%m-%d %H:%M:%S")}Z',
        'calendar': 'standard',
    })
    
    # Update time coordinate
    dset.coords['time'] = ('time', hours_since)
    dset['time'].attrs.update({
        'standard_name': 'time',
        'long_name': 'time at middle of sampling period',
        'units': f'hours since {ref_time.strftime("%Y-%m-%d %H:%M:%S")}Z',
        'calendar': 'standard',
        'axis': 'T',
        'bounds': 'time_bounds',
    })
 
    return dset

def _check_attributes(atthash):
    """Convert numpy arrays in attributes to lists."""
    for key, val in atthash.items():
        if isinstance(val, np.ndarray):
            atthash[key] = list(val)
    return atthash

def rename_concentration_variable(dset, new_name, standard_name=None, long_name=None, data_var_index=0,atts={}):
    """
    @brief Rename the main concentration variable and add CF metadata
    @param dset xarray Dataset to modify 
    @param new_name str: New name for the concentration variable
    @param standard_name str: Optional CF standard name
    @param long_name str: Optional descriptive long name
    @param data_var_index int: Index of data variable to rename (default 0)
    @return Modified dataset
    """
    data_vars = list(dset.data_vars)
    if len(data_vars) == 0:
        warnings.warn("No data variables found in dataset")
        return dset
        
    if data_var_index >= len(data_vars):
        warnings.warn(f"Data variable index {data_var_index} out of range. Max index is {len(data_vars)-1}")
        return dset
        
    old_name = data_vars[data_var_index]
    
    # Create new attributes 
    attrs = dset[old_name].attrs.copy()
    if standard_name is not None:
        attrs['standard_name'] = standard_name
    if long_name is not None:
        attrs['long_name'] = long_name

    attrs.update(atts)
    # Rename with new attributes
    dset = dset.rename({old_name: new_name})
    dset[new_name].attrs = attrs
    
    return dset


def print_attributes(dset):
    for value in dset.attrs:
        if not isinstance(dset.attrs[value], list):
            print(value, dset.attrs[value])
        else:
            print(value, type(dset.attrs[value]))

    print(dset.attrs['Species_ID'])

def reorganize_attributes(dset):
    """@brief Move global attributes into internal_metadata JSON string.
    @param dset xarray.Dataset Dataset to modify
    @return xarray.Dataset Modified dataset with reorganized attributes
    @details Keeps only history, title, meteorological_model, and Conventions as
             direct global attributes. Moves all other attributes into a JSON string
             stored in the internal_metadata attribute.
    """
    keep_attrs = ['history', 'title', 'meteorological_model', 'Conventions']
    internal = {}
    
    def convert_value(val):
        """Convert numpy types to native Python types"""
        if isinstance(val, (np.integer, np.floating)):
            return val.item()
        elif isinstance(val, np.ndarray):
            return [convert_value(x) for x in val.tolist()]
        elif isinstance(val, list):
            return [convert_value(x) for x in val]
        elif isinstance(val, (str, int, float, bool)):
            return val
        else:
            return str(val)  # Fallback for unknown types
    
    # Copy current attributes
    current_attrs = dict(dset.attrs)
    
    # Move attributes to internal dict
    for key, value in current_attrs.items():
        if key not in keep_attrs:
            internal[key] = convert_value(value)
            del dset.attrs[key]
            
    # Add internal metadata as JSON string
    if internal:
        dset.attrs['internal_metadata'] = json.dumps(internal)
    
    return dset



def add_title_history(dset):
    if 'history' not in dset.attrs:
        dset.attrs['history'] = f"Created on {datetime.datetime.utcnow().isoformat()} UTC by HYSPLIT NetCDF conversion script"
    if 'title' not in dset.attrs:
        dset.attrs['title'] = 'HYSPLIT simulation air concentration data'
    return dset 



def decode_cf_time(dset, decode_time=True, decode_bounds=True):
    """@brief Decode CF time coordinates to datetime objects
    @param dset xarray.Dataset CF-compliant dataset
    @param decode_time bool: Whether to decode main time coordinate
    @param decode_bounds bool: Whether to decode time bounds if present
    @return xarray.Dataset Dataset with decoded times
    @details Decodes time coordinates from 'hours since' format to 
             datetime objects. Handles both main time coordinate and 
             time_bounds if present.
    """
    if not decode_time and not decode_bounds:
        return dset
        
    dset = dset.copy()
    
    if decode_time and 'time' in dset.coords:
        if 'units' in dset.time.attrs and 'calendar' in dset.time.attrs:
            # Let xarray's built-in decoder handle the conversion
            dset['time'] = xr.decode_cf(dset[['time']])['time']
            
    if decode_bounds and 'time_bounds' in dset.data_vars:
        if 'units' in dset.time_bounds.attrs:
            # Create temporary dataset with just bounds for decoding
            bounds_ds = xr.Dataset({'time_bounds': dset.time_bounds})
            bounds_ds.time_bounds.attrs['calendar'] = 'standard'
            decoded = xr.decode_cf(bounds_ds)
            dset['time_bounds'] = decoded.time_bounds
            
    return dset

def change_time_reference(dset, new_reference_time):
    """@brief Change the reference time of a CF-compliant dataset
    @param dset xarray.Dataset Dataset with CF-compliant time coordinate
    @param new_reference_time datetime or str: New reference time 
    @return xarray.Dataset Dataset with updated time coordinate
    @details Shifts time values to be relative to new reference time.
             Updates both time and time_bounds coordinates.
             Preserves CF compliance by updating attributes.
    """
    # Convert reference time to pandas Timestamp for consistent handling
    ref_time = pd.Timestamp(new_reference_time)
    
    # Get current time values
    try:
        time_attrs = dset.time.attrs.copy()
        if 'units' not in time_attrs:
            raise ValueError('Time coordinate missing units attribute')
    except (AttributeError, KeyError):
        raise ValueError('Dataset missing CF-compliant time coordinate')

    newset = dset.copy()
    newset = decode_cf_time(dset)
    newset = time2cf(newset,new_reference_time)
    return newset


