# Reads a tdump file, outputs a Pandas DataFrame

import numpy as np
import re
import pandas as pd


def open_dataset(filename):
    """ Opens the tdump file
    Returns the trajectory array (Pandas DataFrame)"""

    tdump = open_tdump(filename)
    traj = get_traj(tdump)
    return traj


def open_tdump(filename):
    """Opens the tdump file"""

    tdump = open(filename)
    return tdump


def get_metinfo(tdump):
    """Reads the meteorological grid info from tdump file
    Need to read the tdump file in directly before executing this function
    Returns list of strings"""

    # Going back to first line of file
    tdump.seek(0)
    # Dimensions of met file array in numpy array
    dim1 = tdump.readline().strip().replace(' ', '')
    dim1 = np.array(list(dim1))
    # Read met file info into array
    metinfo = []
    a = 0
    while a < int(dim1[0]):
        tmp = re.sub(r'\s+', ',', tdump.readline().strip())
        metinfo.append(tmp)
        a += 1
    return metinfo


def get_startlocs(tdump):
    """Reads the starting locations
    Need to read the tdump file in directly before executing this function
    Returns a Pandas DataFrame"""

    # Going back to first line of file
    tdump.seek(0)
    # Gets the metinfo
    metinfo = get_metinfo(tdump)
    # Read next line - get number of starting locations
    dim2 = list(tdump.readline().strip().split(' '))
    start_locs = []
    b = 0
    while b < int(dim2[0]):
        tmp2 = re.sub(r'\s+', ',', tdump.readline().strip())
        tmp2 = tmp2.split(',')
        start_locs.append(tmp2)
        b += 1
    # Putting starting locations array into pandas DataFrame
    stlocs = pd.DataFrame(np.array(start_locs), columns=[
                          'Year', 'Month', 'Dat', 'Hour', 'Latitude', 'Longitude', 'Altitude'])
    return stlocs


def get_traj(tdump):
    """Reads the trajectory information
    Need to read the tdump file in directly before executing this function
    Returns a Pandas DataFrame"""

    # Going back to first line of file
    tdump.seek(0)
    # Gets the starting locations
    stlocs = get_startlocs(tdump)
    # Read the number (and names) of additional variables in traj file
    varibs = re.sub(r'\s+', ',', tdump.readline().strip())
    varibs = varibs.split(',')
    variables = varibs[1:]
    # Read the traj arrays into pandas dataframe
    heads = ['time', 'traj_num', 'met_grid', 'forecast_hour',
             'traj_age', 'latitude', 'longitude', 'altitude'] + variables
    traj = pd.read_csv(tdump, header=None, sep='\s+', parse_dates={'time': [2, 3, 4, 5, 6]})
    traj.columns = heads
    traj.columns = map(str.lower, traj.columns)
    traj = date_format(traj)
    return traj


def date_format(traj):
    """Formats the 'TIME' column into datetime object
    Returns traj Pandas DataFrame"""

    traj['time'] = pd.to_datetime(traj['time'], format='%y %m %d %H %M')
    return traj
