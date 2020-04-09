# Reads a tdump file, outputs a Pandas DataFrame
import numpy as np
import re
import pandas as pd


def open_tdump(fpath, fname):
    """Opens the tdump file"""

    tdump = open(fpath+fname)
    return tdump


def get_metinfo(tdump):
    """Reads the meteorological grid info from tdump file
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
    heads = ['TRAJ', 'MET_GRID', 'YEAR', 'MON', 'DAY', 'HOUR', 'MIN',
             'FCAST_HR', 'TRAJ_AGE', 'LAT', 'LON', 'ALT'] + variables
    traj = pd.read_csv(tdump, header=None, sep='\s+')
    traj.columns = heads
    return traj
