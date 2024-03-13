import pandas as pd

server = "ftp.star.nesdis.noaa.gov"
base_dir = "/pub/smcd/VIIRS_Aerosol/npp.viirs.aerosol.data/epsaot550/"


def build_urls(dates, *, daily=True, res=0.1, sat='noaa20'):
    """Construct URLs for downloading NEPS data.

    Parameters
    ----------
    dates : pd.DatetimeIndex or iterable of datetime
        Dates to download data for.
    daily : bool, optional
        Whether to download daily (default) or sub-daily data.
   res : float, optional
        Resolution of data in km, only used for sub-daily data.
    sat : str, optional
        Satellite platform, only used for sub-daily data.

    Returns
    -------
    pd.Series
        Series with URLs and corresponding file names.

    Notes
    -----
    The `res` and `sat` parameters are only used for sub-daily data.
    """

    from collections.abc import Iterable
    if isinstance(dates,Iterable):
        dates = pd.DatetimeIndex(dates)
    else:
        dates = pd.DatetimeIndex([dates])
    if daily:
        dates = dates.floor("D").unique()
    else:  # monthly
        dates = dates.floor("m").unique()
    sat = sat.lower()
    urls = []
    fnames = []
    print("Building VIIRS URLs...")
    base_url = "https://www.star.nesdis.noaa.gov/pub/smcd/VIIRS_Aerosol/viirs_aerosol_gridded_data/{}/aod/eps/".format(sat)
    if sat == 'snpp':
        sat = 'npp'
    for dt in dates:
        if daily:
            fname = "viirs_eps_{}_aod_{}_deg_{}_nrt.nc".format(sat,str(res).ljust(5,'0'), dt.strftime('%Y%m%d'))
        url = base_url + dt.strftime(r"%Y/") + fname
        urls.append(url)
        fnames.append(fname)

    # Note: files needed for comparison
    urls = pd.Series(urls, index=None)
    fnames = pd.Series(fnames, index=None)
    return urls, fnames


def retrieve(url, fname):
    """Download files from the airnowtech S3 server.

    Parameters
    ----------
    url : string
        Description of parameter `url`.
    fname : string
        Description of parameter `fname`.

    Returns
    -------
    None

    """
    import requests
    import os

    if not os.path.isfile(fname):
        print("\n Retrieving: " + fname)
        print(url)
        print("\n")
        r = requests.get(url)
        r.raise_for_status()
        with open(fname, "wb") as f:
            f.write(r.content)
    else:
        print("\n File Exists: " + fname)


def open_dataset(datestr, sat='noaa20', res=0.1, daily=True, add_timestamp=True):
    import xarray as xr
    import pandas as pd
    if isinstance(datestr,pd.Timestamp) == False:
        d = pd.to_datetime(datestr)
    else:
        d = datestr
    if sat.lower() == 'noaa20':
        sat = 'noaa20'
    else:
        sat = 'snpp'

    #if (res != 0.1) or (res != 0.25):
    #    res = 0.1 # assume resolution is 0.1 if wrong value supplied

    urls, fnames = build_urls(d, sat=sat,res=res, daily=daily)

    url = urls.values[0]
    fname = fnames.values[0]

    retrieve(url,fname)

    dset = xr.open_dataset(fname)

    if add_timestamp:
        dset['time'] = d
        dset = dset.expand_dims('time')
        dset = dset.set_coords(['time'])
    return dset 


def open_mfdataset(datestr, sat='noaa20', res=0.1, daily=True, add_timestamp=True):
    import xarray as xr
    import pandas as pd

    if isinstance(datestr,pd.DatetimeIndex) == False:
        print('Please provide a pandas.DatetimeIndex')
        exit
    else:
        d = datestr

    if sat.lower() == 'noaa20':
	    sat = 'noaa20'
    else:
        sat = 'snpp'

    urls, fnames = build_urls(d, sat=sat,res=res, daily=daily)

    for url, fname in zip(urls, fnames):
        retrieve(url, fname)

    dset = xr.open_mfdataset(fnames,combine='nested', concat_dim={'time':d})
    dset['time'] = d

    return dset
