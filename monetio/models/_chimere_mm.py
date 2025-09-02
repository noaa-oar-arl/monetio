
import xarray as xr

def open_mfdataset(
    files,
    var_list=None,
    surf_only=False,
    **kwargs
):
    """Method to open WRF-chem and RAP-chem netcdf files.
    Parameters
    ----------
    files : string or list
        files is the path to the file or files.  It will accept hot keys in
        strings as well.
    
    Returns
    -------
    xarray.DataSet
        WRF-Chem or RAP-Chem model dataset in standard format for use
        in MELODIES-MONET
    """

    
    datasets = []
    for file in files:
        datasets.append(xr.open_dataset(file))

    # get the data_vars wanted

    drop_data_vars = set(list(datasets[0].data_vars)) - set(var_list)

    for n, ds in enumerate(datasets):
        datasets[n] = ds.drop_vars(drop_data_vars, errors = 'ignore')

    xrds = xr.concat(datasets, 'time_counter')
    
    # attrs = datasets[0].attrs
    # xrds.attrs = attrs

    xrds = xrds.rename({
        'nav_lat': 'latitude',
        'nav_lon': 'longitude',
        'time_counter': 'time',
        'bottom_top': 'z'
    })

    if surf_only:
        xrds = xrds.isel(z=0).expand_dims("z", axis=1)

    xrds = xrds.reset_coords()
    xrds = xrds.set_coords(['latitude','longitude'])

    return xrds