import xarray as xr


def open_mfdataset(files, var_list=None, surf_only=False, **kwargs):
    """Method to open Chimere model netcdf output files.
    Parameters
    ----------
    files : list[str]
        files is a list of path(s) of the file(s).
    var_list: list[str]
        list of variable names meant to be kept for the analysis.
    surf_only: bool
        boolean flag specifying if only surface data (layer 0) should be kept for analysis.
    Returns
    -------
    xarray.Dataset
        Chimere model dataset in standard format for use
        in MELODIES-MONET
    """
    if not isinstance(files, (list, tuple)):
        files = [files]

    datasets = []
    for file in files:
        datasets.append(xr.open_dataset(file))

    # get the data_vars wanted
    if var_list is None:
        var_list = []

    drop_data_vars = set(list(datasets[0].data_vars)) - set(var_list)

    for n, ds in enumerate(datasets):
        datasets[n] = ds.drop_vars(drop_data_vars, errors="ignore")

    xrds = xr.concat(datasets, "time_counter")

    xrds = xrds.rename(
        {"nav_lat": "latitude", "nav_lon": "longitude", "time_counter": "time", "bottom_top": "z"}
    )

    if surf_only:
        xrds = xrds.isel(z=0).expand_dims("z", axis=1)

    xrds = xrds.reset_coords()
    xrds = xrds.set_coords(["latitude", "longitude"])

    return xrds
