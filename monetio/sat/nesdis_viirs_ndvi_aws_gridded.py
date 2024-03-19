def create_daily_vhi_list(date_generated, fs, fail_on_error=True):
    """
    Creates a list of daily vhi (Vegetative Health Index) files and calculates the total size of the files.

    Parameters:
    - date_generated (list): A list of dates for which to check the existence of AOD files.
    - fs (FileSystem): The file system object used to check file existence and size.

    Returns:
    - nodd_file_list (list): A list of paths to the existing AOD files.
    - nodd_total_size (int): The total size of the existing AOD files in bytes.
    """
    # Loop through observation dates & check for files
    nodd_file_list = []
    nodd_total_size = 0
    for date in date_generated:
        file_date = date.strftime("%Y%m%d")
        year = file_date[:4]
        prod_path = "noaa-cdr-ndvi-pds/data/" + year + "/"
        file_name = fs.glob(prod_path + "VIIRS-Land_*_" + file_date + "_*.nc")
        # If file exists, add path to list and add file size to total
        print(file_name)
        if fs.exists(file_name[0]) is True:
            nodd_file_list.append(file_name[0])
            nodd_total_size = nodd_total_size + fs.size(file_name[0])
    return nodd_file_list, nodd_total_size


def open_dataset(date, download=False, save_path="./"):
    """
    Opens a dataset for the given date, satellite, data resolution, and averaging time.

    Parameters:
        date (str or datetime.datetime): The date for which to open the dataset.
        averaging_time (str, optional): The averaging time. Valid values are 'daily', 'weekly', or 'monthly'. Defaults to 'daily'.

    Returns:
        xarray.Dataset: The opened dataset.

    Raises:
        ValueError: If the input values are invalid.
    """
    import pandas as pd
    import s3fs
    import xarray as xr

    if isinstance(date, str):
        date_generated = [pd.Timestamp(date)]
    else:
        date_generated = [date]

    # Access AWS using anonymous credentials
    fs = s3fs.S3FileSystem(anon=True)

    file_list, _ = create_daily_vhi_list(date_generated, fs)

    aws_file = fs.open(file_list[0])

    dset = xr.open_dataset(aws_file)

    # add datetime
    # dset = dset.expand_dims(time=date_generated)

    return dset


def open_mfdataset(dates, download=False, save_path="./"):
    """
    Opens and combines multiple NetCDF files into a single xarray dataset.

    Parameters:
        dates (pandas.DatetimeIndex): The dates for which to retrieve the data.
        satellite (str): The satellite name. Valid values are 'SNPP', 'NOAA20', or 'both'.
        data_resolution (str, optional): The data resolution. Valid values are '0.050', '0.100', or '0.250'. Defaults to '0.1'.
        averaging_time (str, optional): The averaging time. Valid values are 'daily', 'weekly', or 'monthly'. Defaults to 'daily'.
        download (bool, optional): Whether to download the data from AWS. Defaults to False.
        save_path (str, optional): The path to save the downloaded data. Defaults to './'.

    Returns:
        xarray.Dataset: The combined dataset containing the data for the specified dates.

    Raises:
        ValueError: If the input parameters are invalid.

    """
    import pandas as pd
    import s3fs
    import xarray as xr

    if not isinstance(dates, pd.DatetimeIndex):
        raise ValueError("Expecting pandas.DatetimeIndex for 'dates' parameter.")

    # Access AWS using anonymous credentials
    fs = s3fs.S3FileSystem(anon=True)

    file_list, total_size = create_daily_vhi_list(dates, fs)

    aws_files = [fs.open(f) for f in file_list]

    dset = xr.open_mfdataset(aws_files, concat_dim={"time": dates}, combine="nested")

    dset["time"] = dates

    return dset
