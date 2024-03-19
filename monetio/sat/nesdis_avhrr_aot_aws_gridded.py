def create_daily_aod_list(date_generated, fs, fail_on_error=True):
    """
    Creates a list of daily AOD (Aerosol Optical Depth) files and calculates the total size of the files.

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
        prod_path = "noaa-cdr-aerosol-optical-thickness-pds/data/daily/" + year + "/"
        file_name = fs.glob(prod_path + "AOT_AVHRR_*_daily-avg_" + file_date + "_*.nc")
        # If file exists, add path to list and add file size to total
        print(file_name)
        if fs.exists(file_name[0]) is True:
            nodd_file_list.append(file_name[0])
            nodd_total_size = nodd_total_size + fs.size(file_name[0])
    return nodd_file_list, nodd_total_size


def create_monthly_aod_list(date_generated, fs):
    """
    Creates a list of daily AOD (Aerosol Optical Depth) files and calculates the total size of the files.

    Parameters:
    - data_resolution (str): The resolution of the AOD data.
    - satellite (str): The satellite name. Can be 'both', 'SNPP', or 'NOAA20'.
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
        prod_path = "noaa-cdr-aerosol-optical-thickness-pds/data/daily/" + year + "/"
        file_name = fs.glob(prod_path + "AOT_AVHRR_*_daily-avg_" + file_date + "_*.nc")
        # If file exists, add path to list and add file size to total
        if fs.exists(file_name[0]) is True:
            nodd_file_list.append(file_name[0])
            nodd_total_size = nodd_total_size + fs.size(file_name[0])
    return nodd_file_list, nodd_total_size


def open_dataset(date, averaging_time="daily", download=False, save_path="./"):
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

    try:
        if averaging_time.lower() == "monthly":
            file_list, _ = create_monthly_aod_list(date_generated, fs)
        elif averaging_time.lower() == "daily":
            file_list, _ = create_daily_aod_list(date_generated, fs)
        else:
            raise ValueError
    except ValueError:
        print("Invalid input for 'averaging_time': Valid values are 'daily' or 'monthly'")
        return

    try:
        if len(file_list) == 0:
            raise ValueError
        else:
            aws_file = fs.open(file_list[0])
    except ValueError:
        print("Files not available for product and date:", date_generated[0])
        return

    dset = xr.open_dataset(aws_file)

    # add datetime
    # dset = dset.expand_dims(time=date_generated)

    return dset


def open_mfdataset(dates, averaging_time="daily", download=False, save_path="./"):
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

    try:
        if not isinstance(dates, pd.DatetimeIndex):
            raise ValueError("Expecting pandas.DatetimeIndex for 'dates' parameter.")
    except ValueError:
        print("Invalid input for 'dates': Expecting pandas.DatetimeIndex")
        return

    # Access AWS using anonymous credentials
    fs = s3fs.S3FileSystem(anon=True)

    try:
        if averaging_time.lower() == "monthly":
            file_list, _ = create_monthly_aod_list(dates, fs)
        elif averaging_time.lower() == "daily":
            file_list, _ = create_daily_aod_list(dates, fs)
        else:
            raise ValueError
    except ValueError:
        print("Invalid input for 'averaging_time': Valid values are 'daily' or 'monthly'")
        return

    try:
        if not file_list:
            raise ValueError
        aws_files = []
        for f in file_list:
            aws_files.append(fs.open(f))
    except ValueError:
        print("File not available for product and date")
        return

    dset = xr.open_mfdataset(aws_files, concat_dim={"time": dates}, combine="nested")

    dset["time"] = dates

    return dset
