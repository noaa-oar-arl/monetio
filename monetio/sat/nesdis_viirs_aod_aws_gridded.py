def create_daily_aod_list(data_resolution, satellite, date_generated, fs, warning=False):
    """
    Creates a list of daily AOD (Aerosol Optical Depth) files and calculates the total size of the files.

    Parameters:
    - data_resolution (str): The resolution of the AOD data.
    - satellite (str): The satellite name. Can be 'SNPP' or 'NOAA20'.
    - date_generated (list): A list of dates for which to check the existence of AOD files.
    - fs (FileSystem): The file system object used to check file existence and size.

    Returns:
    - nodd_file_list (list): A list of paths to the existing AOD files.
    - nodd_total_size (int): The total size of the existing AOD files in bytes.
    """
    import warnings

    # Loop through observation dates & check for files
    nodd_file_list = []
    nodd_total_size = 0
    for date in date_generated:
        file_date = date.strftime("%Y%m%d")
        year = file_date[:4]

        if satellite == "SNPP":
            sat_name = "npp"
        elif satellite == "NOAA20":
            sat_name = "noaa20"
        file_name = (
            "viirs_eps_" + sat_name + "_aod_" + data_resolution + "_deg_" + file_date + ".nc"
        )
        prod_path = (
            "noaa-jpss/"
            + satellite
            + "/VIIRS/"
            + satellite
            + "_VIIRS_Aerosol_Optical_Depth_Gridded_Reprocessed/"
            + data_resolution[:4]
            + "_Degrees_Daily/"
            + year
            + "/"
        )
        # If file exists, add path to list and add file size to total
        try:
            if fs.exists(prod_path + file_name) is True:
                nodd_file_list.extend(fs.ls(prod_path + file_name))
                nodd_total_size = nodd_total_size + fs.size(prod_path + file_name)
            elif warning:
                warnings.warn("File does not exist on AWS: " + prod_path + file_name)
            else:
                raise ValueError
        except ValueError:
            print("File does not exist on AWS: " + prod_path + file_name)
            return [], 0

    return nodd_file_list, nodd_total_size


# Create list of available monthly data file paths & total size of files
def create_monthly_aod_list(satellite, date_generated, fs, warning=False):
    """
    Creates a list of monthly AOD (Aerosol Optical Depth) files for a given satellite and date range.

    Args:
        satellite (str): The satellite name. Can be 'SNPP' or 'NOAA20'.
        date_generated (list): A list of datetime objects representing the observation dates.
        fs: The file system object used to check for file existence and retrieve file information.

    Returns:
        tuple: A tuple containing the list of file paths and the total size of the files.

    """
    import warnings

    # Loop through observation dates & check for files
    nodd_file_list = []
    nodd_total_size = 0
    year_month_list = []
    for date in date_generated:
        file_date = date.strftime("%Y%m%d")
        year_month = file_date[:6]
        if year_month not in year_month_list:
            year_month_list.append(year_month)

            if satellite == "SNPP":
                sat_name = "snpp"
            elif satellite == "NOAA20":
                sat_name = "noaa20"
            file_name = "viirs_aod_monthly_" + sat_name + "_0.250_deg_" + year_month + ".nc"
            prod_path = (
                "noaa-jpss/"
                + satellite
                + "/VIIRS/"
                + satellite
                + "_VIIRS_Aerosol_Optical_Depth_Gridded_Reprocessed/0.25_Degrees_Monthly/"
            )
            # If file exists, add path to list and add file size to total
            try:
                if fs.exists(prod_path + file_name) is True:
                    nodd_file_list.extend(fs.ls(prod_path + file_name))
                    nodd_total_size = nodd_total_size + fs.size(prod_path + file_name)
                elif warning:
                    warnings.warn("File does not exist on AWS: " + prod_path + file_name)
                else:
                    raise ValueError("File does not exist on AWS: " + prod_path + file_name)
            except ValueError:
                print("A error has occurred:")

    return nodd_file_list, nodd_total_size


# Create list of available weekly data file paths & total size of files
def create_weekly_aod_list(satellite, date_generated, fs, warning=False):
    """
    Creates a list of files and calculates the total size of files for a given satellite, observation dates, and file system.

    Parameters:
    satellite (str): The satellite name. Can be 'SNPP' or 'NOAA20'.
    date_generated (list): A list of observation dates.
    fs (FileSystem): The file system object.

    Returns:
    tuple: A tuple containing the list of files and the total size of files.
    """
    # Loop through observation dates & check for files
    nodd_file_list = []
    nodd_total_size = 0
    for date in date_generated:
        file_date = date.strftime("%Y%m%d")
        year = file_date[:4]

        prod_path = (
            "noaa-jpss/"
            + satellite
            + "/VIIRS/"
            + satellite
            + "_VIIRS_Aerosol_Optical_Depth_Gridded_Reprocessed/0.25_Degrees_Weekly/"
            + year
            + "/"
        )
        # Get list of all files in given year on NODD
        all_files = fs.ls(prod_path)
        # Loop through files, check if file date falls within observation date range
        for file in all_files:
            file_start = file.split("/")[-1].split("_")[7].split(".")[0].split("-")[0]
            file_end = file.split("/")[-1].split("_")[7].split(".")[0].split("-")[1]
            # If file within observation range, add path to list and add file size to total
            if file_date >= file_start and file_date <= file_end:
                if file not in nodd_file_list:
                    nodd_file_list.append(file)
                    nodd_total_size = nodd_total_size + fs.size(file)

    return nodd_file_list, nodd_total_size


def open_dataset(date, satellite="SNPP", data_resolution=0.1, averaging_time="daily"):
    """Load VIIRS AOD data from AWS
    for the given date, satellite, data resolution, and averaging time.

    Parameters:
        date (str or datetime-like): The date for which to open the dataset.
            SNPP has data from 2012-01-19 to 2020-12-31.
            NOAA20 has data from 2018-01-01 to 2020-12-31.
        satellite (str): The satellite to retrieve data from.
            Valid values are 'SNPP' or 'NOAA20'.
        data_resolution (str, optional): The data resolution.
            Valid values are '0.050', '0.100', or '0.250'. Defaults to '0.1'.
            Only has an effect when `averaging_time` is 'daily'.
        averaging_time (str, optional): The averaging time.
            Valid values are 'daily', 'weekly', or 'monthly'. Defaults to 'daily'.

    Returns:
        xarray.Dataset: The opened dataset.

    Raises:
        ValueError: If the input parameters are invalid.
    """
    import pandas as pd
    import s3fs
    import xarray as xr

    if satellite not in {"SNPP", "NOAA20"}:
        raise ValueError(
            f"Invalid input for 'satellite' {satellite!r}: Valid values are 'SNPP' or 'NOAA20'"
        )

    data_resolution_in = data_resolution
    data_resolution = str(data_resolution).ljust(5, "0")
    if data_resolution not in {"0.050", "0.100", "0.250"}:
        raise ValueError(
            f"Invalid input for 'data_resolution' {data_resolution_in!r}: "
            "Valid values are '0.050', '0.100', or '0.250'"
        )

    if isinstance(date, str):
        date_generated = [pd.Timestamp(date)]
    else:
        date_generated = [date]

    # Access AWS using anonymous credentials
    fs = s3fs.S3FileSystem(anon=True)

    if averaging_time.lower() == "monthly":
        file_list, _ = create_monthly_aod_list(satellite, date_generated, fs)
    elif averaging_time.lower() == "weekly":
        file_list, _ = create_weekly_aod_list(satellite, date_generated, fs)
    elif averaging_time.lower() == "daily":
        file_list, _ = create_daily_aod_list(data_resolution, satellite, date_generated, fs)
    else:
        raise ValueError(
            f"Invalid input for 'averaging_time' {averaging_time!r}: "
            "Valid values are 'daily', 'weekly', or 'monthly'"
        )

    if len(file_list) == 0:
        raise ValueError(f"Files not available for product and date: {date_generated[0]}")

    aws_file = fs.open(file_list[0])

    dset = xr.open_dataset(aws_file)

    # Add datetime
    dset = dset.expand_dims(time=date_generated)

    return dset


def open_mfdataset(dates, satellite="SNPP", data_resolution=0.1, averaging_time="daily"):
    """
    Opens and combines multiple NetCDF files into a single xarray dataset.

    Parameters:
        dates (pandas.DatetimeIndex): The dates for which to retrieve the data.
        satellite (str): The satellite name. Valid values are 'SNPP' or 'NOAA20'.
        data_resolution (str, optional): The data resolution. Valid values are '0.050', '0.100', or '0.250'. Defaults to '0.1'.
        averaging_time (str, optional): The averaging time. Valid values are 'daily', 'weekly', or 'monthly'. Defaults to 'daily'.
        download (bool, optional): Whether to download the data from AWS. Defaults to False.
        save_path (str, optional): The path to save the downloaded data. Defaults to './'.

    Returns:
        xarray.Dataset: The combined dataset containing the data for the specified dates.

    Raises:
        ValueError: If the input parameters are invalid.
    """
    from collections.abc import Iterable

    import pandas as pd
    import s3fs
    import xarray as xr

    if satellite not in {"SNPP", "NOAA20"}:
        raise ValueError(
            f"Invalid input for 'satellite' {satellite!r}: Valid values are 'SNPP' or 'NOAA20'"
        )

    data_resolution_in = data_resolution
    data_resolution = str(data_resolution).ljust(5, "0")
    if data_resolution not in {"0.050", "0.100", "0.250"}:
        raise ValueError(
            f"Invalid input for 'data_resolution' {data_resolution_in!r}: "
            "Valid values are '0.050', '0.100', or '0.250'"
        )

    if isinstance(dates, Iterable):
        dates = pd.DatetimeIndex(dates)
    else:
        dates = pd.DatetimeIndex([dates])

    # Access AWS using anonymous credentials
    fs = s3fs.S3FileSystem(anon=True)

    if averaging_time.lower() == "monthly":
        file_list, _ = create_monthly_aod_list(satellite, dates, fs)
    elif averaging_time.lower() == "weekly":
        file_list, _ = create_weekly_aod_list(satellite, dates, fs)
    elif averaging_time.lower() == "daily":
        file_list, _ = create_daily_aod_list(data_resolution, satellite, dates, fs)
    else:
        raise ValueError(
            f"Invalid input for 'averaging_time' {averaging_time!r}: "
            "Valid values are 'daily', 'weekly', or 'monthly'"
        )

    if len(file_list) == 0:
        raise ValueError(f"Files not available for product and dates: {dates}")

    aws_files = [fs.open(f) for f in file_list]

    dset = xr.open_mfdataset(aws_files, concat_dim="time", combine="nested")

    dset["time"] = dates

    return dset
