"""PAMS -- EPA Photochemical Assessment Monitoring Stations

Read JSON data files from AQS's data API(?)

https://www.epa.gov/amtic/photochemical-assessment-monitoring-stations-pams
https://aqs.epa.gov/aqsweb/documents/data_api.html
"""

import json

import pandas as pd


def add_data(filename):
    """Read a JSON file.

    Parameters
    ----------
    filename : string
        File path of JSON file to load.

    Returns
    -------
    pandas.DataFrame
    """

    jsonf = open_json(filename)
    dataf = jsonf["Data"]
    data = pd.DataFrame.from_dict(dataf)

    # Combining state code, county code, and site number into one column
    data["siteid"] = (
        data.state_code.astype(str).str.zfill(2)
        + data.county_code.astype(str).str.zfill(3)
        + data.site_number.astype(str).str.zfill(4)
    )

    # Combining date and time into one column
    data["datetime_local"] = pd.to_datetime(data["date_local"] + " " + data["time_local"])
    data["datetime_utc"] = pd.to_datetime(data["date_gmt"] + " " + data["time_gmt"])

    # Renaming columns
    data = data.rename(
        columns={
            "sample_measurement": "obs",
            "units_of_measure": "units",
            "units_of_measure_code": "unit_code",
        }
    )

    # Dropping some columns, and reordering columns
    data = data.drop(
        columns=[
            "state_code",
            "county_code",
            "site_number",
            "datum",
            "qualifier",
            "uncertainty",
            "county",
            "state",
            "date_of_last_change",
            "date_local",
            "time_local",
            "date_gmt",
            "time_gmt",
            "poc",
            "unit_code",
            "sample_duration_code",
            "method_code",
        ]
    )
    cols = data.columns.tolist()
    cols.insert(0, cols.pop(cols.index("siteid")))
    cols.insert(1, cols.pop(cols.index("latitude")))
    cols.insert(2, cols.pop(cols.index("longitude")))
    cols.insert(3, cols.pop(cols.index("datetime_local")))
    cols.insert(4, cols.pop(cols.index("datetime_utc")))
    data = data.reindex(columns=cols)

    # Adjusting parameter units
    units = data.units.unique()
    for i in units:
        con = data.units == i
        if i.upper() == "Parts per billion Carbon".upper():
            data.loc[con, "units"] = "ppbC"
        if i == "Parts per billion":
            data.loc[con, "units"] = "ppb"
        if i == "Parts per million":
            data.loc[con, "units"] = "ppm"
    return data


def open_json(filename):
    """Opens the json file


    Parameters
    ----------
    filename
        Full path of file.

    Returns
    -------
    dict
        JSON file is opened and ready to be used by other functions in this code.
        Contains two dictionaries: 'Header' and 'Data'
    """

    with open(filename) as f:
        jsonf = json.load(f)
    return jsonf


def get_header(filename):
    """Finds basic header information in the JSON file.

    Parameters
    ----------
    filename
        Full path of file.

    Returns
    -------
    header : pandas.DataFrame
    """
    jsonf = open_json(filename)
    header = jsonf["Header"]
    header = pd.DataFrame.from_dict(header)
    return header


def write_csv(array, filename):
    """Write the dataframe `array` to a CSV file.


    Parameters
    ----------
    array : pandas.DataFrame
    filename
        Full path with filename of CSV file.
    """
    array.to_csv(filename, encoding="utf-8", index=False)
    return "csv file " + filename + " has been generated"
