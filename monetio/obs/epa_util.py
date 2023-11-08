def convert_epa_unit(df, obscolumn="SO2", unit="UG/M3"):
    """
    converts ppb to ug/m3 for SO2 in aqs and airnow datasets
    See 40 CFR Part 50.5, Appendix A-1 to part 50, appendix A=2 to Part 50.
    to convert from ppb to ug/m3 multiply by 2.6178.

    Also will convert from ug/m3 to ppb.

    Parameters
    ----------
    df : pandas dataframe
         self.df attribute from aqs or airnow class.
    obscolumn : string
        name of column with SO2 data in it.
    unit : string
        either 'UG/M3' or 'PPB' (not case sensitive)
        will convert data to this unit.
    inplace : boolean
        if TRUE then changes self.df attribute

    Returns
    -------
    df : pandas dataframe
        returns dataframe identical to original but with data converted to new
        unit.
    """
    factor = 2.6178
    ppb = "ppb"
    ugm3 = "ug/m3"
    if unit.lower() == ugm3:
        df = df[df["units"] == ppb]  # find columns with units of 'ppb'
        df["units"] = unit.upper()
        df[obscolumn] = df[obscolumn] * factor
    elif unit.lower() == ppb:
        df = df[df["units"] == ugm3]  # find columns with units of 'ppb'
        df[obscolumn] = df[obscolumn] / factor
    return df


def check_cmaq_units(df, param="O3", aqs_param="OZONE"):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    param : type
        Description of parameter `param` (the default is 'O3').
    aqs_param : type
        Description of parameter `aqs_param` (the default is 'OZONE').

    Returns
    -------
    type
        Description of returned object.

    """
    aunit = df[df.variable == aqs_param].Units.unique()[0]

    if aunit == "UG/M3":
        fac = 1.0
    elif aunit == "PPB":
        fac = 1000.0
    elif aunit == "ppbC":
        fac = 1000.0
        if aqs_param == "ISOPRENE":
            fac *= 5.0
        elif aqs_param == "BENZENE":
            fac *= 6.0
        elif aqs_param == "TOLUENE":
            fac *= 7.0
        elif aqs_param == "O-XYLENE":
            fac *= 8.0
    else:
        fac = 1.0
    return fac


def ensure_values_indomain(df, lon, lat):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    lon : type
        Description of parameter `lon`.
    lat : type
        Description of parameter `lat`.

    Returns
    -------
    type
        Description of returned object.

    """
    con = (
        (df.Latitude.values > lat.min())
        & (df.Latitude.values < lat.max())
        & (df.Longitude.values > lon.min())
        & (df.Longitude.values < lon.max())
    )

    df = df[con].copy()
    return df


def get_region(df):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import array, concatenate
    from pandas import DataFrame, merge

    se = array(["AL", "FL", "GA", "MS", "NC", "SC", "TN", "VA", "WV"])
    ne = array(["CT", "DE", "DC", "ME", "MD", "MA", "NH", "NJ", "NY", "PA", "RI", "VT"])
    nc = array(["IL", "IN", "IA", "KY", "MI", "MN", "MO", "OH", "WI"])
    sc = array(["AR", "LA", "OK", "TX"])
    r = array(["AZ", "CO", "ID", "KS", "MT", "NE", "NV", "NM", "ND", "SD", "UT", "WY"])
    p = array(["CA", "OR", "WA"])
    ner = array(["Northeast" for i in ne])
    ser = array(["Southeast" for i in se])
    ncr = array(["North_Central" for i in nc])
    scr = array(["South_Central" for i in sc])
    rr = array(["Rockies" for i in r])
    pr = array(["Pacific" for i in p])
    states = concatenate([se, ne, nc, sc, r, p])
    region = concatenate([ser, ner, ncr, scr, rr, pr])
    dd = DataFrame({"state_name": states, "region": region})
    return merge(df, dd, how="left", on="state_name")


def get_epa_location_df(df, param, site="", city="", region="", epa_region="", state=""):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    param : type
        Description of parameter `param`.
    site : type
        Description of parameter `site` (the default is '').
    city : type
        Description of parameter `city` (the default is '').
    region : type
        Description of parameter `region` (the default is '').
    epa_region : type
        Description of parameter `epa_region` (the default is '').
    state : type
        Description of parameter `state` (the default is '').

    Returns
    -------
    type
        Description of returned object.

    """
    new = df.groupby("variable").get_group(param)
    if site != "":
        if site in new.siteid.unique():
            df2 = new.loc[new.siteid == site]
            title = df2.siteid.unique().astype("str")[0].zfill(9)
    elif city != "":
        names = df.msa_name.dropna().unique()
        for i in names:
            if i.upper().find(city.upper()) != -1:
                name = i
                print(name)
        df2 = new[new["msa_name"] == name].copy().drop_duplicates()
        title = name
    elif state != "":
        df2 = new[new["state_name"].str.upper() == state.upper()].copy().drop_duplicates()
        title = "STATE: " + state.upper()
    elif region != "":
        df2 = new[new["Region"].str.upper() == region.upper()].copy().drop_duplicates()
        title = "REGION: " + region.upper()
    elif epa_region != "":
        df2 = new[new["EPA_region"].str.upper() == epa_region.upper()].copy().drop_duplicates()
        title = "EPA_REGION: " + epa_region.upper()
    else:
        df2 = new
        title = "Domain"
    return df2, title


def regulatory_resample(df, col="model", pollutant_standard=None):
    from pandas import concat, to_timedelta

    df["time_local"] = df.time + to_timedelta(df.gmt_offset, unit="H")
    if df.variable.unique()[0] == "CO":
        df1 = calc_daily_max(df, rolling_frequency=1)
        df1["pollutant_standard"] = "CO 1-hour 1971"
        df2 = calc_daily_max(df, rolling_frequency=8)
        df2["pollutant_standard"] = "CO 8-hour 1971"
        dfreturn = concat([df1, df2], ignore_index=True)
    elif df.variable.unique()[0] == "OZONE":
        dfreturn = calc_daily_max(df, rolling_frequency=8)
    elif df.variable.unique()[0] == "SO2":
        df1 = calc_daily_max(df, rolling_frequency=1)
        df1["pollutant_standard"] = "SO2 1-hour 1971"
        df2 = calc_daily_max(df, rolling_frequency=3)
        df2["pollutant_standard"] = "SO2 8-hour 1971"
        dfreturn = concat([df1, df2], ignore_index=True)
    elif df.variable.unique()[0] == "NO2":
        dfreturn = calc_daily_max(df, rolling_frequency=1)
    else:  # do daily average
        dfn = df.drop_duplicates(subset=["siteid"])
        df = (
            df.groupby("siteid")[col]
            .resample("D")
            .mean()
            .reset_index()
            .rename(columns={"level_1": "time_local"})
        )
        dfreturn = df.merge(dfn, how="left", on="siteid")
    return dfreturn


def calc_daily_max(df, param=None, rolling_frequency=8):
    from pandas import Index, to_timedelta

    if param is None:
        temp = df.copy()
    else:
        temp = df.groupby("variable").get_group(param)
    temp.index = temp.time_local
    if rolling_frequency > 1:
        g = (
            temp.groupby("siteid")["model", "gmt_offset"]
            .rolling(rolling_frequency, center=True, win_type="boxcar")
            .mean()
        )
        q = g.reset_index(level=0)
        k = (
            q.groupby("siteid")
            .resample("D")
            .max()
            .reset_index(level=1)
            .reset_index(drop="siteid")
            .dropna()
        )
    else:
        k = (
            temp.groupby("siteid")["model", "gmt_offset"]
            .resample("D")
            .max()
            .reset_index()
            .rename({"level_1": "time_local"})
        )
    columnstomerge = temp.columns[~temp.columns.isin(k.columns) * (temp.columns != "time")].append(
        Index(["siteid"])
    )
    if param is None:
        dff = k.merge(df[columnstomerge], on="siteid", how="left").drop_duplicates(
            subset=["siteid", "time_local"]
        )
    else:
        dff = k.merge(
            df.groupby("variable").get_group(param)[columnstomerge], on="siteid", how="left"
        ).drop_duplicates(subset=["siteid", "time_local"])
    dff["time"] = dff.time_local - to_timedelta(dff.gmt_offset, unit="H")
    return dff


def convert_statenames_to_abv(df):
    d = {
        "Alabama": "AL",
        "Alaska": "AK",
        "Arizona": "AZ",
        "Arkansas": "AR",
        "California": "CA",
        "Colorado": "CO",
        "Connecticut": "CT",
        "Delaware": "DE",
        "Florida": "FL",
        "Georgia": "GA",
        "Hawaii": "HI",
        "Idaho": "ID",
        "Illinois": "IL",
        "Indiana": "IN",
        "Iowa": "IA",
        "Kansas": "KS",
        "Kentucky": "KY",
        "Louisiana": "LA",
        "Maine": "ME",
        "Maryland": "MD",
        "Massachusetts": "MA",
        "Michigan": "MI",
        "Minnesota": "MN",
        "Mississippi": "MS",
        "Missouri": "MO",
        "Montana": "MT",
        "Nebraska": "NE",
        "Nevada": "NV",
        "New Hampshire": "NH",
        "New Jersey": "NJ",
        "New Mexico": "NM",
        "New York": "NY",
        "North Carolina": "NC",
        "North Dakota": "ND",
        "Ohio": "OH",
        "Oklahoma": "OK",
        "Oregon": "OR",
        "Pennsylvania": "PA",
        "Rhode Island": "RI",
        "South Carolina": "SC",
        "South Dakota": "SD",
        "state": "Postal",
        "Tennessee": "TN",
        "Texas": "TX",
        "Utah": "UT",
        "Vermont": "VT",
        "Virginia": "VA",
        "Washington": "WA",
        "West Virginia": "WV",
        "Wisconsin": "WI",
        "Wyoming": "WY",
    }
    for i in d:
        df["state_name"].loc[df.state_name.isin([i])] = d[i]
    df["state_name"].loc[df.state_name.isin(["Canada"])] = "CC"
    df["state_name"].loc[df.state_name.isin(["Mexico"])] = "MM"
    return df


def read_airnow_monitor_file(date=None, *, s3=True, v2=False):
    """Read site metadata from AirNow
    (`files.airnowtech.org <https://files.airnowtech.org/?prefix=airnow/>`__).

    Parameters
    ----------
    date : datetime, optional
        Date to read (object must implement ``.strftime()``).
        If ``None`` (default), the "today" file is read.
    s3 : bool
        Use S3 (default) or HTTP(S).
    v2 : bool
        Read the v2 format file (``Monitoring_Site_Locations_V2.dat``) for the day.
        By default the original format file is read (``monitoring_site_locations.dat``).

    Returns
    -------
    pandas.DataFrame
        Site metadata dataframe, free of ``siteid`` duplicates.
    """
    import sys

    import pandas as pd

    if v2:
        fn = "Monitoring_Site_Locations_V2.dat"
    else:
        fn = "monitoring_site_locations.dat"

    if s3 and sys.version_info >= (3, 7):
        base = "s3://files.airnowtech.org/airnow/"
    else:
        base = "https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/"

    if date is None:
        sub = "today/"
    else:
        sub = date.strftime(r"%Y/%Y%m%d/")

    url = f"{base}{sub}{fn}"

    if v2:
        # Column names are included in the file's first line.
        # |StationID|AQSID|FullAQSID
        # |Parameter|MonitorType
        # |SiteCode|SiteName|Status
        # |AgencyID|AgencyName|EPARegion
        # |Latitude|Longitude|Elevation
        # |GMTOffset|CountryFIPS
        # |CBSA_ID|CBSA_Name
        # |StateAQSCode|StateAbbreviation
        # |CountyAQSCode|CountyName
        names = [
            "siteid",
            "aqsid",
            "aqsid_full",
            "parameter",
            "monitor_type",
            "site_code",
            "site_name",
            "status",
            "agency_id",
            "agency_name",
            "epa_region",
            "latitude",
            "longitude",
            "elevation",
            "gmt_offset",
            "country_code",
            "msa_code",
            "msa_name",
            "state_code",
            "state_name",
            "county_code",
            "county_name",
        ]
        dtype = {i: str for i in range(len(names))}
        for vn in ["latitude", "longitude", "elevation", "gmt_offset"]:
            dtype[names.index(vn)] = float
        df = pd.read_csv(
            url,
            delimiter="|",
            header=None,
            skiprows=1,
            dtype=dtype,
        )
        df.columns = names
    else:
        # Original format.
        # Column names are _not_ included in the file.
        # Documentation is available at:
        # https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/docs/MonitoringSiteFactSheet.pdf
        # But there are more columns than in there (19):
        # - CMSA (consolidated MSA) code and name
        # - city code and name
        # But note that these are usually null
        # (maybe since they are not included in the v2 format?).
        names = [
            "siteid",
            "parameter",
            "site_code",
            "site_name",
            "status",
            "agency",
            "agency_name",
            "epa_region",
            "latitude",
            "longitude",
            "elevation",
            "gmt_offset",
            "country_code",
            "cmsa_code",
            "cmsa_name",
            "msa_code",
            "msa_name",
            "state_code",
            "state_name",
            "county_code",
            "county_name",
            "city_code",
            "city_name",
        ]
        dtype = {i: str for i in range(len(names))}
        for vn in ["latitude", "longitude", "elevation", "gmt_offset"]:
            dtype[names.index(vn)] = float
        df = pd.read_csv(
            url,
            delimiter="|",
            header=None,
            dtype=dtype,
            encoding="ISO-8859-1",
        )
        df.columns = names

    # fmt: off
    df = (
        df
        .drop(columns=["parameter"])
        .drop_duplicates(subset=["siteid"])
        .reset_index(drop=True)
    )
    # fmt: on

    return df


def get_aqs_metadata(*, sites_file=None, monitors_file=None):
    """Create an AQS site metadata dataframe by combining three data sources:

    * sites: https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip
    * monitors: https://aqs.epa.gov/aqsweb/airdata/aqs_monitors.zip
    * AirNow site metadata: :func:`read_airnow_monitor_file`

    Parameters
    ----------
    sites_file, monitors_file : path-like, optional
        Paths to the AQS sites and monitors files
        (``aqs_sites.zip`` and ``aqs_monitors.zip``;
        default is to download from the EPA website).

    """
    import pandas as pd

    # #   Column
    # ---  ------
    # 0   State Code
    # 1   County Code
    # 2   Site Number
    # 3   Latitude
    # 4   Longitude
    # 5   Datum
    # 6   Elevation
    # 7   Land Use
    # 8   Location Setting
    # 9   Site Established Date
    # 10  Site Closed Date
    # 11  Met Site State Code
    # 12  Met Site County Code
    # 13  Met Site Site Number
    # 14  Met Site Type
    # 15  Met Site Distance
    # 16  Met Site Direction  (N, S, etc.)
    # 17  GMT Offset
    # 18  Owning Agency
    # 19  Local Site Name
    # 20  Address
    # 21  Zip Code
    # 22  State Name
    # 23  County Name
    # 24  City Name
    # 25  CBSA Name
    # 26  Tribe Name
    # 27  Extraction Date
    sites = pd.read_csv(
        "https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip",
        encoding="ISO-8859-1",
        dtype=str,
    )
    for vn in ["Latitude", "Longitude", "Elevation", "GMT Offset", "Met Site Distance"]:
        sites[vn] = sites[vn].astype(float)
    for vn in ["Extraction Date"]:
        sites[vn] = pd.to_datetime(sites[vn], format=r"%Y-%m-%d", exact=True)
    sites = sites.rename(columns=lambda vn: vn.replace(" ", "_").lower())

    # monitors = pd.read_csv(
    #     "https://aqs.epa.gov/aqsweb/airdata/aqs_monitors.zip",
    #     encoding="ISO-8859-1",
    # )

    airnow = read_airnow_monitor_file(date=None, v2=False)
    airnow["airnow_flag"] = True


def read_monitor_file(network=None, airnow=False, drop_latlon=True):
    import os

    import pandas as pd

    if airnow:
        return read_airnow_monitor_file(date=None, v2=False)
    else:
        try:
            basedir = os.path.abspath(os.path.dirname(__file__))[:-3]
            fname = os.path.join(basedir, "data", "monitoring_site_locations.hdf")
            if os.path.isfile(fname):
                print("Monitor File Path: " + fname)
                sss = pd.read_hdf(fname)
            # monitor_drop = ['state_code', u'county_code']
            # s.drop(monitor_drop, axis=1, inplace=True)
        except Exception:
            print("Monitor File Not Found... Reprocessing")
            baseurl = "https://aqs.epa.gov/aqsweb/airdata/"
            site_url = baseurl + "aqs_sites.zip"
            # has network info (CSN IMPROVE etc....)
            monitor_url = baseurl + "aqs_monitors.zip"
            # Airnow monitor file
            monitor_airnow_url = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/today/monitoring_site_locations.dat"
            colsinuse = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
            airnow = pd.read_csv(
                monitor_airnow_url,
                delimiter="|",
                header=None,
                usecols=colsinuse,
                dtype={0: str},
                encoding="ISO-8859-1",
            )
            airnow.columns = [
                "siteid",
                "Site_Code",
                "Site_Name",
                "Status",
                "Agency",
                "Agency_Name",
                "EPA_region",
                "latitude",
                "longitude",
                "Elevation",
                "GMT_Offset",
                "Country_Code",
                "CMSA_Code",
                "CMSA_Name",
                "MSA_Code",
                "MSA_Name",
                "state_Code",
                "state_Name",
                "County_Code",
                "County_Name",
                "City_Code",
            ]
            airnow["airnow_flag"] = "AIRNOW"
            airnow.columns = [i.lower() for i in airnow.columns]
            # Read EPA Site file
            site = pd.read_csv(site_url, encoding="ISO-8859-1")
            # read epa monitor file
            monitor = pd.read_csv(monitor_url, encoding="ISO-8859-1")
            # make siteid column
            site["siteid"] = (
                site["State Code"].astype(str).str.zfill(2)
                + site["County Code"].astype(str).str.zfill(3)
                + site["Site Number"].astype(str).str.zfill(4)
            )
            monitor["siteid"] = (
                monitor["State Code"].astype(str).str.zfill(2)
                + monitor["County Code"].astype(str).str.zfill(3)
                + monitor["Site Number"].astype(str).str.zfill(4)
            )
            site.columns = [i.replace(" ", "_") for i in site.columns]
            s = monitor.merge(
                site[["siteid", "Land_Use", "Location_Setting", "GMT_Offset"]],
                on=["siteid"],
                how="left",
            )
            s.columns = [i.replace(" ", "_").lower() for i in s.columns]
            monitor_drop = [
                "state_code",
                "county_code",
                "site_number",
                "extraction_date",
                "parameter_code",
                "parameter_name",
                "poc",
                "last_sample_date",
                "pqao",
                "reporting_agency",
                "exclusions",
                "monitoring_objective",
                "last_method_code",
                "last_method",
                "naaqs_primary_monitor",
                "qa_primary_monitor",
            ]
            s.drop(monitor_drop, axis=1, inplace=True)
            # drop airnow keys for merge
            airnow_drop = [
                "site_Code",
                "site_Name",
                "status",
                "agency",
                "agency_name",
                "country_code",
                "cmsa_code",
                "state_code",
                "county_code",
                "city_code",
                "latitude",
                "longitude",
                "gmt_offset",
                "state_name",
                "county_name",
            ]
            airnow_drop = [i.lower() for i in airnow_drop]
            airnow.drop(airnow_drop, axis=1, inplace=True)
            ss = pd.concat([s, airnow], ignore_index=True, sort=True)
            sss = convert_statenames_to_abv(ss).dropna(subset=["latitude", "longitude"])
        if network is not None:
            sss = sss.loc[sss.networks.isin([network])].drop_duplicates(subset=["siteid"])
        # Getting error that 'latitude' 'longitude' not contained in axis
        drop_latlon = False
        if drop_latlon:
            if pd.Series(sss.keys()).isin(["latitude", "longitude"]):
                return sss.drop(["latitude", "longitude"], axis=1).drop_duplicates()
        else:
            return sss.drop_duplicates()
