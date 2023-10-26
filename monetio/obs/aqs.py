"""
AQS -- the EPA Air Quality System

Primary website: https://www.epa.gov/aqs

The EPA AQS is a collection of ambient air pollution data measured by
the EPA, state and local government agencies, and `tribal <https://www.epa.gov/tribal-air>`__
air pollution control agencies.

MONETIO can retrieve hourly and daily AQS data.
A certain AQS file:

* corresponds to a certain year
* is either hourly or daily
* contains just one variable (e.g. ozone)

For example, the hourly ozone data file for 2018 can be found at:
https://aqs.epa.gov/aqsweb/airdata/hourly_44201_2018.zip

Available Measurements
^^^^^^^^^^^^^^^^^^^^^^

* O3 (OZONE)
* PM2.5 (PM2.5)
* PM2.5_frm (PM2.5)
* PM10
* SO2
* NO2
* CO
* NONOxNOy
* VOC
* Speciated PM (SPEC)
* Speciated PM10 (PM10SPEC)
* Wind Speed and Direction (WIND, WS, WDIR)
* Temperature (TEMP)
* Relative Humidity and Dew Point Temperature (RHDP)

Available Networks
^^^^^^^^^^^^^^^^^^

* NCORE (https://www3.epa.gov/ttn/amtic/ncore.html)
* CSN (https://www.epa.gov/amtic/chemical-speciation-network-csn)
* CASTNET (https://www.epa.gov/castnet)
* IMPROVE (http://vista.cira.colostate.edu/Improve/)
* PAMS (https://www.epa.gov/amtic/photochemical-assessment-monitoring-stations-pams)
* SCHOOL AIR TOXICS (https://www3.epa.gov/ttnamti1/airtoxschool.html)
* NEAR ROAD (NO2; https://www.epa.gov/amtic/no2-monitoring-near-road-monitoring)
* NATTS (https://www3.epa.gov/ttnamti1/natts.html)
"""
import inspect
import os
import warnings

import pandas as pd

from .epa_util import read_monitor_file


def add_data(
    dates,
    param=None,
    *,
    daily=False,
    network=None,
    download=False,
    local=False,
    wide_fmt=True,
    n_procs=1,
    meta=False,
):
    """Retrieve and load AQS data as a DataFrame.

    Parameters
    ----------
    dates : array-like of datetime-like
        Dates to retrieve.
    param : str or list of str, optional
        Parameters to retrieve.
        By default::

            params = [
                "SPEC",
                "PM10", "PM2.5", "PM2.5_FRM",
                "CO", "OZONE", "SO2",
                "VOC", "NONOXNOY",
                "WIND", "TEMP", "RHDP",
            ]
    daily : bool, default: False
        Whether to retrieve daily data. Default is to retrieve hourly.
    network : str, optional
        Subset the returned data by measurement network.
        Only used if combined with ``meta=True``.
    download : bool, default: False
        Download the AQS files to the local directory.
        By default they are loaded into memory and not kept.
    local : bool, default: False
        Load the files from the local directory instead of retrieving from the AQS web server.
    wide_fmt : bool, default: True
        Whether to convert the table to wide format (column for each variable).
    n_procs : int, optional
        For Dask.
        Since the AQS data are in yearly files, this is only relevant if you are
        requesting multiple years.
    meta : bool, default: Falsea
        Whether to add additional site metadata.

    Returns
    -------
    DataFrame
    """
    from ..util import long_to_wide

    # if daily and wide_fmt and not meta:
    #     raise ValueError("returning wide-format daily data requires `meta=True`")
    #         .. important::
    #            Retrieving wide-format daily data (the default) currently requires ``meta=True``
    #            in order to obtain UTC offsets for computing UTC time.

    if network is not None and not meta:
        raise ValueError("subsetting by network before returning requires `meta=True`")

    a = AQS()
    df = a.add_data(
        dates,
        param=param,
        daily=daily,
        network=network,
        download=download,
        local=local,
        n_procs=n_procs,
        meta=meta,
    )

    if wide_fmt:
        df = long_to_wide(df)

    return df.reset_index(drop=True)


class AQS:
    """Short summary.

    Attributes
    ----------
    baseurl : type
        Description of attribute `baseurl`.
    objtype : type
        Description of attribute `objtype`.
    baseurl : type
        Description of attribute `baseurl`.
    dates : type
        Description of attribute `dates`.
    renamedhcols : type
        Description of attribute `renamedhcols`.
    renameddcols : type
        Description of attribute `renameddcols`.
    savecols : type
        Description of attribute `savecols`.
    df : type
        Description of attribute `df`.
    monitor_file : type
        Description of attribute `monitor_file`.
    __class__ : type
        Description of attribute `__class__`.
    monitor_df : type
        Description of attribute `monitor_df`.
    daily : type
        Description of attribute `daily`.
    d_df : type
        Description of attribute `d_df`.

    """

    def __init__(self):
        #        self.baseurl = 'https://aqs.epa.gov/aqsweb/airdata/'
        self.objtype = "AQS"
        self.baseurl = "https://aqs.epa.gov/aqsweb/airdata/"
        # self.renamedhcols = [
        #    'time', 'time_local', 'state_code', 'county_code', 'site_num',
        #    'parameter_code', 'poc', 'latitude', 'longitude', 'datum',
        #    'parameter_name', 'obs', 'units', 'mdl', 'uncertainty',
        #    'qualifier', 'method_type', 'method_code', 'method_name',
        #    'state_name', 'county_name', 'date_of_last_change'
        # ]
        self.renameddcols = [
            "time_local",
            "state_code",
            "county_code",
            "site_num",
            "parameter_code",
            "poc",
            "latitude",
            "longitude",
            "datum",
            "parameter_name",
            "sample_duration",
            "pollutant_standard",
            "units",
            "event_type",
            "observation_Count",
            "observation_Percent",
            "obs",
            "1st_max_Value",
            "1st_max_hour",
            "aqi",
            "method_code",
            "method_name",
            "local_site_name",
            "address",
            "state_name",
            "county_name",
            "city_name",
            "msa_name",
            "date_of_last_change",
        ]
        self.savecols = [
            "time_local",
            "time",
            "siteid",
            "latitude",
            "longitude",
            "obs",
            "units",
            "variable",
        ]
        self.df = pd.DataFrame()  # hourly dataframe
        self.monitor_file = (
            inspect.getfile(self.__class__)[:-10] + "data/monitoring_site_locations.dat"
        )
        self.monitor_df = None
        self.daily = False
        self.d_df = None  # daily dataframe

    def columns_rename(self, columns, verbose=False):
        """
        rename columns for hourly data.
        Parameters
        ----------
        columns : list of strings
                  list of current columns
        verbose : boolean

        Returns
        --------
        rcolumn: list of strings
                 list of new column names to use.
        """
        rcolumn = []
        for ccc in columns:
            if ccc.strip() == "Sample Measurement":
                newc = "obs"
            elif ccc.strip() == "Units of Measure":
                newc = "units"
            else:
                newc = ccc.strip().lower()
                newc = newc.replace(" ", "_")
            if verbose:
                print(ccc + " renamed " + newc)
            rcolumn.append(newc)
        return rcolumn

    def load_aqs_file(self, url, network):
        """Short summary.

        Parameters
        ----------
        url : type
            Description of parameter `url`.
        network : type
            Description of parameter `network`.

        Returns
        -------
        type
            Description of returned object.

        """
        from datetime import datetime

        if "daily" in url:

            def dateparse(x):
                return datetime.strptime(x, "%Y-%m-%d")

            df = pd.read_csv(
                url,
                parse_dates={"time_local": ["Date Local"]},
                date_parser=dateparse,
                dtype={0: str, 1: str, 2: str},
                encoding="ISO-8859-1",
            )
            df.columns = self.renameddcols
            df["pollutant_standard"] = df.pollutant_standard.astype(str)
            self.daily = True
            # df.rename(columns={'parameter_name':'variable'})
        else:
            df = pd.read_csv(
                url,
                parse_dates={
                    "time": ["Date GMT", "Time GMT"],
                    "time_local": ["Date Local", "Time Local"],
                },
                infer_datetime_format=True,
                dtype={17: str},
            )
            # print(df.columns.values)
            df.columns = self.columns_rename(df.columns.values)

        df["siteid"] = (
            df.state_code.astype(str).str.zfill(2)
            + df.county_code.astype(str).str.zfill(3)
            + df.site_num.astype(str).str.zfill(4)
        )
        # df['siteid'] = df.state_code + df.county_code + df.site_num
        df.drop(["state_name", "county_name"], axis=1, inplace=True)
        df.columns = [i.lower() for i in df.columns]
        if "daily" not in url:
            df.drop(["datum", "qualifier"], axis=1, inplace=True)
        if "VOC" in url:
            voc = True
        else:
            voc = False
        df = self.get_species(df, voc=voc)
        return df.drop("date_of_last_change", axis=1)

    def build_url(self, param, year, daily=False, download=False):
        """Short summary.

        Parameters
        ----------
        param : type
            Description of parameter `param`.
        year : type
            Description of parameter `year`.
        daily : type
            Description of parameter `daily` (the default is False).
        download : type
            Description of parameter `download` (the default is False).

        Returns
        -------
        type
            Description of returned object.

        """
        if daily:
            beginning = self.baseurl + "daily_"
            fname = "daily_"
        else:
            beginning = self.baseurl + "hourly_"
            fname = "hourly_"
        if (param.upper() == "OZONE") | (param.upper() == "O3"):
            code = "44201_"
        elif param.upper() == "PM2.5":
            code = "88101_"
        elif param.upper() == "PM2.5_FRM":
            code = "88502_"
        elif param.upper() == "PM10":
            code = "81102_"
        elif param.upper() == "SO2":
            code = "42401_"
        elif param.upper() == "NO2":
            code = "42602_"
        elif param.upper() == "CO":
            code = "42101_"
        elif param.upper() == "NONOxNOy".upper():
            code = "NONOxNOy_"
        elif param.upper() == "VOC":
            # https://aqs.epa.gov/aqsweb/airdata/daily_VOCS_2017.zip
            code = "VOCS_"
        elif param.upper() == "SPEC":
            code = "SPEC_"
        elif param.upper() == "PM10SPEC":
            code = "PM10SPEC_"
        elif param.upper() == "WIND":
            code = "WIND_"
        elif param.upper() == "TEMP":
            code = "TEMP_"
        elif param.upper() == "RHDP":
            code = "RH_DP_"
        elif (param.upper() == "WIND") | (param.upper() == "WS") | (param.upper() == "WDIR"):
            code = "WIND_"
        url = beginning + code + year + ".zip"
        fname = fname + code + year + ".zip"

        return url, fname

    def build_urls(self, params, dates, daily=False):
        """Short summary.

        Parameters
        ----------
        params : type
            Description of parameter `params`.
        dates : list of datetime objects
            dates to retrieve data for. Only the years are taken into account.
        daily : type
            Description of parameter `daily` (the default is False).

        Returns
        -------
        type
            Description of returned object.

        """
        import requests

        years = pd.DatetimeIndex(dates).year.unique().astype(str)
        urls = []
        fnames = []
        for i in params:
            for y in years:
                url, fname = self.build_url(i, y, daily=daily)
                if int(requests.get(url, stream=True).headers["Content-Length"]) < 500:
                    print("File is Empty. Not Processing", url)
                else:
                    urls.append(url)
                    fnames.append(fname)

        return urls, fnames

    def retrieve(self, url, fname):
        """Short summary.

        Parameters
        ----------
        url : type
            Description of parameter `url`.
        fname : type
            Description of parameter `fname`.

        Returns
        -------
        type
            Description of returned object.

        """
        import requests

        if not os.path.isfile(fname):
            print("\n Retrieving: " + fname)
            print(url)
            print("\n")
            r = requests.get(url)
            open(fname, "wb").write(r.content)
        else:
            print("\n File Exists: " + fname)

    def add_data(
        self,
        dates,
        param=None,
        daily=False,
        network=None,
        download=False,
        local=False,
        n_procs=1,
        meta=False,
    ):
        """Short summary.

        Parameters
        ----------
        dates : list of datetime objects
            Description of parameter `dates`.
        param : list of strings
            Description of parameter `param` (the default is None).
        daily : boolean
            Description of parameter `daily` (the default is False).
        network : type
            Description of parameter `network` (the default is None).
        download : type
            Description of parameter `download` (the default is False).

        Returns
        -------
        pandas DataFrame
            Description of returned object.

        """
        import dask
        import dask.dataframe as dd

        if param is None:
            params = [
                "SPEC",
                "PM10",
                "PM2.5",
                "PM2.5_FRM",
                "CO",
                "OZONE",
                "SO2",
                "VOC",
                "NONOXNOY",
                "WIND",
                "TEMP",
                "RHDP",
            ]
        elif isinstance(param, str):
            params = [param]
        else:
            params = param
        urls, fnames = self.build_urls(params, dates, daily=daily)
        if download:
            for url, fname in zip(urls, fnames):
                self.retrieve(url, fname)
            dfs = [dask.delayed(self.load_aqs_file)(i, network) for i in fnames]
        elif local:
            dfs = [dask.delayed(self.load_aqs_file)(i, network) for i in fnames]
        else:
            dfs = [dask.delayed(self.load_aqs_file)(i, network) for i in urls]
        dff = dd.from_delayed(dfs)
        dfff = dff.compute(num_workers=n_procs)
        if daily:
            # Daily data are just by date
            dfff["time"] = dfff.time_local
        if meta:
            return self.add_data2(dfff, daily, network)
        else:
            return dfff

    def add_data2(self, df, daily=False, network=None):
        """Add additional site metadata.

        Parameters
        ------------
        df : dataframe
        daily : boolean
        network :

        Returns:
        self.df.copy() : dataframe
        """
        self.df = df
        self.df = self.change_units(self.df)
        if self.monitor_df is None:
            self.monitor_df = read_monitor_file()
            drop_monitor_cols = True
        else:
            drop_monitor_cols = False
        if daily:
            if drop_monitor_cols:
                monitor_drop = ["msa_name", "city_name", "local_site_name", "address", "datum"]
                self.monitor_df.drop(monitor_drop, axis=1, inplace=True)
        # else:
        #     monitor_drop = [u'datum']
        #     self.monitor_df.drop(monitor_drop, axis=1, inplace=True)
        if network is not None:
            # TODO: really should split the networks strings (on semicolon) to ensure no false positive matchs
            monitors = self.monitor_df.loc[
                self.monitor_df.networks.astype(str).str.contains(network)
            ].drop_duplicates(subset=["siteid"])
        else:
            monitors = self.monitor_df.drop_duplicates(subset=["networks", "siteid"])
        mlist = ["siteid"]
        self.df = pd.merge(self.df, monitors, on=mlist, how="left")
        if "latitude_x" in self.df:
            self.df = self.df.drop(columns=["latitude_y", "longitude_y"]).rename(
                columns={
                    "latitude_x": "latitude",
                    "longitude_x": "longitude",
                }
            )
        if network is not None:
            self.df = self.df.dropna(subset="networks")
        # if daily:
        #     self.df["time"] = self.df.time_local - pd.to_timedelta(self.df.gmt_offset, unit="H")
        if pd.Series(self.df.columns).isin(["parameter_name"]).max():
            self.df.drop("parameter_name", axis=1, inplace=True)
        return self.df  # .copy()

    def get_species(self, df, voc=False):
        """Short summary.

        Parameterssdfdsf
        ----------
        df : type
            Description of parameter `df`.
        voc : type
            Description of parameter `voc` (the default is False).

        Returns
        -------
        type
            Description of returned object.

        """
        pc = df.parameter_code.unique()
        df["variable"] = ""
        if voc:
            df["variable"] = df.parameter_name.str.upper()
            return df
        for i in pc:
            con = df.parameter_code == i
            if (i == 88101) | (i == 88502):
                df.loc[con, "variable"] = "PM2.5"
            if i == 44201:
                df.loc[con, "variable"] = "OZONE"
            if i == 81102:
                df.loc[con, "variable"] = "PM10"
            if i == 42401:
                df.loc[con, "variable"] = "SO2"
            if i == 42602:
                df.loc[con, "variable"] = "NO2"
            if i == 42101:
                df.loc[con, "variable"] = "CO"
            if i == 62101:
                df.loc[con, "variable"] = "TEMP"
            if i == 88305:
                df.loc[con, "variable"] = "OC"
            if i == 88306:
                df.loc[con, "variable"] = "NO3f"
            if i == 88307:
                df.loc[con, "variable"] = "ECf"
            if i == 88316:
                df.loc[con, "variable"] = "ECf_optical"
            if i == 88403:
                df.loc[con, "variable"] = "SO4f"
            if i == 88312:
                df.loc[con, "variable"] = "TCf"
            if i == 88104:
                df.loc[con, "variable"] = "Alf"
            if i == 88107:
                df.loc[con, "variable"] = "Baf"
            if i == 88313:
                df.loc[con, "variable"] = "BCf"
            if i == 88109:
                df.loc[con, "variable"] = "Brf"
            if i == 88110:
                df.loc[con, "variable"] = "Cdf"
            if i == 88111:
                df.loc[con, "variable"] = "Caf"
            if i == 88117:
                df.loc[con, "variable"] = "Cef"
            if i == 88118:
                df.loc[con, "variable"] = "Csf"
            if i == 88203:
                df.loc[con, "variable"] = "Cl-f"
            if i == 88115:
                df.loc[con, "variable"] = "Clf"
            if i == 88112:
                df.loc[con, "variable"] = "Crf"
            if i == 88113:
                df.loc[con, "variable"] = "Cof"
            if i == 88114:
                df.loc[con, "variable"] = "Cuf"
            if i == 88121:
                df.loc[con, "variable"] = "Euf"
            if i == 88143:
                df.loc[con, "variable"] = "Auf"
            if i == 88127:
                df.loc[con, "variable"] = "Hff"
            if i == 88131:
                df.loc[con, "variable"] = "Inf"
            if i == 88126:
                df.loc[con, "variable"] = "Fef"
            if i == 88146:
                df.loc[con, "variable"] = "Laf"
            if i == 88128:
                df.loc[con, "variable"] = "Pbf"
            if i == 88140:
                df.loc[con, "variable"] = "Mgf"
            if i == 88132:
                df.loc[con, "variable"] = "Mnf"
            if i == 88142:
                df.loc[con, "variable"] = "Hgf"
            if i == 88134:
                df.loc[con, "variable"] = "Mof"
            if i == 88136:
                df.loc[con, "variable"] = "Nif"
            if i == 88147:
                df.loc[con, "variable"] = "Nbf"
            if i == 88310:
                df.loc[con, "variable"] = "NO3f"
            if i == 88152:
                df.loc[con, "variable"] = "Pf"
            if i == 88303:
                df.loc[con, "variable"] = "K+f"
            if i == 88176:
                df.loc[con, "variable"] = "Rbf"
            if i == 88162:
                df.loc[con, "variable"] = "Smf"
            if i == 88163:
                df.loc[con, "variable"] = "Scf"
            if i == 88154:
                df.loc[con, "variable"] = "Sef"
            if i == 88165:
                df.loc[con, "variable"] = "Sif"
            if i == 88166:
                df.loc[con, "variable"] = "Agf"
            if i == 88302:
                df.loc[con, "variable"] = "Na+f"
            if i == 88184:
                df.loc[con, "variable"] = "Naf"
            if i == 88168:
                df.loc[con, "variable"] = "Srf"
            if i == 88403:
                df.loc[con, "variable"] = "SO4f"
            if i == 88169:
                df.loc[con, "variable"] = "Sf"
            if i == 88170:
                df.loc[con, "variable"] = "Taf"
            if i == 88172:
                df.loc[con, "variable"] = "Tbf"
            if i == 88160:
                df.loc[con, "variable"] = "Snf"
            if i == 88161:
                df.loc[con, "variable"] = "Tif"
            if i == 88312:
                df.loc[con, "variable"] = "TOT_Cf"
            if i == 88310:
                df.loc[con, "variable"] = "NON-VOLITILE_NO3f"
            if i == 88309:
                df.loc[con, "variable"] = "VOLITILE_NO3f"
            if i == 88186:
                df.loc[con, "variable"] = "Wf"
            if i == 88314:
                df.loc[con, "variable"] = "C_370nmf"
            if i == 88179:
                df.loc[con, "variable"] = "Uf"
            if i == 88164:
                df.loc[con, "variable"] = "Vf"
            if i == 88183:
                df.loc[con, "variable"] = "Yf"
            if i == 88167:
                df.loc[con, "variable"] = "Znf"
            if i == 88185:
                df.loc[con, "variable"] = "Zrf"
            if i == 88102:
                df.loc[con, "variable"] = "Sbf"
            if i == 88103:
                df.loc[con, "variable"] = "Asf"
            if i == 88105:
                df.loc[con, "variable"] = "Bef"
            if i == 88124:
                df.loc[con, "variable"] = "Gaf"
            if i == 88185:
                df.loc[con, "variable"] = "Irf"
            if i == 88180:
                df.loc[con, "variable"] = "Kf"
            if i == 88301:
                df.loc[con, "variable"] = "NH4+f"
            if (i == 88320) | (i == 88355):
                df.loc[con, "variable"] = "OCf"
            if (i == 88357) | (i == 88321):
                df.loc[con, "variable"] = "ECf"
            if i == 42600:
                df.loc[con, "variable"] = "NOY"
            if i == 42601:
                df.loc[con, "variable"] = "NO"
            if i == 42603:
                df.loc[con, "variable"] = "NOX"
            if (i == 61103) | (i == 61101):
                df.loc[con, "variable"] = "WS"
            if (i == 61104) | (i == 61102):
                df.loc[con, "variable"] = "WD"
            if i == 62201:
                df.loc[con, "variable"] = "RH"
            if i == 62103:
                df.loc[con, "variable"] = "DP"

        # For any remaining, use parameter_name but warn
        con = df.variable == ""
        if con.sum() > 0:
            _tbl = (
                df[con][["parameter_name", "parameter_code"]]
                .drop_duplicates("parameter_name")
                .to_string(index=False)
            )
            warnings.warn(f"Short names not available for these variables:\n{_tbl}")
        df.loc[con, "variable"] = df.parameter_name

        return df

    @staticmethod
    def change_units(df):
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
        units = df.units.unique()
        for i in units:
            con = df.units == i
            if i.upper() == "Parts per billion Carbon".upper():
                df.loc[con, "units"] = "ppbC"
            if i == "Parts per billion":
                df.loc[con, "units"] = "ppb"
            if i == "Parts per million":
                df.loc[con, "units"] = "ppm"
            if i == "Micrograms/cubic meter (25 C)":
                df.loc[con, "units"] = "UG/M3".lower()
            if i == "Degrees Centigrade":
                df.loc[con, "units"] = "C"
            if i == "Micrograms/cubic meter (LC)":
                df.loc[con, "units"] = "UG/M3".lower()
            if i == "Knots":
                df.loc[con, "obs"] *= 0.51444
                df.loc[con, "units"] = "M/S".lower()
            if i == "Degrees Fahrenheit":
                df.loc[con, "obs"] = (df.loc[con, "obs"] + 459.67) * 5.0 / 9.0
                df.loc[con, "units"] = "K"
            if i == "Percent relative humidity":
                df.loc[con, "units"] = "%"
        return df
