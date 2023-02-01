"""
NAME: cems_api.py
PGRMMER: Alice Crawford   ORG: ARL
This code written at the NOAA air resources laboratory
Python 3
#################################################################
The key and url for the epa api should be stored in a file called
.epaapirc in the $HOME directory.
The contents should be
key: apikey
url: https://api.epa.gov/FACT/1.0/

TO DO
-----
Date is in local time (not daylight savings)
Need to convert to UTC. This will require an extra package or api.

Classes:
----------
EpaApiObject - Base class
   EmissionsCall
   FacilitiesData
   MonitoringPlan

Emissions
CEMS

Functions:
----------
addquarter
get_datelist
findquarter
sendrequest
getkey

"""

import copy
import datetime
import os
import sys
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests

# import pytz
import seaborn as sns

import monetio.obs.obs_util as obs_util


def test_end(endtime, current):
    # if endtime None return True
    if isinstance(endtime, pd._libs.tslibs.nattype.NaTType):
        return True
    elif not endtime:
        return True
    # if endtime greater than current return true
    elif endtime >= current:
        return True
    # if endtime less than current time return true
    elif endtime < current:
        return False
    else:
        return True


def get_filename(fname, prompt):
    """
    determines if file exists. If prompt is True then will prompt for
    new filename if file does not exist.
    """
    if fname:
        done = False
        iii = 0
        while not done:
            if iii > 2:
                done = True
            iii += 1
            if os.path.isfile(fname):
                done = True
            elif prompt:
                istr = "\n" + fname + " is not a valid name for Facilities Data \n"
                istr += "Please enter a new filename \n"
                istr += "enter None to load from the api \n"
                istr += "enter x to exit program \n"
                fname = input(istr)
                # print('checking ' + fname)
                if fname == "x":
                    sys.exit()
                if fname.lower() == "none":
                    fname = None
                    done = True
            else:
                fname = None
                done = True
    return fname


# def get_timezone_offset(latitude, longitude):
#    """
#    uses geonames API
#    must store username in the $HOME/.epaapirc file
#    geousername: username
#    """
#    username = getkey()
#    print(username)
#    username = username["geousername"]
#    url = "http://api.geonames.org/timezoneJSON?lat="
#    request = url + str(latitude)
#    request += "&lng="
#    request += str(longitude)
#    request += "&username="
#    request += username
#    try:
#        data = requests.get(request)
#    except BaseException:
#        data = -99
#
#    jobject = data.json()
#    print(jobject)
#    print(data)
#    # raw offset should give standard time offset.
#    if data == -99:
#        return 0
#    else:
#        offset = jobject["rawOffset"]
#        return offset


def getkey():
    """
    key and url should be stored in $HOME/.epaapirc
    """
    dhash = {}
    homedir = os.environ["HOME"]
    fname = "/.epaapirc"
    if os.path.isfile(homedir + fname):
        with open(homedir + fname) as fid:
            lines = fid.readlines()
        for temp in lines:
            temp = temp.split(" ")
            dhash[temp[0].strip().replace(":", "")] = temp[1].strip()
    else:
        dhash["key"] = None
        dhash["url"] = None
        dhash["geousername"] = None
    return dhash


def sendrequest(rqq, key=None, url=None):
    """
    Method for sending requests to the EPA API
    Inputs :
    --------
    rqq : string
          request string.
    Returns:
    --------
    data : response object
    """
    if not key or not url:
        keyhash = getkey()
        apiurl = keyhash["url"]
        key = keyhash["key"]
    if key:
        # apiurl = "https://api.epa.gov/FACT/1.0/"
        rqq = apiurl + rqq + "?api_key=" + key
        print("Request: ", rqq)
        data = requests.get(rqq)
        print("Status Code", data.status_code)
        if data.status_code == 429:
            print("Too many requests Please Wait before trying again.")
            sys.exit()
    else:
        print("WARNING: your api key for EPA data was not found")
        print("Please obtain a key from")
        print("https://www.epa.gov/airmarkets/field-audit-checklist_tool-fact-api")
        print("The key should be placed in $HOME/.epaapirc")
        print("Contents of the file should be as follows")
        print("key: apikey")
        print("url: https://api.epa.gov/FACT/1.0/")
        sys.exit()
    return data


def get_lookups():
    """
    Request to get lookups - descriptions of various codes.
    """
    getstr = "emissions/lookUps"
    # rqq = self.apiurl + "emissions/" + getstr
    # rqq += "?api_key=" + self.key
    data = sendrequest(getstr)
    jobject = data.json()
    dstr = unpack_response(jobject)
    return dstr

    # According to lookups MODC values
    # 01 primary monitoring system
    # 02 backup monitoring system
    # 03 alternative monitoring system
    # 04 backup monitoring system

    # 06 average hour before/hour after
    # 07 average hourly

    # 21 negative value replaced with 0.
    # 08 90th percentile value in Lookback Period
    # 09 95th precentile value in Lookback Period
    # etc.

    # it looks like values between 1-4 ok
    # 6-7 probably ok
    # higher values should be flagged.


def quarter2date(year, quarter):
    if quarter == 1:
        dt = datetime.datetime(year, 1, 1)
    elif quarter == 2:
        dt = datetime.datetime(year, 4, 1)
    elif quarter == 3:
        dt = datetime.datetime(year, 7, 1)
    elif quarter == 4:
        dt = datetime.datetime(year, 11, 1)
    return dt


def addquarter(rdate):
    """
    INPUT
    rdate : datetime object
    RETURNS
    newdate : datetime object
    requests for emissions are made per quarter.
    Returns first date in the next quarter from the input date.
    """
    quarter = findquarter(rdate)
    quarter += 1
    year = rdate.year
    if quarter > 4:
        quarter = 1
        year += 1
    month = 3 * quarter - 2
    newdate = datetime.datetime(year, month, 1, 0)
    return newdate


def get_datelist_sub(r1, r2):
    rlist = []
    qt1 = findquarter(r1)
    yr1 = r1.year

    qt2 = findquarter(r2)
    yr2 = r2.year
    done = False
    iii = 0
    while not done:
        rlist.append(quarter2date(yr1, qt1))
        if yr1 > yr2:
            done = True
        elif yr1 == yr2 and qt1 == qt2:
            done = True
        qt1 += 1
        if qt1 > 4:
            qt1 = 1
            yr1 += 1
        iii += 0
        if iii > 30:
            break
    return rlist


def get_datelist(rdate):
    """
    INPUT
    rdate : tuple of datetime objects
    (start date, end date)
    RETURNS:
    rdatelist : list of datetimes covering range specified by rdate by quarter.

    Return list of first date in each quarter from
    startdate to end date.
    """
    if isinstance(rdate, list):
        rdatelist = get_datelist_sub(rdate[0], rdate[1])
    else:
        rdatelist = [rdate]
    return rdatelist


def findquarter(idate):
    if idate.month <= 3:
        qtr = 1
    elif idate.month <= 6:
        qtr = 2
    elif idate.month <= 9:
        qtr = 3
    elif idate.month <= 12:
        qtr = 4
    return qtr


def keepcols(df, keeplist):
    tcols = df.columns.values
    klist = []
    for ttt in keeplist:
        if ttt in tcols:
            # if ttt not in tcols:
            #    print("NOT IN ", ttt)
            #    print('Available', tcols)
            # else:
            klist.append(ttt)
    tempdf = df[klist]
    return tempdf


def get_so2(df):
    """
    drop columns that are not in keep.
    """
    keep = [
        # "DateHour",
        "time local",
        # "time",
        "OperatingTime",
        # "HourLoad",
        # "u so2_lbs",
        "so2_lbs",
        # "AdjustedFlow",
        # "UnadjustedFlow",
        # "FlowMODC",
        "SO2MODC",
        "unit",
        "stackht",
        "oris",
        "latitude",
        "longitude",
    ]
    df = keepcols(df, keep)
    if not df.empty:
        df = df[df["oris"] != "None"]
    return df


class EpaApiObject:
    def __init__(self, fname=None, save=True, prompt=False, fdir=None):
        """
        Base class for all classes that send request to EpaApi.
        to avoid sending repeat requests to the api, the default option
        is to save the data in a file - specified by fname.

        fname : str
        fdir : str
        save : boolean
        prompt : boolean

        """

        # fname is name of file that data would be saved to.
        self.status_code = None
        self.df = pd.DataFrame()
        self.fname = fname
        self.datefmt = "%Y %m %d %H:%M"
        if fdir:
            self.fdir = fdir
        else:
            self.fdir = "./apifiles/"
        if self.fdir[-1] != "/":
            self.fdir += "/"
        # returns None if filename does not exist.
        # if prompt True then will ask for new filename if does not exist.
        fname2 = get_filename(self.fdir + fname, prompt)
        self.getstr = self.create_getstr()
        # if the file exists load data from it.
        getboolean = True
        if fname2:
            print("Loading from file ", self.fdir + self.fname)
            self.fname = fname2
            self.df, getboolean = self.load()
        elif fname:
            self.fname = self.fdir + fname
        # if it doesn't load then get it from the api.
        # if save is True then save.
        if self.df.empty and getboolean:
            # get sends request to api and processes data received.
            self.df = self.get()
            if save:
                self.save()

    def set_filename(self, fname):
        self.fname = fname

    def load(self):
        chash = {"mid": str, "oris": str}
        df = pd.read_csv(self.fname, index_col=[0], converters=chash, parse_dates=True)
        # df = pd.read_csv(self.fname, index_col=[0])
        return df, True

    def save(self):
        """
        save to a csv file.
        """
        print("saving here", self.fname)
        if not self.df.empty:
            self.df.to_csv(self.fname, date_format=self.datefmt)
        else:
            with open(self.fname, "w") as fid:
                fid.write("no data")

    def create_getstr(self):
        # each derived class should have
        # its own create_getstr method.
        return "placeholder" + self.fname

    def printall(self):
        data = sendrequest(self.getstr)
        jobject = data.json()
        rstr = self.getstr + "\n"
        rstr += unpack_response(jobject)
        return rstr

    def return_empty(self):
        return pd.DataFrame()

    def get_raw_data(self):
        data = sendrequest(self.getstr)
        if data.status_code != 200:
            return self.return_empty()
        else:
            return data

    def get(self):
        data = self.get_raw_data()
        try:
            self.status_code = data.status_code
        except AttributeError:
            self.status_code = "None"
        try:
            jobject = data.json()
        except Exception:
            return data
        df = self.unpack(jobject)
        return df

    def unpack(self, data):
        # each derived class should have
        # its own unpack method.
        return pd.DataFrame()


class EmissionsCall(EpaApiObject):
    """
    class that represents data returned by one emissions/hourlydata call to the restapi.
    Attributes
    """

    def __init__(
        self, oris, mid, year, quarter, fname=None, calltype="CEM", save=True, prompt=False
    ):
        self.oris = oris  # oris code of facility
        self.mid = mid  # monitoring location id.
        self.year = str(year)
        self.quarter = str(quarter)
        calltype = calltype.upper().strip()
        if calltype == "F23":
            calltype = "AD"
        if not fname:
            fname = "Emissions." + self.year + ".q" + self.quarter
            if calltype == "AD":
                fname += ".AD"
            fname += "." + str(self.mid) + "." + str(oris) + ".csv"
        self.dfall = pd.DataFrame()

        self.calltype = calltype
        if calltype.upper().strip() == "AD":
            self.so2name = "SO2ADReportedSO2MassRate"
        elif calltype.upper().strip() == "CEM":
            self.so2name = "SO2CEMReportedSO2MassRate"
        elif calltype.upper().strip() == "LME":
            # this should probably be so2mass??? TO DO.
            self.so2name = "LMEReportedSO2Mass"
        else:
            self.so2name = "SO2CEMReportedSO2MassRate"

        self.so2nameB = "UnadjustedSO2"
        super().__init__(fname, save, prompt)
        # if 'DateHour' in df.columns:
        #    df = df.drop(['DateHour'], axis=1)

    def create_getstr(self):
        # for locationID in unitra:
        # efile = "efile.txt"

        if self.calltype.upper().strip() == "AD":
            estr = "emissions/hourlyFuelData/csv"
        elif self.calltype.upper().strip() == "LME":
            estr = "emissions/hourlyData/csv"
        else:
            estr = "emissions/hourlyData/csv"
        getstr = quote("/".join([estr, str(self.oris), str(self.mid), self.year, self.quarter]))
        return getstr

    def load(self):
        # Emissions call
        # datefmt = "%Y %m %d %H:%M"
        datefmt = self.datefmt
        datefmt2 = "%Y %m %d %H:%M:%S"
        chash = {"mid": str, "oris": str, "unit": str}
        df = pd.read_csv(self.fname, index_col=[0], converters=chash, parse_dates=False)
        # if not df.empty:
        if not df.empty:
            self.status_code = 200
            print("SO2 DATA EXISTS")
            temp = df[df["so2_lbs"] > 0]
            if temp.empty:
                print("SO2 lbs all zero")
            # check for  two date formats.

            # -----------------------------------------
            def newdate(x):
                rval = x["time local"]
                if isinstance(rval, float):
                    if np.isnan(rval):
                        return pd.NaT

                rval = rval.replace("-", " ")
                rval = rval.strip()
                fail = 0
                try:
                    rval = datetime.datetime.strptime(rval, datefmt)
                except ValueError:
                    fail = 1
                if fail == 1:
                    try:
                        rval = datetime.datetime.strptime(rval, datefmt2)
                    except ValueError:
                        fail = 2
                        print(self.fname)
                        print("WARNING: Could not parse date " + rval)
                return rval

            # -----------------------------------------

            df["time local"] = df.apply(newdate, axis=1)
            # if 'DateHour' in df.columns:
            #    df = df.drop(['DateHour'], axis=1)
        # df = pd.read_csv(self.fname, index_col=[0])
        else:
            print("NO SO2 DATA in FILE")
        return df, False

    def return_empty(self):
        return None

    def get(self):
        data = self.get_raw_data()
        try:
            self.status_code = data.status_code
        except AttributeError:
            self.status_code = None
        if data:
            df = self.unpack(data)
        else:
            df = pd.DataFrame()
        return df

    def unpack(self, data):
        logfile = "warnings.emit.txt"
        iii = 0
        cols = []
        tra = []
        print("----UNPACK-----------------")
        for line in data.iter_lines(decode_unicode=True):
            # if iii < 5:
            # print('LINE')
            # print(line)
            # 1. Process First line
            temp = line.split(",")
            if temp[-1] and self.calltype == "LME":
                print(line)

            if iii == 0:
                tcols = line.split(",")
                # add columns for unit id and oris code
                tcols.append("unit")
                tcols.append("oris")
                # add columns for other info (stack height, latitude etc).
                # for edata in data2add:
                #    tcols.append(edata[0])
                # 1a write column headers to a file.
                verbose = True
                if verbose:
                    with open("headers.txt", "w") as fid:
                        for val in tcols:
                            fid.write(val + "\n")
                    # print('press a key to continue ')
                    # input()
                # 1b check to see if desired emission variable is in the file.
                if self.so2name not in tcols:
                    with open(logfile, "a") as fid:
                        rstr = "ORIS " + str(self.oris)
                        rstr += " mid " + str(self.mid) + "\n"
                        rstr += "NO adjusted SO2 data \n"
                        if self.so2name not in tcols:
                            rstr += "NO SO2 data \n"
                        rstr += "------------------------\n"
                        fid.write(rstr)
                    print("--------------------------------------")
                    print("ORIS " + str(self.oris))
                    print("UNIT " + str(self.mid) + " no SO2 data")
                    print(self.fname)
                    print("--------------------------------------")
                    # return empty dataframe
                    return pd.DataFrame()
                else:
                    cols = tcols
                    print("--------------------------------------")
                    print("ORIS " + str(self.oris))
                    print("UNIT " + str(self.mid) + " YES SO2 data")
                    print(self.fname)
                    print("--------------------------------------")
            # 2. Process rest of lines
            else:
                lt = line.split(",")
                # add input info to line.
                lt.append(str(self.mid))
                lt.append(str(self.oris))
                # for edata in data2add:
                #    lt.append(edata[1])
                tra.append(lt)
            iii += 1
            # with open(efile, "a") as fid:
            #    fid.write(line)
        # ----------------------------------------------------
        df = pd.DataFrame(tra, columns=cols)
        df.apply(pd.to_numeric, errors="ignore")
        df = self.manage_date(df)
        if self.calltype == "AD":
            df["SO2MODC"] = -8
        if self.calltype == "LME":
            df["SO2MODC"] = -9
        df = self.convert_cols(df)
        df = self.manage_so2modc(df)
        df = get_so2(df)
        # the LME data sometimes has duplicate rows.
        # causing emissions to be over-estimated.
        if self.calltype == "LME":
            df = df.drop_duplicates()
        return df

    # ----------------------------------------------------------------------------------------------
    def manage_date(self, df):
        """DateHour field is originally in string form 4/1/2016 02:00:00 PM
        Here, change to a datetime object.

        # also need to change to UTC.

        # time is local standard time (never daylight savings)
        """

        # Using the %I for the hour field and %p for AM/Pm converts time
        # correctly.
        def newdate(xxx):
            fmt = "%m/%d/%Y %I:%M:%S %p"
            try:
                rdt = datetime.datetime.strptime(xxx["DateHour"], fmt)
            except BaseException:
                # print("LINE WITH NO DATE :", xxx["DateHour"], ":")
                rdt = pd.NaT
            return rdt

        df["time local"] = df.apply(newdate, axis=1)
        df = df.drop(["DateHour"], axis=1)
        return df

    def manage_so2modc(self, df):
        if "SO2CEMSO2FormulaCode" not in df.columns.values:
            return df

        def checkmodc(formula, so2modc, so2_lbs):
            # if F-23 is the formula code and
            # so2modc is Nan then change so2modc to -7.
            if not so2_lbs or so2_lbs == 0:
                return so2modc
            if so2modc != 0 or not formula:
                return so2modc
            else:
                if "F-23" in str(formula):
                    return -7
                else:
                    return -10

        df["SO2MODC"] = df.apply(
            lambda row: checkmodc(row["SO2CEMSO2FormulaCode"], row["SO2MODC"], row["so2_lbs"]),
            axis=1,
        )
        return df

    def convert_cols(self, df):
        """
        All columns are read in as strings and must be converted to the
        appropriate units. NaNs or empty values may be present in the columns.

        OperatingTime : fraction of the clock hour during which the unit
                        combusted any fuel. If unit, stack or pipe did not
                        operate report 0.00.
        """

        # three different ways to convert columns
        # def toint(xxx):
        #    try:
        #        rt = int(xxx)
        #    except BaseException:
        #        rt = -99
        #    return rt

        def tostr(xxx):
            try:
                rt = str(xxx)
            except BaseException:
                rt = "none"
            return rt

        def simpletofloat(xxx):
            try:
                rt = float(xxx)
            except BaseException:
                rt = 0
            return rt

        # calculate lbs of so2 by multiplying rate by operating time.
        # checked this with FACTS
        def getmass(optime, cname):
            # if operating time is zero then emissions are zero.
            if float(optime) < 0.0001:
                rval = 0
            else:
                try:
                    rval = float(cname) * float(optime)
                except BaseException:
                    rval = np.NaN
            return rval

        def lme_getmass(cname):
            try:
                rval = float(cname)
            except BaseException:
                rval = np.NaN
            return rval

        df["SO2MODC"] = df["SO2MODC"].map(simpletofloat)
        # map OperatingTime to a float
        df["OperatingTime"] = df["OperatingTime"].map(simpletofloat)
        # map Adjusted Flow to a float
        # df["AdjustedFlow"] = df["AdjustedFlow"].map(simpletofloat)
        # df["oris"] = df["oris"].map(toint)
        df["oris"] = df.apply(lambda row: tostr(row["oris"]), axis=1)
        # map SO2 data to a float
        # if operating time is zero then map to 0 (it is '' in file)
        optime = "OperatingTime"
        cname = self.so2name
        if self.calltype == "LME":
            df["so2_lbs"] = df.apply(lambda row: lme_getmass(row[cname]), axis=1)
        else:
            df["so2_lbs"] = df.apply(lambda row: getmass(row[optime], row[cname]), axis=1)
        temp = df[["time local", "so2_lbs", cname, optime]]
        temp = df[df["OperatingTime"] > 1.0]
        if not temp.empty:
            print("Operating Time greater than 1 ")
            print(temp[["oris", "unit", "OperatingTime", "time local", "so2_lbs", self.so2name]])
        # -------------------------------------------------------------
        # these were checks to see what values the fields were holding.
        # temp is values that are not valid
        # temp = temp[temp["OperatingTime"] > 0]
        # print("Values that cannot be converted to float")
        # print(temp[cname].unique())
        # print("MODC ", temp["SO2MODC"].unique())
        # ky = "MATSSstartupshutdownflat"
        # if ky in temp.keys():
        #    print("MATSSstartupshutdownflat", temp["MATSStartupShutdownFlag"].unique())
        # print(temp['date'].unique())

        # ky = "Operating Time"
        # if ky in temp.keys():
        #    print("Operating Time", temp["OperatingTime"].unique())
        # if ky in df.keys():
        #    print("All op times", df["OperatingTime"].unique())
        # for line in temp.iterrows():
        #    print(line)
        # -------------------------------------------------------------
        return df


class Emissions:
    """
    class that represents data returned by emissions/hourlydata call to the restapi.
    Attributes
    self.df : DataFrame

    Methods
    __init__
    add

    see
    https://www.epa.gov/airmarkets/field-audit-checklist-tool-fact-field-references#EMISSION

    class that represents data returned by facilities call to the restapi.
    # NOTES
    # BAF - bias adjustment factor
    # MEC - maximum expected concentraiton
    # MPF - maximum potential stack gas flow rate
    # monitoring plan specified monitor range.
    # FlowPMA % of time flow monitoring system available.

    # SO2CEMReportedAdjustedSO2 - average adjusted so2 concentration
    # SO2CEMReportedSO2MassRate - average adjusted so2 rate (lbs/hr)

    # AdjustedFlow - average volumetric flow rate for the hour. adjusted for
    # bias.

    # It looks like MassRate is calculated from concentration of SO2 and flow
    # rate. So flow rate should be rate of all gasses coming out of stack.

    """

    def __init__(self):
        self.df = pd.DataFrame()
        self.orislist = []
        self.unithash = {}
        # self.so2name = "SO2CEMReportedAdjustedSO2"
        self.so2name = "SO2CEMReportedSO2MassRate"
        self.so2nameB = "UnadjustedSO2"

    def add(
        self,
        oris,
        locationID,
        year,
        quarter,
        method,
        logfile="warnings.emit.txt",
    ):
        """
        oris : int
        locationID : str
        year : int
        quarter : int
        ifile : str
        data2add : list of tuples (str, value)
                   str is name of column. value to add to column.

        """

        if oris not in self.orislist:
            self.orislist.append(oris)
        if oris not in self.unithash.keys():
            self.unithash[oris] = []
        self.unithash[oris].append(locationID)
        with open(logfile, "w") as fid:
            dnow = datetime.datetime.now()
            fid.write(dnow.strftime("%Y %m %d %H:%M/n"))
        # if locationID == None:
        #   unitra = self.get_units(oris)
        # else:
        #   unitra = [locationID]
        if int(quarter) > 4:
            print("Warning: quarter greater than 4")
            sys.exit()

        # for locationID in unitra:
        locationID = str(locationID)
        # print('call type :', method)
        ec = EmissionsCall(oris, locationID, year, quarter, calltype=method)
        df = ec.df
        # print('EMISSIONS CALL to DF', year, quarter, locationID)
        # print(df[0:10])
        if self.df.empty:
            self.df = df
        elif not df.empty:
            self.df = self.df.append(df)
        # self.df.to_csv(efile)
        return ec.status_code

    def save(self):
        efile = "efile.txt"
        self.df.to_csv(efile)

    def merge_facilities(self, dfac):
        dfnew = pd.merge(
            self.df,
            dfac,
            how="left",
            left_on=["oris", "unit"],
            right_on=["oris", "unit"],
        )
        return dfnew

    def plot(self):
        import matplotlib.pyplot as plt

        df = self.df.copy()
        temp1 = df[df["date"].dt.year != 1700]
        sns.set()
        for unit in df["unit"].unique():
            temp = temp1[temp1["unit"] == unit]
            temp = temp[temp["SO2MODC"].isin(["01", "02", "03", "04"])]
            plt.plot(temp["date"], temp["so2_lbs"], label=str(unit))
            print("UNIT", str(unit))
            print(temp["SO2MODC"].unique())
        # for unit in df["unit"].unique():
        #    temp = temp1[temp1["unit"] == unit]
        #    temp = temp[temp["SO2MODC"].isin(
        #        ["01", "02", "03", "04"]) == False]
        #    plt.plot(temp["date"], temp["so2_lbs"], label="bad " + str(unit))
        #    print("UNIT", str(unit))
        #    print(temp["SO2MODC"].unique())
        ax = plt.gca()
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles, labels)
        plt.show()
        for unit in df["unit"].unique():
            temp = temp1[temp1["unit"] == unit]
            print("BAF", temp["FlowBAF"].unique())
            print("MODC", temp["FlowMODC"].unique())
            print("PMA", temp["FlowPMA"].unique())
            # plt.plot(temp["date"], temp["AdjustedFlow"], label=str(unit))
        plt.show()


class MonitoringPlan(EpaApiObject):
    """
    Stack height is converted to meters.
    Request to get monitoring plans for oris code and locationID.
    locationIDs for an oris code can be found in

    The monitoring plan has locationAttributes which
    include the stackHeight, crossAreaExit, crossAreaFlow.

    It also includes monitoringSystems which includes
    ststeymTypeDescription (such as So2 Concentration)

    QuarterlySummaries gives so2Mass each quarter.

    # currently stack height is the only information
    # we want to get from monitoring plan

    # request string
    # date which indicates quarter of request
    # oris
    # mid
    # stack height
    ------------------------------------------------------------------------------
    6.0 Monitoring Method Data March 11, 2015
    Environmental Protection Agency Monitoring Plan Reporting Instructions -- Page
    37

    If a location which has an SO2 monitor combusts both high sulfur fuel (e.g., coal
    or oil)
    and a low sulfur fuel, and uses a default SO2 emission rate in conjunction with
    Equation
    F-23 for hours in which very low sulfur fuel is combusted (see ?75.11(e)(1)),
    report one
    monitor method record for parameter SO2 with a monitoring methodology code
    CEMF23. If only low-sulfur fuel is combusted and the F-23 calculation is used
    for every
    hour, report the SO2 monitoring method as F23
    ------------------------------------------------------------------------------

    """

    def __init__(self, oris, mid, date, fname="Mplans.csv", save=True, prompt=False):
        self.oris = oris  # oris code of facility
        self.mid = mid  # monitoring location id.
        self.date = date  # date
        self.dfall = pd.DataFrame()
        self.dfmt = "%Y-%m-%dT%H:%M:%S"
        super().__init__(fname, save, prompt)

    def to_dict(self, unit=None):
        if self.df.empty:
            return None

        if unit:
            df = self.df[self.df["name"] == unit]
        else:
            df = self.df.copy()

        try:
            mhash = df.reset_index().to_dict("records")
        except Exception:
            mhash = None
        return mhash

    def get_stackht(self, unit):
        # print(self.df)
        df = self.df[self.df["name"] == unit]
        # print(df)
        stackhts = df["stackht"].unique()
        # print('get stackht', stackhts)
        return stackhts

    def get_method(self, unit, daterange):
        # TO DO. pick method code based on dates.
        temp = self.df[self.df["name"] == unit]
        sdate = daterange[0]
        edate = daterange[1]

        temp = temp[temp["beginDateHour"] <= sdate]
        if temp.empty:
            return None

        temp["testdate"] = temp.apply(lambda row: test_end(row["endDateHour"], edate), axis=1)
        temp = temp[temp["testdate"]]
        method = temp["methodCode"].unique()

        return method

    def load(self):
        # Multiple mplans may be saved to the same csv file.
        # so this may return an emptly dataframe

        # returns empty dataframe and flag to send request.
        # return pd.DataFrame(), True
        # df = super().load()
        chash = {"mid": str, "oris": str, "name": str}

        def parsedate(x, sfmt):
            if not x:
                return pd.NaT
            elif x == "None":
                return pd.NaT
            else:
                try:
                    return pd.to_datetime(x, format=sfmt)
                except Exception:
                    print("time value", x)
                    return pd.NaT

        df = pd.read_csv(
            self.fname,
            index_col=[0],
            converters=chash,
            parse_dates=["beginDateHour", "endDateHour"],
            date_parser=lambda x: parsedate(x, self.dfmt),
        )

        self.dfall = df.copy()
        df = df[df["oris"] == self.oris]
        df = df[df["mid"] == self.mid]
        if not df.empty:
            self.status_code = 200
        return df, True

    def save(self):
        # do not want to overwrite other mplans in the file.
        df = pd.DataFrame()
        subset = ["oris", "name", "request_date", "methodCode", "beginDateHour", "endDateHour"]
        try:
            df, bval = self.load()
        except BaseException:
            pass
        if not self.dfall.empty:
            df = pd.concat([self.dfall, self.df], sort=True)
            df = df.drop_duplicates(subset=subset)
            df.to_csv(self.fname)
        elif not self.df.empty:
            self.df.to_csv(self.fname)

    def create_getstr(self):
        oris = self.oris
        mid = self.mid
        dstr = self.date.strftime("%Y-%m-%d")
        mstr = "monitoringplan"
        getstr = quote("/".join([mstr, str(oris), str(mid), dstr]))
        return getstr

    def unpack(self, data):
        """
        Returns:

        Information for one oris code and monitoring location.
        columns
        stackname, unit, stackheight, crossAreaExit,
        crossAreaFlow, locID, isunit

        Example ORIS 1571 unit 2 has no stack heigth.
        """
        ihash = data["data"]
        ft2m = 0.3048
        dlist = []

        # The stackname may contain multiple 'units'
        stackname = ihash["unitStackName"]
        # stackhash = {}
        shash = {}

        # first go through the unitStackConfigurations
        # sometimes a unit can have more than one unitStack.
        # TODO - not sure how to handle this.
        # y2009 3788 oris has this issue but both unit stacks have
        # the same stack height so it is not an issue.

        # the api seems to do emissions by the stack and not by
        # the unit. so this may be a non-issue for api data.

        # oris 1305 y2017 has unitStack CP001 and unitID GT1 and GT3.
        # height is given for GT1 and GT3 and NOT CP001.

        for stackconfig in ihash["unitStackConfigurations"]:
            # this maps the unitid to the stack id.
            # after reading in the data, go back and assign
            # stack height to the unit based on the stackconfig.
            if "unitId" in stackconfig.keys():
                name = stackconfig["unitId"]
                if name in shash.keys():
                    wstr = "-----------------------------\n"
                    wstr += "WARNING: unit " + name + "\n"
                    wstr += " oris; " + self.oris + "\n"
                    wstr += "has multiple unitStacks \n"
                    wstr += shash[name] + " " + stackconfig["unitStack"]
                    wstr += "-----------------------------\n"
                    print(wstr)
                shash[name] = stackconfig["unitStack"]
            else:
                print("STACKconfig")
                print(stackconfig)

        # next through the monitoringLocations
        for unithash in ihash["monitoringLocations"]:
            dhash = {}

            name = unithash["name"]
            print("NAME ", name)
            dhash["name"] = name
            if name in shash.keys():
                dhash["stackunit"] = shash[name]
            else:
                dhash["stackunit"] = name

            dhash["isunit"] = unithash["isUnit"]
            dhash["stackname"] = stackname
            for att in unithash["locationAttributes"]:
                if "stackHeight" in att.keys():
                    print("stackheight " + name)
                    print(att["stackHeight"])

                    try:
                        dhash["stackht"] = float(att["stackHeight"]) * ft2m
                    except ValueError:
                        dhash["stackht"] = np.NaN
                else:
                    dhash["stackht"] = np.NaN

                # dhash["crossAreaExit"] = att["crossAreaExit"]
                # dhash["crossAreaFlow"] = att["crossAreaFlow"]
                # dhash["locID"] = att["locId"]
                # dhash["isunit"] = att["isUnit"]
                # dlist.append(dhash)

            # each monitoringLocation has list of monitoringMethods
            iii = 0
            for method in unithash["monitoringMethods"]:
                # print('METHOD LIST', method)
                if "SO2" in method["parameterCode"]:
                    print("SO2 data")
                    dhash["parameterCode"] = method["parameterCode"]
                    dhash["methodCode"] = method["methodCode"]
                    dhash["beginDateHour"] = pd.to_datetime(
                        method["beginDateHour"], format=self.dfmt
                    )
                    dhash["endDateHour"] = pd.to_datetime(method["endDateHour"], format=self.dfmt)
                    dhash["oris"] = self.oris
                    dhash["mid"] = self.mid
                    dhash["request_date"] = self.date
                    print("Monitoring Location ------------------")
                    print(dhash)
                    print("------------------")
                    dlist.append(copy.deepcopy(dhash))
                    iii += 1
            # if there is no monitoring method for SO2
            if iii == 0:
                dhash["parameterCode"] = "None"
                dhash["methodCode"] = "None"
                dhash["beginDateHour"] = pd.NaT
                dhash["endDateHour"] = pd.NaT
                dhash["oris"] = self.oris
                dhash["mid"] = self.mid
                dhash["request_date"] = self.date
                print("Monitoring Location ------------------")
                print(dhash)
                print("------------------")
                dlist.append(copy.deepcopy(dhash))

        # print(dlist)
        df = pd.DataFrame(dlist)
        # print('DF1 ------------------')
        # print(df[['oris','name','methodCode','beginDateHour']])
        nseries = df.set_index("name")
        nseries = nseries["stackht"]
        nhash = nseries.to_dict()

        def find_stackht(name, stackht, shash, nhash):
            if pd.isna(stackht):
                # this handles case when height is specified for the stackId
                # and not the unitId
                if name in shash.keys():
                    sid = shash[name]
                    stackht = nhash[sid]
                # this handles case when height is specified for the unitId
                # and not the stackId
                else:
                    ahash = {y: x for x, y in shash.items()}
                    if name in ahash.keys():
                        sid = ahash[name]
                        stackht = nhash[sid]
            return stackht

        df["stackht"] = df.apply(
            lambda row: find_stackht(row["name"], row["stackht"], shash, nhash), axis=1
        )
        df["stackht_unit"] = "m"
        print("DF2 ------------------")
        print(df)
        return df

        # Then have list of dicts
        #  unitOperations
        #  unitPrograms
        #  unitFuels
        # TO DO need to change this so don't overwrite if more than one fuel.


#       for fuel in unit['unitFuels']:
#           chash={}
#           chash['fuel'] = fuel['fuelCode']
#           chash['fuelname'] = fuel['fuelDesc']
#           chash['fuelindCode'] = fuel['indCode']
#  unitControls
#  monitoringMethods
#      for method in unit['monitoringMethods']:
#           bhash={}
#           if method['parameterCode'] == 'SO2':
#               bhash['methodCode'] = method['methodCode']
#               bhash['subDataCode'] = method['subDataCode']
# mercuryToxicsStandardsMethods
# spanDetails
# systemFlows
# analyzerRanges

# emissionsFormulas
#       for method in unit['emissionsFormulas']:
#           if method['parameterCode'] == 'SO2':
#               bhash['equationCode'] = method['equationCode']

# rectangularDuctWallEffectAdjustmentFactors
# loadlevels (load levels for different date ranges)
# monitoringDefaults

# ******
# monitoringSystems
# some systems may be more accurate than others.
# natural gas plants emissions may have less uncertainty.
# this is complicated because entires for multiple types of equipment.

# monitoringQualifications

# quarterlySummaries
# emissionSummaries

# owners
# qaTestSummaries
# reportingFrequencies

# unitStackConfigurations
# comments
# contacts
# responsibilities


class FacilitiesData(EpaApiObject):
    """
    class that represents data returned by facilities call to the restapi.

    Attributes:
        self.fname : filename for reading and writing df to csv file.
        self.df  : dataframe
         columns are
         begin time,
         end time,
         isunit (boolean),
         latitude,
         longitude,
         facility_name,
         oris,
         unit

    Methods:
        __init__
        printall : returns a string with the unpacked data.
        get : sends request to the restapi and calls unpack.
        oris_by_area : returns list of oris codes in an area
        get_units : returns a list of units for an oris code

        set_filename : set filename to save and load from.
        load : load datafraem from csv file
        save : save dateframe to csv file
        get  : request facilities information from api
        unpack : process the data sent back from the api
                 and put relevant info in a dataframe.

    """

    def __init__(self, fname="Fac.csv", prompt=False, save=True):
        self.plan_hash = {}
        super().__init__(fname, save, prompt)

    def process_time_fields(self, df):
        """
        time fields give year and quarter.
        This converts them to a datetime object with
        date at beginning of quarter.
        """

        def process_unit_time(instr):
            instr = str(instr)
            # there are many None in end time field.
            try:
                year = int(instr[0:4])
            except ValueError:
                return None
            quarter = int(instr[4])
            if quarter == 1:
                dt = datetime.datetime(year, 1, 1)
            elif quarter == 2:
                dt = datetime.datetime(year, 4, 1)
            elif quarter == 3:
                dt = datetime.datetime(year, 7, 1)
            elif quarter == 4:
                dt = datetime.datetime(year, 10, 1)
            return dt

        df["begin time"] = df.apply(lambda row: process_unit_time(row["begin time"]), axis=1)
        df["end time"] = df.apply(lambda row: process_unit_time(row["end time"]), axis=1)
        return df

    def __str__(self):
        cols = self.df.columns
        rstr = ", ".join(cols)
        return rstr

    def create_getstr(self):
        """
        used to send the request.
        """
        return "facilities"

    def oris_by_area(self, llcrnr, urcrnr):
        """
        llcrnr : tuple (float,float)
        urcrnr : tuple (float,float)
        returns list of oris codes in box defined by
        llcrnr and urcrnr
        """
        dftemp = obs_util.latlonfilter(self.df, llcrnr, urcrnr)
        orislist = dftemp["oris"].unique()
        return orislist

    def state_from_oris(self, orislist):
        """
        orislist : list of oris codes
        Returns
        list of state abbreviations
        """
        # statelist = []
        temp = self.df[self.df["oris"].isin(orislist)]
        return temp["state"].unique()

    def get_units(self, oris):
        """
        oris : int
        Returns list of monitoring location ids
        for a particular oris code.
        """
        oris = str(oris)
        # if self.df.empty:
        #    self.facdata()
        temp = self.df[self.df["oris"] == oris]
        units = temp["unit"].unique()
        return units

    def process_unit_time(self, instr):
        instr = str(instr)
        year = int(instr[0:4])
        quarter = int(instr[4])
        if quarter == 1:
            dt = datetime.datetime(year, 1, 1)
        elif quarter == 2:
            dt = datetime.datetime(year, 4, 1)
        elif quarter == 3:
            dt = datetime.datetime(year, 7, 1)
        elif quarter == 4:
            dt = datetime.datetime(year, 10, 1)
        return dt

    def get_unit_start(self, oris, unit):
        oris = str(oris)
        temp = self.df[self.df["oris"] == oris]
        temp = temp[temp["unit"] == unit]

        start = temp["begin time"].unique()
        # end = temp["end time"].unique()
        sdate = []
        for sss in start:
            sdate.append(self.process_unit_time(sss))
        return sdate

    def get_unit_request(self, oris, unit, sdate):
        oris = str(oris)
        temp = self.df[self.df["oris"] == oris]
        temp = temp[temp["unit"] == unit]
        temp = self.process_time_fields(temp)
        temp = temp[temp["begin time"] <= sdate]
        if temp.empty:
            return None
        temp["testdate"] = temp.apply(lambda row: test_end(row["end time"], sdate), axis=1)
        print("--------------------------------------------")
        print("Monitoring Plans available")
        klist = ["testdate", "begin time", "end time", "unit", "oris", "request_string"]
        print(temp[klist])
        print("--------------------------------------------")
        temp = temp[temp["testdate"]]
        rstr = temp["request_string"].unique()
        return rstr

    def unpack(self, data):
        """
        iterates through a response which contains nested dictionaries and lists.
        # facilties 'data' is a list of dictionaries.
        # there is one dictionary for each oris code.
        # Each dictionary has a list under the key monitoringLocations.
        # each monitoryLocation has a name which is what is needed
        # for the locationID input into the get_emissions.

        return is a dataframe with following fields.

        oris
        facility name
        latitude
        longitude
        status
        begin time
        end time
        unit id
        isunit

        """
        # dlist is a list of dictionaries.
        dlist = []

        # originally just used one dictionary but if doing
        # a['dog'] = 1
        # dlist.append(a)
        # a['dog'] = 2
        # dlist.append(a)
        # for some reason dlist will then update the dictionary and will get
        # dlist = [{'dog': 2}, {'dog':2}] instead of
        # dlist = [{'dog': 1}, {'dog':2}]

        slist = []
        for val in data["data"]:
            ahash = {}
            ahash["oris"] = str(val["orisCode"])
            ahash["facility_name"] = val["name"]
            ahash["latitude"] = val["geographicLocation"]["latitude"]
            ahash["longitude"] = val["geographicLocation"]["longitude"]
            state1 = val["state"]
            ahash["state"] = state1["abbrev"]
            # ahash['time_offset'] = get_timezone_offset(ahash['latitude'],
            #                       ahash['longitude'])
            # keep track of which locations belong to a plan
            plan_number = 1
            # list 2
            for sid in val["units"]:
                unithash = {}
                unitid = sid["unitId"]
                unithash["unit"] = unitid
                unithash["oris"] = ahash["oris"]
                for gid in sid["generators"]:
                    capacity = gid["nameplateCapacity"]
                    # capacity_hash['unitid'] = capacity
                    unithash["capacity"] = capacity
                for gid in sid["fuels"]:
                    if gid["indicatorDescription"].strip() == "Primary":
                        fuel = gid["fuelCode"]
                        # fuel_hash['unitid'] = fuel
                        unithash["primary_fuel"] = fuel
                    else:
                        unithash["primary_fuel"] = np.NaN
                for gid in sid["controls"]:
                    if gid["parameterCode"].strip() == "SO2":
                        control = gid["controlCode"]
                        # control_hash['unitid'] = control
                        unithash["so2_contro"] = control
                    else:
                        unithash["so2_contro"] = np.NaN
                slist.append(unithash)
            unitdf = pd.DataFrame(slist)

            for sid in val["monitoringPlans"]:
                bhash = {}

                # if sid["status"] == "Active":
                bhash["plan number"] = plan_number
                bhash["status"] = sid["status"]
                bhash["begin time"] = str(sid["beginYearQuarter"])
                bhash["end time"] = str(sid["endYearQuarter"])
                # bhash["state"] = str(sid["abbrev"])
                plan_name = []
                blist = []
                for unit in sid["monitoringLocations"]:
                    chash = {}
                    chash["unit"] = unit["name"]
                    chash["isunit"] = unit["isUnit"]
                    chash.update(ahash)
                    chash.update(bhash)
                    blist.append(chash)
                    # dlist.append(chash)
                    plan_name.append(chash["unit"])
                plan_name.sort()
                request_string = quote(str.join(",", plan_name))
                self.plan_hash[plan_number] = request_string
                for hhh in blist:
                    hhh["request_string"] = request_string
                plan_number += 1
                dlist.extend(blist)

        df = pd.DataFrame(dlist)
        df = pd.merge(df, unitdf, how="left", left_on=["unit", "oris"], right_on=["unit", "oris"])
        return df


def unpack_response(dhash, deep=100, pid=0):
    """
    iterates through a response which contains nested dictionaries and lists.
    dhash: dictionary which may be nested.
    deep: int
        indicated how deep to print out nested levels.
    pid : int

    """
    rstr = ""
    for k2 in dhash.keys():
        iii = pid
        spc = " " * iii
        rstr += spc + str(k2) + " " + str(type(dhash[k2])) + " : "
        # UNPACK DICTIONARY
        if iii < deep and isinstance(dhash[k2], dict):
            rstr += "\n"
            iii += 1
            rstr += spc
            rstr += unpack_response(dhash[k2], pid=iii)
            rstr += "\n"
        # UNPACK LIST
        elif isinstance(dhash[k2], list):
            iii += 1
            rstr += "\n---BEGIN LIST---" + str(iii) + "\n"
            for val in dhash[k2]:
                if isinstance(val, dict):
                    rstr += unpack_response(val, deep=deep, pid=iii)
                    rstr += "\n"
                else:
                    rstr += spc + "listval " + str(val) + str(type(val)) + "\n"
            rstr += "---END LIST---" + str(iii) + "\n"
        elif isinstance(dhash[k2], str):
            rstr += spc + dhash[k2] + "\n"
        elif isinstance(dhash[k2], int):
            rstr += spc + str(dhash[k2]) + "\n"
        elif isinstance(dhash[k2], float):
            rstr += spc + str(dhash[k2]) + "\n"
        else:
            rstr += "\n"
    return rstr


def get_monitoring_plan(oris, mid, mrequest, date1, dflist):
    """
    oris : oris code
    mid  : unit id
    mrequest : list of strings: request to send
    date1 : date to request
    dflist : in/out  list of tuples [(oris, mid, stackht)]
    """
    # adds to list of oris, mid, stackht which will later be turned into
    # a dataframe with that information.
    status_code = 204
    # mhash = None
    for mr in mrequest:
        print("Get Monitoring Plan " + mr)
        plan = MonitoringPlan(str(oris), mr, date1)
        status_code = plan.status_code  # noqa: F841
        stackht = plan.get_stackht(mid)
    if len(stackht) == 1:
        print(len(stackht))
        stackht = stackht[0]
        print(stackht)
    #    mhash = plan.to_dict(mid)
    # if mhash:
    #    if len(mhash) > 1:
    #        print(
    #            "CEMS class WARNING: more than one \
    #              Monitoring location for this unit\n"
    #        )
    #        print(str(oris) + " " + str(mid) + "---")
    #        for val in mhash.keys():
    #            print(val, mhash[val])
    #        sys.exit()
    #    else:
    #        mhash = mhash[0]
    #        stackht = float(mhash["stackht"])
    else:
        print("Stack height not determined ", stackht)
        stackht = None
        istr = "\n" + "Could not retrieve stack height from monitoring plan \n"
        istr += "Please enter stack height (in meters) \n"
        istr += "for oris " + str(oris) + " and unit" + str(mid) + " \n"
        test = input(istr)
        try:
            stackht = float(test)
        except ValueError:
            stackht = None
    method = plan.get_method(mid, [date1, date1])
    print("METHODS returned", method, mid, str(oris))
    # catchall so do not double count.
    # currently CEM and CEMF23 result in same EmissionCall request string.
    if method:
        if "CEM" in method and "CEMF23" in method:
            method = "CEM"
    dflist.append((str(oris), mid, stackht))

    return dflist, method


def add_data_area(rdate, area, verbose=True):
    fac = FacilitiesData()
    llcrnr = (area[0], area[1])
    urcrnr = (area[2], area[3])
    orislist = fac.oris_by_area(llcrnr, urcrnr)
    return orislist


class CEMS:
    """
     Class for data from continuous emission monitoring systems (CEMS).
     Data from power plants can be downloaded from
     ftp://newftp.epa.gov/DMDNLoad/emissions/

    Attributes
     ----------
     efile : type string
         Description of attribute `efile`.
     url : type string
         Description of attribute `url`.
     info : type string
         Information about data.
     df : pandas DataFrame
         dataframe containing emissions data.
    Methods
     ----------
     __init__(self)
     add_data(self, rdate, states=['md'], download=False, verbose=True):

    """

    def __init__(self):
        self.efile = None
        self.lb2kg = 0.453592  # number of kilograms per pound.
        self.info = "Data from continuous emission monitoring systems (CEMS)\n"
        self.df = pd.DataFrame()
        self.so2name = "SO2CEMReportedAdjustedSO2"
        # Each facility may have more than one unit which is specified by the
        # unit id.
        self.emit = Emissions()
        self.orislist = []

    def add_emissions(self, oris, mid, datelist, method):
        # 5. Call to the Emissions class to add each monitoring location
        #    to the dataframe for each quarter in the time period.
        statuslist = []
        for ndate in datelist:
            quarter = findquarter(ndate)
            print(str(oris) + " " + str(mid) + " Loading data for quarter " + str(quarter))
            status = self.emit.add(oris, mid, ndate.year, quarter, method)
            if status == 200:
                self.orislist.append((oris, mid))
                write_status_message(status, oris, mid, quarter, "log.txt")
            else:
                write_status_message(status, oris, "no mp " + str(mid), quarter, "log.txt")
            statuslist.append(status)
        return statuslist

    def add_data(self, rdate, alist, area=True, verbose=True):
        # CEMS class
        """
        INPUTS:
        rdate :  either datetime object or tuple of two datetime objects.
        alist  : list of 4 floats. (lat, lon, lat, lon)
                OR list of oris codes.
        area : if True then alist defines area to use.
               if False then alist is a list of oris codes.
        verbose : boolean

        RETURNS:
        emitdf : pandas DataFrame with SO2 emission information.

        # 1. get list of oris codes within the area of interest
        # 2. get list of monitoring location ids for each oris code
        # 3. each unit has a monitoring plan.
        # 4. get stack heights for each monitoring location from
        #    class MonitoringPlan
        # 5. Call to the Emissions class to add each monitoring location
        #    to the dataframe.


        TODO - MonitoringPlan contains quarterly summaries of mass and operating
               time. May be able to use those

        TODO - to generalize to emissions besides SO2. Modify get_so2 routine.

        TODO - what is the flow rate?



        """
        # 1. get list of oris codes within the area of interest
        # class FacilitiesData for this
        fac = FacilitiesData()
        if area:
            llcrnr = (alist[0], alist[1])
            urcrnr = (alist[2], alist[3])
            orislist = fac.oris_by_area(llcrnr, urcrnr)
        else:
            orislist = alist
        # date list is list of dates betwen r1, r2 starting each quarter.
        datelist = get_datelist(rdate)
        if verbose:
            print("ORIS to retrieve ", orislist)
        if verbose:
            print("DATES ", datelist)

        # 2. get list of monitoring location ids for each oris code
        # FacilitiesData also provides this.
        facdf = fac.df[fac.df["oris"].isin(orislist)]
        facdf = facdf.drop(["begin time", "end time"], axis=1)

        # dflist is list of tuples (oris, unit, stackht)
        dflist = []
        for oris in orislist:
            units = fac.get_units(oris)
            if verbose:
                print("Units to retrieve ", str(oris), units)
            # 3. each unit has a monitoring plan.
            for mid in units:
                # 4. get stack heights for each monitoring location from
                #    class
                # although the date is included for the monitoring plan request,
                # the information returned is the same most of the time
                # (possibly unless the monitoring plan changes during the time
                # of interst).
                # to reduce number of requests, the monitoring plan is only
                # requested for the first date which returns a valid monitoring
                # plan.

                # find first valid monitoringplan by date.
                mrequest = None
                for udate in datelist:
                    mrequest = fac.get_unit_request(oris, mid, udate)
                    if mrequest:
                        break
                # write to message file which monitoring plan was found.
                if not mrequest:
                    with open("MESSAGE.txt", "a") as fid:
                        fid.write(" No monitoring plan for ")
                        fid.write(" oris: " + str(oris))
                        fid.write(" mid: " + str(mid))
                        fid.write(" date: " + datelist[0].strftime("%y %m/%d"))
                        fid.write("\n")
                else:
                    with open("MESSAGE.txt", "a") as fid:
                        fid.write(" EXISTS  monitoring plan for ")
                        fid.write(" oris: " + str(oris))
                        fid.write(" mid: " + str(mid))
                        fid.write(" date: " + datelist[0].strftime("%y %m/%d"))
                        fid.write("\n")

                    # update dflist from the monitoring plan.
                    dflist, method = get_monitoring_plan(oris, mid, mrequest, udate, dflist)
                    if not method:
                        method = []
                    # add emissions for each quarter list.
                    for meth in method:
                        _ = self.add_emissions(oris, mid, datelist, meth)
        # print(dflist)

        # create dataframe from dflist.
        stackdf = pd.DataFrame(dflist, columns=["oris", "unit", "stackht"])
        stackdf = stackdf.drop_duplicates()
        # keep only the following columns
        # there was a problem when more than one request_string per unit
        facdf = facdf[["oris", "unit", "facility_name", "latitude", "longitude"]]
        facdf = facdf.drop_duplicates()

        # merge stackdf with facdf to get  latitude longitude facility_name
        # information
        facdf = pd.merge(
            stackdf,
            facdf,
            how="left",
            left_on=["oris", "unit"],
            right_on=["oris", "unit"],
        )
        # need to drop duplicates here or else will create duplicate rows in
        # emitdf
        facdf = facdf.drop_duplicates()

        # drop un-needed columns from the emissions DataFrame
        emitdf = get_so2(self.emit.df)

        c1 = facdf.columns.values
        c2 = emitdf.columns.values
        jlist = [x for x in c1 if x in c2]

        if emitdf.empty:
            return emitdf
        emitdf = emitdf.dropna(axis=0, subset=["so2_lbs"])

        # The LME data sometimes has duplicate rows.
        # causing emissions to be over-estimated.
        # emitdf = emitdf.drop_duplicates()
        def badrow(rrr):
            test1 = True
            test2 = True
            if rrr["so2_lbs"] == 0:
                test1 = False
            if not rrr["so2_lbs"]:
                test1 = False

            if rrr["time local"] == pd.NaT:
                test2 = False

            return test1 or test2

        # False if no so2_lbs (0 or Nan) and date is NaT.
        emitdf["goodrow"] = emitdf.apply(badrow, axis=1)
        emitdf = emitdf[emitdf["goodrow"]]

        rowsbefore = emitdf.shape[0]
        if not emitdf.empty:
            # merge data from the facilties DataFrame into the Emissions DataFrame
            emitdf = pd.merge(
                emitdf,
                facdf,
                how="left",
                # left_on=["oris", "unit"],
                # right_on=["oris", "unit"],
                left_on=jlist,
                right_on=jlist,
            )
        rowsafter = emitdf.shape[0]
        if rowsafter != rowsbefore:
            print("WARNING: merge changed number of rows")
            sys.exit()

        diag = False
        if diag:
            for ccc in emitdf.columns.values:
                try:
                    tempdf = emitdf.dropna(axis=0, subset=[ccc])
                    print(ccc + " na dropped", tempdf.shape)
                except Exception:
                    print(ccc + " cannot drop, error")
        # print('stackht is na ---------------------------------')
        # tempdf = emitdf[emitdf['stackht'].isnull()]
        # print(tempdf[['oris','so2_lbs','unit','time local','stackht']])
        # emitdf = emitdf.dropna(axis=0, subset=['so2_lbs'])
        # print('Rows after dropna', emitdf.shape)
        # tempdf = dropna(axis=0, how='any', inplace=True)
        # print('Rows after dropna 2', tempdf)
        # temp = emitdf[not emitdf['so2_lbs']]
        # print('not so2lbs', temp)
        # temp = emitdf[not emitdf['stackht']]
        # print('not stackht', temp)

        emitdf = emitdf.dropna(axis=0, subset=["so2_lbs"])

        # def remove_nans(x):
        #    if np.isnan(x):
        #        return False
        #    elif pd.isna(x):
        #        return False
        #    else:
        #        return True

        # emitdf["keep"] = emitdf.apply(lambda row: remove_nans(row["so2_lbs"]), axis=1)
        # emitdf = emitdf[emitdf["keep"] == True]
        # emitdf.drop(["keep"], axis=1, inplace=True)
        # emitdf.fillna(0, inplace=True)
        # r_duplicates = tempdf.duplicated()
        # if not np.all(r_duplicates):
        #   print('Warning: Duplicate rows in the emitdf dataframe')
        #   print(tempdf[tempdf[r_duplicates]])
        #   print('-------------HERE ')
        #   print(tempdf[tempdf['so2_lbs'].isna()])
        #   print('------------- HERE B')
        #   temp = tempdf[tempdf[r_duplicates]]
        #   temp = temp['so2_lbs']
        #   print(temp[0:10])
        #   print(temp.values)
        #   print(type(temp.values[0]))
        #   print(pd.isna(temp.values[0]))
        #   print(tempdf[tempdf['so2_lbs'].isna()])
        #   #sys.exit()
        return emitdf


def write_status_message(status, oris, mid, quarter, logfile):
    rstr = ""
    # ustr = ""
    if status != 200:
        if status == -99:
            rstr = "NO SO2 \n"
        else:
            rstr = "Failed \n"
        # rstr += datetime.datetime.now().strftime("%Y %d %m %H:%M")
        # rstr += " Oris " + str(oris)
        # rstr += " Mid " + str(mid)
        # rstr += " Qrtr " + str(quarter)
        # rstr += "\n"
    else:
        rstr = "Loaded \n"
        rstr += datetime.datetime.now().strftime("%Y %d %m %H:%M")
    rstr += " Oris " + str(oris)
    rstr += " Mid " + str(mid)
    rstr += " Qrtr " + str(quarter)
    rstr += "\n"
    with open(logfile, "a") as fid:
        fid.write(rstr)
        # fid.write(ustr)


def match_column(df, varname):
    """varname is list of strings.
    returns column name which contains all the strings.
    """
    columns = list(df.columns.values)
    cmatch = None
    for ccc in columns:
        match = 0
        for vstr in varname:
            if vstr.lower() in ccc.lower():
                match += 1
        if match == len(varname):
            cmatch = ccc
    return cmatch


def latlon2str(lat, lon):
    latstr = f"{lat:.4}"
    lonstr = f"{lon:.4}"
    return (latstr, lonstr)
