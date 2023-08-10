import datetime
import sys

import pandas as pd
import xarray as xr
from numpy import NaN


def var_to_da(o, var_name, time):
    unit = o.units(var_name)
    bad_val = NaN
    vals = o[var_name]
    name = var_name
    if "Latitude" in var_name:
        name = "latitude"
        unit = "degrees_north"
    if "Longitude" in var_name:
        name = "longitude"
        unit = "degrees_east"
    da = xr.DataArray(vals, coords=[time], dims=["time"])
    da.name = name
    da.attrs["units"] = unit
    da.attrs["missing_value"] = bad_val
    return da


def class_to_xarray(o, time_str="Time_Start"):
    # calculate the time stamps
    time_index = pd.to_datetime(o.times)
    das = {}
    for i in o.varnames:
        if i != "Time_Start":
            das[i] = var_to_da(o, i, time_index)
    ds = xr.Dataset(das)
    # for j in das:
    # ds[j.name] = j
    ds.attrs["source"] = o.dataSource
    ds.attrs["Date Revised"] = pd.to_datetime(o.dateRevised).strftime("%Y-%m-%d %H:%M:%S")
    ds.attrs["mission"] = o.mission
    ds.attrs["organization"] = o.organization
    ds.attrs["PI"] = o.PI
    if len(o.NCOM) > 1:
        for i in o.NCOM[:-1]:
            # print(i)
            try:
                name = i.split(":")[0].strip()
                val = i.split(":")[1].strip()
                ds.attrs[name] = val
            except IndexError:
                pass
    return ds


def add_data(filename):
    o = Dataset(filename)
    ds = class_to_xarray(o)
    return ds


class Variable:
    """A Variable is a ICARTT variable description with name, units, scale and missing value."""

    @property
    def desc(self):
        """Return variable description string as it appears in an ICARTT file

        Returns
        -------
        str
            Returns the name and units

        """
        return self.splitChar.join([self.name, self.units, self.units])

    def __init__(self, name, units, scale=1.0, miss=-9999999):
        """Short summary.

        Parameters
        ----------
        name : type
            Description of parameter `name`.
        units : type
            Description of parameter `units`.
        scale : type
            Description of parameter `scale`.
        miss : type
            Description of parameter `miss`.

        Returns
        -------
        type
            Description of returned object.

        """
        #: Name
        self.name = name
        #: Units
        self.units = units
        #: Scale factor
        self.scale = scale
        #: Missing value (string, just as it appears in the ICARTT file)
        self.miss = str(miss)
        #: Split character for description string
        self.splitChar = ","


class Dataset:
    """
    An ICARTT dataset that can be created from scratch or read from a file,
    manipulated, and then written to a file.
    """

    @property
    def nheader(self):
        """
        Header line count
        """
        total = 12 + self.ndvar + 1 + self.nscom + 1 + self.nncom
        if self.format == 2110:
            total += self.nauxvar + 5
        return total

    @property
    def ndvar(self):
        """
        Dependent variable count
        """
        return len(self.DVAR)

    @property
    def nauxvar(self):
        """
        Auxiliary variables count
        """
        return len(self.AUXVAR)

    @property
    def nvar(self):
        """
        Variable count (independent + dependent)
        """
        return self.ndvar + 1

    @property
    def nscom(self):
        """
        Special comments count
        """
        return len(self.SCOM)

    @property
    def nncom(self):
        """
        Normal comments count
        """
        return len(self.NCOM)

    @property
    def VAR(self):
        """
        Variables (independent and dependent)
        """
        return [self.IVAR] + self.DVAR

    @property
    def varnames(self):
        """
        Names of variables (independent and dependent)
        """
        return [x.name for x in self.VAR]

    @property
    def times(self):
        """
        Time steps of the data contained.
        """
        return [self.dateValid + datetime.timedelta(seconds=x) for x in self[self.IVAR.name]]

    def __getitem__(self, name):
        """
        Convenience function to access variable data by name::

           ict = icartt.Dataset(<fname>)
           ict['O3']
        """
        idx = self.index(name)
        if idx == -1:
            raise Exception(f"{name:s} not found in data")
        return [x[idx] for x in self.data]

    def units(self, name):
        """
        Units of variable <name>
        """
        res = [x.units for x in self.VAR if x.name == name]
        if len(res) == 0:
            res = [""]
        return res[0]

    def index(self, name):
        """
        Index of variable <name> in data array
        """
        res = [i for i, x in enumerate(self.VAR) if x.name == name]
        if len(res) == 0:
            res = [-1]
        return res[0]

    def write(self, f=sys.stdout):
        """
        Write to file handle <f>
        """

        def prnt(txt):
            f.write(str(txt) + "\n")

        # Number of lines in header, file format index (most files use 1001) - comma delimited.
        prnt(f"{self.nheader:d}, {self.format:d}")
        # PI last name, first name/initial.
        prnt(self.PI)
        # Organization/affiliation of PI.
        prnt(self.organization)
        # Data source description (e.g., instrument name, platform name, model name, etc.).
        prnt(self.dataSource)
        # Mission name (usually the mission acronym).
        prnt(self.mission)
        # File volume number, number of file volumes (these integer values are used when the data require more than one file per day; for data that require only one file these values are set to 1, 1) - comma delimited.
        prnt(self.splitChar.join([str(self.VOL), str(self.NVOL)]))
        # UTC date when data begin, UTC date of data reduction or revision - comma delimited (yyyy, mm, dd, yyyy, mm, dd).
        prnt(
            self.splitChar.join(
                [
                    datetime.datetime.strftime(x, "%Y, %m, %d")
                    for x in [self.dateValid, self.dateRevised]
                ]
            )
        )
        # Data Interval (This value describes the time spacing (in seconds) between consecutive data records. It is the (constant) interval between values of the independent variable. For 1 Hz data the data interval value is 1 and for 10 Hz data the value is 0.1. All intervals longer than 1 second must be reported as Start and Stop times, and the Data Interval value is set to 0. The Mid-point time is required when it is not at the average of Start and Stop times. For additional information see Section 2.5 below.).
        prnt("0")
        # Description or name of independent variable (This is the name chosen for the start time. It always refers to the number of seconds UTC from the start of the day on which measurements began. It should be noted here that the independent variable should monotonically increase even when crossing over to a second day.).
        prnt(self.IVAR.desc)
        if self.format == 2110:
            # Description or name of independent (bound) variable (This is the name chosen for the start time. It always refers to the number of seconds UTC from the start of the day on which measurements began. It should be noted here that the independent variable should monotonically increase even when crossing over to a second day.).
            prnt(self.IBVAR.desc)
        # Number of variables (Integer value showing the number of dependent variables: the total number of columns of data is this value plus one.).
        prnt(self.ndvar)
        # Scale factors (1 for most cases, except where grossly inconvenient) - comma delimited.
        prnt(self.splitChar.join([f"{x.scale:6.3f}" for x in self.DVAR]))
        # Missing data indicators (This is -9999 (or -99999, etc.) for any missing data condition, except for the main time (independent) variable which is never missing) - comma delimited.
        prnt(self.splitChar.join([str(x.miss) for x in self.DVAR]))
        # Variable names and units (Short variable name and units are required, and optional long descriptive name, in that order, and separated by commas. If the variable is unitless, enter the keyword "none" for its units. Each short variable name and units (and optional long name) are entered on one line. The short variable name must correspond exactly to the name used for that variable as a column header, i.e., the last header line prior to start of data.).
        _ = [prnt(x.desc) for x in self.DVAR]
        if self.format == 2110:
            # Number of variables (Integer value showing the number of dependent variables: the total number of columns of data is this value plus one.).
            prnt(self.nauxvar)
            # Scale factors (1 for most cases, except where grossly inconvenient) - comma delimited.
            prnt(self.splitChar.join([f"{x.scale:6.3f}" for x in self.AUXVAR]))
            # Missing data indicators (This is -9999 (or -99999, etc.) for any missing data condition, except for the main time (independent) variable which is never missing) - comma delimited.
            prnt(self.splitChar.join([str(x.miss) for x in self.AUXVAR]))
            # Variable names and units (Short variable name and units are required, and optional long descriptive name, in that order, and separated by commas. If the variable is unitless, enter the keyword "none" for its units. Each short variable name and units (and optional long name) are entered on one line. The short variable name must correspond exactly to the name used for that variable as a column header, i.e., the last header line prior to start of data.).
            _ = [prnt(x.desc) for x in self.AUXVAR]

        # Number of SPECIAL comment lines (Integer value indicating the number of lines of special comments, NOT including this line.).
        prnt(f"{self.nscom:d}")
        # Special comments (Notes of problems or special circumstances unique to this file. An example would be comments/problems associated with a particular flight.).
        _ = [prnt(x) for x in self.SCOM]
        # Number of Normal comments (i.e., number of additional lines of SUPPORTING information: Integer value indicating the number of lines of additional information, NOT including this line.).
        prnt(f"{self.nncom:d}")
        # Normal comments (SUPPORTING information: This is the place for investigators to more completely describe the data and measurement parameters. The supporting information structure is described below as a list of key word: value pairs. Specifically include here information on the platform used, the geo-location of data, measurement technique, and data revision comments. Note the non-optional information regarding uncertainty, the upper limit of detection (ULOD) and the lower limit of detection (LLOD) for each measured variable. The ULOD and LLOD are the values, in the same units as the measurements that correspond to the flags -7777s and -8888s within the data, respectively. The last line of this section should contain all the short variable names on one line. The key words in this section are written in BOLD below and must appear in this section of the header along with the relevant data listed after the colon. For key words where information is not needed or applicable, simply enter N/A.).
        _ = [prnt(x) for x in self.NCOM]
        # data!
        _ = [prnt(self.splitChar.join([str(y) for y in x])) for x in self.data]

    def make_filename(self):
        """
        Create ICARTT-compliant file name based on the information contained in the dataset
        """
        return (
            self.dataID
            + "_"
            + self.locationID
            + "_"
            + datetime.datetime.strftime(self.dateValid, "%Y%m%d")
            + "_"
            + "R"
            + self.revision
            + ".ict"
        )

    # sanitize function
    def __readline(self, do_split=True):
        dmp = self.input_fhandle.readline().replace("\n", "").replace("\r", "")
        if do_split:
            dmp = [word.strip(" ") for word in dmp.split(self.splitChar)]
        return dmp

    def read_header(self):
        """
        Read the ICARTT header (from file)
        """
        if self.input_fhandle.closed:
            self.input_fhandle = open(self.input_fhandle.name)

        # line 1 - Number of lines in header, file format index (most files use
        # 1001) - comma delimited.
        self.format = int(self.__readline()[1])

        # line 2 - PI last name, first name/initial.
        self.PI = self.__readline(do_split=False)

        # line 3 - Organization/affiliation of PI.
        self.organization = self.__readline(do_split=False)

        # line 4 - Data source description (e.g., instrument name, platform name,
        # model name, etc.).
        self.dataSource = self.__readline(do_split=False)

        # line 5 - Mission name (usually the mission acronym).
        self.mission = self.__readline(do_split=False)

        # line 6 - File volume number, number of file volumes (these integer values
        # are used when the data require more than one file per day; for data that
        # require only one file these values are set to 1, 1) - comma delimited.
        dmp = self.__readline()
        self.VOL = int(dmp[0])
        self.NVOL = int(dmp[1])

        # line 7 - UTC date when data begin, UTC date of data reduction or revision
        # - comma delimited (yyyy, mm, dd, yyyy, mm, dd).
        dmp = self.__readline()
        self.dateValid = datetime.datetime.strptime("".join([f"{x:s}" for x in dmp[0:3]]), "%Y%m%d")
        self.dateRevised = datetime.datetime.strptime(
            "".join([f"{x:s}" for x in dmp[3:6]]), "%Y%m%d"
        )

        # line 8 - Data Interval (This value describes the time spacing (in seconds)
        # between consecutive data records. It is the (constant) interval between
        # values of the independent variable. For 1 Hz data the data interval value
        # is 1 and for 10 Hz data the value is 0.1. All intervals longer than 1
        # second must be reported as Start and Stop times, and the Data Interval
        # value is set to 0. The Mid-point time is required when it is not at the
        # average of Start and Stop times. For additional information see Section
        # 2.5 below.).
        self.dataInterval = int(self.__readline()[0])

        # line 9 - Description or name of independent variable (This is the name
        # chosen for the start time. It always refers to the number of seconds UTC
        # from the start of the day on which measurements began. It should be noted
        # here that the independent variable should monotonically increase even when
        # crossing over to a second day.
        dmp = self.__readline()
        self.IVAR = Variable(dmp[0], dmp[1])

        # line 10 - Number of variables (Integer value showing the number of
        # dependent variables: the total number of columns of data is this value
        # plus one.).
        ndvar = int(self.__readline()[0])

        # line 11- Scale factors (1 for most cases, except where grossly
        # inconvenient) - comma delimited.
        dvscale = [float(x) for x in self.__readline()]

        # line 12 - Missing data indicators (This is -9999 (or -99999, etc.) for
        # any missing data condition, except for the main time (independent)
        # variable which is never missing) - comma delimited.
        dvmiss = [x for x in self.__readline()]
        # no float casting here, as we need to do string comparison lateron when reading data...

        # line 13 - Variable names and units (Short variable name and units are
        # required, and optional long descriptive name, in that order, and separated
        # by commas. If the variable is unitless, enter the keyword "none" for its
        # units. Each short variable name and units (and optional long name) are
        # entered on one line. The short variable name must correspond exactly to
        # the name used for that variable as a column header, i.e., the last header
        # line prior to start of data.).
        dmp = self.__readline()
        dvname = [dmp[0]]
        dvunits = [dmp[1]]

        for i in range(1, ndvar):
            dmp = self.__readline()
            dvname += [dmp[0]]
            dvunits += [dmp[1]]

        self.DVAR = [
            Variable(name, unit, scale, miss)
            for name, unit, scale, miss in zip(dvname, dvunits, dvscale, dvmiss)
        ]

        # line 14 + nvar - Number of SPECIAL comment lines (Integer value
        # indicating the number of lines of special comments, NOT including this
        # line.).
        nscom = int(self.__readline()[0])

        # line 15 + nvar - Special comments (Notes of problems or special
        # circumstances unique to this file. An example would be comments/problems
        # associated with a particular flight.).
        self.SCOM = [self.__readline(do_split=False) for i in range(0, nscom)]

        # line 16 + nvar + nscom - Number of Normal comments (i.e., number of
        # additional lines of SUPPORTING information: Integer value indicating the
        # number of lines of additional information, NOT including this line.).
        nncom = int(self.__readline()[0])

        # line 17 + nvar + nscom - Normal comments (SUPPORTING information: This is
        # the place for investigators to more completely describe the data and
        # measurement parameters. The supporting information structure is described
        # below as a list of key word: value pairs. Specifically include here
        # information on the platform used, the geo-location of data, measurement
        # technique, and data revision comments. Note the non-optional information
        # regarding uncertainty, the upper limit of detection (ULOD) and the lower
        # limit of detection (LLOD) for each measured variable. The ULOD and LLOD
        # are the values, in the same units as the measurements that correspond to
        # the flags -7777's and -8888's within the data, respectively. The last line
        # of this section should contain all the "short" variable names on one line.
        # The key words in this section are written in BOLD below and must appear in
        # this section of the header along with the relevant data listed after the
        # colon. For key words where information is not needed or applicable, simply
        # enter N/A.).
        self.NCOM = [self.__readline(do_split=False) for i in range(0, nncom)]

        self.input_fhandle.close()

    def __nan_miss_float(self, raw):
        # vals = [x.replace(self.VAR[i].miss, 'NaN')  for i, x in enumerate(raw)]
        # s = pd.Series(vals).str.replace('NaN.0','NaN')
        # s = s.str.replace('NaN0','NaN')
        # s = s.str.strip().astype(float)
        # return s.values.tolist()
        # return [
        #     float(x.replace(self.VAR[i].miss, 'NaN').replace('NaN.0','NaN').replace('NaN0','NaN').strip())
        #     for i, x in enumerate(raw)
        # ]
        vals = []
        for i, x in enumerate(raw):
            v = x.replace(self.VAR[i].miss, "NaN")
            if "NaN" in v:
                v = "NaN"
            vals.append(float(v.strip()))
        return vals

    def read_data(self):
        """
        Read ICARTT data (from file)
        """
        if self.input_fhandle.closed:
            self.input_fhandle = open(self.input_fhandle.name)

        _ = [self.input_fhandle.readline() for _ in range(self.nheader)]

        self.data = [
            self.__nan_miss_float(line.split(self.splitChar)) for line in self.input_fhandle
        ]

        self.input_fhandle.close()

    def read_first_and_last(self):
        """
        Read first and last ICARTT data line (from file). Useful for quick estimates e.g. of the time extent
        of big ICARTT files, without having to read the whole thing, which would be slow.
        """
        if self.input_fhandle.closed:
            self.input_fhandle = open(self.input_fhandle.name)

        _ = [self.input_fhandle.readline() for _ in range(self.nheader)]

        first = self.input_fhandle.readline()
        self.data = [self.__nan_miss_float(first.split(self.splitChar))]
        for line in self.input_fhandle:
            pass
        last = line
        self.data += [self.__nan_miss_float(last.split(","))]

        self.input_fhandle.close()

    def read(self):
        """
        Read ICARTT data and header
        """
        self.read_header()
        self.read_data()

    def __init__(self, f=None, loadData=True):
        self.format = 1001

        self.revision = "0"
        self.dataID = "dataID"
        self.locationID = "locationID"

        self.PI = "Mustermann, Martin"
        self.organization = "Musterinstitut"
        self.dataSource = "Musterdatenprodukt"
        self.mission = "MUSTEREX"
        self.VOL = 1
        self.NVOL = 1
        self.dateValid = datetime.datetime.today()
        self.dateRevised = datetime.datetime.today()
        self.dataInterval = 0
        self.IVAR = Variable("Time_Start", "seconds_from_0_hours_on_valid_date", 1.0, -9999999)
        self.DVAR = [
            Variable("Time_Stop", "seconds_from_0_hours_on_valid_date", 1.0, -9999999),
            Variable("Some_Variable", "ppbv", 1.0, -9999999),
        ]
        self.SCOM = []
        self.NCOM = []

        self.data = [[1.0, 2.0, 45.0], [2.0, 3.0, 36.0]]

        # for 2210
        self.IBVAR = None
        self.AUXVAR = []

        self.splitChar = ","

        # read data if f is not None
        encoding = "utf-8"
        if f is not None:
            if isinstance(f, str):
                text = f
                decoded = False
                self.input_fhandle = open(f, encoding=encoding)
            else:
                text = f.decode(encoding)  # noqa: F841
                decoded = True  # noqa: F841
            # if isinstance(f, (str, unicode)):
            # self.input_fhandle = open(f, 'r')
            # else:
            # self.input_fhandle = f

            self.read_header()
            if loadData:
                self.read_data()
