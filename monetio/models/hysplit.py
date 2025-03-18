"""
HYPSLIT MODEL READER for binary concentration (cdump) output files

This code developed at the NOAA Air Resources Laboratory.
Alice Crawford
Allison Ring

-------------
Functions:
-------------
open_dataset :
combine_dataset :
get_latlongrid :
getlatlon :
hysp_heights: determines ash top height from HYSPLIT
hysp_massload: determines total mass loading from HYSPLIT
calc_aml: determines ash mass loading for each altitude layer  from HYSPLIT
hysp_thresh: calculates mask array for ash mass loading threshold from HYSPLIT
add_species(dset): adds concentrations due to different species.


--------
Classes
--------
ModelBin

Change log

2021 13 May  AMC  get_latlongrid needed to be updated to match makegrid method.
2022 14 Nov  AMC  initialized self.dset in __init__() in ModelBin class
2022 14 Nov  AMC  modified fix_grid_continuity to not fail if passed empty Dataset.
2022 02 Dec  AMC  modified get_latlongrid inputs. do not need to input dataset, just dictionary.
2022 02 Dec  AMC  replaced makegrid method with get_latlongrid function to reduce duplicate code.
2022 02 Dec  AMC  get_latlongrid function utilizes getlatlon to reduce duplicate code.
2022 02 Dec  AMC  replaced np.arange with np.linspace in getlatlon. np.arange is unstable when step is not an integer.
2023 12 Jan  AMC  modified reset_latlon_coords so will work with data-arrays that have no latitude longitude coordinate.
2023 12 Jan  AMC  get_thickness modified to calculate if the attribute specifying the vertical levels is bad
2023 03 Mar  AMC  get_latlon modified. replace x>=180 with x>=180+lon_tolerance
2023 03 Mar  AMC  get_latlongrid improved exception statements
2023 08 Dec  AMC  add check_attributes to ModelBin and combine_datatset to make sure level height attribute is a list
2024 01 Apr  AMC  added logging. for combine_dataset add continue to exception so it won't fail.
2024 04 Mar  AMC  bug fixes to combine_dataset
2025 07 Mar  AMC  added in modifications by TAdeJong to reduce calls to xr.merge and speed up reading.
2025 18 Mar  AMC  added changes in order to make netcdf file CF compliant. This involves changing some attribute names.
2025 18 Mar  AMC  tried to get rid of convertin non-nanosecond precision datetime valeus to nanosecond precision warnings.

"""

import datetime
import logging
import sys

import numpy as np
import pandas as pd
import xarray as xr

#logger = logging.getLogger(__name__)
import warnings


def open_dataset(
    fname,
    drange=None,
    century=None,
    verbose=False,
    sample_time_stamp="start",
    check_grid=True,
):
    """Short summary.

    Parameters
    ----------
    fname : string
        Name of "cdump" file. Binary HYSPLIT concentration output file.

    drange : list of two datetime objects
        cdump file contains concentration as function of time. The drange
        specifies what times should be loaded from the file. A value of None
        will result in all times being loaded.

    century : integer (1900 or 2000)

    verbose : boolean
        If True will print out extra messages

    sample_time_stamp : str
        if 'end' then time in xarray will be the end of sampling time period.
        else time is start of sampling time period.

    check_grid : boolean
        if True call fix_grid_continuity to check to see that
        xindx and yindx values are sequential (e.g. not [1,2,3,4,5,7]).
        If they are not, then add missing values to the xarray..

    Returns
    -------
    dset : xarray DataSet

    CHANGES for PYTHON 3
    For python 3 the numpy char4 are read in as a numpy.bytes_ class and need to
    be converted to a python
    string by using decode('UTF-8').
    """
    # open the dataset using xarray
    binfile = ModelBin(
        fname,
        drange=drange,
        century=century,
        verbose=verbose,
        readwrite="r",
        sample_time_stamp=sample_time_stamp,
    )
    if binfile.dataflag:
        dset = binfile.dset
        if check_grid:
            return fix_grid_continuity(dset)
        else:
            return dset
    else:
        return xr.Dataset()


def check_drange(drange, pdate1, pdate2):
    """
    drange : list of two datetimes
    pdate1 : datetime
    pdate2 : datetime

    Returns
    savedata : boolean


    returns True if drange is between pdate1 and pdate2
    """
    savedata = True
    testf = True
    # if pdate1 is within drange then save the data.
    # AND if pdate2 is within drange then save the data.
    # if drange[0] > pdate1 then stop looping to look for more data
    # this block sets savedata to true if data within specified time
    # range or time range not specified
    if drange is None:
        savedata = True
    elif pdate1 >= drange[0] and pdate1 <= drange[1] and pdate2 <= drange[1]:
        savedata = True
    elif pdate1 > drange[1] or pdate2 > drange[1]:
        testf = False
        savedata = False
    else:
        savedata = False
    # END block
    # if verbose:
    #    print(savedata, 'DATES :', pdate1, pdate2)
    return testf, savedata


class ModelBin:
    """
    represents a binary cdump (concentration) output file from HYSPLIT
    methods:
    readfile - opens and reads contents of cdump file into an xarray
    self.dset
    """

    def __init__(
        self,
        filename,
        drange=None,
        century=None,
        verbose=True,
        readwrite="r",
        sample_time_stamp="start",
    ):
        """
        drange :  list of two datetime objects.
        The read method will store data from the cdump file for which the
        sample start is greater thand drange[0] and less than drange[1]
        for which the sample stop is less than drange[1].

        sample_time_stamp : str
              if 'end' - time in xarray will indicate end of sampling time.
              else  - time in xarray will indicate start of sampling time.
        century : integer
        verbose : boolean
        read

        """
        self.drange = drange
        self.filename = filename
        self.century = century
        self.verbose = verbose
        # list of tuples (date1, date2)  of averaging periods with zero
        # concentrations
        self.zeroconcdates = []
        # list of tuples  of averaging periods with nonzero concentrtations]
        self.nonzeroconcdates = []
        self.atthash = {}
        self.atthash["Starting Latitudes"] = []
        self.atthash["Starting Longitudes"] = []
        self.atthash["Starting Heights"] = []
        self.atthash["Source Date"] = []
        self.sample_time_stamp = sample_time_stamp
        self.gridhash = {}
        # self.llcrnr_lon = None
        # self.llcrnr_lat = None
        # self.nlat = None
        # self.nlon = None
        # self.dlat = None
        # self.dlon = None
        self.levels = None
        self.dset = xr.Dataset()

        if readwrite == "r":
            if verbose:
                print(f"reading {filename}")
            self.dataflag = self.readfile(filename, drange, verbose=verbose, century=century)

    @staticmethod
    def define_struct():
        """Each record in the fortran binary begins and ends with 4 bytes which
        specify the length of the record. These bytes are called pad below.
        They are not used here, but are thrown out. The following block defines
        a numpy dtype object for each record in the binary file."""
        from numpy import dtype

        real4 = ">f"
        int4 = ">i"
        int2 = ">i2"
        char4 = ">a4"

        rec1 = dtype(
            [
                ("pad1", int4),
                ("model_id", char4),  # meteorological model id
                ("met_year", int4),  # meteorological model starting time
                ("met_month", int4),
                ("met_day", int4),
                ("met_hr", int4),
                ("met_fhr", int4),  # forecast hour
                ("start_loc", int4),  # number of starting locations
                ("conc_pack", int4),  # concentration packing flag (0=no, 1=yes)
                ("pad2", int4),
            ]
        )

        # start_loc in rec1 tell how many rec there are.
        rec2 = dtype(
            [
                ("pad1", int4),
                ("r_year", int4),  # release starting time
                ("r_month", int4),
                ("r_day", int4),
                ("r_hr", int4),
                ("s_lat", real4),  # Release location
                ("s_lon", real4),
                ("s_ht", real4),
                ("r_min", int4),  # release startime time (minutes)
                ("pad2", int4),
            ]
        )

        rec3 = dtype(
            [
                ("pad1", int4),
                ("nlat", int4),
                ("nlon", int4),
                ("dlat", real4),
                ("dlon", real4),
                ("llcrnr_lat", real4),
                ("llcrnr_lon", real4),
                ("pad2", int4),
            ]
        )

        rec4a = dtype(
            [
                ("pad1", int4),
                ("nlev", int4),  # number of vertical levels in concentration grid
            ]
        )

        rec4b = dtype([("levht", int4)])  # height of each level (meters above ground)

        rec5a = dtype(
            [
                ("pad1", int4),
                ("pad2", int4),
                ("pollnum", int4),  # number of different pollutants
            ]
        )

        rec5b = dtype([("pname", char4)])  # identification string for each pollutant

        rec5c = dtype([("pad2", int4)])

        rec6 = dtype(
            [
                ("pad1", int4),
                ("oyear", int4),  # sample start time.
                ("omonth", int4),
                ("oday", int4),
                ("ohr", int4),
                ("omin", int4),
                ("oforecast", int4),
                ("pad3", int4),
            ]
        )

        # rec7 has same form as rec6.            #sample stop time.

        # record 8 is pollutant type identification string, output level.

        rec8a = dtype(
            [
                ("pad1", int4),
                ("poll", char4),  # pollutant identification string
                ("lev", int4),
                ("ne", int4),  # number of elements
            ]
        )

        rec8b = dtype(
            [
                ("indx", int2),  # longitude index
                ("jndx", int2),  # latitude index
                ("conc", real4),
            ]
        )

        rec8c = dtype([("pad2", int4)])
        recs = (
            rec1,
            rec2,
            rec3,
            rec4a,
            rec4b,
            rec5a,
            rec5b,
            rec5c,
            rec6,
            rec8a,
            rec8b,
            rec8c,
        )
        return recs

    def parse_header(self, hdata1):
        """
        hdata1 : dtype
        Returns
        nstartloc : int
           number of starting locations in file.
        """
        if len(hdata1["start_loc"]) != 1:
            warnings.warn(
                f"In ModelBin {self.filename} _readfile - number of starting locations incorrect"
            )
            warnings.warn(str(hdata1["start_loc"]))
            return None
        # in python 3 np.fromfile reads the record into a list even if it is
        # just one number.
        # so if the length of this record is greater than one something is
        # wrong.
        # if it is empty or 0 then the cdump file is probably empty as well.
        nstartloc = hdata1["start_loc"][0]
        self.atthash["Meteorological Model ID"] = hdata1["model_id"][0].decode("UTF-8")
        self.atthash["Number Start Locations"] = nstartloc
        return nstartloc

    def parse_hdata2(self, hdata2, nstartloc, century):
        # Loop through starting locations
        for nnn in range(0, nstartloc):
            # create list of starting latitudes, longitudes and heights.
            lat = hdata2["s_lat"][nnn]
            lon = hdata2["s_lon"][nnn]
            hgt = hdata2["s_ht"][nnn]

            self.atthash["Starting Latitudes"].append(lat)
            self.atthash["Starting Longitudes"].append(lon)
            self.atthash["Starting Heights"].append(hgt)

            # try to guess century if century not given
            if century is None:
                if hdata2["r_year"][0] < 50:
                    century = 2000
                else:
                    century = 1900
                warnings.warn(f"Guessing Century for HYSPLIT concentration file {century}")
            # add sourcedate which is datetime.datetime object
            sourcedate = datetime.datetime(
                century + hdata2["r_year"][nnn],
                hdata2["r_month"][nnn],
                hdata2["r_day"][nnn],
                hdata2["r_hr"][nnn],
                hdata2["r_min"][nnn],
            )

            self.atthash["Source Date"].append(sourcedate.strftime("%Y%m%d.%H%M%S"))

        return century

    def parse_hdata3(self, hdata3):
        # Description of concentration grid
        ahash = {}
        ahash["Number Lat Points"] = hdata3["nlat"][0]
        ahash["Number Lon Points"] = hdata3["nlon"][0]
        ahash["Latitude Spacing"] = hdata3["dlat"][0]
        ahash["Longitude Spacing"] = hdata3["dlon"][0]
        ahash["llcrnr longitude"] = hdata3["llcrnr_lon"][0]
        ahash["llcrnr latitude"] = hdata3["llcrnr_lat"][0]
        # self.llcrnr_lon = hdata3["llcrnr_lon"][0]
        # self.llcrnr_lat = hdata3["llcrnr_lat"][0]
        # self.nlat = hdata3["nlat"][0]
        # self.nlon = hdata3["nlon"][0]
        # self.dlat = hdata3["dlat"][0]
        # self.dlon = hdata3["dlon"][0]
        return ahash

    def parse_hdata4(self, hdata4a, hdata4b):
        self.levels = hdata4b["levht"]
        self.atthash["Number of Levels"] = hdata4a["nlev"][0]
        self.atthash["Level top heights (m)"] = hdata4b["levht"]

    def parse_hdata6and7(self, hdata6, hdata7, century):
        # if no data read then break out of the while loop.
        if not hdata6:
            return False, None, None
        pdate1 = datetime.datetime(
            century + int(hdata6["oyear"][0]),
            int(hdata6["omonth"][0]),
            int(hdata6["oday"][0]),
            int(hdata6["ohr"][0]),
            int(hdata6["omin"][0]),
        )
        pdate2 = datetime.datetime(
            century + int(hdata7["oyear"][0]),
            int(hdata7["omonth"][0]),
            int(hdata7["oday"][0]),
            int(hdata7["ohr"][0]),
            int(hdata7["omin"][0]),
        )
        dt = pdate2 - pdate1
        sample_dt = dt.days * 24 + dt.seconds / 3600.0
        # self.atthash["Sampling Time"] = pdate2 - pdate1
        self.atthash["sample time hours"] = sample_dt
        if self.sample_time_stamp == "end":
            self.atthash["time description"] = "End of sampling time period"
        else:
            self.atthash["time description"] = "start of sampling time period"
        return True, pdate1, pdate2

    @staticmethod
    def parse_hdata8(hdata8a, hdata8b, pdate1):
        """
        hdata8a : dtype
        hdata8b : dtype
        pdate1  : datetime

        Returns:
        concframe : DataFrame
        """
        lev_name = hdata8a["lev"][0]
        col_name = hdata8a["poll"][0].decode("UTF-8")
        #edata = hdata8b.byteswap().newbyteorder()  # otherwise get endian error.
        edata = hdata8b.byteswap()  # otherwise get endian error.
        edata = edata.view(edata.dtype.newbyteorder('little'))
        concframe = pd.DataFrame.from_records(edata)
        concframe["levels"] = lev_name
        concframe["time"] = pdate1

        # rename jndx y
        # rename indx x
        names = concframe.columns.values
        names = ["y" if x == "jndx" else x for x in names]
        names = ["x" if x == "indx" else x for x in names]  # codespell:ignore indx
        names = ["z" if x == "levels" else x for x in names]
        names = [col_name if x == "conc" else x for x in names]
        concframe.columns = names
        # concframe.set_index(
        #     ["time", "z", "y", "x"],
        #     inplace=True,
        # )
        #concframe.rename(columns={"conc": col_name}, inplace=True)
        # mgrid = np.meshgrid(lat, lon)
        return concframe

    # def makegrid(self, xindx, yindx):
    #    """
    #    xindx : list
    #    yindx : list
    #    """
    #    attrs = {}
    #    attrs['llcrnr latitude'] = self.llcrnr_lat
    #    attrs['llcrnr longitude'] = self.llcrnr_lon
    #    attrs['Number Lat Points'] = self.nlat
    #    attrs['Number Lon Points'] = self.nlon
    #    attrs['Latitude Spacing'] = self.dlat
    #    attrs['Longitude Spacing'] = self.dlon
    #    mgrid = get_latlongrid(attrs,xindx,yindx)
    #    return mgrid
    # checked HYSPLIT code. the grid points
    # do represent center of the sampling area.
    # slat = self.llcrnr_lat
    # slon = self.llcrnr_lon
    # lat = np.arange(slat, slat + self.nlat * self.dlat, self.dlat)
    # lon = np.arange(slon, slon + self.nlon * self.dlon, self.dlon)
    # hysplit always uses grid from -180 to 180
    # lon = np.array([x-360 if x>180 else x for x in lon])
    # fortran array indice start at 1. so xindx >=1.
    # python array indice start at 0.
    # lonlist = [lon[x - 1] for x in xindx]
    # latlist = [lat[x - 1] for x in yindx]
    # mgrid = np.meshgrid(lonlist, latlist)
    # return mgrid

    def readfile(self, filename, drange, verbose, century):
        """Data from the file is stored in an xarray, self.dset
        returns False if all concentrations are zero else returns True.
        INPUTS
        filename - name of cdump file to open
        drange - [date1, date2] - range of dates to load data for. if []
                 then loads all data.
                 date1 and date2  should be datetime objects.
        verbose - turns on print statements
        century - if None will try to guess the century by looking
                 at the last two digits of the year.
        For python 3 the numpy char4 are read in as a numpy.bytes_
         class and need to be converted to a python
        string by using decode('UTF-8').

        """
        # 8/16/2016 moved species=[]  to before while loop. Added print
        # statements when verbose.
        # self.dset = xr.Dataset()
        # dictionaries which will be turned into the dset attributes.
        fid = open(filename, "rb")

        # each record in the fortran binary begins and ends with 4 bytes which
        # specify the length of the record.
        # These bytes are called pad1 and pad2 below. They are not used here,
        # but are thrown out.
        # The following block defines a numpy dtype object for each record in
        # the binary file.
        recs = self.define_struct()
        rec1, rec2, rec3, rec4a = recs[0], recs[1], recs[2], recs[3]
        rec4b, rec5a, rec5b, rec5c = recs[4], recs[5], recs[6], recs[7]
        rec6, rec8a, rec8b, rec8c = recs[8], recs[9], recs[10], recs[11]
        # rec7 = rec6
        # start_loc in rec1 tell how many rec there are.
        tempzeroconcdates = []
        # Reads header data. This consists of records 1-5.
        hdata1 = np.fromfile(fid, dtype=rec1, count=1)
        nstartloc = self.parse_header(hdata1)
        if nstartloc is None:
            return False
        hdata2 = np.fromfile(fid, dtype=rec2, count=nstartloc)
        century = self.parse_hdata2(hdata2, nstartloc, century)

        hdata3 = np.fromfile(fid, dtype=rec3, count=1)
        self.gridhash = self.parse_hdata3(hdata3)
        if self.verbose:
            print("Grid specs", self.gridhash)
        # read record 4 which gives information about vertical levels.
        hdata4a = np.fromfile(fid, dtype=rec4a, count=1)
        hdata4b = np.fromfile(
            fid, dtype=rec4b, count=hdata4a["nlev"][0]
        )  # reads levels, count is number of levels.
        self.parse_hdata4(hdata4a, hdata4b)

        # read record 5 which gives information about pollutants / species.
        hdata5a = np.fromfile(fid, dtype=rec5a, count=1)
        np.fromfile(fid, dtype=rec5b, count=hdata5a["pollnum"][0])
        np.fromfile(fid, dtype=rec5c, count=1)
        self.atthash["Number of Species"] = hdata5a["pollnum"][0]
        self.atthash["Species ID"] = []

        # Loop to reads records 6-8. Number of loops is equal to number of
        # output times.
        # Only save data for output times within drange. if drange=[] then
        # save all.
        # Loop to go through each sampling time
        iimax = 0  # check to make sure don't go above max number of iterations
        iii = 0  # checks to see if some nonzero data was saved in xarray
        # Safety valve - will not allow more than 1000 loops to be executed.
        imax = 1e8
        testf = True
        timedslist = []
        while testf:
            hdata6 = np.fromfile(fid, dtype=rec6, count=1)
            hdata7 = np.fromfile(fid, dtype=rec6, count=1)
            check, pdate1, pdate2 = self.parse_hdata6and7(hdata6, hdata7, century)
            if not check:
                break
            testf, savedata = check_drange(drange, pdate1, pdate2)
            if verbose:
                print("sample time", pdate1, " to ", pdate2)
            # datelist = []
            inc_iii = False
            # LOOP to go through each pollutant
            poldslist = []
            for _ in range(self.atthash["Number of Species"]):
                # LOOP to go through each level
                concframes = []
                for _ in range(self.atthash["Number of Levels"]):
                    # record 8a has the number of elements (ne). If number of
                    # elements greater than 0 than there are concentrations.
                    hdata8a = np.fromfile(fid, dtype=rec8a, count=1)
                    # self.atthash["Species ID"].append(
                    #    hdata8a["poll"][0].decode("UTF-8")
                    # )
                    # if number of elements is nonzero then
                    if hdata8a["ne"] >= 1:
                        self.atthash["Species ID"].append(hdata8a["poll"][0].decode("UTF-8"))
                        # get rec8 - indx and jndx
                        hdata8b = np.fromfile(fid, dtype=rec8b, count=hdata8a["ne"][0])
                        # add sample start time to list of start times with
                        # non zero conc
                        self.nonzeroconcdates.append(pdate1)
                    else:
                        tempzeroconcdates.append(
                            pdate1
                        )  # or add sample start time to list of start times
                        # with zero conc.
                    # This is just padding.
                    np.fromfile(fid, dtype=rec8c, count=1)
                    # if savedata is set and nonzero concentrations then save
                    # the data in a pandas dataframe
                    if savedata and hdata8a["ne"] >= 1:
                        self.nonzeroconcdates.append(pdate1)
                        inc_iii = True
                        if self.sample_time_stamp == "end":
                            concframe = self.parse_hdata8(hdata8a, hdata8b, pdate2)
                        else:
                            concframe = self.parse_hdata8(hdata8a, hdata8b, pdate1)
                        concframes += [concframe]
                        # if verbose:
                        #    print("Adding ", "Pollutant", pollutant, "Level", lev)
    
                        iimax += 1
                # END LOOP to go through each level
                if len(concframes) > 0:
                    concframes = pd.concat(concframes)
                    concframes.set_index(
                                    ["time", "z", "y", "x"],
                                    inplace=True,
                    )
                    dset = xr.Dataset.from_dataframe(concframes)
                    poldslist += [dset]
                else:
                    poldslist += [None]
            # END LOOP to go through each pollutant
            # safety check - will stop sampling time while loop if goes over
            #  imax iterations.
            if iimax > imax:
                testf = False
                warnings.warn(f"greater than imax {testf},{iimax},{imax}")
            if inc_iii:
                iii += 1
            if len(poldslist) > 0:
                timedslist += [poldslist]
        # END OF Loop to go through each sampling time
        self.atthash.update(self.gridhash)
        self.atthash["Species ID"] = list(set(self.atthash["Species ID"]))
        self.atthash["Coordinate time description"] = "Beginning of sampling time"

        Ns = range(self.atthash["Number of Species"])
        # Grab per species all relevant datasets in the time list
        dsets = [[ll[n] for ll in timedslist if ll[n] is not None] for n in Ns]
        dsets = [xr.concat(ds, dim='time') for ds in dsets if len(ds) > 0]
        if len(dsets) == 0:
            return False
        self.dset = xr.merge(dsets)
        # if not self.dset.any():
        #     return False
        if self.dset.variables:
            self.atthash = check_attributes(self.atthash)
            self.dset.attrs = self.atthash
            # mgrid = self.makegrid(self.dset.coords["x"], self.dset.coords["y"])
            mgrid = get_latlongrid(self.gridhash, self.dset.coords["x"], self.dset.coords["y"])
            self.dset = self.dset.assign_coords(longitude=(("y", "x"), mgrid[0]))
            self.dset = self.dset.assign_coords(latitude=(("y", "x"), mgrid[1]))

            self.dset = self.dset.reset_coords()
            self.dset = self.dset.set_coords(["time", "latitude", "longitude"])
        if iii == 0 and verbose:
            print("ModelBin class _readfile method: no data in the date range found")
            return False
        return True


class CombineObject:
    """
    Helper class for combine_dataset function.
    """

    def __init__(self, blist: tuple, drange=None, century=None, sample_time_stamp="start"):
        self.fname = blist[0]
        self.source = blist[1]
        self.ens = blist[2]
        self.hxr = self.open(self.fname, drange, century, sample_time_stamp)
        self._attrs = {}
        if not self.empty:
            self.attrs = self.hxr.attrs
        self.xrash = xr.DataArray()  # created in process method.

    def grid_equal(self, other):
        # other: another CombineObject object.
        # checks to see if grid is equal
        mlat, mlon = self.grid_definition
        mlat2, mlon2 = other.grid_definition
        if not np.array_equal(mlat, mlat2):
            return False
        if not np.array_equal(mlon, mlon2):
            return False
        return True

    def __lt__(self, other):
        if self.start_time < other.start_time:
            return True
        if self.source < other.source:
            return True
        if self.ens < other.ens:
            return True
        return False

    @property
    def empty(self):
        if self.hxr.coords:
            return False
        else:
            return True

    @property
    def attrs(self):
        return self._attrs

    @attrs.setter
    def attrs(self, atthash):
        if isinstance(atthash, dict):
            self._attrs.update(atthash)

    @property
    def start_time(self):
        tvals = self.hxr.time.values
        tvals.sort()
        return tvals[0]

    @property
    def grid_definition(self):
        return getlatlon(self.hxr.attrs)

    def process(self, stime=None, dt=None, species=None):
        """
        add species, change time coordinate to an integer for alignment.
        """
        xrash = add_species(self.hxr, species=species)
        xrash = time2index(xrash, stime, dt)
        xrash = xrash.drop_vars("time_values")
        self.xrash = xrash
        self.attrs = self.xrash.attrs

    @staticmethod
    def open(fname, drange, century, sample_time_stamp="start", verbose=False):
        if drange:
            century = int(drange[0].year / 100) * 100
            hxr = open_dataset(
                fname,
                drange=drange,
                century=century,
                verbose=verbose,
                sample_time_stamp=sample_time_stamp,
                check_grid=False,
            )
        else:  # use all dates
            hxr = open_dataset(
                fname,
                century=century,
                verbose=verbose,
                sample_time_stamp=sample_time_stamp,
                check_grid=False,
            )
        return hxr


def combine_dataset(
    blist,
    drange=None,
    species=None,
    century=None,
    verbose=False,
    sample_time_stamp="start",
    check_grid=True,
):
    """
    Inputs :
      blist : list of tuples
      (filename, sourcetag, metdatatag)

    drange : list of two datetime objects.
     d1 datetime object. first date to keep in DatArrayarray
     d2 datetime object. last date to keep in DataArray

    sample_time_stamp : str
        if 'end' then time in xarray will be the end of sampling time period.
        else time is start of sampling time period.

    RETURNS
     newhxr : an xarray data-array with 6 dimensions.
            lat, lon, time, level, ensemble tag, source tag

    Note that if more than one species is present in the files, they are
    added to get concentration from all species. If list of species is provided,
    only those species will be added.

    Files need to have the same concentration grid defined.
    If files have no concentrations then they will be skipped.

    """
    # 2024 04 March. when the input datasets did not have identical time coordinates, the align method of
    #                xarray was not working properly. Changing the time coordinate to an integer first
    #                fixes the problem.
    #                Another issue is that the combination only worked when either the source or the ensemble dimension
    #                had length of 1. Did not work properly with multiple sources and multiple ensembles.
    #                to fix this changed how enslist and sourcelist were defined and utilized.

    # create list of datasets to be combined and their properties.
    # removes any cdumps that are empty.
    xlist = []  # list of CombineObject objects
    for bbb in blist:
        cobject = CombineObject(bbb, drange, century, sample_time_stamp)
        if not cobject.empty:
            xlist.append(cobject)
        else:
            warnings.warn(f"could not open {bbb[0]}")

    # check that grids are equal by comparing each grid to the one before.
    for iii, xobj in enumerate(xlist[1:]):
        if not xobj.grid_equal(xlist[iii]):
            warnings.warn("grids are not the same. cannot combine")
            sys.exit()

    xlist.sort()
    # use earliest time
    svals = [x.start_time for x in xlist]
    svals.sort()
    stime = svals[0]
    # process the data-arrays to be combined.
    # change time coordinate to index, sum species.
    [x.process(stime, dt=1, species=species) for x in xlist]

    # align to get biggest grid
    xbig = xlist[0].xrash.copy()
    for xobj in xlist[1:]:
        aaa, xbig = xr.align(xobj.xrash, xbig, join="outer")

    # First group and concatenate along ensemble dimension.
    # sourcelist = list(set([x.source for x in xlist]))
    sourcelist = list({x.source for x in xlist})
    outlist = []
    for source in sourcelist:
        # get all objects with that source
        elist = [x for x in xlist if x.source == source]
        inlist = []
        for eee in elist:
            aaa, junk = xr.align(eee.xrash, xbig, join="outer")
            aaa = aaa.fillna(0)
            aaa.expand_dims("ens")
            aaa["ens"] = eee.ens
            inlist.append(aaa)
        # concat on ensemble dimension
        inner = xr.concat(inlist, "ens")
        outlist.append(inner)
    # concat on source dimension
    newhxr = xr.concat(outlist, "source")
    newhxr["source"] = sourcelist

    atthash = xlist[0].hxr.attrs
    attrs = check_attributes(atthash)
    newhxr = newhxr.assign_attrs(attrs)
    newhxr = reset_latlon_coords(newhxr)
    # change time coordinate back to datetime
    newhxr = index2time(newhxr)
    if check_grid:
        rval = fix_grid_continuity(newhxr)
    else:
        rval = newhxr
    return rval


def get_time_index(timevals, stime, dt):
    """
    timevals : list of datetimes
    stime    : start time of time grid
    dt       : integer - time resolution in hours of time grid.
    """

    def apply(ttt):
        diff = pd.to_datetime(ttt) - stime
        dh = diff.days * 24 + diff.seconds / 3600
        iii = dh / dt
        return int(iii)

    return [apply(x) for x in timevals]


def get_time_values(index_values, stime, dt):
    def apply(iii):
        ddd = stime + datetime.timedelta(hours=float(iii))
        return ddd

    return [apply(x) for x in index_values]


def time2index(hxr, stime=None, dt=1):
    """
    hxr : xarray DataSet as output from open_dataset or combine_dataset
    make the time

    """
    stime_str = "coordinate start time"
    dt_str = "coordinate time dt (hours)"
    tvals = hxr.time.values
    if stime is None:
        stime = tvals[0]
    ilist = get_time_index(tvals, stime, dt)
    # create a time_index coordinate.
    hxr = hxr.drop_vars("time")
    hxr = hxr.assign_coords(time=("time", ilist))
    hxr = hxr.assign_coords(time_values=("time", tvals))
    atthash = {stime_str: stime}
    atthash[dt_str] = dt
    hxr.attrs.update(atthash)
    # temp = temp.drop_vars('time')
    # temp = temp.assign_coords(time=('t',tvals))
    return hxr


def index2time(hxr):
    """
    hxr : DataSet or DataArray with time coordinate in integer values.
          should either have a time_values coordinate or attribute defining
          coordinate start time and coordinate time delta.
    Reverses changes made in time2index
    """
    if "time_values" in hxr.coords:
        tvals = hxr.time_values.values
        hxr = hxr.drop_vars("time")
        hxr = hxr.drop_vars("time_values")
        hxr = hxr.assign_coords(time=("time", tvals))
    else:
        stime_str = "coordinate start time"
        dt_str = "coordinate time dt (hours)"
        if stime_str in hxr.attrs.keys():
            stime = pd.to_datetime(hxr.attrs[stime_str])
        if dt_str in hxr.attrs.keys():
            dt = hxr.attrs[dt_str]
        ilist = hxr.time.values
        tvals = get_time_values(ilist, stime, dt)
        hxr = hxr.drop_vars("time")
        hxr = hxr.assign_coords(time=("time", tvals))
    return hxr


def reset_latlon_coords(hxr):
    """
    hxr : xarray DataSet as output from open_dataset or combine_dataset
    """
    mgrid = get_latlongrid(hxr.attrs, hxr.x.values, hxr.y.values)
    if "latitude" in hxr.coords:
        hxr = hxr.drop("longitude")
    if "longitude" in hxr.coords:
        hxr = hxr.drop("latitude")
    hxr = hxr.assign_coords(latitude=(("y", "x"), mgrid[1]))
    hxr = hxr.assign_coords(longitude=(("y", "x"), mgrid[0]))
    return hxr


def fix_grid_continuity(dset):
    # if dset is empty don't do anything
    if not dset.any():
        return dset

    # if grid already continuous don't do anything.
    if check_grid_continuity(dset):
        return dset
    xvv = dset.x.values
    yvv = dset.y.values

    xlim = [xvv[0], xvv[-1]]
    ylim = [yvv[0], yvv[-1]]

    xindx = np.arange(xlim[0], xlim[1] + 1)
    yindx = np.arange(ylim[0], ylim[1] + 1)

    mgrid = get_latlongrid(dset.attrs, xindx, yindx)
    # mgrid = get_even_latlongrid(dset, xlim, ylim)
    conc = np.zeros_like(mgrid[0])
    dummy = xr.DataArray(conc, dims=["y", "x"])
    dummy = dummy.assign_coords(latitude=(("y", "x"), mgrid[1]))
    dummy = dummy.assign_coords(longitude=(("y", "x"), mgrid[0]))
    dummy = dummy.assign_coords(x=(("x"), xindx))
    dummy = dummy.assign_coords(y=(("y"), yindx))
    cdset, dummy2 = xr.align(dset, dummy, join="outer")
    cdset = cdset.assign_coords(latitude=(("y", "x"), mgrid[1]))
    cdset = cdset.assign_coords(longitude=(("y", "x"), mgrid[0]))
    return cdset.fillna(0)


def check_grid_continuity(dset):
    """
    checks to see if x and y coords are skipping over any grid points.
    Since cdump files only store above 0 values, it is possible to have
    a grid that is
    y = [1,2,3,4,6,8]
    if there are above zero values at 6 and 8 but not at 7.
    This results in an xarray which has a grid that is not evenly spaced.
    """
    xvv = dset.x.values
    yvv = dset.y.values
    tt1 = np.array([xvv[i] - xvv[i - 1] for i in np.arange(1, len(xvv))])
    tt2 = np.array([yvv[i] - yvv[i - 1] for i in np.arange(1, len(yvv))])
    if np.any(tt1 != 1):
        return False
    if np.any(tt2 != 1):
        return False
    return True


def get_latlongrid(attrs, xindx, yindx):
    """
    INPUTS
    attrs : dictionary with grid specifications
    xindx : list of integers > 0
    yindx : list of integers > 0
    RETURNS
    mgrid : output of numpy meshgrid function.
            Two 2d arrays of latitude, longitude.
    The grid points in cdump file
    represent center of the sampling area.

    NOTES :
    This may return a grid that is not evenly spaced.
    For instance if yindx is something like [1,2,3,4,5,7] then
    the grid will not have even spacing in latitude and will 'skip' a latitude point.

    HYSPLIT grid indexing starts at 1.

    """
    xindx = np.array(xindx)
    yindx = np.array(yindx)
    if np.any(xindx <= 0):
        raise Exception("HYSPLIT grid error xindex <=0")
    if np.any(yindx <= 0):
        raise Exception("HYSPLIT grid error yindex <=0")
    lat, lon = getlatlon(attrs)
    success = True
    try:
        lonlist = [lon[x - 1] for x in xindx]
    except Exception as eee:
        warnings.warn(f"Exception {eee}")
        warnings.warn("try increasing Number Number Lon Points")
        success = False
    try:
        latlist = [lat[x - 1] for x in yindx]
    except Exception as eee:
        warnings.warn(f"Exception {eee}")
        warnings.warn("try increasing Number Number Lat Points")
        success = False

    if not success:
        return None
    mgrid = np.meshgrid(lonlist, latlist)
    return mgrid


# def get_index_fromgrid(dset):
#    llcrnr_lat = dset.attrs["llcrnr latitude"]
#    llcrnr_lon = dset.attrs["llcrnr longitude"]
#    nlat = dset.attrs["Number Lat Points"]
#    nlon = dset.attrs["Number Lon Points"]
#    dlat = dset.attrs["Latitude Spacing"]
#    dlon = dset.attrs["Longitude Spacing"]


def getlatlon(attrs):
    """
    Returns 1d array of lats and lons based on Concentration Grid
    Defined in the dset attribute.
    attrs : dictionary with grid specifications
    RETURNS
    lat : 1D array of latitudes
    lon : 1D array of longitudes
    """
    lon_tolerance = 0.001
    llcrnr_lat = attrs["llcrnr latitude"]
    llcrnr_lon = attrs["llcrnr longitude"]
    nlat = attrs["Number Lat Points"]
    nlon = attrs["Number Lon Points"]
    dlat = attrs["Latitude Spacing"]
    dlon = attrs["Longitude Spacing"]

    lastlon = llcrnr_lon + (nlon - 1) * dlon
    lastlat = llcrnr_lat + (nlat - 1) * dlat
    # = int((lastlon - llcrnr_lon) / dlon)
    lat = np.linspace(llcrnr_lat, lastlat, num=int(nlat))
    lon = np.linspace(llcrnr_lon, lastlon, num=int(nlon))
    #
    lon = np.array([x - 360 if x >= 180 + lon_tolerance else x for x in lon])
    return lat, lon


def hysp_massload(dset, threshold=0, mult=1, zvals=None):
    """Calculate mass loading from HYSPLIT xarray
    INPUTS
    dset: xarray dataset output by open_dataset OR
           xarray data array output by combine_dataset
    threshold : float
    mult : float
    zvals : list of levels to calculate mass loading over.
    Outputs:
    totl_aml : xarray data array
    total ash mass loading (summed over all layers), ash mass loading
    Units in (unit mass / m^2)
    """
    # first calculate mass loading in each level.
    aml_alts = calc_aml(dset)
    # Then choose which levels to use for total mass loading.
    if zvals:
        aml_alts = aml_alts.isel(z=zvals)
        if "z" not in aml_alts.dims:
            aml_alts = aml_alts.expand_dims("z")
    #
    total_aml = aml_alts.sum(dim="z")
    # Calculate conversion factors
    # unitmass, mass63 = calc_MER(dset)
    # Calculating the ash mass loading
    total_aml2 = total_aml * mult
    # Calculating total ash mass loading, accounting for the threshold
    # Multiply binary threshold mask to data
    total_aml_thresh = hysp_thresh(dset, threshold, mult=mult)
    total_aml = total_aml2 * total_aml_thresh
    return total_aml


def hysp_heights(dset, threshold, mult=1, height_mult=1 / 1000.0, mass_load=True, species=None):
    """Calculate top-height from HYSPLIT xarray
    Input: xarray dataset output by open_dataset OR
           xarray data array output by combine_dataset
    threshold : mass loading threshold (threshold = xx)
    mult : convert from meters to other unit. default is 1/1000.0 to
           convert to km.
    Outputs: ash top heights, altitude levels"""

    # either get mass loading of each point
    if mass_load:
        aml_alts = calc_aml(dset)
    # or get concentration at each point
    else:
        aml_alts = add_species(dset, species=species)

    # Create array of 0 and 1 (1 where data exists)
    heights = aml_alts.where(aml_alts == 0.0, 1.0)
    # Multiply each level by the altitude
    height = _alt_multiply(heights)
    height = height * height_mult  # convert to km
    # Determine top height: take max of heights array along z axis
    top_hgt = height.max(dim="z")
    # Apply ash mass loading threshold mask array
    total_aml_thresh = hysp_thresh(dset, threshold, mult=mult)
    top_height = top_hgt * total_aml_thresh
    return top_height


# def calc_total_mass(dset):
#    return -1


def calc_aml(dset, species=None):
    """Calculates the mass loading at each altitude for the dataset
    Input: xarray dataset output by open_dataset OR
           xarray data array output by combine_dataset
    Output: total ash mass loading"""
    # Totals values for all particles
    if isinstance(dset, xr.core.dataset.Dataset):
        total_par = add_species(dset, species=species)
    else:
        total_par = dset.copy()
    # Multiplies the total particles by the altitude layer
    # to create a mass loading for each altitude layer
    aml_alts = _delta_multiply(total_par)
    return aml_alts


def hysp_thresh(dset, threshold, mult=1):
    """Calculates a threshold mask array based on the
    ash mass loading from HYSPLIT xarray
    Inputs: xarray, ash mass loading threshold (threshold = xx)
    Outputs: ash mass loading threshold mask array
    Returns 0 where values are below or equal to threshold.
    Returns 1 where values are greater than threshold

    """
    # Calculate ash mass loading for xarray
    aml_alts = calc_aml(dset)
    total_aml = aml_alts.sum(dim="z")
    # Calculate conversion factors
    # unitmass, mass63 = calc_MER(dset)
    # Calculating the ash mass loading
    total_aml2 = total_aml * mult
    # where puts value into places where condition is FALSE.
    # place 0 in places where value is below or equal to threshold
    total_aml_thresh = total_aml2.where(total_aml2 > threshold, 0.0)
    # place 1 in places where value is greater than threshold
    total_aml_thresh = total_aml_thresh.where(total_aml_thresh <= threshold, 1.0)
    return total_aml_thresh


def add_species(dset, species=None):
    """
    species : list of Species ID's.
              if none then all ids in the "species ID" attribute will be used.
    Calculate sum of particles.
    """
    sflist = []
    splist = dset.attrs["Species ID"]
    if not species:
        species = dset.attrs["Species ID"]
    else:
        for val in species:
            if val not in splist:
                warn = "warnings: hysplit.add_species function"
                warn += ": species not found" + str(val) + "\n"
                warn += " valid species ids are " + str.join(", ", splist)
                warnings.warn(warn)
    sss = 0
    tmp = []
    # Looping through all species in dataset
    while sss < len(splist):
        if splist[sss] in species:
            tmp.append(dset[splist[sss]].fillna(0))
            sflist.append(splist[sss])
        sss += 1  # End of loop through species

    total_par = tmp[0]
    ppp = 1
    # Adding all species together
    while ppp < len(tmp):
        total_par = total_par + tmp[ppp]
        ppp += 1  # End of loop adding all species
    atthash = dset.attrs
    atthash["Species ID"] = sflist
    atthash = check_attributes(atthash)
    total_par = total_par.assign_attrs(atthash)
    return total_par


def calculate_thickness(cdump):
    alts = cdump.z.values
    thash = {}
    aaa = 0
    for avalue in alts:
        thash[avalue] = avalue - aaa
        aaa = avalue
    warnings.warn(f"WARNING: thickness calculated from z values please verify {thash}")
    return thash


def get_thickness(cdump):
    """
    Input:
    cdump : xarray DataArray with 'Level top heights (m)' as an attribute.
    Returns:
    thash : dictionary
    key is the name of the z coordinate and value is the thickness of that layer in meters.
    """
    cstr = "Level top heights (m)"

    calculate = False
    if cstr not in cdump.attrs.keys():
        calculate = True
    # check that the values in the attribute correspond to values in levels.
    # python reads in this attribute as a numpy array
    # but when writing the numpy array to netcdf file, it doesn't write correctly.
    # sometimes cdump file is written with incorrect values in this attribute.
    elif cstr in cdump.attrs.keys():
        alts = cdump.z.values
        zvals = cdump.attrs[cstr]
        # cra = []
        for aaa in alts:
            # if a level is not found in the attribute array then use the calculate method.
            if aaa not in zvals:
                calculate = True

    if calculate:
        warnings.warn(f"{cstr} attribute needed to calculate level thicknesses")
        warnings.warn("alternative calculation from z dimension values")
        thash = calculate_thickness(cdump)
    else:
        levs = cdump.attrs[cstr]
        thash = {}
        aaa = 0
        for level in levs:
            thash[level] = level - aaa
            aaa = level
    return thash


def _delta_multiply(pars):
    """
    # Calculate the delta altitude for each layer and
    # multiplies concentration by layer thickness to return mass load.
    # requires that the 'Level top heights (m)' is an attribute of pars.

    # pars: xarray data array
            concentration with z coordinate.
    # OUTPUT
    # newpar : xarray data array
            mass loading.
    """
    thash = get_thickness(pars)
    for iii, zzz in enumerate(pars.z.values):
        delta = thash[zzz]
        mml = pars.isel(z=iii) * delta
        if iii == 0:
            newpar = mml
        else:
            newpar = xr.concat([newpar, mml], "z")
        if "z" not in newpar.dims:
            newpar = newpar.expand_dims("z")
    return newpar


def _delta_multiply_old(pars):
    """
    # This method was faulty because layers with no concentrations were
    # omitted. e.g. if layers were at 0,1000,2000,3000,4000,5000 but there were
    # no mass below 20000 then would only see layers 3000,4000,5000 and thickness
    # of 3000 layer would be calculated as 3000 instead of 1000.
    # Calculate the delta altitude for each layer and
    # multiplies concentration by layer thickness to return mass load.

    # pars: xarray data array
            concentration with z coordinate.
    # OUTPUT
    # newpar : xarray data array
            mass loading.
    """
    xxx = 1
    alts = pars.coords["z"]
    delta = []
    delta.append(alts[0])
    while xxx < (len(alts)):
        delta.append(alts[xxx] - alts[xxx - 1])
        xxx += 1
    # Multiply each level by the delta altitude
    yyy = 0
    while yyy < len(delta):
        # modify so not dependent on placement of 'z' coordinate.
        # pars[:, yyy, :, :] = pars[:, yyy, :, :] * delta[yyy]
        mml = pars.isel(z=yyy) * delta[yyy]
        if yyy == 0:
            newpar = mml
        else:
            newpar = xr.concat([newpar, mml], "z")
        if "z" not in newpar.dims:
            newpar = newpar.expand_dims("z")
        yyy += 1  # End of loop calculating heights
    return newpar


def _alt_multiply(pars):
    """
    # For calculating the top height
    # Multiply "1s" in the input array by the altitude
    """
    alts = pars.coords["z"]
    yyy = 0
    while yyy < len(alts):
        # modify so not dependent on placement of 'z' coordinate.
        # pars[:, y, :, :] = pars[:, y, :, :] * alts[y]
        mml = pars.isel(z=yyy) * alts[yyy]
        if yyy == 0:
            newpar = mml
        else:
            newpar = xr.concat([newpar, mml], "z")
        yyy += 1  # End of loop calculating heights
        if "z" not in newpar.dims:
            newpar = newpar.expand_dims("z")
    return newpar


def check_attributes(atthash):
    # when writing to netcdf file, attributes which are numpy arrays do not write properly.
    # need to change them to lists.
    for key in atthash.keys():
        val = atthash[key]
        if isinstance(val, np.ndarray):
            newval = list(val)
            atthash[key] = newval
    return atthash


def write_with_compression(cxra, fname):
    atthash = check_attributes(cxra.attrs)
    cxra = cxra.assign_attrs(atthash)
    cxra2 = cxra.to_dataset(name="POL")
    ehash = {"zlib": True, "complevel": 9}
    vlist = [x for x in cxra2.data_vars]
    vhash = {}
    for vvv in vlist:
        vhash[vvv] = ehash
    cxra2.to_netcdf(fname, encoding=vhash)
