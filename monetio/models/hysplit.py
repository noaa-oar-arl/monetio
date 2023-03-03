"""
HYPSLIT MODEL READER

This code developed at the NOAA Air Resources Laboratory.
Alice Crawford
Allison Ring

-------------
Functions:
-------------
open_dataset :
combine_dataset :
get_latlongrid :
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

"""
import datetime
import sys

import numpy as np
import pandas as pd
import xarray as xr


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
    dset = binfile.dset
    if check_grid:
        return fix_grid_continuity(dset)
    else:
        return dset


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
        self.llcrnr_lon = None
        self.llcrnr_lat = None
        self.nlat = None
        self.nlon = None
        self.dlat = None
        self.dlon = None
        self.levels = None
        self.dset = xr.Dataset()

        if readwrite == "r":
            if verbose:
                print("reading " + filename)
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
            print("WARNING in ModelBin _readfile - number of starting locations " "incorrect")
            print(hdata1["start_loc"])
        # in python 3 np.fromfile reads the record into a list even if it is
        # just one number.
        # so if the length of this record is greater than one something is
        # wrong.
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
                print("WARNING: Guessing Century for HYSPLIT concentration file", century)
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

    def parse_hdata3(self, hdata3, ahash):
        # Description of concentration grid
        ahash["Number Lat Points"] = hdata3["nlat"][0]
        ahash["Number Lon Points"] = hdata3["nlon"][0]
        ahash["Latitude Spacing"] = hdata3["dlat"][0]
        ahash["Longitude Spacing"] = hdata3["dlon"][0]
        ahash["llcrnr longitude"] = hdata3["llcrnr_lon"][0]
        ahash["llcrnr latitude"] = hdata3["llcrnr_lat"][0]

        self.llcrnr_lon = hdata3["llcrnr_lon"][0]
        self.llcrnr_lat = hdata3["llcrnr_lat"][0]
        self.nlat = hdata3["nlat"][0]
        self.nlon = hdata3["nlon"][0]
        self.dlat = hdata3["dlat"][0]
        self.dlon = hdata3["dlon"][0]
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
        edata = hdata8b.byteswap().newbyteorder()  # otherwise get endian error.
        concframe = pd.DataFrame.from_records(edata)
        concframe["levels"] = lev_name
        concframe["time"] = pdate1

        # rename jndx x
        # rename indx y
        names = concframe.columns.values
        names = ["y" if x == "jndx" else x for x in names]
        names = ["x" if x == "indx" else x for x in names]
        names = ["z" if x == "levels" else x for x in names]
        concframe.columns = names
        concframe.set_index(
            ["time", "z", "y", "x"],
            inplace=True,
        )
        concframe.rename(columns={"conc": col_name}, inplace=True)
        # mgrid = np.meshgrid(lat, lon)
        return concframe

    def makegrid(self, xindx, yindx):
        """
        xindx : list
        yindx : list
        """
        # checked HYSPLIT code. the grid points
        # do represent center of the sampling area.
        slat = self.llcrnr_lat
        slon = self.llcrnr_lon
        lat = np.arange(slat, slat + self.nlat * self.dlat, self.dlat)
        lon = np.arange(slon, slon + self.nlon * self.dlon, self.dlon)
        # hysplit always uses grid from -180 to 180
        lon = np.array([x - 360 if x > 180 else x for x in lon])
        # fortran array indice start at 1. so xindx >=1.
        # python array indice start at 0.
        lonlist = [lon[x - 1] for x in xindx]
        latlist = [lat[x - 1] for x in yindx]
        mgrid = np.meshgrid(lonlist, latlist)
        return mgrid

    def readfile(self, filename, drange, verbose, century):
        """Data from the file is stored in an xarray, self.dset
        returns False if all concentrations are zero else returns True.
        INPUTS
        filename - name of cdump file to open
        drange - [date1, date2] - range of dates to load data for. if []
                 then loads all data.
                 date1 and date2  should be datetime ojbects.
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
        ahash = {}
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

        hdata2 = np.fromfile(fid, dtype=rec2, count=nstartloc)
        century = self.parse_hdata2(hdata2, nstartloc, century)

        hdata3 = np.fromfile(fid, dtype=rec3, count=1)
        ahash = self.parse_hdata3(hdata3, ahash)

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
            # LOOP to go through each level
            for _ in range(self.atthash["Number of Levels"]):
                # LOOP to go through each pollutant
                for _ in range(self.atthash["Number of Species"]):
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
                        dset = xr.Dataset.from_dataframe(concframe)
                        # if verbose:
                        #    print("Adding ", "Pollutant", pollutant, "Level", lev)
                        # if this is the first time through. create dataframe
                        # for first level and pollutant.
                        if not self.dset.any():
                            self.dset = dset
                        else:  # create dataframe for level and pollutant and
                            # then merge with main dataframe.
                            # self.dset = xr.concat([self.dset, dset],'levels')
                            # self.dset = xr.merge([self.dset, dset],compat='override')
                            self.dset = xr.merge([self.dset, dset])
                            # self.dset = xr.combine_by_coords([self.dset, dset])
                            # self.dset = xr.merge([self.dset, dset], compat='override')
                        iimax += 1
                # END LOOP to go through each pollutant
            # END LOOP to go through each level
            # safety check - will stop sampling time while loop if goes over
            #  imax iterations.
            if iimax > imax:
                testf = False
                print("greater than imax", testf, iimax, imax)
            if inc_iii:
                iii += 1

        self.atthash.update(ahash)
        self.atthash["Species ID"] = list(set(self.atthash["Species ID"]))
        self.atthash["Coordinate time description"] = "Beginning of sampling time"
        # END OF Loop to go through each sampling time
        if not self.dset.any():
            return False
        if self.dset.variables:
            self.dset.attrs = self.atthash
            mgrid = self.makegrid(self.dset.coords["x"], self.dset.coords["y"])
            self.dset = self.dset.assign_coords(longitude=(("y", "x"), mgrid[0]))
            self.dset = self.dset.assign_coords(latitude=(("y", "x"), mgrid[1]))

            self.dset = self.dset.reset_coords()
            self.dset = self.dset.set_coords(["time", "latitude", "longitude"])
        if iii == 0 and verbose:
            print("Warning: ModelBin class _readfile method: no data in the date range found")
            return False
        return True


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
    """
    # iii = 0
    mlat_p = mlon_p = None
    ylist = []
    dtlist = []
    splist = []
    sourcelist = []
    # turn the input list int a dictionary
    #  blist : dictionary.
    #  key is a tag indicating the source
    #  value is tuple (filename, metdata)
    aaa = sorted(blist, key=lambda x: x[1])
    blist = {}
    for val in aaa:
        if val[1] in blist.keys():
            blist[val[1]].append((val[0], val[2]))
        else:
            blist[val[1]] = [(val[0], val[2])]

    # mgrid = []
    # first loop go through to get expanded dataset.
    xlist = []
    sourcelist = []
    enslist = []
    for iii, key in enumerate(blist):
        # fname = val[0]
        xsublist = []
        for fname in blist[key]:
            if drange:
                century = int(drange[0].year / 100) * 100
                hxr = open_dataset(
                    fname[0],
                    drange=drange,
                    century=century,
                    verbose=verbose,
                    sample_time_stamp=sample_time_stamp,
                    check_grid=False,
                )
            else:  # use all dates
                hxr = open_dataset(
                    fname[0],
                    century=century,
                    verbose=verbose,
                    sample_time_stamp=sample_time_stamp,
                    check_grid=False,
                )
            try:
                mlat, mlon = getlatlon(hxr)
            except Exception:
                print("WARNING Cannot open ")
                print(fname[0])
                print(century)
                print(hxr)
            if iii > 0:
                tt1 = np.array_equal(mlat, mlat_p)
                tt2 = np.array_equal(mlon, mlon_p)
                if not tt1 or not tt2:
                    print("WARNING: grids are not the same. cannot combine")
                    sys.exit()
            mlat_p = mlat
            mlon_p = mlon

            # lon = hxr.longitude.isel(y=0).values
            # lat = hxr.latitude.isel(x=0).values
            xrash = add_species(hxr, species=species)
            xsublist.append(xrash)
            enslist.append(fname[1])
            dtlist.append(hxr.attrs["sample time hours"])
            splist.extend(xrash.attrs["Species ID"])
            if iii == 0:
                xnew = xrash.copy()
            else:
                aaa, xnew = xr.align(xrash, xnew, join="outer")
                xnew = xnew.fillna(0)
            # iii += 1
        sourcelist.append(key)
        xlist.append(xsublist)
    # if verbose:
    #    print("aligned --------------------------------------")
    # xnew is now encompasses the area of all the data-arrays
    # now go through and expand each one to the size of xnew.
    # iii = 0
    # jjj = 0
    ylist = []
    slist = []
    for jjj, sublist in enumerate(xlist):
        hlist = []
        for iii, temp in enumerate(sublist):
            # expand to same region as xnew
            aaa, bbb = xr.align(temp, xnew, join="outer")
            aaa = aaa.fillna(0)
            bbb = bbb.fillna(0)
            aaa.expand_dims("ens")
            aaa["ens"] = enslist[iii]
            # iii += 1
            hlist.append(aaa)
        # concat along the 'ens' axis
        new = xr.concat(hlist, "ens")
        ylist.append(new)
        slist.append(sourcelist[jjj])
        # jjj += 1
    if dtlist:
        dtlist = list(set(dtlist))
        dt = dtlist[0]

    newhxr = xr.concat(ylist, "source")
    newhxr["source"] = slist
    # newhxr['ens'] = metlist

    # newhxr is an xarray data-array with 6 dimensions.
    # dt is the averaging time of the hysplit output.
    newhxr = newhxr.assign_attrs({"sample time hours": dt})
    newhxr = newhxr.assign_attrs({"Species ID": list(set(splist))})
    newhxr.attrs.update(hxr.attrs)
    keylist = ["time description"]

    # calculate the lat lon grid for the expanded dataset.
    # and use that for the new coordinates.
    newhxr = reset_latlon_coords(newhxr)

    for key in keylist:
        newhxr = newhxr.assign_attrs({key: hxr.attrs[key]})
    if check_grid:
        rval = fix_grid_continuity(newhxr)
    else:
        rval = newhxr
    return rval


# This function seems not useful.
# def get_even_latlongrid(dset, xlim, ylim):
#    xindx = np.arange(xlim[0], xlim[1] + 1)
#    yindx = np.arange(ylim[0], ylim[1] + 1)
#    return get_latlongrid(dset, xindx, yindx)


def reset_latlon_coords(hxr):
    """
    hxr : xarray DataSet as output from open_dataset or combine_dataset
    """
    mgrid = get_latlongrid(hxr, hxr.x.values, hxr.y.values)
    hxr = hxr.drop("longitude")
    hxr = hxr.drop("latitude")
    hxr = hxr.assign_coords(latitude=(("y", "x"), mgrid[1]))
    hxr = hxr.assign_coords(longitude=(("y", "x"), mgrid[0]))
    return hxr


def fix_grid_continuity(dset):
    # if dset is empty don't do anything
    if not dset.any():
        return dset

    # if grid already continuos don't do anything.
    if check_grid_continuity(dset):
        return dset

    xvv = dset.x.values
    yvv = dset.y.values

    xlim = [xvv[0], xvv[-1]]
    ylim = [yvv[0], yvv[-1]]

    xindx = np.arange(xlim[0], xlim[1] + 1)
    yindx = np.arange(ylim[0], ylim[1] + 1)

    mgrid = get_latlongrid(dset, xindx, yindx)
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


def get_latlongrid(dset, xindx, yindx):
    """
    INPUTS
    dset : xarray data set from ModelBin class
    xindx : list of integers
    yindx : list of integers
    RETURNS
    mgrid : output of numpy meshgrid function.
            Two 2d arrays of latitude, longitude.
    The grid points in cdump file
    represent center of the sampling area.

    NOTES :
    This may return a grid that is not evenly spaced.
    For instance if yindx is something like [1,2,3,4,5,7] then
    the grid will not have even spacing in latitude and will 'skip' a latitude point.
    """
    llcrnr_lat = dset.attrs["llcrnr latitude"]
    llcrnr_lon = dset.attrs["llcrnr longitude"]
    nlat = dset.attrs["Number Lat Points"]
    nlon = dset.attrs["Number Lon Points"]
    dlat = dset.attrs["Latitude Spacing"]
    dlon = dset.attrs["Longitude Spacing"]

    lat = np.arange(llcrnr_lat, llcrnr_lat + nlat * dlat, dlat)
    lon = np.arange(llcrnr_lon, llcrnr_lon + nlon * dlon, dlon)
    lon = np.array([x - 360 if x > 180 else x for x in lon])
    try:
        lonlist = [lon[x - 1] for x in xindx]
        latlist = [lat[x - 1] for x in yindx]
    except Exception as eee:
        print(f"Exception {eee}")
        print("try increasing Number Lat Points or Number Lon Points")
    mgrid = np.meshgrid(lonlist, latlist)
    return mgrid


# def get_index_fromgrid(dset):
#    llcrnr_lat = dset.attrs["llcrnr latitude"]
#    llcrnr_lon = dset.attrs["llcrnr longitude"]
#    nlat = dset.attrs["Number Lat Points"]
#    nlon = dset.attrs["Number Lon Points"]
#    dlat = dset.attrs["Latitude Spacing"]
#    dlon = dset.attrs["Longitude Spacing"]


def getlatlon(dset):
    """
    Returns 1d array of lats and lons based on Concentration Grid
    Defined in the dset attribute.
    dset : xarray returned by hysplit.open_dataset function
    RETURNS
    lat : 1D array of latitudes
    lon : 1D array of longitudes
    """
    llcrnr_lat = dset.attrs["llcrnr latitude"]
    llcrnr_lon = dset.attrs["llcrnr longitude"]
    nlat = dset.attrs["Number Lat Points"]
    nlon = dset.attrs["Number Lon Points"]
    dlat = dset.attrs["Latitude Spacing"]
    dlon = dset.attrs["Longitude Spacing"]
    lat = np.arange(llcrnr_lat, llcrnr_lat + nlat * dlat, dlat)
    lon = np.arange(llcrnr_lon, llcrnr_lon + nlon * dlon, dlon)
    lon = np.array([x - 360 if x > 180 else x for x in lon])
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
    Returns 1 where values are greather than threshold

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
                warn = "WARNING: hysplit.add_species function"
                warn += ": species not found" + str(val) + "\n"
                warn += " valid species ids are " + str.join(", ", splist)
                print(warn)
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
    total_par = total_par.assign_attrs(atthash)
    return total_par


def calculate_thickness(cdump):
    alts = cdump.z.values
    thash = {}
    aaa = 0
    for avalue in alts:
        thash[avalue] = avalue - aaa
        aaa = avalue
    print(f"WARNING: thickness calculated from z values please verify {thash}")
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
    if cstr not in cdump.attrs.keys():
        print(f"warning: {cstr} attribute needed to calculate level thicknesses")
        print("warning: alternative calcuation from z dimension values")
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
