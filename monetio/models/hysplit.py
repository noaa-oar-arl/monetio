""" HYPSLIT MODEL READER """
import sys
import datetime
import pandas as pd
import xarray as xr
import numpy as np
from numpy import fromfile, arange

"""
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

"""


# def _hysplit_latlon_grid_from_dataset(ds):
#    pargs = dict()
#    pargs["lat_0"] = ds.latitude.mean()
#    pargs["lon_0"] = ds.longitude.mean()
#
#    p4 = (
#        "+proj=eqc +lat_ts={lat_0} +lat_0={lat_0} +lon_0={lon_0} "
#        "+ellps=WGS84 +datum=WGS84 +units=m +no_defs".format(**pargs)
#    )
#    return p4


# def get_hysplit_latlon_pyresample_area_def(ds, proj4_srs):
#    from pyresample import geometry
#
#    return geometry.SwathDefinition(lons=ds.longitude.values, lats=ds.latitude.values)


def open_dataset(fname, drange=None, verbose=False):
    """Short summary.

    Parameters
    ----------
    fname : string
        Name of "cdump" file. Binary HYSPLIT concentration output file.

    drange : list of two datetime objects
        cdump file contains concentration as function of time. The drange
        specifies what times should be loaded from the file. A value of None
        will result in all times being loaded.

    verbose : boolean
        If True will print out extra messages

    addgrid : boolean
        assigns an area attribute to each variable

    Returns
    -------
    dset : xarray DataSet

    CHANGES for PYTHON 3
    For python 3 the numpy char4 are read in as a numpy.bytes_ class and need to
    be converted to a python
    string by using decode('UTF-8').
    """
    # open the dataset using xarray
    binfile = ModelBin(fname, drange=drange, verbose=verbose, readwrite="r")
    dset = binfile.dset
    # return dset
    # get the grid information
    # May not need the proj4 definitions now that lat lon defined properly.
    # if addarea:
    #    p4 = _hysplit_latlon_grid_from_dataset(dset)
    #    swath = get_hysplit_latlon_pyresample_area_def(dset, p4)
    # now assign this to the dataset and each dataarray
    #    dset = dset.assign_attrs({"proj4_srs": p4})
    #    for iii in dset.variables:
    #        dset[iii] = dset[iii].assign_attrs({"proj4_srs": p4})
    #        for jjj in dset[iii].attrs:
    #            dset[iii].attrs[jjj] = dset[iii].attrs[jjj].strip()
    #        dset[iii] = dset[iii].assign_attrs({"area": swath})
    #    dset = dset.assign_attrs(area=swath)
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
        self, filename, drange=None, century=None, verbose=True, readwrite="r"
    ):
        """
        drange :  list of two datetime objects.
        The read method will store data from the cdump file for which the
        sample start is greater thand drange[0] and less than drange[1]
        for which the sample stop is less than drange[1].

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
        self.sourcedate = []
        self.slat = []
        self.slon = []
        self.sht = []
        self.atthash = {}
        self.atthash["Starting Locations"] = []
        self.atthash["Source Date"] = []
        self.llcrnr_lon = None
        self.llcrnr_lat = None
        self.nlat = None
        self.nlon = None
        self.dlat = None
        self.dlon = None
        self.levels = None

        if readwrite == "r":
            self.dataflag = self.readfile(
                filename, drange, verbose=verbose, century=century
            )

    @staticmethod
    def define_struct():
        """Each record in the fortran binary begins and ends with 4 bytes which
        specify the length of the record. These bytes are called pad below.
        They are not used here, but are thrown out. The following block defines
        a numpy dtype object for each record in the binary file. """
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
            print(
                "WARNING in ModelBin _readfile - number of starting locations "
                "incorrect"
            )
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
            self.slat.append(hdata2["s_lat"][nnn])
            self.slon.append(hdata2["s_lon"][nnn])
            self.sht.append(hdata2["s_ht"][nnn])
            self.atthash["Starting Locations"].append(
                (hdata2["s_lat"][nnn], hdata2["s_lon"][nnn])
            )

            # try to guess century if century not given
            if century is None:
                if hdata2["r_year"][0] < 50:
                    century = 2000
                else:
                    century = 1900
                print(
                    "WARNING: Guessing Century for HYSPLIT concentration file", century
                )
            # add sourcedate which is datetime.datetime object
            sourcedate = datetime.datetime(
                century + hdata2["r_year"][nnn],
                hdata2["r_month"][nnn],
                hdata2["r_day"][nnn],
                hdata2["r_hr"][nnn],
                hdata2["r_min"][nnn],
            )
            self.sourcedate.append(sourcedate)
            self.atthash["Source Date"].append(sourcedate)
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
            century + hdata6["oyear"], hdata6["omonth"], hdata6["oday"], hdata6["ohr"]
        )
        pdate2 = datetime.datetime(
            century + hdata7["oyear"], hdata7["omonth"], hdata7["oday"], hdata7["ohr"]
        )
        dt = pdate2 - pdate1
        sample_dt = dt.days * 24 + dt.seconds / 3600.0
        self.atthash["Sampling Time"] = pdate2 - pdate1
        self.atthash["sample time hours"] = sample_dt
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
            # ['time', 'levels', 'longitude', 'latitude'],
            # ['time', 'levels', 'longitude', 'latitude','x','y'],
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
        lat = arange(
            self.llcrnr_lat, self.llcrnr_lat + self.nlat * self.dlat, self.dlat
        )
        lon = arange(
            self.llcrnr_lon, self.llcrnr_lon + self.nlon * self.dlon, self.dlon
        )
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
        self.dset = None
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
        hdata1 = fromfile(fid, dtype=rec1, count=1)
        nstartloc = self.parse_header(hdata1)

        hdata2 = fromfile(fid, dtype=rec2, count=nstartloc)
        century = self.parse_hdata2(hdata2, nstartloc, century)

        hdata3 = fromfile(fid, dtype=rec3, count=1)
        ahash = self.parse_hdata3(hdata3, ahash)

        # read record 4 which gives information about vertical levels.
        hdata4a = fromfile(fid, dtype=rec4a, count=1)  # gets nmber of levels
        hdata4b = fromfile(
            fid, dtype=rec4b, count=hdata4a["nlev"][0]
        )  # reads levels, count is number of levels.
        self.parse_hdata4(hdata4a, hdata4b)

        # read record 5 which gives information about pollutants / species.
        hdata5a = fromfile(fid, dtype=rec5a, count=1)
        fromfile(fid, dtype=rec5b, count=hdata5a["pollnum"][0])
        fromfile(fid, dtype=rec5c, count=1)
        self.atthash["Number of Species"] = hdata5a["pollnum"][0]

        # Loop to reads records 6-8. Number of loops is equal to number of
        # output times.
        # Only save data for output times within drange. if drange=[] then
        # save all.
        # Loop to go through each sampling time
        ii = 0  # check to make sure don't go above max number of iterations
        iii = 0  # checks to see if some nonzero data was saved in xarray
        # Safety valve - will not allow more than 1000 loops to be executed.
        imax = 1e3
        testf = True
        while testf:
            hdata6 = fromfile(fid, dtype=rec6, count=1)
            hdata7 = fromfile(fid, dtype=rec6, count=1)
            check, pdate1, pdate2 = self.parse_hdata6and7(hdata6, hdata7, century)
            if not check:
                print(check, pdate1, pdate2)
                break
            testf, savedata = check_drange(drange, pdate1, pdate2)
            print("sample time", pdate1, " to ", pdate2)
            # datelist = []
            self.atthash["Species ID"] = []
            inc_iii = False
            # LOOP to go through each level
            for lev in range(self.atthash["Number of Levels"]):
                # LOOP to go through each pollutant
                for pollutant in range(self.atthash["Number of Species"]):
                    # record 8a has the number of elements (ne). If number of
                    # elements greater than 0 than there are concentrations.
                    hdata8a = fromfile(fid, dtype=rec8a, count=1)
                    self.atthash["Species ID"].append(
                        hdata8a["poll"][0].decode("UTF-8")
                    )
                    # if number of elements is nonzero then
                    if hdata8a["ne"] >= 1:
                        # get rec8 - indx and jndx
                        hdata8b = fromfile(fid, dtype=rec8b, count=hdata8a["ne"][0])
                        # add sample start time to list of start times with
                        # non zero conc
                        self.nonzeroconcdates.append(pdate1)
                    else:
                        tempzeroconcdates.append(
                            pdate1
                        )  # or add sample start time to list of start times
                        # with zero conc.
                    # This is just padding.
                    fromfile(fid, dtype=rec8c, count=1)
                    # if savedata is set and nonzero concentrations then save
                    # the data in a pandas dataframe
                    if savedata and hdata8a["ne"] >= 1:
                        self.nonzeroconcdates.append(pdate1)
                        inc_iii = True
                        concframe = self.parse_hdata8(hdata8a, hdata8b, pdate1)
                        dset = xr.Dataset.from_dataframe(concframe)
                        if verbose:
                            print("Adding ", "Pollutant", pollutant, "Level", lev)
                        # if this is the first time through. create dataframe
                        # for first level and pollutant.
                        if self.dset is None:
                            self.dset = dset
                        else:  # create dataframe for level and pollutant and
                            # then merge with main dataframe.
                            # self.dset = xr.concat([self.dset, dset],'levels')
                            self.dset = xr.merge([self.dset, dset])
                        ii += 1
                # END LOOP to go through each pollutant
            # END LOOP to go through each level
            # safety check - will stop sampling time while loop if goes over
            #  imax iterations.
            if ii > imax:
                testf = False
            if inc_iii:
                iii += 1
        self.atthash["Concentration Grid"] = ahash
        print('KEYS', self.atthash.keys())
        self.atthash["Species ID"] = list(set(self.atthash["Species ID"]))
        self.atthash["Coordinate time description"] = "Beginning of sampling time"
        # END OF Loop to go through each sampling time
        if self.dset.variables:
            self.dset.attrs = self.atthash
            mgrid = self.makegrid(self.dset.coords["x"], self.dset.coords["y"])
            self.dset = self.dset.assign_coords(longitude=(("y", "x"), mgrid[0]))
            self.dset = self.dset.assign_coords(latitude=(("y", "x"), mgrid[1]))

            self.dset = self.dset.reset_coords()
            self.dset = self.dset.set_coords(["time", "latitude", "longitude"])
        if verbose:
            print(self.dset)
        if iii == 0:
            print(
                "Warning: ModelBin class _readfile method: no data in the date range found"
            )
            return False
        return True


# import datetime
# import os
# import sys
# import xarray as xr
# import numpy as np
# from monet.models import hysplit
# import monet.utilhysplit.hysp_func as hf
# from netCDF4 import Dataset
# import matplotlib.pyplot as plt

# combine_cdump creates a 6 dimensional xarray dataarray object from cdump files.
#

def combine_dataset(blist, drange=None, species=None, verbose=False):
    """
    Inputs :
      blist : list of tuples
      (filename, sourcetag, metdatatag)

    drange : list of two datetime objects.
     d1 datetime object. first date to keep in DatArrayarray
     d2 datetime object. last date to keep in DataArray

    RETURNS
     newhxr : an xarray data-array with 6 dimensions.
            lat, lon, time, level, ensemble tag, source tag

    Note that if more than one species is present in the files, they are
    added to get concentration from all species.
    """
    iii = 0
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

    mgrid = []
    # first loop go through to get expanded dataset.
    xlist = []
    sourcelist = []
    enslist = []
    for key in blist:
        fname = val[0]
        xsublist = []
        for fname in blist[key]:
            # print('ALIGNING', iii, fname, key)
            if drange:
                binfile = ModelBin(
                    fname[0], drange=drange, verbose=verbose, readwrite="r"
                )
                hxr = binfile.dset
                hxr = open_dataset(fname[0], drange=drange)
            else:  # use all dates
                print("open ", fname[0])
                binfile = ModelBin(fname[0], verbose=verbose, readwrite="r")
                hxr = binfile.dset
                # try:
                #    hxr = open_dataset(fname[0])
                # except:
                #    print('failed to open ', fname[0])
                #    sys.exit()
            mlat, mlon = getlatlon(hxr)
            if iii > 0:
                t1 = np.array_equal(mlat, mlat_p)
                t2 = np.array_equal(mlon, mlon_p)
                if not t1 or not t2:
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
                a, xnew = xr.align(xrash, xnew, join="outer")
                xnew = xnew.fillna(0)
            iii += 1
        sourcelist.append(key)
        xlist.append(xsublist)
    if verbose:
        print("aligned --------------------------------------")
    # xnew is now encompasses the area of all the data-arrays
    # now go through and expand each one to the size of xnew.
    iii = 0
    jjj = 0
    ylist = []
    slist = []
    for sublist in xlist:
        hlist = []
        for temp in sublist:
            # expand to same region as xnew
            aaa, bbb = xr.align(temp, xnew, join="outer")
            aaa = aaa.fillna(0)
            bbb = bbb.fillna(0)
            aaa.expand_dims("ens")
            aaa["ens"] = enslist[iii]
            iii += 1
            hlist.append(aaa)
        # concat along the 'ens' axis
        new = xr.concat(hlist, "ens")
        print("HERE NEW", new)
        ylist.append(new)
        slist.append(sourcelist[jjj])
        jjj += 1

    print('DTLIST', dtlist)
    dtlist = list(set(dtlist))
    print("DT", dtlist, dtlist[0])
    dt = dtlist[0]
    newhxr = xr.concat(ylist, "source")
    print("sourcelist", slist)
    newhxr["source"] = slist
    print("NEW NEW NEW", newhxr)
    # newhxr['ens'] = metlist

    # calculate the lat lon grid for the expanded dataset.
    # and use that for the new coordinates.
    mgrid = get_latlongrid(hxr, newhxr.x.values, newhxr.y.values)
    newhxr = newhxr.drop("longitude")
    newhxr = newhxr.drop("latitude")
    newhxr = newhxr.assign_coords(latitude=(("y", "x"), mgrid[1]))
    newhxr = newhxr.assign_coords(longitude=(("y", "x"), mgrid[0]))

    # newhxr is an xarray data-array with 6 dimensions.
    # dt is the averaging time of the hysplit output.
    newhxr = newhxr.assign_attrs({"sample time hours": dt})
    newhxr = newhxr.assign_attrs({"Species ID": list(set(splist))})
    return newhxr


def get_latlongrid(dset, xindx, yindx):
    """
    INPUTS
    dset : xarray data set from ModelBin class
    xindx : list of integers
    yindx : list of integers
    RETURNS
    mgrid : output of numpy meshgrid function.
            Two 2d arrays of latitude, longitude. 
    """
    llcrnr_lat = dset.attrs["Concentration Grid"]["llcrnr latitude"]
    llcrnr_lon = dset.attrs["Concentration Grid"]["llcrnr longitude"]
    nlat = dset.attrs["Concentration Grid"]["Number Lat Points"]
    nlon = dset.attrs["Concentration Grid"]["Number Lon Points"]
    dlat = dset.attrs["Concentration Grid"]["Latitude Spacing"]
    dlon = dset.attrs["Concentration Grid"]["Longitude Spacing"]

    lat = np.arange(llcrnr_lat, llcrnr_lat + nlat * dlat, dlat)
    lon = np.arange(llcrnr_lon, llcrnr_lon + nlon * dlon, dlon)
    print(nlat, nlon, dlat, dlon)
    print("lon shape", lon.shape)
    print("lat shape", lat.shape)
    print(lat)
    print(lon)
    lonlist = [lon[x - 1] for x in xindx]
    latlist = [lat[x - 1] for x in yindx]
    mgrid = np.meshgrid(lonlist, latlist)
    return mgrid


def getlatlon(dset):
    """
    Returns 1d array of lats and lons based on Concentration Grid
    Defined in the dset attribute.
    dset : xarray returned by hysplit.open_dataset function
    RETURNS
    lat : 1D array of latitudes
    lon : 1D array of longitudes
    """
    llcrnr_lat = dset.attrs["Concentration Grid"]["llcrnr latitude"]
    llcrnr_lon = dset.attrs["Concentration Grid"]["llcrnr longitude"]
    nlat = dset.attrs["Concentration Grid"]["Number Lat Points"]
    nlon = dset.attrs["Concentration Grid"]["Number Lon Points"]
    dlat = dset.attrs["Concentration Grid"]["Latitude Spacing"]
    dlon = dset.attrs["Concentration Grid"]["Longitude Spacing"]
    lat = np.arange(llcrnr_lat, llcrnr_lat + nlat * dlat, dlat)
    lon = np.arange(llcrnr_lon, llcrnr_lon + nlon * dlon, dlon)
    return lat, lon


def hysp_massload(dset, threshold=0, mult=1):
    """ Calculate mass loading from HYSPLIT xarray
    INPUTS
    dset: xarray dataset output by open_dataset OR
           xarray data array output by combine_dataset
    threshold : float
    mult : float
    Outputs: 
    totl_aml : xarray data array
    total ash mass loading (summed over all layers), ash mass loading
    Units in (unit mass / m^2)
    """
    aml_alts = calc_aml(dset)
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


def hysp_heights(dset, threshold, mult=1, height_mult=1 / 1000.0,
                 mass_load=True, species=None):
    """ Calculate ash top-height from HYSPLIT xarray
    Input: xarray dataset output by open_dataset OR
           xarray data array output by combine_dataset
    threshold : ash mass loading threshold (threshold = xx)
    mult : convert from meters to other unit. default is 1/1000.0 to
           convert to km.
    Outputs: ash top heights, altitude levels """

    # either get mass loading of each point
    if mass_load:
        aml_alts = calc_aml(dset)
    # or get concentration at each point
    else:
        aml_alts = add_species(dset)

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


def calc_aml(dset, species=None):
    """ Calculates the ash mass loading at each altitude for the dataset
    Input: xarray dataset output by open_dataset OR
           xarray data array output by combine_dataset
    Output: total ash mass loading """
    # Totals values for all particles
    if isinstance(dset, xr.core.dataset.Dataset):
        total_par = add_species(dset)
    else:
        total_par = dset.copy()
    # Multiplies the total particles by the altitude layer
    # to create a mass loading for each altitude layer
    aml_alts = _delta_multiply(total_par)
    return aml_alts


def hysp_thresh(dset, threshold, mult=1):
    """ Calculates a threshold mask array based on the
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
              warn = 'WARNING: hysplit.add_species function : species not found '
              warn += str(val) + '\n'
              warn += ' valid species ids are ' + str.join(', ', splist)
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
    total_par = total_par.assign_attrs({"Species ID": sflist})
    return total_par


def _delta_multiply(pars):
    """
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
    return newpar
