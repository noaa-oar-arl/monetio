"""
HYPSLIT MODEL READER for binary concentration (cdump) output files.
This code produces xarray datasets which are CF compliant and is a major update to the
code found in hysplit.py

This code developed at the NOAA Air Resources Laboratory.
Alice Crawford
Allison Ring

-------------
MAIN Functions:
-------------
1. `open_dataset` - Opens and processes a HYSPLIT cdump file to produce CF compliant xarray dataset.
2. `combine_dataset` - Combines multiple HYSPLIT dataset
3. `calc_massload` - Calculates column mass loading. Replaces hysp_massload.

-------------
Helper Functions:
-------------
2. `check_drange` - Checks if dates are within a specified range
4. `reduce_dims` - Reduces dimensions of boundary variables
5. `add_massunit` - Adds mass unit to species in dataset
6. `get_time_index` - Creates time indices based on reference time
7. `reset_latlon_coords` - Resets latitude and longitude coordinates
8. `fix_grid_continuity` - Fixes discontinuous grid points
9. `check_grid_continuity` - Checks if grid has evenly spaced points
10. `get_latlongrid` - Gets latitude and longitude grid from attributes
11. `getlatlon` - Returns 1D arrays of lats and lons
16. `add_species` - Adds data variables for each species
22. `check_attributes` - Ensures attributes are in proper format
23. `sum_datavars` - Sums data variables with matching coordinates
24. `calc_thickness` - Calculates thickness from z_bounds
25. `calc_massload` - Calculates column mass loading


--------
Classes
--------
ModelBin : Represents a binary cdump file
CombineOjbect : Helper class for combine_dataset function

Change log

2021 13 May  AMC  get_latlongrid needed to be updated to match makegrid method.
2022 14 Nov  AMC  initialized self.dset in __init__() in ModelBin class
2022 14 Nov  AMC  modified fix_grid_continuity to not fail if passed empty Dataset.
2022 02 Dec  AMC  modified get_latlongrid inputs. do not need to input dataset, just dictionary.
2022 02 Dec  AMC  replaced makegrid method with getlatlongrid function to reduce duplicate code.
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
2025 24 Mar  AMC  modified parse_hdata8 so it can use end time, start time, or middle of time as time stamp
2025 24 Mar  AMC  added sum_datavars function to improve add_species
2025 24 Mar  AMC  corrected combine_dataset to correctly add ens as a coordinate
2025 07 May  AMC  modifications to produce and use CF compliant netcdf files.

"""

import datetime
import sys
import warnings

import numpy as np
import pandas as pd
import xarray as xr
from hysplit2cf_helper import (
    add_title_history,
    calculate_bounds,
    make_coordinates_cf_compliant,
    make_z_coordinate_cf_compliant,
    time2cf,
)

# Suppress specific UserWarning about datetime precision
warnings.filterwarnings(
    "ignore",
    message="Converting non-nanosecond precision datetime values to nanosecond precision.",
)


def open_dataset(
    fname, drange=None, century=None, verbose=False, massunit="1", pollutant="pollutant"
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
        sample_time_stamp="mid",
        massunit=massunit,
        pollutant_desc=pollutant,
    )
    if binfile.dataflag:
        dset = binfile.dset
        rval = make_coordinates_cf_compliant(dset)
        rval = calculate_bounds(rval)
        rval = make_z_coordinate_cf_compliant(rval)
        rval = time2cf(rval)
        rval = add_title_history(rval)
        return rval
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
        massunit="1",  # Add massunit parameter
        pollutant_desc="pollutant",
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
        # Update attribute names to be CF compliant
        self.atthash["source_latitudes"] = []  # Changed from Starting_Latitudes
        self.atthash["source_longitudes"] = []  # Changed from Starting_Longitudes
        self.atthash["source_heights"] = []  # Changed from Starting_Heights
        self.atthash["source_dates"] = []  # Changed from Source_Date
        self.sample_time_stamp = sample_time_stamp
        self.massunit = massunit  # Store massunit attribute
        self.pollutant = pollutant_desc
        self.gridhash = {}
        # self.llcrnr_lon = None
        # self.llcrnr_lat = None
        # self.nlat = None
        # self.nlon = None
        # self.dlat = None
        # self.dlon = None
        self.levels = None
        self.dset = xr.Dataset()
        self.time_bounds_data = None  # Add new instance variable

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
        self.atthash["meteorological_model"] = hdata1["model_id"][0].decode(
            "UTF-8"
        )  # Changed from Meteorological Model ID
        self.atthash["source_count"] = nstartloc  # Changed from Number Start Locations
        return nstartloc

    def parse_hdata2(self, hdata2, nstartloc, century):
        # Loop through starting locations
        for nnn in range(0, nstartloc):
            # create list of starting latitudes, longitudes and heights.
            lat = hdata2["s_lat"][nnn]
            lon = hdata2["s_lon"][nnn]
            hgt = hdata2["s_ht"][nnn]

            self.atthash["source_latitudes"].append(lat)
            self.atthash["source_longitudes"].append(lon)
            self.atthash["source_heights"].append(hgt)

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

            self.atthash["source_dates"].append(sourcedate.strftime("%Y%m%d.%H%M%S"))

        return century

    def parse_hdata3(self, hdata3):
        # Description of concentration grid
        ahash = {}
        ahash["latitude_point_count"] = hdata3["nlat"][0]
        ahash["longitude_point_count"] = hdata3["nlon"][0]
        ahash["latitude_spacing"] = hdata3["dlat"][0]
        ahash["longitude_spacing"] = hdata3["dlon"][0]
        ahash["latitude_min"] = hdata3["llcrnr_lat"][0]
        ahash["longitude_min"] = hdata3["llcrnr_lon"][0]
        return ahash

    def parse_hdata4(self, hdata4a, hdata4b):
        self.levels = hdata4b["levht"]
        self.atthash["level_count"] = hdata4a["nlev"][0]
        self.atthash["level_heights"] = hdata4b["levht"]

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
        self.atthash["sampling_period_hours"] = sample_dt  # Changed from sample time hours
        # if self.sample_time_stamp == "end":
        #    self.atthash["time_bounds"] = "end"  # Changed from time description
        # else:
        #    self.atthash["time_bounds"] = "start"
        return True, pdate1, pdate2

    @staticmethod
    def parse_hdata8(hdata8a, hdata8b, pdate1, pdate2, time_stamp, massunit):
        """
        @brief Parse concentration data record
        @param hdata8a Header data for record
        @param hdata8b Concentration data
        @param pdate1 Datetime for record
        @return DataFrame with parsed concentration data
        @details Handles byteswapping and datetime precision conversion
        """
        lev_name = hdata8a["lev"][0]
        col_name = hdata8a["poll"][0].decode("UTF-8")

        # Handle endianness
        edata = hdata8b.byteswap()
        edata = edata.view(edata.dtype.newbyteorder("little"))

        # Create initial DataFrame
        concframe = pd.DataFrame.from_records(edata)
        concframe["levels"] = lev_name

        # Convert datetime to nanosecond precision explicitly
        if time_stamp == "start":
            time_ns = pd.Timestamp(pdate1).asm8  # Convert to numpy.datetime64[ns]
        elif time_stamp == "end":
            time_ns = pd.Timestamp(pdate2).asm8  # Convert to numpy.datetime64[ns]
        elif time_stamp == "mid":
            time_ns = pd.Timestamp(
                pdate1 + (pdate2 - pdate1) / 2
            ).asm8  # Convert to numpy.datetime64[ns]
        concframe["time"] = time_ns

        # Create time bounds for each row
        bound1 = pd.Timestamp(pdate1).strftime("%Y-%m-%dT%H:%M:%SZ")
        bound2 = pd.Timestamp(pdate2).strftime("%Y-%m-%dT%H:%M:%SZ")
        concframe["time_bounds"] = [[bound1, bound2] for _ in range(len(concframe))]

        # Rename columns
        names = concframe.columns.values
        names = ["y" if x == "jndx" else x for x in names]
        names = ["x" if x == "indx" else x for x in names]
        names = ["z" if x == "levels" else x for x in names]
        names = [col_name if x == "conc" else x for x in names]
        concframe.columns = names

        ## Add concentration unit information
        # if 'conc' in edata.dtype.names:
        #    concframe[col_name].attrs['units'] = massunit

        return concframe

    def add_time_bounds(self, time_bounds):
        """@brief Add time bounds as coordinate to dataset
        @param time_bounds: DataFrame with time and time_bounds columns
        @details Creates time_bounds coordinate from the stored bounds data
        """
        if time_bounds is None or len(time_bounds) == 0:
            return

        # Get unique time bounds for each time
        unique_bounds = time_bounds.groupby("time")["time_bounds"].first()

        # Convert to array with bnds dimension
        bounds_array = np.zeros((len(unique_bounds), 2), dtype="datetime64[ns]")
        for i, (_, bounds) in enumerate(unique_bounds.items()):
            # Remove timezone info from string before converting to datetime64
            bounds_array[i, 0] = pd.Timestamp(bounds[0].replace("Z", "")).asm8
            bounds_array[i, 1] = pd.Timestamp(bounds[1].replace("Z", "")).asm8

        # Add bounds dimension if not present
        if "bnds" not in self.dset.dims:
            self.dset["bnds"] = np.array([0, 1])

        # Add time bounds coordinate
        self.dset["time_bounds"] = xr.DataArray(
            bounds_array,
            dims=["time", "bnds"],
            coords={"time": self.dset.time, "bnds": self.dset.bnds},
            attrs={"long_name": "start and end times of sampling period", "units": "UTC"},
        )

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
        self.atthash["number_of_species"] = hdata5a["pollnum"][0]  # Changed from Number of Species
        self.atthash["Species_ID"] = []

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
            for _ in range(self.atthash["number_of_species"]):  # Use new attribute name
                # LOOP to go through each level
                concframes = []
                for _ in range(self.atthash["level_count"]):
                    # record 8a has the number of elements (ne). If number of
                    # elements greater than 0 than there are concentrations.
                    hdata8a = np.fromfile(fid, dtype=rec8a, count=1)
                    # self.atthash["Species ID"].append(
                    #    hdata8a["poll"][0].decode("UTF-8")
                    # )
                    # if number of elements is nonzero then
                    if hdata8a["ne"] >= 1:
                        self.atthash["Species_ID"].append(hdata8a["poll"][0].decode("UTF-8"))
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
                        concframe = self.parse_hdata8(
                            hdata8a, hdata8b, pdate1, pdate2, self.sample_time_stamp, self.massunit
                        )

                        # Split out time_bounds into separate frame and store in instance
                        if "time_bounds" in concframe:
                            time_bounds = concframe[["time", "time_bounds"]]
                            self.time_bounds_data = (
                                pd.concat([self.time_bounds_data, time_bounds])
                                if self.time_bounds_data is not None
                                else time_bounds
                            )
                            concframe = concframe.drop("time_bounds", axis=1)

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
                    # varname = list(dset.data_vars.keys())
                    # varname = varname[0]
                    # dra = fix_grid_continuity(dset[varname],verbose=verbose)
                    # dset = dra.to_dataset(name=varname)
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
        self.atthash["Species_ID"] = list(set(self.atthash["Species_ID"]))
        if self.sample_time_stamp == "start":
            self.atthash["Coordinate time description"] = "Beginning of sampling time"
        elif self.sample_time_stamp == "end":
            self.atthash["Coordinate time description"] = "End of sampling time"
        elif self.sample_time_stamp == "mid":
            self.atthash["Coordinate time description"] = "Middle of sampling time"

        Ns = range(self.atthash["number_of_species"])  # Use new attribute name
        # Grab per species all relevant datasets in the time list
        dsets = [[ll[n] for ll in timedslist if ll[n] is not None] for n in Ns]
        dsets = [
            xr.concat(ds, dim="time") for ds in dsets if len(ds) > 0
        ]  # note to copilot - do not change this line!
        if len(dsets) == 0:
            return False
        self.dset = xr.merge(dsets)
        if self.dset.variables:
            self.atthash = check_attributes(self.atthash)
            self.dset.attrs = self.atthash
            self.dset = fix_grid_continuity(self.dset, verbose=verbose)
            # Add time bounds before setting up coordinates
            if self.time_bounds_data is not None:
                # print('Adding time bounds')
                self.add_time_bounds(self.time_bounds_data)

            # Continue with existing coordinate setup
            # self.atthash = check_attributes(self.atthash)
            # self.dset.attrs = self.atthash
            # mgrid = self.makegrid(self.dset.coords["x"], self.dset.coords["y"])
            mgrid = get_latlongrid(self.gridhash, self.dset.coords["x"], self.dset.coords["y"])
            self.dset = self.dset.assign_coords(longitude=(("x"), mgrid[0]))
            self.dset = self.dset.assign_coords(latitude=(("y"), mgrid[1]))

            self.dset = self.dset.reset_coords()
            self.dset = self.dset.set_coords(["time", "latitude", "longitude"])
        if iii == 0 and verbose:
            print("ModelBin class _readfile method: no data in the date range found")
            return False
        # Only apply attributes to data variables, not coordinates
        for var in self.dset.data_vars:
            self.dset[var].attrs["units"] = f"{self.massunit} m-3"
            self.dset[var].attrs["standard_name"] = f"mass_concentration_of_{self.pollutant}_in_air"
        return True


class CombineObject:
    """
    Helper class for combine_dataset function.
    """

    def __init__(
        self, blist: tuple, drange=None, century=None, massunit="1", pollutant="pollutant"
    ):
        self.fname = blist[0]
        self.source = blist[1]
        self.ens = blist[2]
        self.hxr = self.open(self.fname, drange, century, massunit, pollutant)
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
        self.xrash = xrash
        self.attrs = self.xrash.attrs

    @staticmethod
    def open(fname, drange, century, massunit, pollutant, verbose=False):
        if drange:
            century = int(drange[0].year / 100) * 100
            hxr = open_dataset(
                fname,
                drange=drange,
                century=century,
                verbose=verbose,
                massunit=massunit,
                pollutant=pollutant
                # sample_time_stamp=sample_time_stamp,
                # check_grid=False,
                # cf_compliant=True
            )
        else:  # use all dates
            hxr = open_dataset(
                fname,
                century=century,
                verbose=verbose,
                massunit=massunit,
                pollutant=pollutant
                # sample_time_stamp=sample_time_stamp,
                # check_grid=False,
                # cf_compliant=True
            )
        return hxr


def combine_dataset(
    blist,
    drange=None,
    species=None,
    century=None,
    verbose=False,
    massunit="1",
    pollutant="pollutant",
    # sample_time_stamp="start",
    # check_grid=True,
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
    # Convert times to nanosecond precision before combining

    # def convert_time_precision(ds):
    #    if "time" in ds.coords:
    #        times = pd.to_datetime(ds.time.values).to_numpy(dtype="datetime64[ns]")
    #        ds = ds.assign_coords(time=times)
    #    return ds

    # Create list of datasets to combine
    xlist = []
    for bbb in blist:
        cobject = CombineObject(bbb, drange, century, massunit=massunit, pollutant=pollutant)
        if not cobject.empty:
            # Convert time precision when loading
            # cobject.hxr = convert_time_precision(cobject.hxr)
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
    sourcelist = list({x.source for x in xlist})
    outlist = []
    for source in sourcelist:
        # get all objects with that source
        elist = [x for x in xlist if x.source == source]
        inlist = []
        for eee in elist:
            aaa, junk = xr.align(eee.xrash, xbig, join="outer")
            aaa = aaa.fillna(0)
            # Create proper ensemble coordinate
            aaa = aaa.expand_dims("ens")
            aaa = aaa.assign_coords({"ens": [eee.ens]})
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
    newhxr = reduce_dims(newhxr)
    # change time coordinate back to datetime
    # rval = fix_grid_continuity(newhxr,verbose=verbose)
    # rval = add_massunit(rval,unit,pollutant)
    return newhxr


def reduce_dims(dset):
    """@brief Reduce dimensions of boundary variables to their core dimensions.
    @param dset xarray.Dataset Dataset with boundary variables to simplify
    @return xarray.Dataset Dataset with simplified boundary variables
    @details For variables like latitude_bounds, longitude_bounds, etc., reduces dimensions
             from complex (e.g., source, ens, y, bnds) to just core dims (e.g., y, bnds).
             Handles all standard boundary variables: latitude_bounds, longitude_bounds,
             time_bounds, and z_bounds.
    """
    # Dictionary mapping boundary variables to their core dimensions
    bounds_core_dims = {
        "latitude_bounds": ("y", "bnds"),
        "longitude_bounds": ("x", "bnds"),
        "time_bounds": ("time", "bnds"),
        "z_bounds": ("z", "bnds"),
    }

    # Copy the dataset to avoid modifying the original
    new_dset = dset.copy()

    # Process each boundary variable if it exists
    for var_name, core_dims in bounds_core_dims.items():
        if var_name in new_dset.data_vars:
            var = new_dset[var_name]

            # Check if the variable has extra dimensions
            current_dims = var.dims
            if set(current_dims) != set(core_dims) and all(
                dim in current_dims for dim in core_dims
            ):
                # Variable has extra dimensions, need to simplify

                # Get the coordinate values for the core dimensions
                core_coords = {dim: new_dset[dim] for dim in core_dims if dim in new_dset.coords}

                # For boundary variables that span extra dimensions (like ens, source),
                # we'll take the first element along those dimensions
                # Create indexing dictionary for selecting the first element of non-core dims
                idx = {dim: 0 for dim in current_dims if dim not in core_dims}

                # Extract the values using the indexing
                if idx:
                    reduced_values = var.isel(**idx).values
                else:
                    reduced_values = var.values

                # Create new DataArray with only core dimensions
                attrs = var.attrs.copy()
                new_dset[var_name] = xr.DataArray(
                    reduced_values, dims=core_dims, coords=core_coords, attrs=attrs
                )

                # Update the bounds attribute on the corresponding coordinate if needed
                if core_dims[0] in new_dset.coords:
                    coord = new_dset[core_dims[0]]
                    # if 'bounds' not in coord.attrs or coord.attrs['bounds'] != var_name:
                    #    coord.attrs['bounds'] = var_name

    return new_dset


def add_massunit(dset, massunit, pollutant="pollutant"):
    """
    Adds mass unit to each species in the dataset.
    """
    for var in dset.variables:
        if set(dset[var].dims).issuperset({"x", "y", "z"}):
            dset[var].attrs["units"] = f"{massunit} m-3"
            dset[var].attrs["standard_name"] = f"mass_concentration_of_{pollutant}_in_air"
            # print('adding attributes {}'.format(var))

    return dset


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


def reset_latlon_coords(hxr):
    """
    hxr : xarray DataSet as output from open_dataset or combine_dataset
    """
    mgrid = get_latlongrid(hxr.attrs, hxr.x.values, hxr.y.values)
    lon_attrs = {}
    lat_attrs = {}
    if "latitude" in hxr.coords:
        lat_attrs = hxr.latitude.attrs
        hxr = hxr.drop("latitude")
    if "longitude" in hxr.coords:
        lon_attrs = hxr.longitude.attrs
        hxr = hxr.drop("longitude")
    hxr = hxr.assign_coords(latitude=("y", mgrid[1]))
    hxr = hxr.assign_coords(longitude=("x", mgrid[0]))
    hxr.latitude.attrs = lat_attrs
    hxr.longitude.attrs = lon_attrs
    return hxr


def fix_grid_continuity(dset, verbose=False):
    """@brief Fix discontinuous grid points by filling missing values
    @param dset xarray Dataset to fix
    @return xarray Dataset with continuous grid
    """
    # Check if dataset is empty
    if dset is None or len(dset.data_vars) == 0:
        return dset

    # Check if grid already continuous
    if check_grid_continuity(dset):
        return dset
    if verbose:
        print("Grid is not continuous, attempting to fix...")

    # Get grid indices
    xvv = dset.x.values
    yvv = dset.y.values

    xlim = [xvv[0], xvv[-1]]
    ylim = [yvv[0], yvv[-1]]

    # Create continuous index arrays
    xindx = np.arange(xlim[0], xlim[1] + 1)
    yindx = np.arange(ylim[0], ylim[1] + 1)
    try:
        dset = dset.reindex(x=xindx, y=yindx, method=None, fill_value=0)
    except ValueError as e:
        warnings.warn(f"Failed to reindex dataset: {e}")
    return dset


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
    # mgrid = np.meshgrid(lonlist, latlist)
    return lonlist, latlist


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
    llcrnr_lat = attrs["latitude_min"]
    llcrnr_lon = attrs["longitude_min"]
    nlat = attrs["latitude_point_count"]
    nlon = attrs["longitude_point_count"]
    dlat = attrs["latitude_spacing"]
    dlon = attrs["longitude_spacing"]

    lastlon = llcrnr_lon + (nlon - 1) * dlon
    lastlat = llcrnr_lat + (nlat - 1) * dlat
    # = int((lastlon - llcrnr_lon) / dlon)
    lat = np.linspace(llcrnr_lat, lastlat, num=int(nlat))
    lon = np.linspace(llcrnr_lon, lastlon, num=int(nlon))
    #
    lon = np.array([x - 360 if x >= 180 + lon_tolerance else x for x in lon])
    return lat, lon


def add_species(dset, species=None):
    """
    @brief Add datavariables representing each species in the species list.
    @param dset : xarray dataset
    @param speices : list of Species ID's which are names of data varialbes in dset.
              if none then all ids in the "species ID" attribute will be used.
              if 'Species_ID' is not in the attributes then all variables in the
                dataset will be used.
    @return dset : xarray dataset with added data variables.
    """
    if not species:
        if "Species_ID" in dset.attrs.keys():
            species = dset.attrs["Species_ID"]

    # Sum the variables
    dset = sum_datavars(dset.copy(), varlist=species)

    splist = [x for x in species if x in dset.variables]

    # drop data variables for individual species and return only the sum.
    returnset = dset.drop_vars(splist)

    return returnset


def check_attributes(atthash):
    # when writing to netcdf file, attributes which are numpy arrays do not write properly.
    # need to change them to lists.
    for key in atthash.keys():
        val = atthash[key]
        if isinstance(val, np.ndarray):
            newval = list(val)
            atthash[key] = newval
    return atthash


def sum_datavars(dset, varlist=None, newname="SUM"):
    """@brief Sum data variables that share time,z,y,x coordinates.
    @param dset xarray.Dataset Dataset containing variables to sum
    @param varlist list: List of variable names to sum. If None, sums all variables with matching coords
    @param newname str: Name for the summed variable (default: 'SUM')
    @return xarray.Dataset Dataset with new summed variable
    """
    # Required coordinates
    req_coords = {"time", "z", "y", "x"}

    # If varlist provided, verify variables exist
    if varlist is not None:
        missing_vars = [var for var in varlist if var not in dset.data_vars]
        if missing_vars:
            warnings.warn(f"Requested variables not found in dataset: {missing_vars}")
            # Filter varlist to only existing variables
            varlist = [var for var in varlist if var in dset.data_vars]
            if not varlist:
                warnings.warn("No requested variables found in dataset")
                return dset

    # Find variables with matching coordinates
    matching_vars = []
    for var in dset.data_vars:
        var_coords = set(dset[var].coords)
        if req_coords.issubset(var_coords):
            if varlist is None or var in varlist:
                matching_vars.append(var)

    if not matching_vars:
        warnings.warn("No variables found with required coordinates (time,z,y,x)")
        return dset

    # Check units and standard_names across variables
    units = None
    pollutants = []
    for var in matching_vars:
        # Check units
        if "units" in dset[var].attrs:
            var_units = dset[var].attrs["units"]
            if units is None:
                units = var_units
            elif var_units != units:
                warnings.warn(f"Mismatched units found: {var} has {var_units}, expected {units}")
                return dset

        # Extract pollutant name from standard_name if it exists
        if "standard_name" in dset[var].attrs:
            std_name = dset[var].attrs["standard_name"]
            if std_name.startswith("mass_concentration_of_") and std_name.endswith("_in_air"):
                # Extract pollutant name from between prefix and suffix
                pollutant = std_name[len("mass_concentration_of_") : -len("_in_air")]
                if pollutant not in pollutants:
                    pollutants.append(pollutant)

    # Sum the matching variables
    total = dset[matching_vars[0]].copy()
    for var in matching_vars[1:]:
        total = total + dset[var]

    # Create new dataset with sum
    dset[newname] = total

    # Add attributes to new variable
    attrs = {
        "long_name": f'Sum of variables: {", ".join(matching_vars)}',
        "constituent_variables": matching_vars,
    }

    # Add units if they were found
    if units is not None:
        attrs["units"] = units

    # Create combined standard_name if pollutants were found
    if pollutants:
        combined_pollutant = "_".join(pollutants)
        attrs["standard_name"] = f"mass_concentration_of_{combined_pollutant}_in_air"

    dset[newname].attrs.update(attrs)
    return dset


def calc_thickness(dset):
    """@brief Calculate thickness of each z-level from z_bounds
    @param dset xarray.Dataset Dataset with z_bounds coordinate
    @return xarray.DataArray Layer thickness values
    """
    if "z_bounds" not in dset:
        raise ValueError("Dataset missing z_bounds coordinate")

    # Calculate thickness as difference between upper and lower bounds
    thickness = dset.z_bounds.isel(bnds=1) - dset.z_bounds.isel(bnds=0)

    # Add attributes
    thickness.attrs.update(
        {
            "units": "m",
            "long_name": "thickness of vertical layer",
            "standard_name": "layer_thickness",
        }
    )

    return thickness


def calc_massload(dset, species=None, varname=None):
    """@brief Calculate column mass loading by summing mass in each layer over height
    @param dset xarray.Dataset HYSPLIT dataset with z_bounds
    @param species list: Optional list of species to include
    @return xarray.Dataset Dataset with added column_mass_loading variable
    """
    # Get summed concentration for specified species
    dset = dset.copy()
    if isinstance(species, (list, np.ndarray)) and len(species) > 1:
        conc = sum_datavars(dset, varlist=species)
        varname = "SUM"
    elif not species:
        conc = add_species(dset, species=species)
        varname = "SUM"
    else:
        conc = dset
        if not varname:
            if "SUM" in dset.data_vars:
                varname = "SUM"

    if varname not in dset.data_vars:
        raise ValueError(f"Variable '{varname}' not found in dataset")

    # Calculate layer thickness
    thickness = calc_thickness(dset)
    # Multiply concentration by thickness to get mass in each layer
    layer_mass = conc[varname] * thickness

    # Sum over z dimension to get column mass loading
    column_mass = layer_mass.sum(dim="z")

    # Add as new variable with attributes
    dset["column_mass_loading"] = column_mass
    units = conc[varname].attrs.get("units", "1 m-3")
    units = units.replace("m-3", "m-2")
    dset.column_mass_loading.attrs.update(
        {
            "units": units,
            "long_name": "column integrated mass loading",
            "standard_name": "atmosphere_mass_content_of_air",
            "coordinates": "time latitude longitude",
        }
    )

    # Drop z-related coordinates and variables
    dset = dset.drop_vars([varname, "z", "z_bounds"], errors="ignore")
    dset = dset.drop_dims("z", errors="ignore")

    return dset
