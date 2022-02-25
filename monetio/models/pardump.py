# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
"""
PGRMMR: Alice Crawford ORG: NOAA/ARL
PYTHON 3
ABSTRACT: classes and functions for reading and writing binary HYSPLIT
PARDUMP file

CLASSES
    Pardump - contains methods to write or read a binary pardump file

FUNCTIONS
    open_dataset()



"""
import datetime

import numpy as np
import pandas as pd


def open_dataset(fname, drange=None, century=2000, verbose=False):
    """
    fname : str
          full path to pardump file
    drange : list of two datetime objects.
              read in only particle positions between these two dates.
    century : int
              Only the last two digits of the year are stored in the pardump
              file. century must be specified (1900 or 2000) to read in the
              correct year.
    verbose : boolean
    """
    pdump = Pardump(fname)
    pardf = pdump.read(drange=drange, century=century, verbose=verbose)
    return pardf


class Pardump:
    """methods for writing and reading a pardump file.
    __init__  initializes structure of binary file.
    write   writes a pardump file.
    read    reads a pardump file. returns a  pandas DataFrame.
    """

    def __init__(self, fname="PARINIT"):
        """
        ##initializes structures which correspond to the binary records.
        ##'p' variables coorespond to padding that fortran adds.
        """
        self.fname = fname
        # self.dtfmt = "%Y%m%d%H%M"

        tp1 = ">f"  # big endian float.
        tp2 = ">i"  # big endian integer.

        # header record in fortran file.
        self.hdr_dt = np.dtype(
            [
                ("padding", tp2),
                ("parnum", tp2),
                ("pollnum", tp2),
                ("year", tp2),
                ("month", tp2),
                ("day", tp2),
                ("hour", tp2),
                ("minute", tp2),
            ]
        )

        # data record in fortran file.
        self.pardt = np.dtype(
            [
                ("p1", tp2),
                ("p2", tp2),
                ("pmass", tp1),
                ("p3", ">l"),
                ("lat", tp1),
                ("lon", tp1),
                ("ht", tp1),
                ("su", tp1),
                ("sv", tp1),
                ("sx", tp1),
                ("p4", ">l"),
                ("age", tp2),
                ("dist", tp2),
                ("poll", tp2),
                ("mgrid", tp2),
                ("sorti", tp2),
            ]
        )

    def write(self, numpar, pmass, lon, lat, ht, pollnum, sdate):
        """
        numpar : integer
                 number of particles
        pmass   : lists or numpy array
        lon     : list or numpy array
        lat     : list or numpy array
        ht      : list or numpy array
        pollnum : integer
                  pollutant index
        sdate   : datetime.datetime object
        """
        with open(self.fname, "wb") as fpoint:
            pad1 = np.ones(numpar) * 28
            pad2 = np.ones(numpar) * 4
            pad3 = np.ones(numpar) * 17179869208
            pad4 = np.ones(numpar) * 103079215124.0
            zrs = np.zeros(numpar)
            ones = np.ones(numpar)
            sorti = np.arange(1, numpar + 1)

            a = np.zeros((numpar,), dtype=self.pardt)
            a["p1"] = pad1
            a["p2"] = pad2
            a["p3"] = pad3
            a["p4"] = pad4

            a["lat"] = lat
            a["lon"] = lon
            a["ht"] = ht
            a["pmass"] = pmass
            a["poll"] = pollnum

            a["age"] = zrs
            a["dist"] = zrs

            a["mgrid"] = ones
            a["sorti"] = sorti

            # print a

            hdr = np.zeros((1,), dtype=self.hdr_dt)
            hdr["padding"] = 28
            hdr["parnum"] = numpar
            hdr["pollnum"] = 1
            hdr["year"] = sdate.year
            hdr["month"] = sdate.month
            hdr["day"] = sdate.day
            hdr["hour"] = sdate.hour
            hdr["minute"] = sdate.minute
            print(hdr)

            endrec = np.array([20], dtype=">i")

            fpoint.write(hdr)
            fpoint.write(a)
            fpoint.write(endrec)
            fpoint.write(endrec)

    # def writeascii(self, drange=[], verbose=1, century=2000, sorti=[]):
    #    read(self, drange=[], verbose=1, century=2000, sorti=[]):

    def read(self, drange=None, verbose=False, century=2000, sorti=None):
        """
        daterange should be a list of two datetime.datetime objects
        indicating the beginning
        and ending date of the particle positions of interest.
        Returns
           parframe_all : pandas DataFrame

        The key is the date of the particle positions in YYMMDDHH.
        The value is a pandas dataframe object with the particle information.
        sorti is a list of sort indices. If sorti not None then will
        only return particles
        with those sort indices.
        nsort keeps track of which particle it is throughout the time.
        Could use this to keep track of initial height and time of release.
        """

        imax = 100000
        # returns a dictionary of pandas dataframes. Date valid is the key.
        # pframe_hash = {}
        parframe_all = pd.DataFrame()
        with open(self.fname, "rb") as fpoint:
            iii = 0
            testf = True
            while testf:
                hdata = np.fromfile(fpoint, dtype=self.hdr_dt, count=1)
                if verbose:
                    print("Record Header ", hdata)
                if not hdata:
                    print("Done reading ", self.fname)
                    break
                if hdata["year"] < 1000:
                    year = hdata["year"] + century
                else:
                    year = hdata["year"]
                pdate = datetime.datetime(
                    int(year),
                    int(hdata["month"]),
                    int(hdata["day"]),
                    int(hdata["hour"]),
                    int(hdata["minute"]),
                )
                # if drange==[]:
                #   drange = [pdate, pdate]
                parnum = hdata["parnum"]
                data = np.fromfile(fpoint, dtype=self.pardt, count=parnum[0])
                # n = parnum - 1
                # padding at end of each record
                np.fromfile(fpoint, dtype=">i", count=1)
                if verbose:
                    print("Date ", pdate, " **** ", drange)

                testdate = False
                if not drange:
                    testdate = True
                elif pdate >= drange[0] and pdate <= drange[1]:
                    testdate = True

                # Only store data if it is in the daterange specified.
                if testdate:
                    # print("Adding data ", hdata, pdate)
                    # otherwise get endian error message when create dataframe.
                    ndata = data.byteswap().newbyteorder()
                    par_frame = pd.DataFrame.from_records(ndata)  # create data frame
                    # drop the fields which were padding
                    par_frame.drop(["p1", "p2", "p3", "p4"], inplace=True, axis=1)
                    par_frame.drop(
                        ["su", "sv", "sx", "mgrid"], inplace=True, axis=1
                    )  # drop other fields
                    # drop where the lat field is 0. because
                    par_frame = par_frame.loc[par_frame["lat"] != 0]
                    # in pardump file particles which have not been
                    # released yet

                    if sorti:
                        # returns only particles with
                        par_frame = par_frame.loc[par_frame["sorti"].isin(sorti)]
                        # sort index in list sorti
                    par_frame["date"] = pdate
                    # par_frame.sort('ht', inplace=True)  # sort by height
                    if iii == 0:
                        parframe_all = par_frame.copy()
                    else:
                        parframe_all = pd.concat([parframe_all, par_frame], axis=0)
                    par_frame = pd.concat([par_frame], keys=[self.fname])  # add a filename key

                iii += 1

                # Assume data is written sequentially by date.
                if drange:
                    if pdate > drange[1]:
                        testf = False
                        if verbose:
                            print("Past date. Closing file.", drange[1], pdate)
                    # elif  drange[0] < pdate:
                    #   testf=False
                    #   if verbose:
                    #      print "Before date. Closing file"
                if iii > imax:
                    print("Read pardump. Limited to" + str(imax) + "  iterations. Stopping")
                    testf = False
        parframe_all = pd.concat([parframe_all], keys=[self.fname])  # add a filename key
        return parframe_all
