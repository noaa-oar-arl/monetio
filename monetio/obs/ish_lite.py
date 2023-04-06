"""Python module for reading NOAA ISH files"""
import numpy as np
import pandas as pd


def add_data(
    dates,
    box=None,
    country=None,
    state=None,
    site=None,
    resample=True,
    window="H",
    n_procs=1,
    verbose=False,
):
    ish = ISH()
    return ish.add_data(
        dates,
        box=box,
        country=country,
        state=state,
        site=site,
        resample=resample,
        window=window,
        n_procs=n_procs,
        verbose=verbose,
    )


class ISH:
    """Integrated Surface Hourly (also known as ISD, Integrated Surface Data)

    Attributes
    ----------
    WIDTHS : type
        Description of attribute `WIDTHS`.
    DTYPES : type
        Description of attribute `DTYPES`.
    NAMES : type
        Description of attribute `NAMES`.
    history_file : type
        Description of attribute `history_file`.
    history : type
        Description of attribute `history`.
    daily : type
        Description of attribute `daily`.

    """

    def __init__(self):
        self.WIDTHS = [
            4,
            2,
            8,
            4,
            1,
            6,
            7,
            5,
            5,
            5,
            4,
            3,
            1,
            1,
            4,
            1,
            5,
            1,
            1,
            1,
            6,
            1,
            1,
            1,
            5,
            1,
            5,
            1,
            5,
            1,
        ]
        self.DTYPES = [
            ("varlength", "i2"),
            ("station_id", "S11"),
            ("date", "i4"),
            ("htime", "i2"),
            ("source_flag", "S1"),
            ("latitude", "float"),
            ("longitude", "float"),
            ("code", "S5"),
            ("elev", "i2"),
            ("call_letters", "S5"),
            ("qc_process", "S4"),
            ("wdir", "i2"),
            ("wdir_quality", "S1"),
            ("wdir_type", "S1"),
            ("ws", "i2"),
            ("ws_quality", "S1"),
            ("ceiling", "i4"),
            ("ceiling_quality", "S1"),
            ("ceiling_code", "S1"),
            ("ceiling_cavok", "S1"),
            ("vsb", "i4"),
            ("vsb_quality", "S1"),
            ("vsb_variability", "S1"),
            ("vsb_variability_quality", "S1"),
            ("t", "i2"),
            ("t_quality", "S1"),
            ("dpt", "i2"),
            ("dpt_quality", "S1"),
            ("p", "i4"),
            ("p_quality", "S1"),
        ]
        self.NAMES, _ = list(zip(*self.DTYPES))
        self.history_file = "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"
        self.history = None
        self.daily = False
        self.dates = None
        self.verbose = False

    def read_data_frame(self, file_object):
        """Create a data frame from an ISH file.

        Parameters
        ----------
        file_object : type
            Description of parameter `file_object`.

        Returns
        -------
        type
            Description of returned object.

        """
        frame_as_array = np.genfromtxt(file_object, delimiter=self.WIDTHS, dtype=self.DTYPES)
        frame = pd.DataFrame.from_records(frame_as_array)
        df = self._clean(frame)
        df.drop(["latitude", "longitude"], axis=1, inplace=True)
        index = (df.index >= self.dates.min()) & (df.index <= self.dates.max())

        return df.loc[index, :].reset_index()

    def read_ish_history(self, dates):
        """read ISH history file

        Returns
        -------
        type
            Description of returned object.

        """
        fname = self.history_file
        self.history = pd.read_csv(fname, parse_dates=["BEGIN", "END"], infer_datetime_format=True)
        self.history.columns = [i.lower() for i in self.history.columns]

        index1 = (self.history.end >= self.dates.min()) & (self.history.begin <= self.dates.max())
        self.history = self.history.loc[index1, :].dropna(subset=["lat", "lon"])

        self.history.loc[:, "usaf"] = self.history.usaf.astype("str").str.zfill(6)
        self.history.loc[:, "wban"] = self.history.wban.astype("str").str.zfill(5)
        self.history["station_id"] = self.history.usaf + self.history.wban
        self.history.rename(columns={"lat": "latitude", "lon": "longitude"}, inplace=True)

    def subset_sites(self, latmin=32.65, lonmin=-113.3, latmax=34.5, lonmax=-110.4):
        """find sites within designated region"""
        latindex = (self.history.latitude >= latmin) & (self.history.latitude <= latmax)
        lonindex = (self.history.longitude >= lonmin) & (self.history.longitude <= lonmax)
        dfloc = self.history.loc[latindex & lonindex, :]
        print("SUBSET")
        print(dfloc.latitude.unique())
        print(dfloc.longitude.unique())
        return dfloc

    def build_urls(self, dates, dfloc):
        """Short summary.

        Returns
        -------
        helper function to build urls

        """
        unique_years = pd.to_datetime(dates.year.unique(), format="%Y")
        furls = []
        # fnames = []
        if self.verbose:
            print("Building ISH-Lite URLs...")
        url = "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite"
        # get each yearly urls available from the isd-lite site
        if len(unique_years) > 1:
            all_urls = []
            if self.verbose:
                print("multiple years needed... Getting all available years")
            for date in unique_years.strftime("%Y"):
                if self.verbose:
                    print("Year:", date)
                year_url = (
                    pd.read_html(f"{url}/{date}/")[0]["Name"].iloc[2:-1].to_frame(name="name")
                )
                all_urls.append(
                    f"{url}/{date}/" + year_url
                )  # add the full url path to the file name only

            all_urls = pd.concat(all_urls, ignore_index=True)
        else:
            year = unique_years.strftime("%Y")[0]
            all_urls = pd.read_html(f"{url}/{year}/")[0]["Name"].iloc[2:-1].to_frame(name="name")
            all_urls = f"{url}/{year}/" + all_urls

        # get the dfloc meta data
        dfloc["fname"] = dfloc.usaf.astype(str) + "-" + dfloc.wban.astype(str) + "-"
        for date in unique_years.strftime("%Y"):
            dfloc["fname"] = (
                dfloc.usaf.astype(str) + "-" + dfloc.wban.astype(str) + "-" + date[0:4] + ".gz"
            )
            for fname in dfloc.fname.values:
                furls.append(f"{url}/{date[0:4]}/{fname}")

        # files needed for comparison
        url = pd.Series(furls, index=None)

        # ensure that all urls built are available
        final_urls = pd.merge(url.to_frame(name="name"), all_urls, how="inner")

        return final_urls

    def read_csv(self, fname):
        from numpy import NaN

        columns = [
            "year",
            "month",
            "day",
            "hour",
            "temp",
            "dew_pt_temp",
            "press",
            "wdir",
            "ws",
            "sky_condition",
            "precip_1hr",
            "precip_6hr",
        ]
        df = pd.read_csv(
            fname,
            delim_whitespace=True,
            header=None,
            names=columns,
            parse_dates={"time": [0, 1, 2, 3]},
            infer_datetime_format=True,
        )
        # print(fname)
        filename = fname.split("/")[-1].split("-")
        # print(filename)
        siteid = filename[0] + filename[1]
        df["temp"] /= 10.0
        df["dew_pt_temp"] /= 10.0
        df["press"] /= 10.0
        df["ws"] /= 10.0
        df["precip_1hr"] /= 10.0
        df["precip_6hr"] /= 10.0
        df["siteid"] = siteid
        df = df.replace(-9999, NaN)
        return df

    def aggregrate_files(self, urls, n_procs=1):
        import dask
        import dask.dataframe as dd

        # =======================
        # for manual testing
        # =======================
        # import pandas as pd
        # dfs=[]
        # for u in urls.name:
        #     print(u)
        #     dfs.append(self.read_csv(u))

        dfs = [dask.delayed(self.read_csv)(f) for f in urls.name]
        dff = dd.from_delayed(dfs)
        df = dff.compute(num_workers=n_procs)

        return df

    def add_data(
        self,
        dates,
        box=None,
        country=None,
        state=None,
        site=None,
        resample=True,
        window="H",
        verbose=False,
        n_procs=1,
    ):
        """Short summary.

        Parameters
        ----------
        dates : list of datetime objects
        box : list of floats
             [latmin, lonmin, latmax, lonmax]
        country : type
            Description of parameter `country`.
        state : type
            Description of parameter `state`.
        site : type
            Description of parameter `site`.
        resample : type
            Description of parameter `resample`.
        window : type
            Description of parameter `window`.

        Returns
        -------
        type
            Description of returned object.

        """
        self.dates = dates
        self.verbose = verbose
        if verbose:
            print("Reading ISH history file...")
        if self.history is None:
            self.read_ish_history(dates)
        dfloc = self.history.copy()

        if box is not None:  # type(box) is not type(None):
            if verbose:
                print("Retrieving Sites in: " + " ".join(map(str, box)))
            dfloc = self.subset_sites(latmin=box[0], lonmin=box[1], latmax=box[2], lonmax=box[3])
        elif country is not None:
            if verbose:
                print("Retrieving Country: " + country)
            dfloc = dfloc.loc[dfloc.ctry == country, :]
        elif state is not None:
            if verbose:
                print("Retrieving State: " + state)
            dfloc = dfloc.loc[dfloc.state == state, :]
        elif site is not None:
            if verbose:
                print("Retrieving Site: " + site)
            dfloc = dfloc.loc[dfloc.station_id == site, :]
        urls = self.build_urls(
            dates, dfloc
        )  # this is the overall urls built from the total ISH history file
        # return urls
        # print(urls.iloc[0])
        if verbose:
            print(f"Aggregating {len(urls.name)} URLs...")
        df = self.aggregrate_files(urls, n_procs=n_procs)

        # narrow in time
        df = df.loc[(df.time >= dates.min()) & (df.time <= dates.max())]
        df = df.replace(-999.9, np.NaN)

        # merge in dfloc to df
        df = pd.merge(df, dfloc, how="left", left_on="siteid", right_on="station_id")
        return df.drop(["station_id", "fname"], axis=1)

    def get_url_file_objs(self, fname):
        """Short summary.

        Parameters
        ----------
        fname : type
            Description of parameter `fname`.

        Returns
        -------
        type
            Description of returned object.

        """
        import gzip
        import shutil

        import requests

        objs = []
        print("  Constructing ISH file objects from urls...")
        mmm = 0
        jjj = 0
        for iii in fname:
            #            print i
            try:
                r2 = requests.get(iii, stream=True)
                temp = iii.split("/")
                temp = temp[-1]
                fname = "isd." + temp.replace(".gz", "")
                if r2.status_code != 404:
                    objs.append(fname)
                    with open(fname, "wb") as fid:
                        # TODO. currently shutil writes the file to the hard
                        # drive. try to find way around this step, so file does
                        # not need to be written and then read.
                        gzip_file = gzip.GzipFile(fileobj=r2.raw)
                        shutil.copyfileobj(gzip_file, fid)
                        print("SUCCEEDED REQUEST for " + iii)
                else:
                    print("404 message " + iii)
                mmm += 1
            except RuntimeError:
                jjj += 1
                print("REQUEST FAILED " + iii)
                pass
            if jjj > 100:
                print("Over " + str(jjj) + " failed. break loop")
                break
        return objs
