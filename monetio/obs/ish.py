"""Python module for reading NOAA ISH files"""
import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd


def add_data(
    dates,
    *,
    box=None,
    country=None,
    state=None,
    site=None,
    resample=True,
    window="H",
    download=False,
    n_procs=1,
    verbose=False,
):
    """Add ISH data (Integrated Surface Data, hourly)."""
    ish = ISH()
    df = ish.add_data(
        dates,
        box=box,
        country=country,
        state=state,
        site=site,
        resample=resample,
        window=window,
        download=download,
        n_procs=n_procs,
        verbose=verbose,
    )
    return df


class ISH:
    """Integrated Surface Hourly (ISH; also known as ISD, Integrated Surface Data).

    https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database

    Attributes
    ----------
    history_file : str
        URL for the ISD history file.
    history : DataFrame, optional
        ISD history file frame, read by :meth:`read_ish_history`, ``None`` until read.
    """

    def __init__(self):
        self.WIDTHS = [  # TODO: consolidate with dtypes one
            4,
            11,
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

    def delimit(self, file_object, delimiter=","):
        """Iterate over the lines in a file yielding comma delimited versions.

        Parameters
        ----------
        file_object : file or filename
            Description of parameter `file_object`.
        delimiter : type
            Description of parameter `delimiter`.
        ' : type
            Description of parameter `'`.

        Returns
        -------
        type
            Description of returned object.

        """

        try:
            file_object = open(file_object)
        except TypeError:
            pass

        for line in file_object:
            items = []
            index = 0
            for w in self.WIDTHS:
                items.append(line[index : index + w])
                index = index + w
            yield ",".join(items)

    def _clean_column(self, series, missing=9999, multiplier=1):
        series = series.apply(float)
        series[series == missing] = np.nan
        return series // multiplier

    def _clean_column_by_name(self, frame, name, *args, **kwargs):
        frame[name] = self._clean_column(frame[name], *args, **kwargs)
        return frame

    def _clean(self, frame):
        """Clean up the data frame"""

        # index by time
        frame["time"] = [
            pd.Timestamp(f"{date:08}{htime:04}")
            for date, htime in zip(frame["date"], frame["htime"])
        ]
        # these fields were combined into 'time'
        frame.drop(["date", "htime"], axis=1, inplace=True)
        frame.set_index("time", drop=True, inplace=True)
        frame = self._clean_column_by_name(frame, "wdir", missing=999)  # angular degrees
        frame = self._clean_column_by_name(frame, "ws", multiplier=10)  # m/s
        frame = self._clean_column_by_name(frame, "ceiling", missing=99999)
        frame = self._clean_column_by_name(frame, "vsb", missing=999999)
        frame = self._clean_column_by_name(
            frame, "t", multiplier=10, missing=9999
        )  # degrees Celcius
        frame = self._clean_column_by_name(
            frame, "dpt", multiplier=10, missing=9999
        )  # degrees Celcius
        frame = self._clean_column_by_name(frame, "p", multiplier=10, missing=99999)  # Hectopascals
        frame = self._clean_column_by_name(frame, "vsb", missing=99999)  # m
        return frame

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
        # df.latitude = self.history.groupby('station_id').get_group(
        #     df.station_id[0]).LAT.values[0]
        # df.longitude = self.history.groupby('station_id').get_group(
        #     df.station_id[0]).lon.values[0]
        # df['STATION_NAME'] = self.history.groupby('station_id').get_group(
        #     df.station_id[0])['STATION NAME'].str.strip().values[0]
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

    def add_data(
        self,
        dates,
        *,
        box=None,
        country=None,
        state=None,
        site=None,
        resample=True,
        window="H",
        download=False,
        n_procs=1,
        verbose=False,
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
        from numpy import NaN

        self.dates = pd.to_datetime(dates)
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

        # this is the overall urls built from the total ISH history file
        urls = self.build_urls(dates, dfloc)
        # return urls, dfloc
        if download:
            objs = self.get_url_file_objs(urls.name)
            print("  Reading ISH into pandas DataFrame...")
            dfs = [dask.delayed(self.read_data_frame)(f) for f in objs]
            dff = dd.from_delayed(dfs)
            self.df = dff.compute(num_workers=n_procs)
            self.df.loc[self.df.vsb == 99999, "vsb"] = NaN
        else:
            if verbose:
                print(f"Aggregating {len(urls.name)} URLs...")
            self.df = self.aggregrate_files(urls, n_procs=n_procs)
            # self.df.loc[self.df.vsb == 99999, "vsb"] = NaN
        if resample:
            if verbose:
                print("Resampling to every " + window)
            self.df.index = self.df.time
            self.df = self.df.groupby("station_id").resample(window).mean().reset_index()
        # this was encoded as byte literal but in dfloc it is a string so could
        # not merge on station_id correctly.
        try:
            self.df["station_id"] = self.df["station_id"].str.decode("utf-8")
        except RuntimeError:
            pass
        self.df = self.df.merge(
            dfloc[["station_id", "latitude", "longitude", "station name"]],
            on=["station_id"],
            how="left",
        )

        return self.df

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
            print("Building ISH URLs...")
        url = "https://www1.ncdc.noaa.gov/pub/data/noaa/"
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

        dfs = [dask.delayed(self.read_data_frame)(f) for f in urls.name]
        dff = dd.from_delayed(dfs)
        df = dff.compute(num_workers=n_procs)

        return df
