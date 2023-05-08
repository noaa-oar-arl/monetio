"""NOAA Integrated Surface Hourly (ISH; also known as ISD, Integrated Surface Data).

https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database
"""
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
    """Retrieve and load ISH data as a DataFrame.

    Parameters
    ----------
    dates : sequence of datetime-like
    box : list of float, optional
            ``[latmin, lonmin, latmax, lonmax]``.
    country, state, site : str, optional
        Select sites in a country or state or one specific site.
        Can use one at most of `box` and these.
    resample : bool
        If false, return data at original resolution, which may be sub-hourly.
        Use ``resample=False`` if you want to obtain the full set of columns, including quality flags.
    window
        Resampling window, e.g. ``'3H'``.
    n_procs : int
        For Dask.
    verbose : bool
        Print debugging messages.

    Returns
    -------
    DataFrame
    """
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
    """
    Attributes
    ----------
    history_file : str
        URL for the ISD history file.
    history : DataFrame, optional
        ISD history file frame, read by :meth:`read_ish_history`, ``None`` until read.
    """

    _VAR_INFO = [
        # name, dtype, width
        ("varlength", "i2", 4),
        ("station_id", "S11", 11),
        ("date", "i4", 8),
        ("htime", "i2", 4),
        ("source_flag", "S1", 1),
        ("latitude", "float", 6),
        ("longitude", "float", 7),
        ("code", "S5", 5),
        ("elev", "i2", 5),
        ("call_letters", "S5", 5),
        ("qc_process", "S4", 4),
        ("wdir", "i2", 3),
        ("wdir_quality", "S1", 1),
        ("wdir_type", "S1", 1),
        ("ws", "i2", 4),
        ("ws_quality", "S1", 1),
        ("ceiling", "i4", 5),
        ("ceiling_quality", "S1", 1),
        ("ceiling_code", "S1", 1),
        ("ceiling_cavok", "S1", 1),
        ("vsb", "i4", 6),
        ("vsb_quality", "S1", 1),
        ("vsb_variability", "S1", 1),
        ("vsb_variability_quality", "S1", 1),
        ("t", "i2", 5),
        ("t_quality", "S1", 1),
        ("dpt", "i2", 5),
        ("dpt_quality", "S1", 1),
        ("p", "i4", 5),
        ("p_quality", "S1", 1),
    ]

    DTYPES = [(name, dtype) for name, dtype, _ in _VAR_INFO]
    WIDTHS = [width for _, _, width in _VAR_INFO]

    def __init__(self):
        self.history_file = "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"
        self.history = None
        self.df = None
        self.dates = None
        self.verbose = False

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

    @staticmethod
    def _clean_column(series, missing=9999, multiplier=1):
        series = series.apply(float)
        series[series == missing] = np.nan
        return series // multiplier

    @staticmethod
    def _clean_column_by_name(frame, name, *args, **kwargs):
        frame[name] = ISH._clean_column(frame[name], *args, **kwargs)
        return frame

    @staticmethod
    def _clean(frame):
        """Clean up the data frame"""

        # index by time
        frame["time"] = [
            pd.Timestamp(f"{date:08}{htime:04}")
            for date, htime in zip(frame["date"], frame["htime"])
        ]
        # these fields were combined into 'time'
        frame.drop(["date", "htime"], axis=1, inplace=True)
        frame.set_index("time", drop=True, inplace=True)
        frame = ISH._clean_column_by_name(frame, "wdir", missing=999)  # angular degrees
        frame = ISH._clean_column_by_name(frame, "ws", multiplier=10)  # m/s
        frame = ISH._clean_column_by_name(frame, "ceiling", missing=99999)
        frame = ISH._clean_column_by_name(frame, "vsb", missing=999999)
        frame = ISH._clean_column_by_name(frame, "vsb", missing=99999)  # m
        frame = ISH._clean_column_by_name(frame, "t", multiplier=10, missing=9999)  # degC
        frame = ISH._clean_column_by_name(frame, "dpt", multiplier=10, missing=9999)  # degC
        frame = ISH._clean_column_by_name(frame, "p", multiplier=10, missing=99999)  # hPa
        return frame

    @staticmethod
    def _decode_bytes(df):
        if df.empty:
            return df
        bytes_cols = [col for col in df.columns if type(df[col][0]) == bytes]
        with pd.option_context("mode.chained_assignment", None):
            df.loc[:, bytes_cols] = df[bytes_cols].apply(
                lambda x: x.str.decode("utf-8"),
                axis="columns",
            )
        return df

    def read_data_frame(self, url_or_file):
        """Create a data frame from an ISH file.

        URL is assumed if `url_or_file` is a string that starts with ``http``.
        """
        if isinstance(url_or_file, str) and url_or_file.startswith("http"):
            import gzip
            import io

            import requests

            r = requests.get(url_or_file, timeout=10, stream=True)
            r.raise_for_status()
            with gzip.open(io.BytesIO(r.content), "rb") as f:
                frame_as_array = np.genfromtxt(f, delimiter=self.WIDTHS, dtype=self.DTYPES)
        else:
            frame_as_array = np.genfromtxt(url_or_file, delimiter=self.WIDTHS, dtype=self.DTYPES)

        frame = pd.DataFrame.from_records(np.atleast_1d(frame_as_array))
        df = self._clean(frame)
        df.drop(["latitude", "longitude"], axis=1, inplace=True)
        # df.latitude = self.history.groupby('station_id').get_group(
        #     df.station_id[0]).LAT.values[0]
        # df.longitude = self.history.groupby('station_id').get_group(
        #     df.station_id[0]).lon.values[0]
        # df['STATION_NAME'] = self.history.groupby('station_id').get_group(
        #     df.station_id[0])['STATION NAME'].str.strip().values[0]

        if self.dates is not None:
            index = (df.index >= self.dates.min()) & (df.index <= self.dates.max())
            df = df.loc[index, :]

        df = ISH._decode_bytes(df)

        return df.reset_index()

    def read_ish_history(self, dates=None):
        """Read ISH history file (:attr:`history_file`) and subset based on
        `dates` (or :attr:`dates` if unset),
        setting the :attr:`history` attribute.
        If both are unset, you get the entire history file.

        https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv

        The constructed 'station_id' column is a combination of the USAF and WBAN columns.
        This is done since USAF and WBAN alone are not unique in the history file.
        For example, USAF 720481, 722158, and 725244 appear twice, as do
        WBAN 13752, 23176, 24267, 41231, and 41420.
        Additionally, there are many cases of unset (999999 for USAF or 99999 for WBAN),
        though more so for WBAN than USAF.
        However, combining USAF and WBAN does give a unique station ID.
        """
        if dates is None:
            dates = self.dates

        fname = self.history_file
        self.history = pd.read_csv(fname, parse_dates=["BEGIN", "END"], infer_datetime_format=True)
        self.history.columns = [i.lower() for i in self.history.columns]

        if dates is not None:
            index1 = (self.history.end >= dates.min()) & (self.history.begin <= dates.max())
            self.history = self.history.loc[index1, :]
        self.history = self.history.dropna(subset=["lat", "lon"])

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
        """Retrieve and load ISH data as a DataFrame.

        Parameters
        ----------
        dates : sequence of datetime-like
        box : list of float, optional
             ``[latmin, lonmin, latmax, lonmax]``.
        country, state, site : str, optional
            Select sites in a country or state or one specific site.
            Can use one at most of `box` and these.
        resample : bool
            If false, return data at original resolution, which may be sub-hourly.
            Use ``resample=False`` if you want to obtain the full set of columns, including quality flags.
        window
            Resampling window, e.g. ``'3H'``.
        n_procs : int
            For Dask.
        verbose : bool
            Print debugging messages.

        Returns
        -------
        DataFrame
        """
        self.dates = pd.to_datetime(dates)
        self.verbose = verbose
        if verbose:
            print("Reading ISH history file...")
        if self.history is None:
            self.read_ish_history()
        dfloc = self.history.copy()

        if sum([box is not None, country is not None, state is not None, site is not None]) > 1:
            raise ValueError("Only one of `box`, `country`, `state`, or `site` can be used")

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
        urls = self.build_urls(sites=dfloc)
        if urls.empty:
            raise ValueError("No data URLs found for the given dates and site selection")
        if download:
            objs = self.get_url_file_objs(urls.name)
            print("  Reading ISH into pandas DataFrame...")
            dfs = [dask.delayed(self.read_data_frame)(f) for f in objs]
            dff = dd.from_delayed(dfs)
            self.df = dff.compute(num_workers=n_procs)
        else:
            if verbose:
                print(f"Aggregating {len(urls.name)} URLs...")
            self.df = self.aggregrate_files(urls, n_procs=n_procs)

        if resample and not self.df.empty:
            if verbose:
                print("Resampling to every " + window)
            self.df.index = self.df.time
            self.df = self.df.groupby("station_id").resample(window).mean().reset_index()

        self.df = self.df.merge(dfloc, on="station_id", how="left")
        self.df = self.df.rename(columns={"station_id": "siteid", "ctry": "country"}).drop(
            columns=["fname"]
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

    def build_urls(self, dates=None, sites=None):
        """Build URLs.

        Parameters
        ----------
        dates
            Dates of interest.
            If unset, uses :attr:`dates`.
        sites
            Metadata frame for the stations of interest.
            If unset, uses :attr:`history` (all sites by default).

        Returns
        -------
        DataFrame
        """
        if dates is None:
            dates = self.dates
        if sites is None:
            sites = self.history

        unique_years = pd.to_datetime(dates.year.unique(), format="%Y")
        furls = []
        # fnames = []
        if self.verbose:
            print("Building ISH URLs...")
        url = "https://www1.ncdc.noaa.gov/pub/data/noaa"
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
        sites["fname"] = sites.usaf.astype(str) + "-" + sites.wban.astype(str) + "-"
        for date in unique_years.strftime("%Y"):
            sites["fname"] = (
                sites.usaf.astype(str) + "-" + sites.wban.astype(str) + "-" + date + ".gz"
            )
            for fname in sites.fname.values:
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
