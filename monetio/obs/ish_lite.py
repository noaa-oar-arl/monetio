"""NOAA Integrated Surface Hourly (ISH; also known as ISD, Integrated Surface Data) lite version.

https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/isd-lite-format.txt

ISDLite is a derived product that makes it easier to work with for general research and scientific purposes.
It is a subset of the full ISD containing eight common surface parameters
in a fixed-width format free of duplicate values, sub-hourly data, and complicated flags.

--- https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database
"""
import numpy as np
import pandas as pd


def add_data(
    dates,
    *,
    box=None,
    country=None,
    state=None,
    site=None,
    resample=False,
    window="H",
    n_procs=1,
    verbose=False,
):
    """Retrieve and load ISH-lite data as a DataFrame.

    Parameters
    ----------
    dates : sequence of datetime-like
    box : list of float, optional
            ``[latmin, lonmin, latmax, lonmax]``.
    country, state, site : str, optional
        Select sites in a country or state or one specific site.
        Can use one at most of `box` and these.
    resample : bool
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
    """
    Attributes
    ----------
    history_file : str
        URL for the ISD history file.
    history : DataFrame, optional
        ISD history file frame, read by :meth:`read_ish_history`, ``None`` until read.
    """

    def __init__(self):
        self.history_file = "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"
        self.history = None
        self.dates = None
        self.verbose = False

    def read_ish_history(self, dates=None):
        """Read ISH history file (:attr:`history_file`) and subset based on
        `dates` (or :attr:`dates` if unset),
        setting the :attr:`history` attribute.
        If both are unset, you get the entire history file.

        https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv

        The constructed 'station_id' column is a combination of the USAF and WBAN columns.
        This is done since USAF and WBAN alone are not unique in the history file.
        For example, USAF 725244 and 722158 appear twice, as do
        WBAN 24267, 41420, 23176, 13752, and 41231.
        Additionally, there are many cases of unset (999999 for USAF or 99999 for WBAN),
        though more so for WBAN than USAF.
        However, combining USAF and WBAN does give a unique station ID.
        """
        # TODO: one function for both ISH-lite and ISH
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
        if self.verbose:
            print("Building ISH-Lite URLs...")
        url = "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite"
        # Get each yearly urls available from the isd-lite site
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

        # Get the meta data
        sites["fname"] = sites.usaf.astype(str) + "-" + sites.wban.astype(str) + "-"
        for date in unique_years.strftime("%Y"):
            sites["fname"] = (
                sites.usaf.astype(str) + "-" + sites.wban.astype(str) + "-" + date + ".gz"
            )
            for fname in sites.fname.values:
                furls.append(f"{url}/{date[0:4]}/{fname}")

        # Files needed for comparison
        url = pd.Series(furls, index=None)

        # Only available URLs
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
        *,
        box=None,
        country=None,
        state=None,
        site=None,
        resample=False,
        window="H",
        n_procs=1,
        verbose=False,
    ):
        """Retrieve and load ISH-lite data as a DataFrame.

        Parameters
        ----------
        dates : sequence of datetime-like
        box : list of float, optional
             ``[latmin, lonmin, latmax, lonmax]``.
        country, state, site : str, optional
            Select sites in a country or state or one specific site.
            Can use one at most of `box` and these.
        resample : bool
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

        if box is not None:
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
        urls = self.build_urls(sites=dfloc)
        if urls.empty:
            raise ValueError("No data URLs found for the given dates and site selection")
        if verbose:
            print(f"Aggregating {len(urls.name)} URLs...")
        df = self.aggregrate_files(urls, n_procs=n_procs)

        # Narrow in time (each file contains a year)
        df = df.loc[(df.time >= self.dates.min()) & (df.time <= self.dates.max())]
        df = df.replace(-999.9, np.NaN)

        if resample:
            print("Resampling to every " + window)
            df = df.set_index("time").groupby("siteid").resample(window).mean().reset_index()

        # Add site metadata
        df = pd.merge(df, dfloc, how="left", left_on="siteid", right_on="station_id").rename(
            columns={"ctry": "country"}
        )
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
