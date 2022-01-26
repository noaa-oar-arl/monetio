# this is written to retrive airnow data concatenate and add to pandas array
# for usage
from builtins import object, str
from datetime import datetime

import pandas as pd
from numpy import NaN, array, ndarray

try:
    from joblib import Parallel, delayed

    has_joblib = True
except:
    has_joblib = False


def dateparse(x):
    return datetime.strptime(x, "%d:%m:%Y %H:%M:%S")


def add_local(fname, product="AOD15", dates=None, latlonbox=None, freq=None, interp_to_values=None):
    a = AERONET()
    a.url = fname
    # df = a.read_aeronet(fname)
    self.prod = product.upper()
    if daily:
        a.daily = 20  # daily data
    else:
        a.daily = 10  # all points
    if inv_type is not None:
        a.inv_type = "ALM15"
    else:
        a.inv_type = inv_type
    if "AOD" in self.prod:
        if interp_to_values is not None:
            if ~isinstance(interp_to_values, ndarray):
                a.new_aod_values = array(interp_to_values)
            else:
                a.new_aod_values = interp_to_values
    # a.build_url()
    try:
        a.url = fname
        a.read_aeronet()
    except:
        print("Error reading:" + fname)
    if freq is not None:
        a.df = a.df.groupby("siteid").resample(freq).mean().reset_index()
    if detect_dust:
        a.dust_detect()
    if a.new_aod_values is not None:
        a.calc_new_aod_values()
    return a.df


def add_data(
    dates=None,
    product="AOD15",
    latlonbox=None,
    daily=False,
    interp_to_aod_values=None,
    inv_type=None,
    freq=None,
    siteid=None,
    detect_dust=False,
    n_procs=1,
    verbose=10,
):
    a = AERONET()
    if interp_to_aod_values is not None:
        if ~isinstance(interp_to_aod_values, ndarray):
            interp_to_aod_values = array(interp_to_aod_values)
    if has_joblib and (n_procs > 1):
        min_date = dates.min()
        max_date = dates.max()
        # find days from here to there
        days = pd.date_range(start=min_date, end=max_date, freq="D")
        days1 = pd.date_range(start=min_date, end=max_date, freq="D") + pd.Timedelta(1, unit="D")
        vars = dict(
            product=product,
            latlonbox=latlonbox,
            daily=daily,
            interp_to_aod_values=interp_to_aod_values,
            inv_type=inv_type,
            siteid=siteid,
            freq=None,
            detect_dust=detect_dust,
        )
        dfs = Parallel(n_jobs=n_procs, verbose=verbose)(
            delayed(_parallel_aeronet_call)(pd.DatetimeIndex([d1, d2]), **vars)
            for d1, d2 in zip(days, days1)
        )
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()
        if freq is not None:
            df.index = df.time
            df = df.groupby("siteid").resample(freq).mean().reset_index()
        return df.reset_index(drop=True)
    else:
        if ~has_joblib and (n_procs > 1):
            print(
                "Please install joblib to use the parallel feature of monetio.aeronet. Proceeding in serial mode..."
            )
        df = a.add_data(
            dates=dates,
            product=product,
            latlonbox=latlonbox,
            daily=daily,
            interp_to_aod_values=interp_to_aod_values,
            inv_type=inv_type,
            siteid=siteid,
            freq=freq,
            detect_dust=detect_dust,
        )
    return df.reset_index(drop=True)


def _parallel_aeronet_call(
    dates=None,
    product="AOD15",
    latlonbox=None,
    daily=False,
    interp_to_aod_values=None,
    inv_type=None,
    freq=None,
    siteid=None,
    detect_dust=False,
):
    a = AERONET()
    df = a.add_data(
        dates,
        product=product,
        latlonbox=latlonbox,
        daily=daily,
        interp_to_aod_values=interp_to_aod_values,
        inv_type=inv_type,
        siteid=siteid,
        freq=freq,
        detect_dust=detect_dust,
    )
    return df


class AERONET(object):
    def __init__(self):
        from numpy import arange, concatenate

        self.baseurl = "https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?"
        self.dates = [
            datetime.strptime("2016-06-06 12:00:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2016-06-10 13:00:00", "%Y-%m-%d %H:%M:%S"),
        ]
        self.datestr = []
        self.df = pd.DataFrame()
        self.daily = None
        self.prod = None
        self.inv_type = None
        self.siteid = None
        self.objtype = "AERONET"
        self.usecols = concatenate((arange(30), arange(65, 83)))
        # [21.1,-131.6686,53.04,-58.775] #[latmin,lonmin,latmax,lonmax]
        self.latlonbox = None
        self.url = None
        self.new_aod_values = None

    def build_url(self):
        sy = self.dates.min().strftime("%Y")
        sm = self.dates.min().strftime("%m").zfill(2)
        sd = self.dates.min().strftime("%d").zfill(2)
        sh = self.dates.min().strftime("%H").zfill(2)
        ey = self.dates.max().strftime("%Y").zfill(2)
        em = self.dates.max().strftime("%m").zfill(2)
        ed = self.dates.max().strftime("%d").zfill(2)
        eh = self.dates.max().strftime("%H").zfill(2)
        if self.prod in [
            "AOD10",
            "AOD15",
            "AOD20",
            "SDA10",
            "SDA15",
            "SDA20",
            "TOT10",
            "TOT15",
            "TOT20",
        ]:
            base_url = "https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?"
            inv_type = None
        else:
            base_url = "https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_inv_v3?"
            if self.inv_type == "ALM15":
                inv_type = "&ALM15=1"
            else:
                inv_type = "&AML20=1"
        date_portion = (
            "year="
            + sy
            + "&month="
            + sm
            + "&day="
            + sd
            + "&hour="
            + sh
            + "&year2="
            + ey
            + "&month2="
            + em
            + "&day2="
            + ed
            + "&hour2="
            + eh
        )
        # print(self.prod, inv_type)
        if self.inv_type is not None:
            product = "&product=" + self.prod
        else:
            product = "&" + self.prod + "=1"
            self.inv_type = ""
        time = "&AVG=" + str(self.daily)
        if self.siteid is not None:
            latlonbox = "&site={}".format(self.siteid)
        elif self.latlonbox is None:
            latlonbox = ""
        else:
            lat1 = str(float(self.latlonbox[0]))
            lon1 = str(float(self.latlonbox[1]))
            lat2 = str(float(self.latlonbox[2]))
            lon2 = str(float(self.latlonbox[3]))
            latlonbox = "&lat1=" + lat1 + "&lat2=" + lat2 + "&lon1=" + lon1 + "&lon2=" + lon2
        # print(base_url)
        # print(date_portion)
        # print(product)
        # print(inv_type)
        # print(time)
        # print(latlonbox)
        if inv_type is None:
            inv_type = ""
        self.url = base_url + date_portion + product + inv_type + time + latlonbox + "&if_no_html=1"

    def read_aeronet(self):
        print("Reading Aeronet Data...")
        # header = self.get_columns()
        df = pd.read_csv(
            self.url,
            engine="python",
            header=None,
            skiprows=6,
            parse_dates={"time": [1, 2]},
            date_parser=dateparse,
            na_values=-999,
        )
        # df.rename(columns={'date_time': 'time'}, inplace=True)
        columns = self.get_columns()
        df.columns = columns  # self.get_columns()
        df.index = df.time
        df.rename(
            columns={
                "site_latitude(degrees)": "latitude",
                "site_longitude(degrees)": "longitude",
                "site_elevation(m)": "elevation",
                "aeronet_site": "siteid",
            },
            inplace=True,
        )
        df.dropna(subset=["latitude", "longitude"], inplace=True)
        df.dropna(axis=1, how="all", inplace=True)
        self.df = df

    def get_columns(self):
        header = pd.read_csv(self.url, skiprows=5, header=None, nrows=1).values.flatten()
        final = ["time"]
        for i in header:
            if "Date(" in i or "Time(" in i:
                pass
            else:
                final.append(i.lower())
        return final

    def add_data(
        self,
        dates=None,
        product="AOD15",
        latlonbox=None,
        daily=False,
        interp_to_aod_values=None,
        inv_type=None,
        freq=None,
        siteid=None,
        detect_dust=False,
    ):
        self.latlonbox = latlonbox
        self.siteid = siteid
        if dates is None:  # get the current day
            self.dates = pd.date_range(
                start=pd.to_datetime("today"), end=pd.to_datetime("now"), freq="H"
            )
        else:
            self.dates = dates
        self.prod = product.upper()
        if daily:
            self.daily = 20  # daily data
        else:
            self.daily = 10  # all points
        if inv_type is not None:
            self.inv_type = "ALM15"
        else:
            self.inv_type = inv_type
        if "AOD" in self.prod:
            self.new_aod_values = interp_to_aod_values
        self.build_url()
        try:
            self.read_aeronet()
        except:
            print(self.url)
        if freq is not None:
            self.df = self.df.groupby("siteid").resample(freq).mean().reset_index()
        if detect_dust:
            self.dust_detect()
        if self.new_aod_values is not None:
            self.calc_new_aod_values()
        return self.df

    def calc_550nm(self):
        """Since AOD at 500nm is not calculated we use the extrapolation of
        V. Cesnulyte et al (ACP,2014) for the calculation

        aod550 = aod500 * (550/500) ^ -alpha
        """
        self.df["aod_550nm"] = self.df.aod_500nm * (550.0 / 500.0) ** (
            -self.df["440-870_angstrom_exponent"]
        )

    def calc_new_aod_values(self):
        def _tspack_aod_interp(row, new_wv=[440.0, 470.0, 550.0, 670.0, 870.0, 1020.0, 1240.0]):
            try:
                import pytspack
            except ImportError:
                print("You must install pytspack before using this function")
            # df_aod_nu = self._aeronet_aod_and_nu(row)
            aod_columns = [aod_column for aod_column in row.index if "aod_" in aod_column]
            aods = row[aod_columns]
            wv = [
                float(aod_column.replace("aod_", "").replace("nm", ""))
                for aod_column in aod_columns
            ]
            a = pd.DataFrame({"aod": aods}).reset_index()
            a["wv"] = wv
            df_aod_nu = a.dropna()
            df_aod_nu_sorted = df_aod_nu.sort_values(by="wv").dropna()
            if len(df_aod_nu_sorted) < 2:
                return new_wv * NaN
            else:
                x, y, yp, sigma = pytspack.tspsi(
                    df_aod_nu_sorted.wv.values, df_aod_nu_sorted.aod.values
                )
                yi = pytspack.hval(self.new_aod_values, x, y, yp, sigma)
                return yi

        out = self.df.apply(
            _tspack_aod_interp, axis=1, result_type="expand", new_wv=self.new_aod_values
        )
        names = "aod_" + pd.Series(self.new_aod_values.astype(int).astype(str)) + "nm"
        out.columns = names.values
        self.df = pd.concat([self.df, out], axis=1)

    @staticmethod
    def _tspack_aod_interp(row, new_wv=[440.0, 470.0, 550.0, 670.0, 870.0, 1020.0, 1240.0]):
        try:
            import pytspack
        except ImportError:
            print("You must install pytspack before using this function")
        # df_aod_nu = self._aeronet_aod_and_nu(row)
        aod_columns = [aod_column for aod_column in row.index if "aod_" in aod_column]
        aods = row[aod_columns]
        wv = [float(aod_column.replace("aod_", "").replace("nm", "")) for aod_column in aod_columns]
        a = pd.DataFrame({"aod": aods}).reset_index()
        a["wv"] = wv
        df_aod_nu = a.dropna()
        df_aod_nu_sorted = df_aod_nu.sort_values(by="wv").dropna()
        if len(df_aod_nu_sorted) < 2:
            return xi * NaN
        else:
            x, y, yp, sigma = pytspack.tspsi(
                df_aod_nu_sorted.wv.values, df_aod_nu_sorted.aod.values
            )
            yi = pytspack.hval(self.new_aod_values, x, y, yp, sigma)
            return yi

    @staticmethod
    def _aeronet_aod_and_nu(row):
        import pandas as pd

        # print(row)
        aod_columns = [aod_column for aod_column in row.index if "aod_" in aod_column]
        wv = [float(aod_column.replace("aod_", "").replace("nm", "")) for aod_column in aod_columns]
        aods = row[aod_columns]
        a = pd.DataFrame({"aod": aods}).reset_index()
        # print(a.index,wv)
        a["wv"] = wv
        return a.dropna()

    def dust_detect(self):
        """Detect dust from AERONET. See [Dubovik et al., 2002].

        AOD_1020 > 0.3 and AE(440,870) < 0.6

        Returns
        -------
        type
            Description of returned object.

        """
        self.df["dust"] = (self.df["aod_1020nm"] > 0.3) & (
            self.df["440-870_angstrom_exponent"] < 0.6
        )

    def set_daterange(self, begin="", end=""):
        dates = pd.date_range(start=begin, end=end, freq="H").values.astype("M8[s]").astype("O")
        self.dates = dates
