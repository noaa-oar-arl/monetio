"""
AERONET
"""
from datetime import datetime
from functools import lru_cache

import numpy as np
import pandas as pd

try:
    from joblib import Parallel, delayed

    has_joblib = True
except ImportError:
    has_joblib = False


def add_local(
    fname,
    product="AOD15",
    freq=None,
    interp_to_values=None,
    daily=False,
    inv_type=None,
    detect_dust=False,
):
    """Read a local file downloaded from the AERONET Web Service."""
    a = AERONET()
    a.url = fname
    # df = a.read_aeronet(fname)
    a.prod = product.upper()
    if daily:
        a.daily = 20  # daily data
    else:
        a.daily = 10  # all points
    if inv_type is not None:
        a.inv_type = "ALM15"
    else:
        a.inv_type = inv_type
    if "AOD" in a.prod:
        if interp_to_values is not None:
            # TODO: could probably use np.asanyarray here
            if not isinstance(interp_to_values, np.ndarray):
                a.new_aod_values = np.array(interp_to_values)
            else:
                a.new_aod_values = interp_to_values
    # a.build_url()
    try:
        a.url = fname
        a.read_aeronet()
    except Exception:
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
    *,
    inv_type=None,
    latlonbox=None,
    siteid=None,
    daily=False,
    #
    # post-proc
    freq=None,
    detect_dust=False,
    interp_to_aod_values=None,
    #
    # joblib
    n_procs=1,
    verbose=10,
):
    """Load AERONET data from the AERONET Web Service.

    Parameters
    ----------
    dates : array-like of datetime-like
        Expressing the desired min and max dates to retrieve.
        If unset, the current day will be fetched.
    product : str
    inv_type : str
        Inversion product type.
    latlonbox : array-like of float
        ``[lat1, lon1, lat2, lon2]``,
        where ``lat1, lon1`` is the lower-left corner
        and ``lat2, lon2`` is the upper-right corner.
    siteid : str
        <https://aeronet.gsfc.nasa.gov/aeronet_locations_v3.txt>

        Note that `siteid` takes precendence over `latlonbox`
        if both are specified.
    daily : bool
        Load daily averaged data.
    freq : str
        Frequency used to resample the DataFrame.
    detect_dust : bool
    interp_to_aod_values : array-like of float
        Values to interpolate AOD values to.
    n_procs : int
        For joblib.
    verbose : int
        For joblib.

    Returns
    -------
    pandas.DataFrame
    """
    a = AERONET()

    if interp_to_aod_values is not None:
        interp_to_aod_values = np.asarray(interp_to_aod_values)

    kwargs = dict(
        product=product,
        inv_type=inv_type,
        latlonbox=latlonbox,
        siteid=siteid,
        daily=daily,
        detect_dust=detect_dust,
        interp_to_aod_values=interp_to_aod_values,
    )

    requested_parallel = n_procs > 1 or n_procs == -1
    if has_joblib and requested_parallel:
        # Split up by day
        min_date = dates.min()
        max_date = dates.max()
        days = pd.date_range(start=min_date, end=max_date, freq="D")  # TODO: subtract 1?
        days1 = days + pd.Timedelta(days=1)
        dfs = Parallel(n_jobs=n_procs, verbose=verbose)(
            delayed(_parallel_aeronet_call)(pd.DatetimeIndex([d1, d2]), **kwargs, freq=None)
            for d1, d2 in zip(days, days1)
        )
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()
        if freq is not None:
            df.index = df.time
            df = df.groupby("siteid").resample(freq).mean().reset_index()
        return df.reset_index(drop=True)
    else:
        if not has_joblib and requested_parallel:
            print(
                "Please install joblib to use the parallel feature of monetio.aeronet. "
                "Proceeding in serial mode..."
            )
        df = a.add_data(
            dates=dates,
            **kwargs,
            freq=freq,
        )
    return df  # .reset_index(drop=True)


@lru_cache(1)
def get_valid_sites():
    """Load the AERONET site list as a DataFrame,
    reading from <https://aeronet.gsfc.nasa.gov/aeronet_locations_v3.txt>.
    """
    from urllib.error import URLError

    try:
        df = pd.read_csv(
            "https://aeronet.gsfc.nasa.gov/aeronet_locations_v3.txt",
            skiprows=1,
        ).rename(
            columns={
                "Site_Name": "siteid",
                "Longitude(decimal_degrees)": "longitude",
                "Latitude(decimal_degrees)": "latitude",
                "Elevation(meters)": "elevation",
            },
        )
    except URLError:
        print("getting valid sites failed")
        return None
    except Exception:
        raise

    return df


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


class AERONET:
    """Read AERONET data, with some post-processing capabilities."""

    objtype = "AERONET"
    usecols = np.r_[:30, 65:83]

    _valid_prod_noninv = (
        "AOD10",
        "AOD15",
        "AOD20",
        "SDA10",
        "SDA15",
        "SDA20",
        "TOT10",
        "TOT15",
        "TOT20",
    )
    _valid_prod_inv = (
        "SIZ",
        "RIN",
        "CAD",
        "VOL",
        "TAB",
        "AOD",
        "SSA",
        "ASY",
        "FRC",
        "LID",
        "FLX",
        # "ALL",
        # "PFN",
        # "U27",
    )
    _valid_inv_type = (
        "ALM15",
        "ALM20",
        "HYB15",
        "HYB20",
    )

    def __init__(self):
        # Main attributes
        self.url = None
        self.df = None

        # Settings controlling the URL generation
        self.dates = None  # array-like of datetime-like
        self.prod = None
        self.inv_type = None
        self.daily = None
        self.latlonbox = None
        self.siteid = None

        self.new_aod_values = None

    def build_url(self):
        """Use attributes to build a URL and set :attr:`url`.

        Targetting either of
        - https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_new.html
        - https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_inv_new.html
        """
        assert self.dates is not None, "required parameter"
        d1, d2 = self.dates.min(), self.dates.max()
        sy = d1.strftime(r"%Y")
        sm = d1.strftime(r"%m")
        sd = d1.strftime(r"%d")
        sh = d1.strftime(r"%H")
        ey = d2.strftime(r"%Y")
        em = d2.strftime(r"%m")
        ed = d2.strftime(r"%d")
        eh = d2.strftime(r"%H")
        dates_ = (
            f"year={sy}&month={sm}&day={sd}&hour={sh}"
            f"&year2={ey}&month2={em}&day2={ed}&hour2={eh}"
        )

        assert self.prod is not None, "required parameter"

        if self.inv_type is None:
            # AOD products
            # https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_new.html

            if self.prod in self._valid_prod_noninv:
                base_url = "https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?"
            else:
                raise ValueError(f"invalid product {self.prod!r}")
            inv_type_ = ""
            product_ = f"&{self.prod}=1"

        elif self.inv_type in self._valid_inv_type:
            # Inversion products
            # https://aeronet.gsfc.nasa.gov/print_web_data_help_v3_inv_new.html

            if self.prod in self._valid_prod_inv:
                base_url = "https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_inv_v3?"
            else:
                raise ValueError(f"invalid product {self.prod!r}")
            inv_type_ = f"&{self.inv_type}=1"
            product_ = f"&product={self.prod}"

        else:
            raise ValueError(f"invalid inv type: {self.inv_type!r}")

        assert self.daily in {10, 20}, "required parameter"
        avg_ = f"&AVG={self.daily}"

        if self.siteid is not None:
            # Validate here, since the Web Service doesn't do any validation and just returns all
            # sites if the site isn't valid.
            # Note that having a valid site ID doesn't mean there will be any data
            # (depends on time period).
            if self.siteid in get_valid_sites().siteid.values:
                loc_ = f"&site={self.siteid}"
            else:
                raise ValueError(f"invalid site {self.siteid}")
        elif self.latlonbox is None:
            loc_ = ""
        else:
            lat1 = str(float(self.latlonbox[0]))
            lon1 = str(float(self.latlonbox[1]))
            lat2 = str(float(self.latlonbox[2]))
            lon2 = str(float(self.latlonbox[3]))
            loc_ = f"&lat1={lat1}&lat2={lat2}&lon1={lon1}&lon2={lon2}"

        self.url = f"{base_url}{dates_}{product_}{avg_}{inv_type_}{loc_}&if_no_html=1"

    def read_aeronet(self):
        """Use :meth:`build_url` to set a URL and then load a DataFrame from it,
        settings :attr:`df`.
        """
        print("Reading Aeronet Data...")
        df = pd.read_csv(
            self.url,
            engine="python",
            header="infer",
            skiprows=5,
            parse_dates={"time": [1, 2]},
            date_parser=lambda x: datetime.strptime(x, r"%d:%m:%Y %H:%M:%S"),
            na_values=-999,
        )
        df.rename(columns=str.lower, inplace=True)
        df.rename(
            columns={
                "site_latitude(degrees)": "latitude",
                "site_longitude(degrees)": "longitude",
                "site_elevation(m)": "elevation",
                "aeronet_site": "siteid",
            },
            inplace=True,
        )
        if df.siteid.unique().size == 1:
            df.set_index("time", inplace=True)
        df.dropna(subset=["latitude", "longitude"], inplace=True)
        df.dropna(axis=1, how="all", inplace=True)  # empty columns
        self.df = df

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
        if self.prod.startswith("AOD"):
            self.new_aod_values = interp_to_aod_values

        self.build_url()
        try:
            self.read_aeronet()
        except Exception as e:
            raise Exception(
                f"loading from URL {self.url!r} failed. "
                "If using `siteid`, check that the site is valid."
            ) from e

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
            import numpy as np

            try:
                import pytspack
            except ImportError:
                print("You must install pytspack before using this function")

            new_wv = np.asarray(new_wv)

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
                return new_wv * np.NaN
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

    # @staticmethod
    # def _tspack_aod_interp(row, new_wv=[440.0, 470.0, 550.0, 670.0, 870.0, 1020.0, 1240.0]):
    #     try:
    #         import pytspack
    #     except ImportError:
    #         print("You must install pytspack before using this function")

    #     # df_aod_nu = self._aeronet_aod_and_nu(row)
    #     aod_columns = [aod_column for aod_column in row.index if "aod_" in aod_column]
    #     aods = row[aod_columns]
    #     wv = [float(aod_column.replace("aod_", "").replace("nm", "")) for aod_column in aod_columns]
    #     a = pd.DataFrame({"aod": aods}).reset_index()
    #     a["wv"] = wv
    #     df_aod_nu = a.dropna()
    #     df_aod_nu_sorted = df_aod_nu.sort_values(by="wv").dropna()
    #     if len(df_aod_nu_sorted) < 2:
    #         return xi * np.NaN
    #     else:
    #         x, y, yp, sigma = pytspack.tspsi(
    #             df_aod_nu_sorted.wv.values, df_aod_nu_sorted.aod.values
    #         )
    #         yi = pytspack.hval(self.new_aod_values, x, y, yp, sigma)
    #         return yi

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

        Looks for

            AOD_1020 > 0.3 and AE(440,870) < 0.6

        and adds a Boolean `'dust'` column to :attr:`df`.
        """
        self.df["dust"] = (self.df["aod_1020nm"] > 0.3) & (
            self.df["440-870_angstrom_exponent"] < 0.6
        )

    def set_daterange(self, begin="", end=""):
        dates = pd.date_range(start=begin, end=end, freq="H").values.astype("M8[s]").astype("O")
        self.dates = dates
