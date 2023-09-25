"""
AERONET
"""
import warnings
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
    *,
    #
    # post-proc
    freq=None,
    detect_dust=False,
    interp_to_aod_values=None,
):
    """Read a local file downloaded from the AERONET Web Service.

    Parameters
    ----------
    fname
        Suitable input for :func:`pandas.read_csv`, e.g. a relative path as a string
        or a :term:`path-like <path-like object>`.
    """
    a = AERONET()

    # Detect whether inv should be left as None or not
    with open(fname) as f:
        if "Inversion" in f.readline():
            a.inv_type = True
    # TODO: actually detect the specific inv type

    a.new_aod_values = interp_to_aod_values
    if a.new_aod_values is not None and not a.prod.startswith("AOD"):
        print("`interp_to_aod_values` will be ignored")

    a.url = fname
    try:
        a.read_aeronet()
    except Exception as e:
        raise Exception(f"loading file {fname!r} failed.") from e

    # TODO: DRY wrt. class?
    if freq is not None:
        a.df = a.df.set_index("time").groupby("siteid").resample(freq).mean().reset_index()

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
    lunar=False,
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
        Site identifier string.

        See https://aeronet.gsfc.nasa.gov/aeronet_locations_v3.txt for all valid site IDs.

        .. warning::
           Whether you will obtain data depends on the sites active
           during the `dates` time period.

        .. note::
           `siteid` takes precedence over `latlonbox`
           if both are specified.
    daily : bool
        Load daily averaged data.
    lunar : bool
        Load provisional lunar "Direct Moon" data instead of the default "Direct Sun".
        Only for non-inversion products.
    freq : str
        Frequency used to resample the DataFrame.
    detect_dust : bool
    interp_to_aod_values : array-like of float
        Values to interpolate AOD values to.

        Currently requires pytspack.
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
        lunar=lunar,
        detect_dust=detect_dust,
        interp_to_aod_values=interp_to_aod_values,
    )

    requested_parallel = n_procs != 1

    # Split up by day
    dates = pd.to_datetime(dates)
    if dates is not None:
        min_date = dates.min()
        max_date = dates.max()
        time_bounds = pd.date_range(start=min_date, end=max_date, freq="D")
        if max_date not in time_bounds:
            time_bounds = time_bounds.append(pd.DatetimeIndex([max_date]))

    if has_joblib and requested_parallel and dates is not None and len(time_bounds) > 2:
        dfs = Parallel(n_jobs=n_procs, verbose=verbose)(
            delayed(_parallel_aeronet_call)(pd.DatetimeIndex([t1, t2]), **kwargs, freq=None)
            for t1, t2 in zip(time_bounds[:-1], time_bounds[1:])
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
    """Load the AERONET site list as a :class:`~pandas.DataFrame`,
    reading from https://aeronet.gsfc.nasa.gov/aeronet_locations_v3.txt.
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
    lunar=False,
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
        lunar=lunar,
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
        self.lunar = None
        self.latlonbox = None
        self.siteid = None

        self.new_aod_values = None

    def build_url(self):
        """Use attributes to build a URL and set :attr:`url`.

        Targeting either of
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

        if self.lunar is not None:
            if self.lunar in {0, 1}:
                lunar_ = f"&lunar_merge={self.lunar}"
            else:
                raise ValueError(f"invalid lunar setting {self.lunar!r}")
        else:
            lunar_ = ""

        if self.siteid is not None:
            # Validate here, since the Web Service doesn't do any validation and just returns all
            # sites if the site isn't valid.
            # Note that having a valid site ID doesn't mean there will be any data
            # (depends on time period).
            if self.siteid in get_valid_sites().siteid.values:
                loc_ = f"&site={self.siteid}"
            else:
                raise ValueError(f"invalid site {self.siteid!r}")
        elif self.latlonbox is None:
            loc_ = ""
        else:
            lat1 = str(float(self.latlonbox[0]))
            lon1 = str(float(self.latlonbox[1]))
            lat2 = str(float(self.latlonbox[2]))
            lon2 = str(float(self.latlonbox[3]))
            loc_ = f"&lat1={lat1}&lat2={lat2}&lon1={lon1}&lon2={lon2}"

        self.url = f"{base_url}{dates_}{product_}{avg_}{lunar_}{inv_type_}{loc_}&if_no_html=1"

    def _lines_from_url(self, *, n=10):
        """Read the first `n` lines from the URL using `requests`,
        returning the result as a string.
        """
        from itertools import islice

        if isinstance(self.url, str) and self.url.startswith("http"):
            import requests

            r = requests.get(self.url, stream=True)
            r.raise_for_status()
            s = "\n".join(islice(r.iter_lines(decode_unicode=True), n))
        else:
            with open(self.url) as f:
                s = "\n".join(islice(f, n))

        return s

    def read_aeronet(self):
        """Load a DataFrame from :attr:`url`, setting :attr:`df`."""
        print("Reading Aeronet Data...")
        inv = self.inv_type is not None
        skiprows = 5 if not inv else 6

        # Get info lines (before the header line with column names)
        info = self._lines_from_url(n=skiprows)
        if len(info.splitlines()) == 1:
            # e.g. "AERONET Data Download (Version 3 Direct Sun)"
            raise Exception("valid query but no data found")
        elif info.startswith("<html>"):
            # Web Service showing an error message on the page (or `&if_no_html=1` manually removed)
            # With the `build_url` validation, we shouldn't get here
            raise Exception("invalid query, open the URL to check the error")

        df = pd.read_csv(
            self.url,
            engine="python",
            header="infer",
            skiprows=skiprows,
            parse_dates={"time": [1, 2]},
            usecols=None,
            # ^ SDA header is missing one column (80 vs 81 in data) and we lose one making 'time'
            date_parser=lambda x: datetime.strptime(x, r"%d:%m:%Y %H:%M:%S"),
            na_values=-999,
        )
        df.rename(columns=str.lower, inplace=True)
        df.rename(
            columns={
                "aeronet_site": "siteid",
                "aeronet_aeronet_site": "siteid",  # sometimes happens?
                # non-inv
                "site_latitude(degrees)": "latitude",
                "site_longitude(degrees)": "longitude",
                "site_elevation(m)": "elevation",
                # inv
                "latitude(degrees)": "latitude",
                "longitude(degrees)": "longitude",
                "elevation(m)": "elevation",
            },
            inplace=True,
        )
        if df.siteid.unique().size == 1:
            df.set_index("time", inplace=True)
        df.dropna(subset=["latitude", "longitude"], inplace=True)
        df.dropna(axis=1, how="all", inplace=True)  # empty columns
        if hasattr(df, "attrs"):
            df.attrs["info"] = info
        self.df = df

    def add_data(
        self,
        dates=None,
        product="AOD15",
        *,
        inv_type=None,
        siteid=None,
        latlonbox=None,
        daily=False,
        lunar=False,
        #
        # post-proc
        freq=None,
        detect_dust=False,
        interp_to_aod_values=None,
    ):
        """Use :meth:`build_url` to set a URL, then read a DataFrame from it
        and set :attr:`df`.
        """
        self.latlonbox = latlonbox
        self.siteid = siteid
        if dates is None:  # get the current day
            now = datetime.utcnow()
            self.dates = pd.date_range(start=now.date(), end=now, freq="H")
        else:
            self.dates = pd.DatetimeIndex(dates)
        if product is not None:
            self.prod = product.upper()
        else:
            self.prod = product
        self.inv_type = inv_type
        if daily:
            self.daily = 20  # daily data
        else:
            self.daily = 10  # all points
        if lunar:
            self.lunar = 1  # provisional lunar data
        else:
            self.lunar = 0  # no lunar
        self.new_aod_values = interp_to_aod_values
        if self.new_aod_values is not None and not self.prod.startswith("AOD"):
            print("`interp_to_aod_values` will be ignored")

        self.build_url()
        try:
            self.read_aeronet()
        except Exception as e:
            raise Exception(
                f"loading from URL {self.url!r} failed. "
                "If using `siteid`, check that the site is valid."
            ) from e

        if freq is not None:
            self.df = (
                self.df.set_index("time").groupby("siteid").resample(freq).mean().reset_index()
            )

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
            except ImportError as e:
                raise RuntimeError(
                    "You must install pytspack before using this function.\n"
                    "See https://github.com/noaa-oar-arl/pytspack/"
                ) from e

            new_wv = np.asarray(new_wv)

            # df_aod_nu = self._aeronet_aod_and_nu(row)
            aod_columns = [aod_column for aod_column in row.index if aod_column.startswith("aod_")]
            aods = row[aod_columns]
            wv = [
                float(aod_column.replace("aod_", "").replace("nm", ""))
                for aod_column in aod_columns
            ]
            # TODO: the non-daily product has `exact_wavelengths_of_aod(um)_<wavelength>nm` that could be used
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
        dup_names = list(set(self.df) & set(out))
        if dup_names:
            # Rename old cols, assuming preference for the specified new wavelengths
            suff = "_orig"
            warnings.warn(
                f"Renaming duplicate AOD columns {dup_names} by adding suffix '{suff}'.",
                stacklevel=2,
            )
            for name in dup_names:
                self.df = self.df.rename(columns={name: f"{name}{suff}"})
                if self.daily == 10:  # all data
                    wl = name[4:-2]
                    ename = f"exact_wavelengths_of_aod(um)_{wl}nm"
                    ename_new = f"exact_wavelengths_of_aod(um)_{wl}nm{suff}"
                    self.df = self.df.rename(columns={ename: ename_new})
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
