""" this will read the goes_r data"""

import pandas as pd
import xarray as xr

try:
    import s3fs

    has_s3fs = True
except ImportError:
    print(
        "Please install s3fs if retrieving from the Amazon S3 Servers.  Otherwise continue with local data"
    )
    has_s3fs = False

try:
    import h5py  # noqa: F401

    has_h5py = True
except ImportError:
    print("Please install h5py to open files from the Amazon S3 servers.")
    has_h5py = False

try:
    import h5netcdf  # noqa: F401

    has_h5netcdf = True
except ImportError:
    print("Please install h5netcdf to open files from the Amazon S3 servers.")
    has_h5netcdf = False

from ..grids import _geos_16_grid


def _get_swath_from_fname(fname):
    vert_grid_num = fname.split(".")[-4].split("v")[-1]
    hori_grid_num = fname.split(".")[-4].split("v")[0].split("h")[-1]
    return hori_grid_num, vert_grid_num


def _get_time_from_fname(fname):
    import pandas as pd

    u = pd.Series([fname.split(".")[-2]])
    date = pd.to_datetime(u, format="%Y%j%H%M%S")[0]
    return date


def _open_single_file(fname):
    # open the file
    dset = xr.open_dataset(fname)
    dset = dset.rename({"t": "time"})
    # get the area def
    area = _geos_16_grid(dset)
    dset.attrs["area"] = area
    # get proj4 string
    dset.attrs["proj4_srs"] = area.proj_str
    # get longitude and latitudes
    lon, lat = area.get_lonlats_dask()
    dset.coords["longitude"] = (("y", "x"), lon)
    dset.coords["latitude"] = (("y", "x"), lat)

    for i in dset.variables:
        dset[i].attrs["proj4_srs"] = area.proj_str
        dset[i].attrs["area"] = area

    # expand dimensions for time
    dset = dset.expand_dims("time")
    return dset


def open_dataset(date=None, filename=None, satellite="16", product=None):
    g = GOES()
    if filename is None:
        try:
            if date is None:
                raise ValueError
            if product is None:
                raise ValueError
        except ValueError:
            print("Please provide a date and product to be able to retrieve data from Amazon S3")
        ds = g.open_amazon_file(date=date, satellite=satellite, product=product)
    else:
        ds = g.open_local(filename)
    return ds


class GOES:
    def __init__(self):
        self.date = None
        self.satellite = "16"
        self.product = "ABI-L2-AODF"
        self.baseurl = f"s3://noaa-goes{self.satellite}/"
        self.url = f"{self.baseurl}"
        self.filename = None
        self.fs = None

    def _update_baseurl(self):
        self.baseurl = f"s3://noaa-goes{self.satellite}/"

    def set_product(self, product=None):
        try:
            if product is None:
                raise ValueError
            else:
                self.url = f"{self.baseurl}{product}/"
        except ValueError:
            print("kwarg product must have a value")

    def get_products(self):
        products = [value.split("/")[-1] for value in self.fs.ls(self.baseurl)[:-1]]
        return products

    def date_to_url(self):
        date = pd.Timestamp(self.date)
        date_url_bit = date.strftime("%Y/%j/%H/")
        self.url = f"{self.url}{date_url_bit}"

    def _get_files(self, url=None):
        try:
            files = self.fs.ls(url)
            if len(files) < 1:
                raise ValueError
            else:
                return files
        except ValueError:
            print("Files not available for product and date")

    def _get_closest_date(self, files=[]):
        file_dates = [pd.to_datetime(f.split("_")[-1][:-4], format="c%Y%j%H%M%S") for f in files]
        date = pd.Timestamp(self.date)
        nearest_date = min(file_dates, key=lambda x: abs(x - date))
        nearest_date_str = nearest_date.strftime("c%Y%j%H%M%S")
        found_file = [f for f in files if nearest_date_str in f][0]
        return found_file

    def _set_s3fs(self):
        if has_s3fs:
            self.fs = s3fs.S3FileSystem(anon=True)
        else:
            self.fs = None

    def _product_exists(self, product):
        try:
            if has_s3fs:
                products = self.get_products()
                if product not in products:
                    raise ValueError
                else:
                    return product
            else:
                raise ImportError
        except ImportError:
            print("Please install s3fs to retrieve product information from Amazon S3")
        except ValueError:
            print("Product: ", product, "not found")
            print("Available products:")
            for i in products:
                print("    ", i)

    def open_amazon_file(self, date=None, product=None, satellite="16"):
        self.date = pd.Timestamp(date)
        self.satellite = satellite
        self._update_baseurl()
        self._set_s3fs()
        self.product = self._product_exists(product)
        self.url = f"{self.baseurl}{self.product}/"  # add product to url
        self.date_to_url()  # add date to url

        # find closest file to give date
        files = self._get_files(url=self.url)
        f = self._get_closest_date(files=files)

        # open file object
        fo = self.fs.open(f)
        out = xr.open_dataset(fo, engine="h5netcdf")
        out = self._get_grid(out)
        return out

    def _get_grid(self, ds):
        from numpy import meshgrid, ndarray
        from pyproj import CRS, Proj

        proj_dict = ds.goes_imager_projection.attrs
        for i in proj_dict.keys():
            if type(proj_dict[i]) is ndarray:
                proj_dict[i] = proj_dict[i][0]
        crs = CRS.from_cf(proj_dict)
        ds.attrs["projection"] = crs.to_wkt()
        proj = Proj(crs)
        satellite_height = ds.goes_imager_projection.perspective_point_height
        xx, yy = meshgrid(ds.x.values * satellite_height, ds.y.values * satellite_height)
        lon, lat = proj(xx, yy, inverse=True)
        ds["latitude"] = (("y", "x"), lat)
        ds["longitude"] = (("y", "x"), lon)
        ds["longitude"] = ds.longitude.where(ds.longitude < 400).fillna(1e30)
        ds["latitude"] = ds.latitude.where(ds.latitude < 100).fillna(1e30)
        ds = ds.set_coords(["latitude", "longitude"])
        return ds

    def open_local(self, f):
        # open file object
        fo = self.fs.open(f)
        out = xr.open_dataset(fo, engine="h5netcdf")
        out = self._get_grid(out)
        return out
