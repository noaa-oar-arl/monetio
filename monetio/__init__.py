from . import grids, models, obs, profile, sat

__version__ = "0.1"

# __all__ = ["models", "obs", "sat", "util", "grids", "profile", "__version__"]
__all__ = [
    "__version__",
    #
    # utility functions here
    "rename_latlon",
    "rename_to_monet_latlon",
    "dataset_to_monet",
    "coards_to_netcdf",
    #
    # utility modules
    "grids",
    #
    "aeronet",
    "airnow",
    "aqs",
]

# point observations
airnow = obs.airnow
aeronet = obs.aeronet
aqs = obs.aqs
cems = obs.cems
crn = obs.crn
improve = obs.improve
ish = obs.ish
ish_lite = obs.ish_lite
nadp = obs.nadp
openaq = obs.openaq
pams = obs.pams

# models
fv3chem = models.fv3chem
cmaq = models.cmaq
camx = models.camx
prepchem = models.prepchem
ncep_grib = models.ncep_grib
# emitimes = models.emitimes
# cdump2netcdf = models.cdump2netcdf
hysplit = models.hysplit
hytraj = models.hytraj
pardump = models.pardump

# profiles
icartt = profile.icartt
tolnet = profile.tolnet

# sat
goes = sat.goes


def rename_latlon(ds):
    """Rename latitude/longitude to ``'lat'``/``'lon'``.

    Parameters
    ----------
    ds : xarray.Dataset

    Returns
    -------
    xarray.Dataset
        Dataset with possibly renamed latitude/longitude.
    """
    if "latitude" in ds.coords:
        return ds.rename({"latitude": "lat", "longitude": "lon"})
    elif "Latitude" in ds.coords:
        return ds.rename({"Latitude": "lat", "Longitude": "lon"})
    elif "Lat" in ds.coords:
        return ds.rename({"Lat": "lat", "Lon": "lon"})
    else:
        return ds


def rename_to_monet_latlon(ds):
    """Rename latitude/longitude to ``'latitude'``/``'longitude'``.

    Parameters
    ----------
    ds : xarray.Dataset

    Returns
    -------
    xarray.Dataset
        Dataset with possibly renamed latitude/longitude.

    See Also
    --------
    rename_latlon : renames to ``'lat'``/``'lon'`` instead
    """
    if "lat" in ds.coords:
        return ds.rename({"lat": "latitude", "lon": "longitude"})
    elif "Latitude" in ds.coords:
        return ds.rename({"Latitude": "latitude", "Longitude": "longitude"})
    elif "Lat" in ds.coords:
        return ds.rename({"Lat": "latitude", "Lon": "longitude"})
    elif "grid_lat" in ds.coords:
        return ds.rename({"grid_lat": "latitude", "grid_lon": "longitude"})
    else:
        return ds


def dataset_to_monet(ds, *, lat_name="lat", lon_name="lon", latlon2d=None):
    """Apply :func:`coards_to_netcdf` if `latlon2d` is False.

    Parameters
    ----------
    ds : xarray.Dataset
    lat_name, lon_name : str
        Current latitude and longitude names in `ds`.
    latlon2d : bool, optional
        If not provided, the value will be detected by examining ``.ndim``
        of the latitude variable.

    Returns
    -------
    xarray.Dataset
    """
    if latlon2d is None:
        ndim_lat = ds[lat_name].ndim
        assert ndim_lat <= 2
        latlon2d = ndim_lat == 2
    # TODO: apply rename_to_monet_latlon ?
    if latlon2d is False:
        ds = coards_to_netcdf(ds, lat_name=lat_name, lon_name=lon_name)
    return ds


def coards_to_netcdf(ds, *, lat_name="lat", lon_name="lon"):
    """Assign 2-D latitude/longitude grid from 1-D latitude/longitude variables,
    setting ``'x'`` and ``'y'`` as 1-D zero-based index arrays.

    Also normalizes the latitude/longitude names to ``'latitude'``/``'longitude'``,
    with dimensions ``('y', 'x')``.

    .. note::
       The name is a reference to the COARDS conventions.

    Parameters
    ----------
    ds : xarray.Dataset
    lat_name, lon_name : str
        Current latitude and longitude names in `ds`.

    Returns
    -------
    xarray.Dataset
    """
    from numpy import arange, meshgrid

    lon = ds[lon_name]
    lat = ds[lat_name]
    assert lon.ndim == lat.ndim == 1
    lons, lats = meshgrid(lon, lat)
    x = arange(len(lon))
    y = arange(len(lat))
    ds = ds.rename({lon_name: "x", lat_name: "y"})
    ds.coords["longitude"] = (("y", "x"), lons)
    ds.coords["latitude"] = (("y", "x"), lats)
    ds["x"] = x
    ds["y"] = y
    ds = ds.set_coords(["latitude", "longitude"])
    return ds
