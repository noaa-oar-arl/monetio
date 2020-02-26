from . import grids, models, obs, profile, sat

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

# models
fv3chem = models.fv3chem
cmaq = models.cmaq
camx = models.camx
prepchem = models.prepchem
ncep_grib = models.ncep_grib

# profiles
icartt = profile.icartt
tolnet = profile.tolnet

# sat

__all__ = ["models", "obs", "sat", "util", "grids", "profile"]


def rename_latlon(ds):
    """Short summary.

    Parameters
    ----------
    ds : type
        Description of parameter `ds`.

    Returns
    -------
    type
        Description of returned object.

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
    """Short summary.

    Parameters
    ----------
    ds : type
        Description of parameter `ds`.

    Returns
    -------
    type
        Description of returned object.

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


def dataset_to_monet(dset, lat_name="lat", lon_name="lon", latlon2d=False):
    if len(dset[lat_name].shape) != 2:
        latlon2d = False
    if latlon2d is False:
        dset = coards_to_netcdf(dset, lat_name=lat_name, lon_name=lon_name)
    return dset


def coards_to_netcdf(dset, lat_name="lat", lon_name="lon"):
    from numpy import meshgrid, arange

    lon = dset[lon_name]
    lat = dset[lat_name]
    lons, lats = meshgrid(lon, lat)
    x = arange(len(lon))
    y = arange(len(lat))
    dset = dset.rename({lon_name: "x", lat_name: "y"})
    dset.coords["longitude"] = (("y", "x"), lons)
    dset.coords["latitude"] = (("y", "x"), lats)
    dset["x"] = x
    dset["y"] = y
    dset = dset.set_coords(["latitude", "longitude"])
    return dset
