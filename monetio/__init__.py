import importlib

__version__ = "0.2.7"


# Map reader names to their module paths for lazy loading
_READER_MODULES = {
    # Models
    "camx": ".readers.camx",
    "chimere": ".readers.chimere",
    "cmaq": ".readers.cmaq",
    "gdas": ".readers.gfs",
    "gefs": ".readers.gfs",
    "gfs": ".readers.gfs",
    "grib2": ".readers.grib2",
    "hrrr": ".readers.hrrr",
    "hysplit": ".readers.hysplit",
    "hytraj": ".readers.hytraj",
    "icap_mme": ".readers.icap_mme",
    "nam": ".readers.nam",
    "ncep_grib": ".readers.ncep_grib",
    "pardump": ".readers.pardump",
    "rap": ".readers.rap",
    "raqms": ".readers.raqms",
    "rrfs": ".readers.rrfs",
    "ufs": ".readers.ufs",
    "wrfchem": ".readers.wrfchem",
    # Obs
    "airnow": ".readers.airnow",
    "aeronet": ".readers.aeronet",
    "aqs": ".readers.aqs",
    "cems": ".readers.cems",
    "crn": ".readers.crn",
    "improve": ".readers.improve",
    "eprofile": ".readers.eprofile",
    "ish": ".readers.ish",
    "ish_lite": ".readers.ish_lite",
    "madis": ".readers.madis",
    "nadp": ".readers.nadp",
    "openaq": ".readers.openaq",
    "openaq_v2": ".readers.openaq_v2",
    "openaq_aws": ".readers.openaq_aws",
    "pams": ".readers.pams",
    "ndbc": ".readers.ndbc",
    "surfrad": ".readers.surfrad",
    "solrad": ".readers.solrad",
    "ndacc": ".readers.ndacc",
    "pandora": ".readers.pandora",
    "skynet": ".readers.skynet",
    # Profile
    "icartt": ".readers.icartt",
    "tolnet": ".readers.tolnet",
    "geoms": ".readers.geoms",
    "gml_ozonesonde": ".readers.gml_ozonesonde",
    "igra2": ".readers.igra2",
    "mplnet": ".readers.mplnet",
    "earlinet": ".readers.earlinet",
    "actris": ".readers.actris",
    "amdar": ".readers.amdar",
    "iagos": ".readers.iagos",
    "umbc_aerosol": ".readers.umbc_aerosol",
    # Sat
    "goes": ".readers.goes",
    "nesdis_edr_viirs": ".readers.nesdis_edr_viirs",
    "nesdis_eps_viirs": ".readers.nesdis_eps_viirs",
    "modis_ornl": ".readers.modis_ornl",
    "nasa_modis": ".readers.nasa_modis",
    "modis_l2": ".readers.modis_l2",
    "nesdis_frp": ".readers.nesdis_frp",
    "omps": ".readers.omps",
    "omps_nadir": ".readers.omps_nadir",
    "mopitt": ".readers.mopitt",
    "smap": ".readers.smap",
    "tempo": ".readers.tempo",
    "tropomi": ".readers.tropomi",
    "merra2": ".readers.merra2",
    "era5": ".readers.era5",
    "jpss_cris": ".readers.jpss_cris",
    "jpss_atms": ".readers.jpss_atms",
    "ncep_reanalysis": ".readers.ncep_reanalysis",
    "gpm_imerg": ".readers.gpm_imerg",
    "mrms": ".readers.mrms",
    "nesdis_viirs_jrr": ".readers.nesdis_viirs_jrr",
    "viirs_jrr": ".readers.nesdis_viirs_jrr",
    "gems": ".readers.gems",
    "sentinel4": ".readers.sentinel4",
    "calipso": ".readers.calipso",
    "earthcare": ".readers.earthcare",
    "tccon": ".readers.tccon",
    "ameriflux": ".readers.ameriflux",
}


def load(source: str, files=None, **kwargs):
    """
    Universal load function.

    Usage:
        ds = monetio.load("cmaq", files="/path/to/data*.nc")
        df = monetio.load("airnow", files=["2023-01-01", "2023-01-02"])

    Available sources:
        Models: camx, chimere, cmaq, era5, gdas, gefs, gfs, grib2, hrrr, hysplit, hytraj, icap_mme, merra2, nam, ncep_grib, ncep_reanalysis, pardump, rap, raqms, rrfs, ufs, wrfchem
        Obs: aeronet, airnow, ameriflux, aqs, cems, crn, eprofile, improve, ish, ish_lite, madis, nadp, ndacc, ndbc, openaq, openaq_aws, openaq_v2, pams, pandora, skynet, solrad, surfrad, tccon
        Profile: actris, amdar, earlinet, geoms, gml_ozonesonde, iagos, icartt, igra2, mplnet, tolnet, umbc_aerosol
        Sat: calipso, earthcare, gems, goes, gpm_imerg, jpss_atms, jpss_cris, modis_l2, modis_ornl, mopitt, mrms, nasa_modis, nesdis_edr_viirs, nesdis_eps_viirs, nesdis_frp, nesdis_viirs_jrr, omps, omps_nadir, sentinel4, smap, tempo, tropomi, viirs_jrr
    """
    from .readers.base import READER_REGISTRY

    if source not in READER_REGISTRY:
        if source in _READER_MODULES:
            # Lazy import
            importlib.import_module(_READER_MODULES[source], package="monetio")
        else:
            raise ValueError(
                f"Unknown source '{source}'. Available: {list(_READER_MODULES.keys())}"
            )

    if source not in READER_REGISTRY:
        # Should be registered by now if module was valid
        raise RuntimeError(f"Source '{source}' found in lazy index but failed to register itself.")

    # Instantiate the reader class and open data
    reader_cls = READER_REGISTRY[source]
    reader = reader_cls()

    return reader.open_dataset(files=files, **kwargs)


def virtualize(
    source: str,
    files,
    output: str,
    backend: str = "kerchunk",
    concat_dim: str = "time",
    **kwargs,
):
    """Pre-compute and persist VirtualiZarr references for a supported source.

    Parameters
    ----------
    source : str
        Registered reader source name (e.g. ``"gfs"`` or ``"merra2"``).
    files : str | list[str]
        Input file path(s) or glob expression.
    output : str
        Output reference path. For ``backend="kerchunk"``, this is a JSON file.
        For ``backend="icechunk"``, this is an Icechunk repository URL/path.
    backend : {"kerchunk", "icechunk"}, optional
        Virtualization backend, by default ``"kerchunk"``.
    concat_dim : str, optional
        Dimension used when combining multiple files, by default ``"time"``.
    **kwargs : dict
        Additional keyword arguments forwarded to :func:`load`.

    Returns
    -------
    xarray.Dataset
        Dataset opened through the virtualized references.
    """
    if backend not in {"kerchunk", "icechunk"}:
        raise ValueError(f"Invalid backend '{backend}'. Expected one of: 'kerchunk', 'icechunk'.")

    use_icechunk = backend == "icechunk"
    return load(
        source,
        files=files,
        use_virtualizarr=True,
        virtualizarr_file=None if use_icechunk else output,
        use_icechunk=use_icechunk,
        icechunk_url=output if use_icechunk else None,
        concat_dim=concat_dim,
        **kwargs,
    )


from . import grids
from .models import (
    camx,
    chimere,
    cmaq,
    hysplit,
    hytraj,
    ncep_grib,
    pardump,
    raqms,
)
from .obs import (
    aeronet,
    airnow,
    aqs,
    cems,
    crn,
    improve,
    ish,
    ish_lite,
    nadp,
    openaq,
    openaq_v2,
    openaq_v3,
    pams,
)
from .profile import geoms, gml_ozonesonde, icartt, tolnet
from .sat import goes

__all__ = [
    "__version__",
    "load",
    "virtualize",
    #
    # utility functions
    "rename_latlon",
    "rename_to_monet_latlon",
    "dataset_to_monet",
    "coards_to_netcdf",
    #
    # utility modules
    "grids",
    #
    # point obs
    "airnow",
    "aeronet",
    "aqs",
    "cems",
    "crn",
    "improve",
    "ish",
    "ish_lite",
    "nadp",
    "openaq",
    "openaq_v2",
    "openaq_v3",
    "pams",
    #
    # profile obs
    "geoms",
    "gml_ozonesonde",
    "icartt",
    "tolnet",
    #
    # satellite obs
    "goes",
    #
    # models
    "camx",
    "cmaq",
    "hysplit",
    "hytraj",
    "icap_mme",
    "ncep_grib",
    "pardump",
    "raqms",
    "chimere",
    "gfs",
    "gefs",
    "gdas",
    "hrrr",
    "nam",
    "rap",
]


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

    if latlon2d is False:
        ds = coards_to_netcdf(ds, lat_name=lat_name, lon_name=lon_name)

    ds = rename_to_monet_latlon(ds)

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

# Try to register grib2io codecs for zarr v3 / xarray if installed
try:
    import grib2io.codecs
except ImportError:
    pass
