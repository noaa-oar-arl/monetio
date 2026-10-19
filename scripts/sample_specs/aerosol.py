"""Ground-based remote-sensing / profile / ship capture specs.

These sources retrieve aerosol lidar and sun-photometer profiles (ACTRIS,
EARLINET, ICARTT, GEOMS/PANDORA-family), speciated-monitor and MPLNet networks,
aircraft particle mass spectrometry (PAMS), and ship-based Integrated Surface
Database files (ISH/ISH-lite). Payloads are text or HDF/NetCDF pulled over
HTTP(S)/FTP/S3. A recorded payload exercises the reader's own location
construction and parser against bytes the service actually served;
credential-gated or non-redistributable sources fall back to a synthetic
reproduction or a reasoned exclusion.
"""

from __future__ import annotations

CAPTURES: dict[str, dict] = {
    "surfrad": {
        "urls": [
            (
                "https://gml.noaa.gov/aftp/data/radiation/surfrad/Bondville_IL/2024/bon24001.dat",
                "bon24001.dat",
            )
        ],
        # The SURFRAD 5-minute product is a full day of fixed-width rows; the
        # first rows carry the header pair and the column layout the reader
        # maps onto its standardized variable names.
        "max_lines": {"bon24001.dat": 40},
        "note": "one SURFRAD station-day file, trimmed",
    },
    "actris": {
        "urls": [
            (
                "https://thredds.nilu.no/thredds/fileServer/ebas/"
                "AT0001R.19721101000000.20181210133000.filter_1pack."
                "sulphate_total.aerosol.2mo.1d.AT01L_f1p_01.AT01L_XRF.lev2.nc",
                "AT0001R.sulphate_total.lev2.nc",
            )
        ],
        # The EBAS THREDDS catalog serves one NetCDF per compound and averaging
        # interval; the smallest daily sulphate record is a complete lev2 file
        # that runs the reader's NetCDF branch (open_mfdataset, coordinate
        # renames, history) without any trimming -- binary payloads are never
        # line-truncated, so the whole recording stands as served.
        "note": "one EBAS daily sulphate NetCDF from the THREDDS file server",
    },
    "ndacc": {
        "urls": [
            (
                "https://www-air.larc.nasa.gov/pub/NDACC/PUBLIC/stations/"
                "mauna.loa.hi/hdf/uvvis.dobson/"
                "groundbased_uvvis.dobson_noaa.esrl076_mauna.loa.hi_"
                "20190801t203100z_20190831t181808z_002.hdf",
                "groundbased_uvvis.dobson_mauna.loa.hi_201908.hdf",
            )
        ],
        # The NDACC reader inherits the GEOMS file dispatch, which decodes the
        # HDF4 Dobson records the LaRC public station tree serves; one monthly
        # file is already small enough to check in whole, exercising the pyhdf
        # branch and the GEOMS preprocessing.
        "requires": "pyhdf",
        "note": "one NDACC Dobson monthly HDF4 from the LaRC public station tree",
    },
    "geoms": {
        "urls": [
            (
                "https://www-air.larc.nasa.gov/pub/NDACC/PUBLIC/stations/"
                "mauna.loa.hi/hdf/lidar/"
                "groundbased_lidar.aerosol_nasa.jpl002_glass.1.1_"
                "mauna.loa.hi_20040109t045500z_20040109t065531z_001.hdf",
                "groundbased_lidar_aerosol_mauna.loa.hi_20040109.hdf",
            )
        ],
        # A GEOMS HDF4 lidar profile from the same public NDACC tree: the reader
        # dispatches on the .hdf extension into its pyhdf branch, so the
        # recording runs the HDF4 decode, the variable renames, and the squeeze
        # of the profile dimensions end to end.
        "requires": "pyhdf",
        "note": "one GEOMS HDF4 lidar profile, recorded whole",
    },
}

# Ground-based/profile sources not sampleable offline, with reasons. A source
# lands here only when the cause is verified against the service or the reader
# itself, not merely assumed.
EXCLUSIONS: dict[str, str] = {
    "solrad": (
        "the reader's fixed-width column specification is off by one against the "
        "service payload — every row parses with blank date fields and a NaT "
        "timestamp, on both the standard and the Madison layouts and on the "
        "untruncated station-day files — so the expansion to (time, node) fails "
        "before any data is returned; the bytes are retrievable but the current "
        "parser cannot load them"
    ),
    "mplnet": (
        "the reader has no remote location construction — open_dataset takes only "
        "user-supplied file paths — and no anonymous copy of the NetCDF exists: the "
        "LP DAAC public-collection paths return HTTP 404, the protected collection "
        "answers HTTP 401 without Earthdata credentials, and the mission's own data "
        "hosts do not resolve"
    ),
    "skynet": (
        "the data path the reader's build_urls constructs answers HTTP 200 with an "
        "HTML portal page (Content-Type text/html) instead of the .AOT payload for "
        "every site and date tried, and the bulk-download agreement page exposes no "
        "direct file links, so no payload can be recorded"
    ),
    "earlinet": (
        "the reader has no remote location construction — open_dataset takes only "
        "user-supplied file paths — and the mission's archive is not anonymously "
        "reachable: the data host returns HTTP 404 for its L2 listing and the "
        "download portal requires a registered account"
    ),
    "eprofile": (
        "the reader has no remote location construction — open_dataset raises "
        "NotImplementedError when asked to retrieve by date — and the hub host the "
        "network documented does not complete a TLS handshake (unrecognized name "
        "alert), so no payload can be recorded"
    ),
}
