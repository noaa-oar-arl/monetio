"""Gridded/model capture specs (gridded products, reanalysis, model output).

Gridded sources retrieve NetCDF/HDF/grib products from NASA/NOAA archives, or
are model output that must be reproduced synthetically. Recorded payloads are
binary and cannot be trimmed line-by-line; where a public small product exists
it is recorded, otherwise a synthetic reproduction is added to
``sample_specs.model``. Sources gated behind credentials (Earthdata, an API key)
that cannot be anonymized are recorded as reasoned exclusions instead.
"""

from __future__ import annotations

# The NCEP GRIB products on the AWS Public Dataset are large because each file
# stacks hundreds of fields; the capture records only the leading GRIB2 message
# (see ``_fetch_first_grib2_message`` in record_samples.py), which is a complete
# single-field file that still runs the whole decode path. The 1.0-degree GFS/GDAS
# products are used where offered so the message stays ~75 KB; GEFS ships only at
# 0.25/0.5 degree, so its 0.5-degree ensemble-mean field is the smallest
# available. The URLs are the exact objects the readers' own build_urls produce
# for the sampled date, so a location-construction regression is caught.
#
# ``save_index=False`` is recorded as a load argument for every GRIB sample:
# the decoding engine caches a binary message index next to each file it opens,
# and those cache files must never be written into the checked-in sample tree
# (the payload enumeration ignores them, but keeping them out is cleaner).
# ``requires="grib2io"`` marks the engine needed to decode them: the load rules
# skip (rather than fail) in an environment where it is not installed.
_GRIB_KWARGS = {"save_index": False}
_GRIB = {"transform": "first-grib2-message", "load_kwargs": _GRIB_KWARGS, "requires": "grib2io"}

_CAP = "scripts/record_samples.py"

CAPTURES: dict[str, dict] = {
    "gfs": {
        **_GRIB,
        "urls": [
            (
                "s3://noaa-gfs-bdp-pds/gfs.20250324/00/atmos/gfs.t00z.pgrb2.1p00.f000",
                "gfs.t00z.pgrb2.1p00.f000",
            )
        ],
        "layout": "pgrb2.1p00",
        "note": "leading field of a GFS 1.0-degree GRIB2 product",
    },
    "gefs": {
        **_GRIB,
        "urls": [
            (
                "s3://noaa-gefs-pds/gefs.20250324/00/atmos/pgrb2ap5/geavg.t00z.pgrb2a.0p50.f000",
                "geavg.t00z.pgrb2a.0p50.f000",
            )
        ],
        "layout": "pgrb2a.0p50",
        "note": "leading field of a GEFS 0.5-degree ensemble-mean GRIB2 product",
    },
    "gdas": {
        **_GRIB,
        "urls": [
            (
                "s3://noaa-gfs-bdp-pds/gdas.20250324/00/atmos/gdas.t00z.pgrb2.1p00.f000",
                "gdas.t00z.pgrb2.1p00.f000",
            )
        ],
        "layout": "pgrb2.1p00",
        "note": "leading field of a GDAS 1.0-degree GRIB2 product",
    },
    "ncep_grib": {
        # The generic NCEP GRIB reader takes a plain file path and decodes through
        # the same engine as the GFS family; a GFS analysis field exercises it.
        **_GRIB,
        "urls": [
            (
                "s3://noaa-gfs-bdp-pds/gfs.20250324/00/atmos/gfs.t00z.pgrb2.1p00.f000",
                "ncep_grib_sample.grib2",
            )
        ],
        "note": "one NCEP GRIB2 field decoded by the generic reader",
    },
    "grib2": {
        # The generalized GRIB2 reader opens any GRIB2 file; a GDAS field gives a
        # distinct payload from the ncep_grib sample so the two are not clones.
        **_GRIB,
        "urls": [
            (
                "s3://noaa-gfs-bdp-pds/gdas.20250324/00/atmos/gdas.t00z.pgrb2.1p00.f000",
                "grib2_sample.grib2",
            )
        ],
        "note": "one GRIB2 field decoded by the generalized reader",
    },
}

# Gridded/model sources not sampleable offline (C-SAMP-10), with reasons.
EXCLUSIONS: dict[str, str] = {
    "rrfs": (
        "RRFS operational output (s3://noaa-rrfs-pds/rrfs_a/...) has been retired from the "
        "NOAA Public Dataset, so the reader's own build_urls target no longer resolves and the "
        "payload cannot be recorded; the GRIB2 format itself is covered by the gfs/gefs/gdas "
        "samples, so a synthetic reproduction would add no decode coverage."
    ),
    "merra2": (
        "MERRA-2 is served only over NASA Earthdata OPeNDAP, which the reader reaches via "
        "build_urls and a .netrc login (setup_netrc); an anonymous read is rejected, so the "
        "payload cannot be recorded without a credential the offline suite cannot present. The "
        "format is plain NetCDF, already exercised by the recorded reanalysis/GRIB samples, so a "
        "synthetic reproduction would add no decode coverage beyond those."
    ),
    "icap_mme": (
        "The ICAP-MME archive the reader's build_urls targets "
        "(https://usgodae.org/ftp/outgoing/nrl/ICAP-MME/...) no longer serves the product — an "
        "anonymous listing returns HTTP 404 — so the payload cannot be recorded. Its NetCDF "
        "aerosol-optimical-depth field is the same container the recorded gridded samples "
        "decode, so a synthetic reproduction would add no format coverage."
    ),
}
