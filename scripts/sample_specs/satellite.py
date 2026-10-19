"""Satellite source capture specs (HDF-EOS5, NetCDF4, JSON).

Satellite products are typically retrieved from a NASA/NOAA archive over HTTPS
or an anonymous object store. The payload is binary (HDF, NetCDF), so it cannot
be trimmed line-by-line; a sample is either a small product file or a subset
built by the capture script from a larger retrieval.

Two archives are reachable without any credential and are recorded here: the
JPSS Risk Reduction VIIRS products on the NOAA public S3 dataset, and the
NESDIS STAR aerosol archive. The NASA Earthdata-gated pools (MODIS, TROPOMI)
and the NESDIS services that have been withdrawn or taken offline are recorded
as reasoned exclusions rather than approximated with synthetic files, because a
synthetic reproduction cannot certify what the real service returns.
"""

from __future__ import annotations

CAPTURES: dict[str, dict] = {
    # The JPSS Risk Reduction VIIRS aerosol products are published on the NOAA
    # public S3 dataset, anonymously readable, and are NetCDF4/HDF5 that the
    # reader opens directly. A granule is a full 768 x 3200 orbit swath, so even
    # the smallest available is ~8.4 MB: larger than the usual sample, but the
    # file is a single self-contained granule and there is no smaller official
    # product to record — trimming bytes would not leave a valid NetCDF. The
    # granule is the smallest one found across the three JPSS satellites and
    # several days, which keeps the recorded payload to the format's floor.
    #
    # Only the most recent date range is retained in the bucket (older years are
    # aged out), so the location is current rather than historical; refreshing
    # this sample means re-listing the bucket for a fresh day. The AOD product is
    # recorded because it is the reader's default product and the one whose
    # variable renaming (AOD550 to aod_550) the sample exercises. viirs_jrr is a
    # second name for the same reader class, so this one granule covers both.
    "nesdis_viirs_jrr": {
        "urls": [
            (
                "s3://noaa-nesdis-snpp-pds/VIIRS-JRR-AOD/2026/08/29/"
                "JRR-AOD_v3r2_npp_s202608290135520_e202608290137144_c202608290303114.nc",
                "JRR-AOD_v3r2_npp_s202608290135520_e202608290137144_c202608290303114.nc",
            )
        ],
        "layout": "AOD",
        "note": (
            "one SNPP VIIRS JRR AOD orbit granule from the NOAA public S3 dataset; "
            "~8.4 MB is the smallest granule available because the product is a fixed "
            "full-swath file that cannot be truncated and stay valid"
        ),
    },
    # The NESDIS EPS VIIRS aerosol product is served from the STAR archive. The
    # reader's own URLs name the historical layout
    # (pub/smcd/VIIRS_Aerosol/npp.viirs.aerosol.data/epsaot550/<year>/
    #  npp_eaot_ip_gridded_0.25_<date>.high.nc); those year directories still
    # exist but are now empty, the product having been reissued under a new path
    # and filename. The recorded file is the same daily 0.25-degree global EPS
    # AOD grid at its current location, so the payload is the real product even
    # though the location construction has moved.
    #
    # The archive is plain FTP at ftp.star.nesdis.noaa.gov, but fsspec's FTP
    # backend cannot parse this server's directory listing (it raises KeyError
    # 'type' while building the entry details), so a ftp:// location cannot be
    # fetched by the capture script. The identical bytes are served over HTTPS
    # from the same host tree, verified to hash the same as the FTP transfer, so
    # the location is recorded as the HTTPS form.
    #
    # The grid carries AOD550, AE, and their count/min/max/std statistics on a
    # 720 x 1440 lat-lon grid, which the reader turns into the standardized y/x
    # dimensions with generated latitude/longitude coordinates.
    "nesdis_eps_viirs": {
        "urls": [
            (
                "https://www.star.nesdis.noaa.gov/pub/smcd/VIIRS_Aerosol/"
                "viirs_aerosol_gridded_data/snpp/aod/eps/2025/"
                "viirs_eps_npp_aod_0.250_deg_20251112_nrt.nc",
                "viirs_eps_npp_aod_0.250_deg_20251112_nrt.nc",
            )
        ],
        "layout": "eps-0.25deg",
        "note": (
            "one daily 0.25-degree global NESDIS EPS VIIRS AOD grid; recorded from the "
            "HTTPS form of the STAR archive because the FTP listing cannot be parsed by "
            "the capture script's file store, and from the product's current location "
            "since the path the reader builds is now an empty directory"
        ),
    },
    # The GOES ABI aerosol optical depth product is published on the NOAA public
    # S3 dataset (noaa-goes16), anonymously readable, and is NetCDF4 that the
    # reader opens directly. The reader's default product is the full-disk AODF,
    # but that granule is ~19 MB; the smaller CONUS AODC granule (~4 MB) carries
    # the identical goes_imager_projection variable, so it drives the same
    # decode path (fixed grid, projection-based lat/lon, time from
    # time_coverage_start) at a quarter of the size. The layout records the
    # product so the multi-layout coverage is explicit; the file is a single
    # self-contained granule that cannot be truncated and stay valid NetCDF.
    "goes": {
        "urls": [
            (
                "s3://noaa-goes16/ABI-L2-AODC/2025/083/12/"
                "OR_ABI-L2-AODC-M6_G16_s20250831201172_e20250831203545_c20250831206433.nc",
                "OR_ABI-L2-AODC-M6_G16_s20250831201172_e20250831203545_c20250831206433.nc",
            )
        ],
        "layout": "ABI-L2-AODC",
        "load_kwargs": {"product": "ABI-L2-AODC"},
        "note": (
            "one GOES-16 ABI CONUS AOD granule from the NOAA public S3 dataset; the "
            "smaller AODC product is used over the default full-disk AODF because it "
            "carries the same goes_imager_projection and so exercises the identical "
            "decode path at a quarter of the payload size"
        ),
    },
}

# Satellite sources with no offline sample, each with the reason it cannot be
# recorded. Most NASA archive payloads require an Earthdata login the offline
# suite cannot present, and several serving hosts have been taken offline.
EXCLUSIONS: dict[str, str] = {
    "tropomi": (
        "TROPOMI L2 granules cannot be recorded from a location a reviewer can re-fetch. "
        "NASA's ASDC answers an anonymous request with HTTP 301 to the Earthdata Login OAuth "
        "prompt, Copernicus requires a registered account, and the one anonymous mirror found "
        "(Microsoft Planetary Computer) only serves time-limited signed URLs — the unsigned "
        "blob answers HTTP 409 'PublicAccessNotPermitted' — so the plain location the capture "
        "would record stops working within the hour. Independently, the reader cannot load a "
        "real granule at all: its preprocessing raises ValueError \"the new name 'time' "
        'conflicts", because the product ships the time coordinate as a scalar dimension '
        "alongside the scanline axis, so the rename that harmonizes scanline onto time "
        "collides with the name already in use. Verified on both an aerosol-layer granule "
        "(4.2 MB) and the canonical NO2 granule (64.8 MB), which is also far above the sample "
        "size budget."
    ),
    "modis_l2": (
        "MODIS L2 swath products are HDF4, and none of the installed xarray engines "
        "(netcdf4, h5netcdf, scipy, grib2io) can open an HDF4 file, so a recorded payload "
        "could never load — the failure is a missing format engine, not a wrong group "
        "selection. The archive is gated as well: an anonymous request to the LP DAAC data "
        "pool returns HTTP 303 redirecting to its license page, the Earthdata Cloud host "
        "returns HTTP 401 with an Earthdata Login OAuth prompt, and the public S3 bucket "
        "refuses anonymous listing with AccessDenied."
    ),
    "nasa_modis": (
        "NASA MODIS level-3 tiled products are HDF4 like the L2 swath files, which no "
        "installed xarray engine can open, so a recorded payload could not load. The serving "
        "archive is also credential-gated: anonymous requests to the LP DAAC data pool get "
        "HTTP 303 to a license page or HTTP 401 with an Earthdata Login OAuth prompt, and the "
        "public S3 bucket answers AccessDenied to anonymous listing."
    ),
    "modis_ornl": (
        "The reader ignores any local file and retrieves every subset through the ORNL MODIS "
        "SOAP service, so there is no payload that could be checked in for it to open offline. "
        "That service has also been retired: the WSDL address the reader hard-codes answers "
        "HTTP 301 to a NASA news page (which then returns HTTP 403), no longer a WSDL. The "
        "SOAP client package the reader requires is not installed either, so an offline load "
        "fails with ImportError before any request is made."
    ),
    "nesdis_edr_viirs": (
        "The EDR VIIRS gridded AOD product the reader's URLs target has been withdrawn from "
        "the NESDIS STAR archive: the directory the reader builds "
        "(/pub/smcd/jhuang/npp.viirs.aerosol.data/edraot550) answers FTP 550 'No such file or "
        "directory' because the jhuang tree no longer exists, and no edraot550 directory is "
        "present anywhere else on the server. The surviving VIIRS aerosol archive holds only "
        "the EPS product, which is recorded separately as nesdis_eps_viirs, so there is no "
        "EDR payload to record."
    ),
    "nesdis_frp": (
        "The fire-radiative-power tiles are served only from the Nextcloud share at the base "
        "URL the reader hard-codes, gsce-dtn.sdstate.edu, and that host no longer resolves "
        "(name lookup fails with 'nodename nor servname provided, or not known'), so the "
        "payload cannot be retrieved at all. There is no mirror of these FV3-C384 gridded "
        "binary tiles on the public NOAA or NASA object stores."
    ),
    "umbc_aerosol": (
        "The UMBC CL51 ceilometer files live on the university's phoebus.cl.umbc.edu archive, "
        "which no longer resolves (name lookup fails with 'nodename nor servname provided, or "
        "not known'), and the reader builds no URL of its own — it only opens paths the caller "
        "supplies — so there is no location to record a payload from. The files themselves are "
        "small HDF5 that the reader would open offline without difficulty if an accessible "
        "archive existed."
    ),
}
