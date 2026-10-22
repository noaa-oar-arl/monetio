from monetio.readers.gfs import GDASReader, GEFSReader, GFSReader
from monetio.readers.hrrr import HRRRReader
from monetio.readers.nam import NAMReader
from monetio.readers.rap import RAPReader


def test_hrrr_urls():
    reader = HRRRReader()
    dates = "2025-03-25"

    # Default (AWS, prs)
    urls = reader.build_urls(dates, hour=0, lead_time=1)
    assert urls[0] == "s3://noaa-hrrr-bdp-pds/hrrr.20250325/conus/hrrr.t00z.wrfprsf01.grib2"

    # NOMADS
    urls_nomads = reader.build_urls(dates, hour=0, lead_time=1, source="nomads")
    assert (
        urls_nomads[0]
        == "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.20250325/conus/hrrr.t00z.wrfprsf01.grib2"
    )

    # Native product
    urls_nat = reader.build_urls(dates, hour=0, lead_time=1, product="nat")
    assert "wrfnatf01" in urls_nat[0]


def test_nam_urls():
    reader = NAMReader()
    dates = "2025-03-25"

    # AWS
    urls_aws = reader.build_urls(dates, hour=0, lead_time=1, source="aws")
    assert urls_aws[0] == "s3://noaa-nam-pds/nam.20250325/nam.t00z.conusnest.hiresf01.tm00.grib2"

    # NOMADS
    urls_nomads = reader.build_urls(dates, hour=0, lead_time=1, source="nomads")
    assert (
        urls_nomads[0]
        == "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.20250325/nam.t00z.conusnest.hiresf01.tm00.grib2"
    )


def test_rap_urls():
    reader = RAPReader()
    dates = "2025-03-25"

    # AWS
    urls_aws = reader.build_urls(dates, hour=0, lead_time=1, source="aws")
    assert urls_aws[0] == "s3://noaa-rap-pds/rap.20250325/rap.t00z.awp130pgrbf01.grib2"

    # NOMADS
    urls_nomads = reader.build_urls(dates, hour=0, lead_time=1, source="nomads")
    assert (
        urls_nomads[0]
        == "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.20250325/rap.t00z.awp130pgrbf01.grib2"
    )


def test_gfs_urls():
    reader = GFSReader()
    dates = "2025-03-25"

    urls_aws = reader.build_urls(dates, hour=0, lead_time=0, source="aws")
    assert urls_aws[0] == "s3://noaa-gfs-bdp-pds/gfs.20250325/00/atmos/gfs.t00z.pgrb2.0p25.f000"

    urls_nomads = reader.build_urls(dates, hour=0, lead_time=0, source="nomads")
    assert (
        urls_nomads[0]
        == "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20250325/00/atmos/gfs.t00z.pgrb2.0p25.f000"
    )


def test_gefs_urls():
    reader = GEFSReader()
    dates = "2025-03-25"

    urls_aws = reader.build_urls(dates, hour=0, lead_time=0, source="aws")
    assert "noaa-gefs-pds" in urls_aws[0]

    urls_nomads = reader.build_urls(dates, hour=0, lead_time=0, source="nomads")
    assert "nomads.ncep.noaa.gov" in urls_nomads[0]


def test_gdas_urls():
    reader = GDASReader()
    dates = "2025-03-25"

    urls_aws = reader.build_urls(dates, hour=0, lead_time=0, source="aws")
    assert "gdas.20250325" in urls_aws[0]

    urls_nomads = reader.build_urls(dates, hour=0, lead_time=0, source="nomads")
    assert "nomads.ncep.noaa.gov" in urls_nomads[0]


def test_parameter_propagation():
    # Test that open_dataset correctly propagates parameters to build_urls
    from unittest.mock import MagicMock, patch

    reader = HRRRReader()
    mock_ds = MagicMock()
    with patch(
        "monetio.readers.base.GriddedReader.open_dataset", return_value=mock_ds
    ) as mock_open:
        with patch.object(reader, "harmonize", return_value=mock_ds):
            reader.open_dataset(dates="2025-03-25", source="nomads", product="nat")

            open_args, _ = mock_open.call_args
            files = open_args[0]
            assert isinstance(files, list)
            assert len(files) == 1
            assert "nomads.ncep.noaa.gov" in files[0]
            assert "wrfnat" in files[0]
