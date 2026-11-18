"""Point readers share one whole-file pandas partition contract."""

from importlib import import_module

import pytest

from monetio.readers.errors import OptionUnsupportedError

_POINT_READERS = [
    ("ameriflux", "AmeriFluxReader"),
    ("gml_ozonesonde", "GMLOzonesondeReader"),
    ("icartt", "ICARTTReader"),
    ("igra2", "IGRA2Reader"),
    ("improve", "IMPROVEReader"),
    ("ish", "ISHReader"),
    ("ish_lite", "ISHLiteReader"),
    ("nadp", "NADPReader"),
    ("openaq", "OpenAQReader"),
    ("openaq_aws", "OpenAQAWSReader"),
    ("openaq_v2", "OpenAQV2Reader"),
    ("pams", "PAMSReader"),
    ("skynet", "SKYNETReader"),
    ("solrad", "SOLRADReader"),
    ("surfrad", "SURFRADReader"),
]


@pytest.mark.parametrize("source,class_name", _POINT_READERS, ids=[x[0] for x in _POINT_READERS])
@pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
def test_whole_file_point_readers_reject_array_chunks_before_parse(
    tmp_path, source, class_name, as_xarray
):
    reader_class = getattr(import_module(f"monetio.readers.{source}"), class_name)
    path = tmp_path / f"{source}.input"
    path.touch()

    with pytest.raises(OptionUnsupportedError, match=f"{source}.*chunks"):
        reader_class().open_dataset(
            str(path), use_dask=True, chunks={"time": 1}, as_xarray=as_xarray
        )
