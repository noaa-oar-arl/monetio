"""Extract one report of each supported type from NOAA's PREPBUFR test file.

Download https://ftp.emc.ncep.noaa.gov/static_files/public/bufr-12.4.0.tgz,
extract bufr/testfiles/data/prepbufr, then run with that path as the argument.
Requires NCEPLIBS-bufr's ncepbufr Python bindings. Tested with BUFRLIB 12.4.0.
"""

from __future__ import annotations

import argparse
import hashlib
import tempfile
from pathlib import Path

from make_sample_manifest import REPO_ROOT, write_manifest

SOURCE_SHA256 = "09b94c2463deaa55986b2ab7cbc2d8a6325f9a1d6935c9f8c78f0a26be9297e4"


def main():
    """Copy native report subsets and record the sample's provenance."""
    import ncepbufr

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    args = parser.parse_args()
    if hashlib.sha256(args.source.read_bytes()).hexdigest() != SOURCE_SHA256:
        raise ValueError("Expected NOAA's bufr-12.4.0.tgz bufr/testfiles/data/prepbufr")
    destination = REPO_ROOT / "tests/data/samples/prepbufr"
    destination.mkdir(parents=True, exist_ok=True)
    remaining = {"ADPUPA", "ADPSFC", "SFCSHP", "AIRCFT", "AIRCAR"}
    source = ncepbufr.open(str(args.source))
    try:
        with tempfile.TemporaryDirectory() as temporary:
            table = str(Path(temporary) / "prepbufr.table")
            source.dump_table(table)
            output = ncepbufr.open(str(destination / "reports.bufr"), mode="w", table=table)
            try:
                while remaining and source.advance() == 0:
                    if source.msg_type not in remaining:
                        continue
                    if source.load_subset() != 0:
                        raise ValueError(f"No report in {source.msg_type}")
                    output.open_message(source.msg_type, source.msg_date)
                    output.copy_subset(source)
                    output.close_message()
                    remaining.remove(source.msg_type)
            finally:
                output.close()
    finally:
        source.close()
    if remaining:
        raise ValueError(f"Missing report types: {remaining}")
    write_manifest(
        destination,
        producer="scripts/make_prepbufr_sample.py",
        location="https://ftp.emc.ncep.noaa.gov/static_files/public/bufr-12.4.0.tgz",
        kind="recorded",
        layout="one native report per ADPUPA/ADPSFC/SFCSHP/AIRCFT/AIRCAR subset",
        reason="Subset of bufr/testfiles/data/prepbufr; source SHA256 " + SOURCE_SHA256,
        requires="ncepbufr",
    )


if __name__ == "__main__":
    main()
