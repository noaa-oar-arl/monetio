"""Point/observation source capture specs (line-oriented archives).

These sources retrieve text payloads (station files, hourly CSVs, fixed-width
profiles) over HTTP(S) or an anonymous object store. Each spec records the exact
remote location the reader's own URL builder produces — so a change to date
normalization that builds a wrong URL is caught — and trims the payload to the
minimum that still exercises every parse stage.
"""

from __future__ import annotations

CAPTURES: dict[str, dict] = {
    "igra2": {
        "urls": [
            (
                "https://www.ncei.noaa.gov/pub/data/igra/data/data-por/AEM00041217-data.txt.zip",
                "AEM00041217.dat",
            )
        ],
        # The archive ships as a multi-megabyte zip of every sounding for one
        # station; the sample keeps the first few profiles as a plain text file
        # (the reader parses a non-zip payload through the same code path) so it
        # stays minimal while still exercising header, level, and clean stages.
        "unpack": {"AEM00041217.dat": "zip"},
        "max_lines": {"AEM00041217.dat": 80},
        # The station-metadata join reaches the network for the station list,
        # so the offline sample exercises the profile read, harmonization, and
        # shape conversion without it.
        "load_kwargs": {"add_metadata": False},
        "note": "one trimmed IGRA2 station profile",
    },
    "ndbc": {
        "urls": [
            (
                "https://www.ndbc.noaa.gov/data/historical/stdmet/41002h2020.txt.gz",
                "41002h2020.txt",
            )
        ],
        "unpack": {"41002h2020.txt": "gz"},
        "max_lines": {"41002h2020.txt": 40},
        "note": "one NDBC historical station-year file, trimmed",
    },
    "ish": {
        "urls": [
            (
                "https://www.ncei.noaa.gov/pub/data/noaa/2025/040520-99999-2025.gz",
                "040520-99999-2025.dat",
            )
        ],
        # The station-year archive is served gzipped; the reader parses a plain
        # file through the same fixed-width path, so the sample keeps the first
        # few observations unpacked and trimmed. BASAR is a station whose record
        # spans only a few weeks, so one year is already a small payload.
        "unpack": {"040520-99999-2025.dat": "gz"},
        "max_lines": {"040520-99999-2025.dat": 40},
        "layout": "ish",
        "note": "one trimmed ISH station-year file (fixed-width layout)",
    },
    "ish_lite": {
        "urls": [
            (
                "https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/2025/040520-99999-2025.gz",
                "040520-99999-2025.txt",
            )
        ],
        # The same station-year from the ISH-Lite branch of the archive: the
        # whitespace-delimited layout is parsed by a different reader function,
        # so it needs its own recorded payload.
        "unpack": {"040520-99999-2025.txt": "gz"},
        "max_lines": {"040520-99999-2025.txt": 40},
        "layout": "ish_lite",
        "note": "one trimmed ISH-Lite station-year file (whitespace layout)",
    },
    "crn": {
        "urls": [
            (
                "https://www1.ncdc.noaa.gov/pub/data/uscrn/products/hourly02/2021/"
                "CRNH0203-2021-TX_Monahans_6_ENE.txt",
                "CRNH0203-2021-TX_Monahans_6_ENE.txt",
            )
        ],
        # The hourly product is one station-year of fixed-width rows; the first
        # few hours plus the column layout exercise the parse, the UTC/local
        # time construction, and the join against the bundled station table.
        "max_lines": {"CRNH0203-2021-TX_Monahans_6_ENE.txt": 40},
        "note": "one CRN hourly station-year file, trimmed",
    },
    "gml_ozonesonde": {
        "urls": [
            (
                "https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/Boulder,%20Colorado/"
                "100%20Meter%20Average%20Files/abl001_1967_04_27_16.l100",
                "abl001_1967_04_27_16.l100",
            )
        ],
        # One balloon ascent of 100-metre averages. The reader discovers files
        # by listing the station directory over the network, so the sample
        # records a single sounding and the suite loads it by path.
        "note": "one GML ozonesonde 100 m average sounding",
    },
}

# Point sources that cannot be sampled offline, with the reason. A source
# lands here only when its payload is not retrievable anonymously AND
# its format cannot be reproduced synthetically; the reason is surfaced in every
# coverage run rather than the source being dropped.
EXCLUSIONS: dict[str, str] = {
    "icartt": (
        "the reader has no remote location construction — open_dataset parses only "
        "user-supplied ICARTT files, so there is no service payload to record; format "
        "coverage comes from generated fixtures instead"
    ),
    "nadp": (
        "the archive the reader's build_url targets is no longer served — every "
        "datalib path (weekly, annual, AMoN, AIRMoN, AMNet) returns HTTP 404 after "
        "the site's redirect to https, so no payload can be retrieved"
    ),
    "iagos": (
        "retrieval requires the IAGOS_API_KEY credential — build_urls returns no "
        "location without one, and an anonymous request to the data service answers "
        "HTTP 403"
    ),
    "pams": (
        "the reader has no remote location construction — open_dataset parses only "
        "user-supplied PAMS JSON files, so there is no service payload to record; "
        "format coverage comes from generated fixtures instead"
    ),
}
