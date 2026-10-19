"""Air-quality network capture specs (EPA/AirNow/OpenAQ style services).

These sources retrieve tabular air-quality payloads from EPA and OpenAQ
endpoints — hourly/daily CSV zips on an object store or an FTP mirror, or a
paginated API. Recorded payloads exercise the reader's own URL/key construction,
so a change to date normalization or bucket layout that produces a wrong
location is caught. API-key-gated endpoints that cannot be reached
anonymously are recorded as reasoned exclusions rather than faked with a
synthetic file (a synthetic cannot certify a network-facing
reader's retrieval path).
"""

from __future__ import annotations

CAPTURES: dict[str, dict] = {
    "airnow": {
        "urls": [
            (
                "s3://files.airnowtech.org/airnow/2023/20230101/HourlyData_2023010100.dat",
                "HourlyData_2023010100.dat",
            )
        ],
        # The hourly broadcast covers every reporting site in the country and
        # runs to hundreds of kilobytes; the first few rows carry the full pipe
        # layout (site id, pollutant, units, observation), which is all the
        # fixed-column parser and the wide pivot need. The site-metadata join
        # reaches the AirNow monitor file over the network, so the suite
        # exercises the recorded payload through the parse and postprocess.
        "max_lines": {"HourlyData_2023010100.dat": 40},
        "note": "one AirNow hourly broadcast, trimmed",
    },
    "aqs": {
        "urls": [
            (
                "https://aqs.epa.gov/aqsweb/airdata/daily_88502_1998.zip",
                "daily_88502_1998.csv",
            )
        ],
        # EPA ships each daily parameter-year as a single CSV in a zip; the
        # reader parses the CSV through the same path, so the sample stores the
        # unpacked file trimmed to the first rows. 1998 PM2.5 FRM is one of the
        # smallest parameter-years in the archive.
        "unpack": {"daily_88502_1998.csv": "zip"},
        "max_lines": {"daily_88502_1998.csv": 40},
        "note": "one AQS daily parameter-year CSV, trimmed",
    },
    "openaq_aws": {
        "urls": [
            (
                "s3://openaq-data-archive/records/csv.gz/locationid=10000/year=2018/"
                "month=10/location-10000-20181017.csv.gz",
                "location-10000-20181017.csv",
            )
        ],
        # The archive partitions one CSV.gz per site-day; a single day is only
        # a few hundred bytes, so the sample keeps the whole unpacked payload
        # rather than trimming it. The reader's own bucket listing for path
        # discovery needs credentials-free S3 access to a different partition
        # layout, so the suite loads the recorded file by path.
        "unpack": {"location-10000-20181017.csv": "gz"},
        "note": "one OpenAQ archive site-day record",
    },
}

# Air-quality sources not sampleable offline, with reasons. A source
# lands here only when its payload cannot be retrieved anonymously AND its
# format cannot be reproduced synthetically in a way that exercises the reader.
EXCLUSIONS: dict[str, str] = {
    "improve": (
        "the reader has no remote location construction — open_dataset parses only "
        "user-supplied IMPROVE text files, so there is no service payload to record; "
        "format coverage comes from generated fixtures instead"
    ),
    "openaq_v2": (
        "the v2 API the reader calls is retired — an anonymous GET to "
        "https://api.openaq.org/v2/locations answers HTTP 410 ('Version 1 and "
        "Version 2 API endpoints are retired'), and the successor v3 endpoint "
        "requires the OPENAQ_API_KEY credential"
    ),
    "openaq": (
        "the v1 openaq-fetches bucket no longer allows anonymous reads — listing "
        "answers Access Denied and a direct object GET returns HTTP 403, so the "
        "JSON payloads the reader parses cannot be retrieved; the openaq_aws "
        "reader covers the successor archive"
    ),
    "cems": (
        "the FTP archive the reader builds URLs for is unreachable — the connection "
        "to newftp.epa.gov port 21 times out, and the HTTPS equivalent of the same "
        "path answers HTTP 404"
    ),
}
