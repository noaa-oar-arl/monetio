# Observations

This section describes how to use MONETIO to load supported observational datasets.

## Unified Access

We recommend using the unified `monetio.load()` function for all observational data:

```python
import monetio as mio
import pandas as pd

# Load AirNow data
dates = pd.date_range(start="2018-05-01", end="2018-05-05", freq="h")
df = mio.load("airnow", files=dates)
```

## Supported Observation Networks

### NCEP PREPBUFR

The `prepbufr` reader uses the **NCEPLIBS-bufr Python bindings (`ncepbufr`)**:
[API and installation instructions](https://noaa-emc.github.io/NCEPLIBS-bufr/python/ncepbufr.html).
Install that library separately with its Python support enabled; it is optional
and is imported only when a PREPBUFR file is decoded.

To build the bindings in a virtual environment, first install C and Fortran
compilers (for example GCC and gfortran). From the MONETIO checkout:

```bash
python -m venv .venv
.venv/bin/python -m pip install -e . cmake meson ninja setuptools
git clone https://github.com/NOAA-EMC/NCEPLIBS-bufr.git ../NCEPLIBS-bufr
git -C ../NCEPLIBS-bufr checkout ebf1cbd7bba12f46f90ce8d0dfbceeaedb01ace4
.venv/bin/cmake -S ../NCEPLIBS-bufr -B ../NCEPLIBS-bufr/build \
    -DENABLE_PYTHON=ON -DBUILD_TESTING=OFF -DBUILD_UTILS=OFF \
    -DCMAKE_INSTALL_PREFIX="$PWD/.venv" -DCMAKE_INSTALL_LIBDIR=lib \
    -DPython3_EXECUTABLE="$PWD/.venv/bin/python"
PATH="$PWD/.venv/bin:$PATH" .venv/bin/cmake --build ../NCEPLIBS-bufr/build --parallel 4
.venv/bin/cmake --install ../NCEPLIBS-bufr/build
.venv/bin/python -c 'import ncepbufr; print(ncepbufr.__bufrlib_version__)'
.venv/bin/python -m pytest tests/test_prepbufr_native.py -q
```

The build places `ncepbufr` and its compiled `_bufrlib` extension in `.venv`.
The native integration tests use a checked-in subset of NOAA's PREPBUFR test
data and skip when these optional bindings are absent. Their fixture can be
reproduced using `scripts/make_prepbufr_sample.py`.

```python
import monetio as mio

ds = mio.load(
    "prepbufr",
    files="/data/gdas.*.prepbufr",
    subsets=["ADPUPA", "ADPSFC", "SFCSHP", "AIRCFT", "AIRCAR"],
    use_dask=True,
)
# Select a subset or report without combining collocated mass/wind reports.
soundings = ds.where(ds.subset == "ADPUPA")
result = ds.compute()
```

Omitting `subsets` selects all five types. A single string selects one type.
Files must be local and accessible to every Dask worker. Embedded BUFR tables
are used by default; `table="/data/prepbufr.table"` selects an external table.

The default output is an xarray Dataset with one `node` per report level,
including repeated station IDs and times. `source_file`, `message`, `report`,
and zero-based `level` identify each observation. `expand2d=True` is rejected
because a station/time reshape could combine distinct profile levels.

Variables include pressure (Pa), temperature and dewpoint (degC), specific
humidity (kg/kg), height and station elevation (m), wind components and speed
(m/s), wind direction (meteorological degrees from north), and quality marks.
Xarray output derives wind speed/direction and relative humidity over liquid
water; missing dewpoints are calculated from pressure and specific humidity.
Calm wind direction is missing. BUFR missing values become NaN or NaT.
All quality marks and separate mass/wind reports are retained without filtering
or merging. The current decoded values are read, rather than the full event stack.
Locations and times use report headers; balloon drift corrections are not applied.

Surface temperatures and winds retain the observations' reported meaning; the
reader does not interpolate them to 2 m or 10 m or infer sensor heights. Inspect
report type and level category when selecting observations for verification.

Eager NumPy output is the default. `use_dask=True` (or `lazy=True`) constructs
one delayed partition per file. BUFRLIB calls are serialized within a process;
use process workers for parallel file decoding. Array `chunks` mappings are
unsupported for this whole-file decoder. Xarray construction performs a full
decode of each file to collect row counts and time/station keys, retaining those
keys in memory; payload variables remain Dask-backed and files are decoded again
when evaluated. `as_xarray=False, use_dask=True` returns a Dask DataFrame without
this preparation scan. `defer=True` is not advertised for this new reader.

### AirNow

Near real-time air quality data for the United States.

- **Source ID**: `airnow`
- **Variables**: `O3`, `PM2.5`, `PM10`, `SO2`, `NO2`, `CO`, etc.

### EPA AQS

Historical air quality data from the U.S. EPA.

- **Source ID**: `aqs`

### AERONET

Aerosol Robotic Network (global).

- **Source ID**: `aeronet`

### SKYNET

SKYNET sun photometer network.

- **Source ID**: `skynet`
### ACTRIS/EBAS

European Research Infrastructure for the observation of Aerosol, Clouds and Trace Gases.

- **Source ID**: `actris`
- **Variables**: `ozone`, `carbon_monoxide`, `aerosol_light_scattering_coefficient`, etc.

### OpenAQ

Global air quality data platform.

- **Source ID**: `openaq` (Legacy/Version 1)
- **Source ID**: `openaq_v2` (Modern REST API)
- **Source ID**: `openaq_v3` (Latest REST API)
- **Source ID**: `openaq_aws` (S3-based public dataset)

### OpenAQ v3 Usage

```python
import monetio as mio
# Set environment variable OPENAQ_API_KEY
df = mio.load("openaq_v3", dates='2024-01-01', parameters=['pm25', 'o3'])
```

### IAGOS

In-service Aircraft for a Global Observing System (global airborne in-situ measurements).

- **Source ID**: `iagos`
- **Variables**: `ozone`, `carbon_monoxide`, `water_vapor`, `nitrogen_oxides`, etc.

### NDACC

Network for the Detection of Atmospheric Composition Change (ground-based remote sensing).

- **Source ID**: `ndacc`
- **Formats**: GEOMS (HDF4/HDF5)
- **Instruments**: Lidar, FTIR, UV-Vis spectrometer, etc.

### Pandora

Pandonia Global Network (ground-based remote sensing).

- **Source ID**: `pandora`
- **Formats**: GEOMS (HDF5)
- **Variables**: `nitrogen_dioxide`, `ozone`, `formaldehyde`, `sulfur_dioxide`, etc.

### Integrated Surface Database (ISH)

Global hourly and synoptic surface observations.

- **Source ID**: `ish` or `ish_lite`

### Other Networks

- **HYTRAJ**: HYSPLIT Trajectories (`hytraj`)
- **NADP**: National Atmospheric Deposition Program (`nadp`)
- **CRN**: Climate Reference Network (`crn`)
- **CEMS**: Continuous Emission Monitoring Systems (`cems`)
- **IMPROVE**: Interagency Monitoring of Protected Visual Environments (`improve`)
- **E-PROFILE**: European Automatic Lidar and Ceilometer network (`eprofile`)
