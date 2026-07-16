# Observations

This section describes how to use MONETIO to load supported observational datasets.

## Unified Access

We recommend using the unified `monetio.load()` function for all observational data:

```python
import monetio as mio
import pandas as pd

# Load AirNow data
dates = pd.date_range(start='2018-05-01', end='2018-05-05', freq='h')
df = mio.load("airnow", files=dates)
```

## Supported Observation Networks

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
