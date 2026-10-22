# Models

MONETIO supports reading output from several major atmospheric chemistry and transport models.

## Unified Access

We recommend using the unified `monetio.load()` function:

```python
import monetio as mio

# Load CMAQ data
ds = mio.load("cmaq", files="aqm.t12z.aconc.ncf")
```

## Supported Models

### CMAQ

Community Multiscale Air Quality Model.

- **Source ID**: `cmaq`

### HYSPLIT

Hybrid Single-Particle Lagrangian Integrated Trajectory model.

- **Source ID**: `hysplit` (Concentration) or `hytraj` (Trajectories)
- **Features**:
    - Lazy loading via Dask.
    - Automatic grid continuity fixing.
    - Optimized mass loading calculations.

### WRF-Chem

Weather Research and Forecasting model coupled with Chemistry.

- **Source ID**: `wrfchem`

### UFS-AQM

Unified Forecast System with Chemistry.

- **Source ID**: `ufs`

### NCEP GRIB

National Centers for Environmental Prediction (NCEP) GRIB2 model outputs.

- **Source ID**: `ncep_grib`: Generic NCEP GRIB2 reader.
- **Source ID**: `gfs`: Global Forecast System.
- **Source ID**: `gefs`: Global Ensemble Forecast System.
- **Source ID**: `gdas`: Global Data Assimilation System.
- **Source ID**: `rrfs`: Rapid Refresh Forecast System.
- **Source ID**: `grib2`: Standard GRIB2 files.
- **Recommended API**: Use `open_dataset(...)` with the grib2io xarray backend
    options (`filters`, `use_icechunk`, `storage_options`, etc.).

### ICAP-MME

International Cooperative for Aerosol Prediction Multi-Model Ensemble.

- **Source ID**: `icap_mme`

### PARDUMP

HYSPLIT Particle Dump files.

- **Source ID**: `pardump`

### Other Models

- **CAMx**: Comprehensive Air Quality Model with extensions (`camx`)
- **CHIMERE**: Multi-scale chemistry-transport model (`chimere`)
- **RAQMS**: Realtime Air Quality Modeling System (`raqms`)
