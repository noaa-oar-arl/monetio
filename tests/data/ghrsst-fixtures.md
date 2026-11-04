# Synthetic GHRSST fixtures

`tests/test_ghrsst.py` builds small temporary NetCDF files rather than storing product-sized globals in the repository. The fixtures represent the shared GDS Level 4 contract with a 1×2×3 time/latitude/longitude array, packed kelvin `analysed_sst`, one fill value, `analysis_error`, `mask`, `sea_ice_fraction`, decoded time, and provider/product identification metadata.

The NCEI and OSPO fixtures differ only in product identity and selected reader behavior. Their tiny grids are schema fixtures, not realistic-resolution data. Product grids in actual files remain native: 0.25° for NCEI AVHRR OI and 0.05° for OSPO Geo-Polar Blended.
