# models/ufschem_no2_mm.py
from glob import glob
import numpy as np
import xarray as xr

mwtair = 28.9644        # g/mol
grav = 9.80665         # m/s2
avogan = 6.02214076e23 # molec/mol
PTROP = 15000.0  # Pa (≈150 hPa tropopause)

def open_ufschem_no2(files: str | list[str], keep_layers: bool = True) -> xr.Dataset:
    flist = sorted(glob(files)) if isinstance(files, str) else list(files)
    if not flist:
        raise FileNotFoundError(f"No UFS files matched: {files}")

    ds = xr.open_mfdataset(
        flist,
        combine="by_coords",
        decode_times=True,
        data_vars="minimal",
        coords="minimal",
        compat="override",
    )

    # Required variables
    for v in ["dpres", "pressfc", "no2", "lat", "lon"]:
        if v not in ds:
            raise KeyError(f"Missing required variable '{v}' in UFS output")

    ak = np.array(ds.attrs["ak"])
    bk = np.array(ds.attrs["bk"])

    dp = ds["dpres"].values        # Pa
    ps = ds["pressfc"].values      # Pa
    no2_ppm = ds["no2"].values     # ppm
    
    time_utc_1_1 = ds["time"].dt.hour.values
    
    # Pressure at interfaces (Pa)
    preslev = ak[None, :, None, None] + bk[None, :, None, None] * ps[:, None, :, :]

    # Mid-layer pressure (Pa)
    presmid = dp / (np.log(preslev[:, 1:, :, :]) - np.log(preslev[:, :-1, :, :]))

    # Air column per layer (molec/cm2)
    airmass = dp / grav
    airlayer = airmass * 1e3 / mwtair * avogan * 1e-4

    # NO2 layer column (molec/cm2)
    no2layer = no2_ppm * 1e-6 * airlayer

    lev_dim = ds["dpres"].dims[1]

    no2_layer_da = xr.DataArray(
        no2layer,
        dims=ds["dpres"].dims,
        coords=ds["dpres"].coords,
        name="no2_layer",
        attrs={"units": "molec cm-2"},
    )


    pres_mid_da = xr.DataArray(
        presmid,
        dims=ds["dpres"].dims,
        coords=ds["dpres"].coords,
        name="pres_pa_mid",
        attrs={"units": "Pa"},
    )
     # --------------------------------------------------
    # Troposphere / stratosphere masks (pressure-based)
    # --------------------------------------------------
    trop_mask = pres_mid_da > PTROP
    strat_mask = pres_mid_da <= PTROP

    # --------------------------------------------------
    # Partial-column NO2
    # --------------------------------------------------
    no2_total = no2_layer_da.sum(dim=lev_dim, skipna=True)
    no2_total.name = "no2_totalcolumn_model"
    no2_total.attrs.update(long_name="UFS-Chem NO2 total column", units="molec cm-2")
    
    no2_trop = no2_layer_da.where(trop_mask).sum(dim=lev_dim, skipna=True)
    no2_trop.name = "no2_tropocolumn_model"
    no2_trop.attrs.update(
    long_name="UFS-Chem NO2 tropospheric column",
    units="molec cm-2",
    tropopause_pressure="150 hPa",
    )

    no2_strat = no2_layer_da.where(strat_mask).sum(dim=lev_dim, skipna=True)
    no2_strat.name = "no2_stratcolumn_model"
    no2_strat.attrs.update(
    long_name="UFS-Chem NO2 stratospheric column",
    units="molec cm-2",
    tropopause_pressure="150 hPa",
    )

    
    time_dim = ds["time"].dims[0]

    out = xr.Dataset(
        data_vars={"no2_totalcolumn_model": no2_total,
            "no2_tropocolumn_model": no2_trop,
            "no2_stratcolumn_model": no2_strat,
            "time_utc_hour": (time_dim, ds["time"].dt.hour.values),},
        coords=dict(
            time=ds["time"],
            latitude=(ds["lat"].dims, ds["lat"].values),
            longitude=(ds["lon"].dims, ds["lon"].values),
        ),
        attrs=dict(source="UFS-Chem"),
    )

    if keep_layers:
        out["no2_layer"] = no2_layer_da
        out["pres_pa_mid"] = pres_mid_da

    ds.close()
    return out

