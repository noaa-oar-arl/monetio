# sat/omps_l2_no2_mm.py
from __future__ import annotations

from glob import glob
import numpy as np
import pandas as pd
import xarray as xr
from netCDF4 import Dataset

DU_TO_MOLEC_CM2 = 2.687e16


def _decode_str_array(a) -> np.ndarray:
    """
    Decode netCDF string/char arrays into a numpy array of Python strings.
    Handles: bytes arrays, char arrays, object arrays.
    """
    a = np.asarray(a)

    # netCDF4 sometimes returns numpy bytes_ or object
    if a.dtype.kind in ("S", "O"):
        return np.array([x.decode() if isinstance(x, (bytes, np.bytes_)) else str(x) for x in a.ravel()]).reshape(a.shape)

    # char array: (..., strlen) -> join along last axis
    if a.dtype.kind == "U":
        return a
    if a.dtype.kind == "i" or a.dtype.kind == "u":
        # unlikely for time
        return a.astype(str)

    # char arrays often appear as dtype('S1')
    if a.dtype.kind == "S" and a.ndim >= 1 and a.dtype.itemsize == 1:
        # join last dimension
        return np.apply_along_axis(lambda row: b"".join(row).decode(errors="ignore"), -1, a)

    return a.astype(str)


def _parse_time_utc_ccsda(utc_ccsda_a: np.ndarray) -> np.ndarray:
    """
    OMPS UTC_CCSDA_A is typically a string like:
      '2023-08-05T00:36:29Z'  OR  '2023-08-05 00:36:29'
    Your downstream needs a 1D time coordinate per scanline/time-index.

    We take the first cross-track element if utc_ccsda_a is 2D (time, xtrack).
    """
    s = _decode_str_array(utc_ccsda_a)

    # If 2D (time, xtrack), pick xtrack=0 as representative per time index
    if s.ndim == 2:
        s1 = s[:, 0]
    elif s.ndim == 1:
        s1 = s
    else:
        # fallback
        s1 = s.reshape(-1)

    # robust parse in UTC
    t = pd.to_datetime(s1, utc=True, errors="coerce")
    if t.isna().any():
        # Sometimes OMPS strings include trailing/leading spaces
        t = pd.to_datetime(pd.Series(s1).astype(str).str.strip(), utc=True, errors="coerce")

    # Convert to numpy datetime64[ns] (timezone removed but UTC-based)
    return t.to_numpy(dtype="datetime64[ns]")


#def open_omps_l2_no2(files: str | list[str]) -> xr.Dataset:
def open_omps_l2_no2(files: str | list[str],control_dict: dict | None = None) -> xr.Dataset:

    """
    Read OMPS-N20 NMNO2 L2 files and return an xarray.Dataset with standardized coords:
      - time (datetime64[ns])
      - latitude, longitude (2D on time,xtrack)
    and a key variable:
      - no2_totalcolumn (molec/cm2) derived from ColumnAmountNO2 (DU)

    Also includes fields needed later for AK/apriori pairing:
      - PressureLevel, NO2_ShapeFactor, AveragingKernel
      - PixelQualityFlags, CloudFraction
    """
    flist = sorted(glob(files)) if isinstance(files, str) else list(files)
    if len(flist) == 0:
        raise FileNotFoundError(f"No OMPS files matched: {files}")

    dsets = []
    for fn in flist:
        with Dataset(fn, mode="r") as f:
            geo = f.groups["GeolocationData"]
            apr = f.groups["aPriori"]
            sci = f.groups["ScienceData"]

            # Pull arrays
            utc_ccsda_a = geo.variables["UTC_CCSDA_A"][:]      # (time, xtrack) strings
            utc_hour_1 = [t[11:13] for t in utc_ccsda_a]
            lat = geo.variables["Latitude"][:]                # (time, xtrack)
            lon = geo.variables["Longitude"][:]               # (time, xtrack)
            ground_qf = geo.variables["GroundPixelQualityFlags"][:]      # (time, xtrack) strings
            sza = geo.variables["SolarZenithAngle"][:]     # (time, xtrack)

            col_no2_total_du = sci.variables["ColumnAmountNO2"][:]  # (time, xtrack) DU #total NO2
            col_no2_tropo_du = sci.variables["ColumnAmountNO2tropo"][:]  # (time, xtrack) DU #tropospheric no2
            col_no2_strat_du = sci.variables["ColumnAmountNO2strat"][:]  # (time, xtrack) DU #stratospheric no2

            fill_no2 = getattr(sci.variables["ColumnAmountNO2"], "_FillValue", None)

            qaflag = sci.variables["PixelQualityFlags"][:]    # (time, xtrack)
            cldfrac = sci.variables["CloudFraction"][:]       # (time, xtrack)
            
            pres_hpa = apr.variables["PressureLevel"][:]      # (time, xtrack, edge) hPa
            shp_prior = apr.variables["NO2_ShapeFactor"][:]   # (time, xtrack, layer)
            ak = apr.variables["AveragingKernel"][:]          # (time, xtrack, layer)

        # time 1D
        time = _parse_time_utc_ccsda(utc_ccsda_a)

        col_no2_total_du = col_no2_total_du.astype("float64")
        col_no2_tropo_du = col_no2_tropo_du.astype("float64")
        col_no2_strat_du = col_no2_strat_du.astype("float64")
      

        # Mask fill values in native units FIRST
        if fill_no2 is not None:
            col_no2_total_du[np.isclose(col_no2_total_du, fill_no2)] = np.nan
            col_no2_tropo_du[np.isclose(col_no2_tropo_du, fill_no2)] = np.nan
            col_no2_strat_du[np.isclose(col_no2_strat_du, fill_no2)] = np.nan
        # convert DU -> molec/cm2
        col_molec_total_cm2 = col_no2_total_du * DU_TO_MOLEC_CM2
        col_molec_tropo_cm2 = col_no2_tropo_du * DU_TO_MOLEC_CM2
        col_molec_strat_cm2 = col_no2_strat_du * DU_TO_MOLEC_CM2


        # normalize lon if you want (optional; keep as file-native here)
        # lon = lon % 360.0  # if you want 0-360

        ds = xr.Dataset(
            data_vars=dict(
                no2_totalcolumn=(("time", "xtrack"), col_molec_total_cm2),
                no2_tropocolumn=(("time", "xtrack"), col_molec_tropo_cm2),
                no2_stratcolumn=(("time", "xtrack"), col_molec_strat_cm2),
                PixelQualityFlags=(("time", "xtrack"), qaflag),
                CloudFraction=(("time", "xtrack"), cldfrac),
                GroundPixelQualityFlags=(("time", "xtrack"), ground_qf),
                SolarZenithAngle = (("time", "xtrack"), sza),
                PressureLevel=(("time", "xtrack", "edge"), pres_hpa),
                NO2_ShapeFactor=(("time", "xtrack", "layer"), shp_prior),
                AveragingKernel=(("time", "xtrack", "layer"), ak),
                utc_hour = (("time"),utc_hour_1),
                # keep original DU column too (optional but handy)
                no2_totalcolumn_DU=(("time", "xtrack"), col_no2_total_du.astype("float64")),
                no2_tropocolumn_DU=(("time", "xtrack"), col_no2_tropo_du.astype("float64")),
                no2_stratcolumn_DU=(("time", "xtrack"), col_no2_strat_du.astype("float64")),
            ),
            coords=dict(
                time=(("time",), time),
                latitude=(("time", "xtrack"), lat.astype("float64")),
                longitude=(("time", "xtrack"), lon.astype("float64")),
            ),
            attrs=dict(
                source="OMPS-N20 NMNO2 L2",
                du_to_molec_cm2=DU_TO_MOLEC_CM2,
                ColumnAmountNO2_fillvalue=float(fill_no2) if fill_no2 is not None else None,
                file=fn,
            ),
        )

        # Add nice attrs for plotting
        ds["no2_totalcolumn"].attrs.update(
            long_name="OMPS NO2 total column",
            units="molec cm-2",
        )
        ds["no2_totalcolumn_DU"].attrs.update(
            long_name="OMPS NO2 total column",
            units="DU",
        )
        ds["no2_tropocolumn"].attrs.update(
            long_name="OMPS NO2 tropo column",
            units="molec cm-2",
        )
        ds["no2_tropocolumn_DU"].attrs.update(
            long_name="OMPS NO2 tropo column",
            units="DU",
        )
        ds["no2_stratcolumn"].attrs.update(
            long_name="OMPS NO2 strat column",
            units="molec cm-2",
        )
        ds["no2_stratcolumn_DU"].attrs.update(
            long_name="OMPS NO2 strat column",
            units="DU",
        )

        dsets.append(ds)

    # concat along time and sort
    out = xr.concat(dsets, dim="time").sortby("time")
    
    # ======================================================
    # YAML-DRIVEN QA FILTERING
    # ======================================================
    COLUMN_MAP = {
    "total": "no2_totalcolumn",
    "tropospheric": "no2_tropocolumn",
    "stratospheric": "no2_stratcolumn",
    }
    obs_cfg = control_dict["obs"]["omps_l2_no2"]

    column_type = obs_cfg.get("no2_column_type", "total").lower()

    if column_type not in COLUMN_MAP:
        raise ValueError(
            f"Invalid no2_column_type='{column_type}'. "
            f"Must be one of {list(COLUMN_MAP.keys())}"
        )

    obs_no2_var = COLUMN_MAP[column_type]

    if control_dict is not None:
        #var_cfgs = control_dict["obs"]["omps_l2_no2"]["variables"]["no2_tropocolumn"]
        var_cfgs = control_dict["obs"]["omps_l2_no2"]["variables"][obs_no2_var]

        qa_vars = {
        v: cfg for v, cfg in var_cfgs.items()
        if any(k in cfg for k in ["pixel_qf", "cloud_max_qf", "cloud_min_qf", "ground_qf", "sza_max", "sza_min"])
        }
        
        # Apply variable-specific fill values
        for var, cfg in var_cfgs.items():
            if var not in out:
                continue
            if "fillvalue" in cfg:
                fv = cfg["fillvalue"]
                out[var] = out[var].where(out[var] != fv)

        # Build combined QA mask
        mask = xr.ones_like(out["no2_totalcolumn"], dtype=bool)

        for var, cfg in qa_vars.items():
            if var not in out:
                raise KeyError(f"QA variable '{var}' defined in YAML but not found in OMPS dataset")
                

            if "pixel_qf" in cfg:
                mask = mask & (out[var] == cfg["pixel_qf"])

            if "ground_qf" in cfg:
                #mask = mask & (out[var] == cfg["ground_qf"])
                mask = mask & np.isin(out[var], cfg["ground_qf"])

            if "cloud_max_qf" in cfg:
                mask = mask & (out[var] <= cfg["cloud_max_qf"])

            if "cloud_min_qf" in cfg:
                mask = mask & (out[var] >= cfg["cloud_min_qf"])

            if "sza_max" in cfg:
                mask = mask & (out[var] <= cfg["sza_max"])

            if "sza_min" in cfg:
                mask = mask & (out[var] >= cfg["sza_min"])
        # --------------------------------------------------
        # Step 2: apply mask to NO2 column
        # --------------------------------------------------
        out["no2_totalcolumn"] = out["no2_totalcolumn"].where(mask)
        out["no2_totalcolumn_DU"] = out["no2_totalcolumn_DU"].where(mask)

        out["no2_tropocolumn"] = out["no2_tropocolumn"].where(mask)
        out["no2_tropocolumn_DU"] = out["no2_tropocolumn_DU"].where(mask)

        out["no2_stratcolumn"] = out["no2_stratcolumn"].where(mask)
        out["no2_stratcolumn_DU"] = out["no2_stratcolumn_DU"].where(mask)
        # --------------------------------------------------
        # Step 3: FORCE mask into AK-driving variables and other quality filtering variables
        # (this is the critical enforcement step)
        # --------------------------------------------------
        for v in ["PixelQualityFlags", "CloudFraction", "GroundPixelQualityFlags", "SolarZenithAngle"]:
            if v in out:
                out[v] = out[v].where(mask)
# ======================================================



# ======================================================

    # Count pixels where ALL pressure levels are NaN
    masked_pixels = (
    out["PressureLevel"]
    .isnull()
    .all(dim="edge")
    .sum()
    .item()
    )

    total_pixels = out.dims["time"] * out.dims["xtrack"]

    print("Masked pixels:", masked_pixels)
    print("Total pixels :", total_pixels)


    print(
    "NO2 masked pixels:",
    out["no2_totalcolumn"].isnull().sum().item()
    )


    return out

