"""NCEP PREPBUFR observations decoded by NCEPLIBS-bufr's ``ncepbufr``.

Reports are kept as individual levels, including collocated mass/wind reports.
No quality filtering, vertical interpolation, or event-stack merging is applied.
"""

from __future__ import annotations

import os
import threading
from functools import partial

import numpy as np
import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .sat_utils import update_history

# BUFRLIB and its Python wrapper maintain process-global Fortran state.
_BUFR_LOCK = threading.Lock()
SUBSETS = ("ADPUPA", "ADPSFC", "SFCSHP", "AIRCFT", "AIRCAR")
_HEADER = "SID XOB YOB DHR TYP ELV"
_FIELDS = {
    "POB": ("pressure", "Pa", 100.0),
    "QOB": ("specific_humidity", "kg kg-1", 1e-6),
    "TOB": ("temperature", "degC", 1.0),
    "ZOB": ("height", "m", 1.0),
    "UOB": ("u_wind", "m s-1", 1.0),
    "VOB": ("v_wind", "m s-1", 1.0),
    "TDO": ("dewpoint", "degC", 1.0),
    "CAT": ("category", "1", 1.0),
    "PQM": ("pressure_quality", "1", 1.0),
    "QQM": ("humidity_quality", "1", 1.0),
    "TQM": ("temperature_quality", "1", 1.0),
    "ZQM": ("height_quality", "1", 1.0),
    "WQM": ("wind_quality", "1", 1.0),
}


def _schema():
    """Return the stable, empty post-decoding table schema."""
    types = dict.fromkeys(
        ["latitude", "longitude", "elevation", "report_type"]
        + [field[0] for field in _FIELDS.values()],
        "float64",
    )
    types.update(dict.fromkeys(["siteid", "subset", "source_file"], "object"))
    types.update(dict.fromkeys(["message", "report", "level"], "int64"))
    types["time"] = "datetime64[ns]"
    return pd.DataFrame({name: pd.Series(dtype=dtype) for name, dtype in types.items()})


def _subsets(subsets):
    """Validate the PREPBUFR subset selection before any file access."""
    selected = (
        SUBSETS if subsets is None else ((subsets,) if isinstance(subsets, str) else tuple(subsets))
    )
    if not selected or set(selected) - set(SUBSETS):
        raise ValueError(f"subsets must contain one or more of {SUBSETS}")
    return selected


def read_prepbufr(fname, *, subsets=None, table=None):
    """Decode one local PREPBUFR file into a table with one row per level.

    Parameters
    ----------
    fname : str
        Local PREPBUFR path with embedded tables (or supply ``table``).
    subsets : str or sequence of str, optional
        Selected message types; defaults to all five supported types.
    table : str, optional
        External BUFR table path, passed to ``ncepbufr.open``.

    Returns
    -------
    pandas.DataFrame
        Decoded observations, quality marks, and file/message/report/level keys.
        Missing BUFR values become NaN; missing offsets become NaT.
    """
    selected = _subsets(subsets)
    try:
        import ncepbufr
    except ImportError as exc:
        raise ImportError(
            "PREPBUFR requires the ncepbufr Python bindings from NCEPLIBS-bufr. "
            "See https://noaa-emc.github.io/NCEPLIBS-bufr/python/ncepbufr.html"
        ) from exc

    fname = os.fspath(fname)
    if not os.path.isfile(fname):
        raise FileNotFoundError(fname)
    rows = []
    with _BUFR_LOCK:
        bufr = ncepbufr.open(fname, table=table)
        try:
            message = 0
            while bufr.advance() == 0:
                message += 1
                subset = bufr.msg_type
                if isinstance(subset, bytes):
                    subset = subset.decode("ascii")
                subset = subset.strip()
                if subset not in selected:
                    continue
                reference = pd.to_datetime(str(bufr.msg_date), format="%Y%m%d%H")
                report = 0
                while bufr.load_subset() == 0:
                    report += 1
                    header = np.ma.asarray(bufr.read_subset(_HEADER), dtype="float64")
                    # SID is an IA5 string packed into the bytes of a float64,
                    # not a numeric station identifier. Decode before filling masks.
                    sid = header[0, 0]
                    siteid = (
                        ""
                        if np.ma.is_masked(sid)
                        else np.float64(sid)
                        .tobytes()
                        .decode("ascii", errors="replace")
                        .strip(" \x00")
                    )
                    hdr = header.filled(np.nan)[:, 0]
                    obs = np.ma.asarray(
                        bufr.read_subset(" ".join(_FIELDS)), dtype="float64"
                    ).filled(np.nan)
                    obs[obs == bufr.missing_value] = np.nan
                    hdr[hdr == bufr.missing_value] = np.nan
                    time = (
                        reference + pd.to_timedelta(hdr[3], unit="h")
                        if np.isfinite(hdr[3])
                        else pd.NaT
                    )
                    for level in range(obs.shape[1]):
                        row = dict(
                            siteid=siteid,
                            time=time,
                            latitude=hdr[2],
                            longitude=(hdr[1] + 180) % 360 - 180,
                            elevation=hdr[5],
                            report_type=hdr[4],
                            subset=subset,
                            source_file=fname,
                            message=message,
                            report=report,
                            level=level,
                        )
                        for i, (name, _, scale) in enumerate(_FIELDS.values()):
                            row[name] = obs[i, level] * scale
                        rows.append(row)
        finally:
            bufr.close()
    schema = _schema()
    return pd.DataFrame(rows, columns=schema.columns).astype(schema.dtypes.to_dict())


@register_reader("prepbufr")
class PREPBUFRReader(PointReader):
    """Read PREPBUFR surface, marine, aircraft, and sounding reports."""

    fixed_location = False

    def open_dataset(self, files, *, subsets=None, table=None, expand2d=False, **kwargs):
        """Open PREPBUFR files with optional Dask decoding.

        Parameters
        ----------
        files : str or list of str
            Local file paths or glob pattern.
        subsets : str or sequence of str, optional
            Any of ADPUPA, ADPSFC, SFCSHP, AIRCFT, AIRCAR.
        table : str, optional
            External BUFR table; default uses embedded tables.
        expand2d : bool, optional
            Must be False to preserve repeated times, stations, and levels.
        **kwargs : dict
            Shared PointReader options, including use_dask/lazy and as_xarray.
            One complete file is one Dask partition. Xarray construction scans
            each partition for lengths and time/site keys, then rereads payload
            at compute time. Use as_xarray=False to defer all decoding.

        Returns
        -------
        xarray.Dataset or pandas.DataFrame or dask.dataframe.DataFrame
            Observations indexed by node, preserving every selected level.

        Examples
        --------
        >>> ds = PREPBUFRReader().open_dataset("gdas.prepbufr", subsets="ADPUPA", use_dask=True)
        """
        if expand2d:
            raise ValueError(
                "PREPBUFR requires expand2d=False to preserve individual report levels"
            )
        return super().open_dataset(
            files,
            read_method=partial(read_prepbufr, subsets=_subsets(subsets), table=table),
            meta=_schema(),
            expand2d=False,
            **kwargs,
        )

    def harmonize(self, df):
        """Preserve reports including missing locations and all quality marks."""
        return df

    def to_xarray(self, obj, **kwargs) -> xr.Dataset:
        """Convert to node observations and attach native decoded units."""
        ds = super().to_xarray(obj, **kwargs)
        # A node dimension is required because report times are deliberately
        # non-unique. The shared converter keeps records on node, so only the
        # legacy time-promoted layouts need rewriting back.
        if "time" in ds.dims and "node" not in ds.dims:
            ds = ds.drop_vars("node").rename_dims({"time": "node"})
            ds = ds.assign_coords(node=np.arange(ds.sizes["node"]))
        decoded_fields = {name for name, _, _ in _FIELDS.values()}
        for mnemonic, (name, units, _) in _FIELDS.items():
            if name in ds.variables:
                ds[name].attrs.update(units=units, bufr_mnemonic=mnemonic)
        # Derived quantities need the full decoded BUFR schema; the shared
        # converter contract must also hold for minimal station frames.
        if decoded_fields <= set(ds.variables):
            ds["wind_speed"] = np.hypot(ds.u_wind, ds.v_wind)
            ds["wind_dir"] = (np.degrees(np.arctan2(-ds.u_wind, -ds.v_wind)) % 360).where(
                ds.wind_speed > 0
            )
            # Specific humidity is available in PREPBUFR even when TDO is absent.
            # Bolton's liquid-water saturation formula uses temperature in degC
            # and vapor pressure in hPa. Preserve reported dewpoint when present.
            q = ds.specific_humidity.where((ds.specific_humidity > 0) & (ds.specific_humidity < 1))
            vapor_pressure = (q * ds.pressure / (0.622 + 0.378 * q) / 100).where(ds.pressure > 0)
            log_ratio = np.log(vapor_pressure / 6.112)
            calculated_dewpoint = 243.5 * log_ratio / (17.67 - log_ratio)
            ds["dewpoint"] = ds.dewpoint.fillna(calculated_dewpoint)
            ds.dewpoint.attrs.update(
                units="degC",
                long_name="Reported dewpoint, falling back to pressure and specific humidity",
            )
            ds["rel_humidity"] = 100 * np.exp(
                17.67 * ds.dewpoint / (243.5 + ds.dewpoint)
                - 17.67 * ds.temperature / (243.5 + ds.temperature)
            )
            ds.rel_humidity.attrs.update(
                units="%", long_name="Relative humidity with respect to liquid water"
            )
        if "wind_speed" in ds.variables:
            ds.wind_speed.attrs["units"] = "m s-1"
            ds.wind_dir.attrs.update(units="degree", long_name="Wind direction from true north")
        ds.attrs["source"] = "NCEP PREPBUFR / NCEPLIBS-bufr ncepbufr"
        return update_history(
            ds, "Decoded PREPBUFR report levels; retained quality marks without filtering."
        )
