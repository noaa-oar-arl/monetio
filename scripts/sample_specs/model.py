"""Model/gridded source synthetic specs (formats not redistributable).

Model output (CAMx, WRF-Chem, and friends) is produced by research and
operational models whose files are large, model-internal, and generally not
redistributable as test fixtures. The sample-coverage contract (C-SAMP-11)
accepts a checked-in synthetic reproduction of the *format* for these sources,
provided the generator is in the tree and the reason is recorded — the same
discipline ``scripts/make_multifile_fixture.py`` established.

Each builder writes the smallest file that drives every stage of its reader
(grid decode, variable harmonization, coordinate attach, shape conversion,
provenance), deterministically, and names it the way the reader's own file
matching expects.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


def _ioapi_attrs(ncols: int, nrows: int, nlays: int) -> dict:
    """IOAPI grid attributes for a small Lambert-Conformal domain (GDTYP=2).

    Shared by the CAMx builder: the reader decodes the grid from these attrs, so
    they must be a self-consistent projection or coordinate attachment fails.
    """
    return {
        "IOAPI_VERSION": "3.2",
        "EXEC_ID": "synthetic",
        "FTYPE": 101,
        "DTSTEP": 1,
        "OTHOUR": 0,
        "SDATE": 2023001,
        "STIME": 0,
        "GDTYP": 2,
        "P_ALP": 33.0,
        "P_BET": 45.0,
        "P_GAM": -97.0,
        "XCENT": -97.0,
        "YCENT": 40.0,
        "XORIG": -800000.0,
        "YORIG": -1200000.0,
        "XCELL": 36000.0,
        "YCELL": 36000.0,
        "NCOLS": ncols,
        "NROWS": nrows,
        "NLAYS": nlays,
        "VGTOP": 1.0,
    }


def build_camx(outdir: Path) -> None:
    """A minimal CAMx netCDF: TFLAG + a few gas species on an IOAPI grid.

    Mirrors the shape the CAMx reader expects (TSTEP/LAY/ROW/COL, a TFLAG
    (YYYYDDD, HHMMSS) coordinate, ppm gas units) so the grid decode, the
    ppm->ppb conversion, the NOx diagnostic, and coordinate attachment all run.
    """
    ntime, nlay, nrow, ncol = 2, 1, 3, 4
    rng = np.random.default_rng(20230001)
    tflag = xr.DataArray(
        np.array([[[2023001, 0], [2023001, 10000]]] * 3).transpose(1, 0, 2),
        dims=("TSTEP", "VAR", "DATE_TIME"),
        name="TFLAG",
    )
    species = {
        "O3": np.zeros((ntime, nlay, nrow, ncol)),
        "NO": np.zeros((ntime, nlay, nrow, ncol)),
        "NO2": np.zeros((ntime, nlay, nrow, ncol)),
    }
    for k in species:
        species[k] = rng.random(species[k].shape, dtype="float32") * 0.05
    ds = xr.Dataset(
        {
            **{
                name: (("TSTEP", "LAY", "ROW", "COL"), arr, {"units": "ppm"})
                for name, arr in species.items()
            },
            "TFLAG": tflag,
        },
        attrs=_ioapi_attrs(ncol, nrow, nlay),
    )
    ds.to_netcdf(outdir / "camx_synthetic.nc", engine="h5netcdf")


def build_wrfchem(outdir: Path) -> None:
    """A minimal WRF-Chem netCDF: Times, XLAT/XLONG, gas + aerosol species.

    Follows the reader's expectations: WRF ``Times`` as a character array, the
    (time, bottom_top, south_north, west_east) species layout, and the XLAT/XLONG
    grid the reader promotes to coordinates.
    """
    times = pd.date_range("2023-01-01", periods=3, freq="h")
    times_bytes = np.array([list(t.strftime("%Y-%m-%d_%H:%M:%S")) for t in times], dtype="|S1")
    nz, ny, nx = 2, 5, 5
    shape = (len(times), nz, ny, nx)
    rng = np.random.default_rng(20230002)
    gas = {n: rng.random(shape, dtype="float32") * 0.05 for n in ("O3", "NO", "NO2")}
    aer = {n: rng.random(shape, dtype="float32") for n in ("P25", "BC1")}
    data_vars = {
        "Times": (("time", "DateStrLen"), times_bytes),
        "XLAT": (("time", "south_north", "west_east"), np.zeros((len(times), ny, nx))),
        "XLONG": (("time", "south_north", "west_east"), np.zeros((len(times), ny, nx))),
        **{
            n: (("time", "bottom_top", "south_north", "west_east"), a, {"units": "ppmv"})
            for n, a in gas.items()
        },
        **{
            n: (("time", "bottom_top", "south_north", "west_east"), a, {"units": "ug/kg"})
            for n, a in aer.items()
        },
    }
    ds = xr.Dataset(data_vars)
    ds = ds.set_coords(["XLAT", "XLONG"])
    ds.to_netcdf(outdir / "wrfout_d01_2023-01-01_00-00-00.nc")


def build_pardump(outdir: Path) -> None:
    """A minimal HYSPLIT PARDUMP binary: two records, five particles.

    Mirrors the fixed-width big-endian layout the PARDUMP reader parses
    (a 7-field header per record, then one 16-field record per particle), so
    the header decode, the per-particle loop, the lat/lon/height columns, the
    ``sorti``->``siteid`` harmonization and the xarray reshape all run. This is
    the same byte layout the unit test's mock writer produces, lifted into a
    checked-in generator so the source has an offline sample rather than only a
    throwaway temp file.
    """
    tp1 = ">f4"
    tp2 = ">i4"
    tp3 = ">i8"
    hdr_dt = np.dtype(
        [
            ("padding", tp2),
            ("parnum", tp2),
            ("pollnum", tp2),
            ("year", tp2),
            ("month", tp2),
            ("day", tp2),
            ("hour", tp2),
            ("minute", tp2),
        ]
    )
    pardt = np.dtype(
        [
            ("p1", tp2),
            ("p2", tp2),
            ("pmass", tp1),
            ("p3", tp3),
            ("lat", tp1),
            ("lon", tp1),
            ("ht", tp1),
            ("su", tp1),
            ("sv", tp1),
            ("sx", tp1),
            ("p4", tp3),
            ("age", tp2),
            ("dist", tp2),
            ("poll", tp2),
            ("mgrid", tp2),
            ("sorti", tp2),
        ]
    )

    def record(parnum, hour, pmass, lat0):
        hdr = np.array([(4 * 7, parnum, 1, 2023, 1, 1, hour, 0)], dtype=hdr_dt)
        parts = np.zeros(parnum, dtype=pardt)
        parts["lat"] = np.linspace(lat0, lat0 + 10, parnum)
        parts["lon"] = np.linspace(-100, -90, parnum)
        parts["sorti"] = np.arange(parnum)
        parts["pmass"] = pmass
        return hdr.tobytes() + parts.tobytes() + np.array([4 * 7], dtype=tp2).tobytes()

    (outdir / "PARDUMP.bin").write_bytes(record(5, 12, 1.0, 30) + record(3, 13, 2.0, 31))


def build_raqms(outdir: Path) -> None:
    """A minimal RAQMS netCDF: one time, three levels, a small global lat/lon grid.

    Reproduces the exact variable set and shapes the RAQMS reader's
    preprocessing consumes — the WRF-style ``Times``/``IDATE`` time fields, the
    ``lat``/``lon``/``lev`` grid, the ``psfc``/``delp``/``pdash`` pressures in
    ``mb`` (promoted to Pa), a ``ppv`` gas (``o3vmr``) scaled to ppbv, and
    ``ttheta`` for the derived temperature — so grid decode, time parse,
    pressure rename/scale, unit conversion and the temperature diagnostic all
    run. The reader only accepts files named ``uwhyb*.nc``.
    """
    ntime, nlev, nlat, nlon = 1, 3, 4, 8
    lat = np.linspace(-90.0, 90.0, nlat, dtype="float32")
    lon = np.linspace(0.0, 360.0, nlon, endpoint=False, dtype="float32")
    lev = np.array([233.2, 228.45, 224.5], dtype="float32")[:nlev]
    times = np.array([b"2017_06_01_18:00:00"], dtype="|S19")[:ntime]
    idate = np.array([2017060118], dtype="int32")[:ntime]
    rng = np.random.default_rng(20170601)
    shape4 = (ntime, nlev, nlat, nlon)

    def f4(scale):
        return rng.random(shape4, dtype="float32") * scale

    ds = xr.Dataset(
        {
            "IDATE": (("time",), idate),
            "Times": (("time",), times),
            "delp": (("time", "lev", "lat", "lon"), f4(100.0), {"units": "mb"}),
            "geop": (("time", "lev", "lat", "lon"), f4(1000.0), {"units": "m"}),
            "o3vmr": (("time", "lev", "lat", "lon"), f4(1e-7), {"units": "ppv"}),
            "pdash": (("time", "lev", "lat", "lon"), f4(500.0), {"units": "mb"}),
            "psfc": (
                ("time", "lat", "lon"),
                np.full((ntime, nlat, nlon), 1000.0, "float32"),
                {"units": "mb"},
            ),
            "ttheta": (("time", "lev", "lat", "lon"), f4(250.0) + 200.0, {"units": "K"}),
            "lat": (("lat",), lat, {"units": "degrees_north"}),
            "lev": (("lev",), lev),
            "lon": (("lon",), lon, {"units": "degrees_east"}),
        }
    )
    ds = ds.set_coords(["lat", "lon", "lev"])
    ds.to_netcdf(outdir / "uwhyb_06_01_2017_18Z.chem.assim.nc", engine="h5netcdf")


def build_hysplit(outdir: Path) -> None:
    """A minimal HYSPLIT binary concentration (cdump) file.

    Writes the flat big-endian record stream the reader consumes in order: a
    10-field header (start-location count), the start-location records, a grid
    record (points/spacing/lower-left corner), the level-heights record, the
    species record, then one block per sampling period holding an
    ``[start][end]`` time pair followed by ``(nlev x species)`` concentration
    records, each a 4-field sub-header, a full grid of ``(x, y, conc)`` triples,
    and a trailing pad. The grid is emitted contiguously over ``1..nlon`` and
    ``1..nlat`` so the coordinate attach and grid-continuity check run, and two
    sampling periods exercise the multi-time merge.
    """
    real4, int4, int2, char4 = ">f4", ">i4", ">i2", ">S4"
    rec1 = np.dtype(
        [
            ("pad1", int4),
            ("model_id", char4),
            ("met_year", int4),
            ("met_month", int4),
            ("met_day", int4),
            ("met_hr", int4),
            ("met_fhr", int4),
            ("start_loc", int4),
            ("conc_pack", int4),
            ("pad2", int4),
        ]
    )
    rec2 = np.dtype(
        [
            ("pad1", int4),
            ("r_year", int4),
            ("r_month", int4),
            ("r_day", int4),
            ("r_hr", int4),
            ("s_lat", real4),
            ("s_lon", real4),
            ("s_ht", real4),
            ("r_min", int4),
            ("pad2", int4),
        ]
    )
    rec3 = np.dtype(
        [
            ("pad1", int4),
            ("nlat", int4),
            ("nlon", int4),
            ("dlat", real4),
            ("dlon", real4),
            ("llcrnr_lat", real4),
            ("llcrnr_lon", real4),
            ("pad2", int4),
        ]
    )
    rec4a = np.dtype([("pad1", int4), ("nlev", int4)])
    rec4b = np.dtype([("levht", int4)])
    rec5a = np.dtype([("pad1", int4), ("pad2", int4), ("pollnum", int4)])
    rec5b = np.dtype([("pname", char4)])
    rec5c = np.dtype([("pad2", int4)])
    rec6 = np.dtype(
        [
            ("pad1", int4),
            ("oyear", int4),
            ("omonth", int4),
            ("oday", int4),
            ("ohr", int4),
            ("omin", int4),
            ("oforecast", int4),
            ("pad3", int4),
        ]
    )
    rec8a = np.dtype([("pad1", int4), ("poll", char4), ("lev", int4), ("ne", int4)])
    rec8b = np.dtype([("indx", int2), ("jndx", int2), ("conc", real4)])
    rec8c = np.dtype([("pad2", int4)])

    nlat, nlon, nlev = 3, 4, 2
    species = b"O3  "
    buf = bytearray()

    def write(arr):
        buf.extend(np.asarray(arr, dtype=arr.dtype).tobytes())

    # rec1 header: one start location, species not packed.
    h1 = np.zeros(1, dtype=rec1)
    h1["model_id"] = b"ARW "
    h1["met_year"] = 23
    h1["met_month"] = 1
    h1["met_day"] = 1
    h1["met_hr"] = 0
    h1["start_loc"] = 1
    h1["conc_pack"] = 1
    write(h1)

    # rec2 start location (year is 2-digit -> century guessed to 2000).
    h2 = np.zeros(1, dtype=rec2)
    h2["r_year"] = 23
    h2["r_month"] = 1
    h2["r_day"] = 1
    h2["r_hr"] = 0
    h2["s_lat"] = 40.0
    h2["s_lon"] = -100.0
    h2["s_ht"] = 100.0
    write(h2)

    # rec3 grid: lower-left (-10, 30), 0.5 deg spacing, contiguous 1..nlat/1..nlon.
    h3 = np.zeros(1, dtype=rec3)
    h3["nlat"] = nlat
    h3["nlon"] = nlon
    h3["dlat"] = 0.5
    h3["dlon"] = 0.5
    h3["llcrnr_lat"] = 30.0
    h3["llcrnr_lon"] = -10.0
    write(h3)

    # rec4 level count + heights.
    write(np.array([(0, nlev)], dtype=rec4a))
    write(np.array([(100,), (200,)], dtype=rec4b))

    # rec5 species count + name.
    write(np.array([(0, 0, 1)], dtype=rec5a))
    write(np.array([(species,)], dtype=rec5b))
    write(np.array([(0,)], dtype=rec5c))

    # Two sampling periods; each is a [start][end] pair then (nlev) concentration
    # records for the single species, the grid filled contiguously.
    rng = np.random.default_rng(20230101)
    for hour in (0, 1):
        start = np.zeros(1, dtype=rec6)
        start["oyear"] = 23
        start["omonth"] = 1
        start["oday"] = 1
        start["ohr"] = hour
        write(start)
        end = np.zeros(1, dtype=rec6)
        end["oyear"] = 23
        end["omonth"] = 1
        end["oday"] = 1
        end["ohr"] = hour + 1
        write(end)
        for lev in range(1, nlev + 1):
            write(np.array([(0, species, lev, nlat * nlon)], dtype=rec8a))
            grid = np.zeros(nlat * nlon, dtype=rec8b)
            grid["indx"] = np.tile(np.arange(1, nlon + 1, dtype="int16"), nlat)
            grid["jndx"] = np.repeat(np.arange(1, nlat + 1, dtype="int16"), nlon)
            grid["conc"] = rng.random(nlat * nlon, dtype="float32") * 0.05
            write(grid)
            write(np.array([(0,)], dtype=rec8c))

    (outdir / "cdump_1.bin").write_bytes(bytes(buf))


# source -> (builder, reason, load_kwargs)
SYNTHETICS: dict[str, tuple] = {
    "camx": (
        build_camx,
        "CAMx model output is not redistributable; a synthetic IOAPI grid "
        "reproduces the format the reader decodes.",
        {"engine": "h5netcdf"},
    ),
    "wrfchem": (
        build_wrfchem,
        "WRF-Chem output is model-internal and not redistributable; a synthetic "
        "file reproduces the WRF layout the reader parses.",
        {},
    ),
    "pardump": (
        build_pardump,
        "HYSPLIT PARDUMP output is model-internal and not redistributable; a "
        "synthetic binary reproduces the fixed-width particle record layout the "
        "reader parses.",
        {},
    ),
    "raqms": (
        build_raqms,
        "RAQMS assimilation output is not redistributable; a synthetic netCDF "
        "reproduces the variable set and grid the reader preprocesses.",
        {},
    ),
    "hysplit": (
        build_hysplit,
        "HYSPLIT concentration (cdump) output is model-internal and not "
        "redistributable; a synthetic binary reproduces the big-endian record "
        "stream the reader parses.",
        {},
    ),
}

# Model/gridded sources not sampleable offline (C-SAMP-10), with reasons. A
# source lands here only when its payload is not retrievable anonymously AND its
# format cannot be reproduced synthetically in a way that exercises the reader.
EXCLUSIONS: dict[str, str] = {}
