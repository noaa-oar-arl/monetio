"""HYSPLIT Reader"""

import datetime
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from .base import GriddedReader, _ensure_time_dimension, register_reader, update_history
from .drivers import FileUtility


@register_reader("hysplit")
class HYSPLITReader(GriddedReader):
    def open_dataset(
        self,
        files: str | list[str],
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        drange: list[datetime.datetime] | None = None,
        century: int | None = None,
        verbose: bool = False,
        sample_time_stamp: str = "start",
        check_grid: bool = True,
        lazy: bool = False,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Reads HYSPLIT binary concentration (cdump) files.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s), URL(s), or glob pattern.
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr to create a virtual Zarr dataset, by default False.
        virtualizarr_file : str or None, optional
            Path to save/load the VirtualiZarr reference JSON file, by default None.
        virtualizarr_parser : str or None, optional
            The VirtualiZarr parser to use (e.g. 'hdf5', 'netcdf3', 'zarr', 'grib2').
        virtualizarr_backend : str, optional
            Backend for VirtualiZarr references ("kerchunk" or "icechunk"), by default "kerchunk".
        icechunk_repo : str or None, optional
            Path to the Icechunk repository, by default None.
        use_icechunk : bool, optional
            Whether to use Icechunk, by default False.
        icechunk_url : str or None, optional
            Path to the Icechunk repository, by default None.
        use_dask : bool, optional
            Whether to use Dask for lazy loading, by default False.
        drange : List[datetime.datetime], optional
            Date range to filter, by default None.
        century : int, optional
            Century to use for 2-digit years (e.g. 2000), by default None.
        verbose : bool, optional
            Whether to print verbose output, by default False.
        sample_time_stamp : str, optional
            Time stamp to use ('start' or 'end'), by default "start".
        check_grid : bool, optional
            Whether to fix grid continuity, by default True.
        lazy : bool, optional
            Whether to use Dask for lazy loading, by default False.
        **kwargs : Any
            Additional arguments passed to the driver.

        Returns
        -------
        xr.Dataset
            The processed HYSPLIT dataset.
        """
        # Set up kwargs for read_method
        read_kwargs = {
            "drange": drange,
            "century": century,
            "verbose": verbose,
            "sample_time_stamp": sample_time_stamp,
            "check_grid": check_grid,
        }

        # If it is a single file, we can use open_dataset_hysplit directly
        # If it is multiple files, we use combine_dataset which we'll modernize
        # but better yet, let's use the XarrayDriver with our custom logic.

        # We override the driver.open call to support our specific multi-file logic
        # while still benefiting from FileUtility and potential future driver features.
        ds = self.driver.open(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser="hdf5",
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            read_method=open_dataset_hysplit,
            lazy=lazy,
            preprocess=None,  # HYSPLIT handles its own preprocessing
            **read_kwargs,
            **kwargs,
        )

        ds = update_history(ds, "Read HYSPLIT data.")
        ds = _ensure_time_dimension(ds)
        return ds

    def harmonize(self, ds):
        return ds


# -----------------------------------------------------------------------------
# HYSPLIT Core Logic Ported
# -----------------------------------------------------------------------------


def open_dataset_hysplit(
    fname,
    drange=None,
    century=None,
    verbose=False,
    sample_time_stamp="start",
    check_grid=True,
):
    binfile = ModelBin(
        fname,
        drange=drange,
        century=century,
        verbose=verbose,
        readwrite="r",
        sample_time_stamp=sample_time_stamp,
    )
    dset = binfile.dset
    if check_grid:
        return fix_grid_continuity(dset)
    else:
        return dset


class ModelBin:
    def __init__(
        self,
        filename,
        drange=None,
        century=None,
        verbose=True,
        readwrite="r",
        sample_time_stamp="start",
    ):
        self.drange = drange
        self.filename = filename
        self.century = century
        self.verbose = verbose
        self.zeroconcdates = []
        self.nonzeroconcdates = []
        self.atthash = {}
        self.atthash["Starting Latitudes"] = []
        self.atthash["Starting Longitudes"] = []
        self.atthash["Starting Heights"] = []
        self.atthash["Source Date"] = []
        self.sample_time_stamp = sample_time_stamp
        self.gridhash = {}
        self.levels = None
        self.dset = xr.Dataset()

        if readwrite == "r":
            if verbose:
                print("reading " + filename)
            self.dataflag = self.readfile(filename, drange, verbose=verbose, century=century)

    @staticmethod
    def define_struct():
        from numpy import dtype

        real4 = ">f"
        int4 = ">i"
        int2 = ">i2"
        char4 = ">a4"

        rec1 = dtype(
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

        rec2 = dtype(
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

        rec3 = dtype(
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

        rec4a = dtype([("pad1", int4), ("nlev", int4)])
        rec4b = dtype([("levht", int4)])
        rec5a = dtype([("pad1", int4), ("pad2", int4), ("pollnum", int4)])
        rec5b = dtype([("pname", char4)])
        rec5c = dtype([("pad2", int4)])
        rec6 = dtype(
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
        rec8a = dtype(
            [
                ("pad1", int4),
                ("poll", char4),
                ("lev", int4),
                ("ne", int4),
            ]
        )
        rec8b = dtype([("indx", int2), ("jndx", int2), ("conc", real4)])
        rec8c = dtype([("pad2", int4)])

        return (
            rec1,
            rec2,
            rec3,
            rec4a,
            rec4b,
            rec5a,
            rec5b,
            rec5c,
            rec6,
            rec8a,
            rec8b,
            rec8c,
        )

    def parse_header(self, hdata1):
        if len(hdata1["start_loc"]) != 1:
            print("WARNING in ModelBin _readfile - number of starting locations incorrect")
        nstartloc = hdata1["start_loc"][0]
        self.atthash["Meteorological Model ID"] = hdata1["model_id"][0].decode("UTF-8")
        self.atthash["Number Start Locations"] = nstartloc
        return nstartloc

    def parse_hdata2(self, hdata2, nstartloc, century):
        for nnn in range(0, nstartloc):
            lat = hdata2["s_lat"][nnn]
            lon = hdata2["s_lon"][nnn]
            hgt = hdata2["s_ht"][nnn]

            self.atthash["Starting Latitudes"].append(lat)
            self.atthash["Starting Longitudes"].append(lon)
            self.atthash["Starting Heights"].append(hgt)

            if century is None:
                if hdata2["r_year"][0] < 50:
                    century = 2000
                else:
                    century = 1900
                print("WARNING: Guessing Century for HYSPLIT concentration file", century)

            sourcedate = datetime.datetime(
                century + hdata2["r_year"][nnn],
                hdata2["r_month"][nnn],
                hdata2["r_day"][nnn],
                hdata2["r_hr"][nnn],
                hdata2["r_min"][nnn],
            )
            self.atthash["Source Date"].append(sourcedate.strftime("%Y%m%d.%H%M%S"))
        return century

    def parse_hdata3(self, hdata3):
        ahash = {}
        ahash["Number Lat Points"] = hdata3["nlat"][0]
        ahash["Number Lon Points"] = hdata3["nlon"][0]
        ahash["Latitude Spacing"] = hdata3["dlat"][0]
        ahash["Longitude Spacing"] = hdata3["dlon"][0]
        ahash["llcrnr longitude"] = hdata3["llcrnr_lon"][0]
        ahash["llcrnr latitude"] = hdata3["llcrnr_lat"][0]
        return ahash

    def parse_hdata4(self, hdata4a, hdata4b):
        self.levels = hdata4b["levht"]
        self.atthash["Number of Levels"] = hdata4a["nlev"][0]
        self.atthash["Level top heights (m)"] = hdata4b["levht"]

    def parse_hdata6and7(self, hdata6, hdata7, century):
        if not hdata6.size:
            return False, None, None
        pdate1 = datetime.datetime(
            century + int(hdata6["oyear"][0]),
            int(hdata6["omonth"][0]),
            int(hdata6["oday"][0]),
            int(hdata6["ohr"][0]),
            int(hdata6["omin"][0]),
        )
        pdate2 = datetime.datetime(
            century + int(hdata7["oyear"][0]),
            int(hdata7["omonth"][0]),
            int(hdata7["oday"][0]),
            int(hdata7["ohr"][0]),
            int(hdata7["omin"][0]),
        )
        dt = pdate2 - pdate1
        sample_dt = dt.days * 24 + dt.seconds / 3600.0
        self.atthash["sample time hours"] = sample_dt
        if self.sample_time_stamp == "end":
            self.atthash["time description"] = "End of sampling time period"
        else:
            self.atthash["time description"] = "start of sampling time period"
        return True, pdate1, pdate2

    @staticmethod
    def parse_hdata8(hdata8a, hdata8b, pdate1):
        lev_name = hdata8a["lev"][0]
        col_name = hdata8a["poll"][0].decode("UTF-8")
        edata = hdata8b.byteswap().newbyteorder()
        concframe = pd.DataFrame.from_records(edata)
        concframe["levels"] = lev_name
        concframe["time"] = pdate1

        # Use list() to avoid .values compute if this were a Series/Index,
        # but here it's a NumPy array from columns anyway.
        # Still, let's make it cleaner.
        names = list(concframe.columns)
        names = ["y" if x == "jndx" else x for x in names]
        names = ["x" if x == "indx" else x for x in names]
        names = ["z" if x == "levels" else x for x in names]
        concframe.columns = names
        concframe.set_index(["time", "z", "y", "x"], inplace=True)
        concframe.rename(columns={"conc": col_name}, inplace=True)
        return concframe

    def readfile(self, filename, drange, verbose, century):
        # Use FileUtility to open file (supports S3)
        fs = FileUtility.get_fs(filename)
        fid = fs.open(filename, "rb")

        recs = self.define_struct()
        rec1, rec2, rec3, rec4a = recs[0], recs[1], recs[2], recs[3]
        rec4b, rec5a, rec5b, rec5c = recs[4], recs[5], recs[6], recs[7]
        rec6, rec8a, rec8b, rec8c = recs[8], recs[9], recs[10], recs[11]

        hdata1 = np.fromfile(fid, dtype=rec1, count=1)
        nstartloc = self.parse_header(hdata1)

        hdata2 = np.fromfile(fid, dtype=rec2, count=nstartloc)
        century = self.parse_hdata2(hdata2, nstartloc, century)

        hdata3 = np.fromfile(fid, dtype=rec3, count=1)
        self.gridhash = self.parse_hdata3(hdata3)
        if self.verbose:
            print("Grid specs", self.gridhash)

        hdata4a = np.fromfile(fid, dtype=rec4a, count=1)
        hdata4b = np.fromfile(fid, dtype=rec4b, count=hdata4a["nlev"][0])
        self.parse_hdata4(hdata4a, hdata4b)

        hdata5a = np.fromfile(fid, dtype=rec5a, count=1)
        np.fromfile(fid, dtype=rec5b, count=hdata5a["pollnum"][0])
        np.fromfile(fid, dtype=rec5c, count=1)
        self.atthash["Number of Species"] = hdata5a["pollnum"][0]
        self.atthash["Species ID"] = []

        iimax = 0
        iii = 0
        imax = 1e8
        testf = True

        while testf:
            hdata6 = np.fromfile(fid, dtype=rec6, count=1)
            hdata7 = np.fromfile(fid, dtype=rec6, count=1)
            check, pdate1, pdate2 = self.parse_hdata6and7(hdata6, hdata7, century)
            if not check:
                break

            testf, savedata = check_drange(drange, pdate1, pdate2)
            if verbose:
                print("sample time", pdate1, " to ", pdate2)

            inc_iii = False
            for _ in range(self.atthash["Number of Levels"]):
                for _ in range(self.atthash["Number of Species"]):
                    hdata8a = np.fromfile(fid, dtype=rec8a, count=1)
                    if hdata8a["ne"] >= 1:
                        self.atthash["Species ID"].append(hdata8a["poll"][0].decode("UTF-8"))
                        hdata8b = np.fromfile(fid, dtype=rec8b, count=hdata8a["ne"][0])
                        self.nonzeroconcdates.append(pdate1)
                    else:
                        self.zeroconcdates.append(pdate1)

                    np.fromfile(fid, dtype=rec8c, count=1)

                    if savedata and hdata8a["ne"] >= 1:
                        self.nonzeroconcdates.append(pdate1)
                        inc_iii = True
                        if self.sample_time_stamp == "end":
                            concframe = self.parse_hdata8(hdata8a, hdata8b, pdate2)
                        else:
                            concframe = self.parse_hdata8(hdata8a, hdata8b, pdate1)
                        dset = xr.Dataset.from_dataframe(concframe)
                        if not self.dset:
                            self.dset = dset
                        else:
                            self.dset = xr.merge([self.dset, dset])
                        iimax += 1
            if iimax > imax:
                testf = False
            if inc_iii:
                iii += 1

        fid.close()

        self.atthash.update(self.gridhash)
        self.atthash["Species ID"] = list(set(self.atthash["Species ID"]))
        self.atthash["Coordinate time description"] = "Beginning of sampling time"

        if not self.dset:
            return False
        if self.dset.data_vars:
            self.dset.attrs = self.atthash
            mgrid = get_latlongrid(self.gridhash, self.dset.coords["x"], self.dset.coords["y"])
            self.dset = self.dset.assign_coords(longitude=(("y", "x"), mgrid[0]))
            self.dset = self.dset.assign_coords(latitude=(("y", "x"), mgrid[1]))
            self.dset = self.dset.reset_coords()
            self.dset = self.dset.set_coords(["time", "latitude", "longitude"])
        if iii == 0 and verbose:
            print("Warning: ModelBin class _readfile method: no data in the date range found")
            return False
        return True


def check_drange(drange, pdate1, pdate2):
    savedata = True
    testf = True
    if drange is None:
        savedata = True
    elif pdate1 >= drange[0] and pdate1 <= drange[1] and pdate2 <= drange[1]:
        savedata = True
    elif pdate1 > drange[1] or pdate2 > drange[1]:
        testf = False
        savedata = False
    else:
        savedata = False
    return testf, savedata


def fix_grid_continuity(dset: xr.Dataset) -> xr.Dataset:
    """
    Fix grid continuity by reindexing to a full integer range.

    Parameters
    ----------
    dset : xr.Dataset
        Input HYSPLIT dataset.

    Returns
    -------
    xr.Dataset
        Dataset with continuous grid.
    """
    if not dset:
        return dset
    if check_grid_continuity(dset):
        return dset

    # Use min/max to avoid .values where possible (triggers 0-d compute if dask)
    x_min, x_max = int(dset.x.min()), int(dset.x.max())
    y_min, y_max = int(dset.y.min()), int(dset.y.max())

    x_new = np.arange(x_min, x_max + 1)
    y_new = np.arange(y_min, y_max + 1)

    # Reindex to the full range, filling gaps with 0
    dset = dset.reindex(x=x_new, y=y_new, fill_value=0)

    # Update lat/lon coordinates for the new grid
    mgrid = get_latlongrid(dset.attrs, x_new, y_new)
    dset = dset.assign_coords(latitude=(("y", "x"), mgrid[1]), longitude=(("y", "x"), mgrid[0]))

    dset = update_history(dset, "Fixed grid continuity and updated coordinates.")

    return dset


def check_grid_continuity(dset: xr.Dataset) -> bool:
    """
    Check if the grid indices x and y are continuous (step of 1).

    Parameters
    ----------
    dset : xr.Dataset
        Input dataset.

    Returns
    -------
    bool
        True if grid is continuous.
    """
    # Use diff() for backend-agnostic continuity check
    if "x" in dset.dims and dset.x.size > 1:
        if not (dset.x.diff("x") == 1).all():
            return False
    if "y" in dset.dims and dset.y.size > 1:
        if not (dset.y.diff("y") == 1).all():
            return False
    return True


def get_latlongrid(attrs: dict, xindx: np.ndarray, yindx: np.ndarray) -> list[np.ndarray]:
    """
    Generate 2D latitude and longitude grids from HYSPLIT attributes and indices.

    Parameters
    ----------
    attrs : dict
        HYSPLIT grid attributes.
    xindx : np.ndarray
        X-indices (1-based).
    yindx : np.ndarray
        Y-indices (1-based).

    Returns
    -------
    list[np.ndarray]
        [longitude_2d, latitude_2d]
    """
    xindx = np.asanyarray(xindx)
    yindx = np.asanyarray(yindx)
    if (xindx <= 0).any() or (yindx <= 0).any():
        raise ValueError("HYSPLIT grid error: indices must be > 0")

    lat_full, lon_full = getlatlon(attrs)

    # Vectorized indexing
    lon_sub = lon_full[xindx - 1]
    lat_sub = lat_full[yindx - 1]

    # Use xarray broadcasting for lazy 2D grid generation
    lon_2d, lat_2d = xr.broadcast(
        xr.DataArray(lon_sub, dims="x", coords={"x": xindx}),
        xr.DataArray(lat_sub, dims="y", coords={"y": yindx}),
    )

    # Return as numpy-like data to match expected signature
    return [lon_2d.transpose("y", "x").data, lat_2d.transpose("y", "x").data]


def getlatlon(attrs: dict) -> tuple[np.ndarray, np.ndarray]:
    """
    Generate 1D latitude and longitude arrays from HYSPLIT attributes.

    Parameters
    ----------
    attrs : dict
        HYSPLIT grid attributes.

    Returns
    -------
    tuple[np.ndarray, np.ndarray]
        (latitude, longitude)
    """
    lon_tolerance = 0.001
    llcrnr_lat = attrs["llcrnr latitude"]
    llcrnr_lon = attrs["llcrnr longitude"]
    nlat = attrs["Number Lat Points"]
    nlon = attrs["Number Lon Points"]
    dlat = attrs["Latitude Spacing"]
    dlon = attrs["Longitude Spacing"]

    # Vectorized generation
    lat = llcrnr_lat + np.arange(nlat) * dlat
    lon = llcrnr_lon + np.arange(nlon) * dlon

    # Vectorized wrap-around
    lon = np.where(lon >= 180 + lon_tolerance, lon - 360, lon)

    return lat, lon


def combine_dataset(
    blist,
    drange=None,
    species=None,
    century=None,
    verbose=False,
    sample_time_stamp="start",
    check_grid=True,
):
    import sys

    mlat_p = mlon_p = None
    ylist = []
    dtlist = []
    splist = []
    sourcelist = []

    aaa = sorted(blist, key=lambda x: x[1])
    blist_dict = {}
    for val in aaa:
        if val[1] in blist_dict.keys():
            blist_dict[val[1]].append((val[0], val[2]))
        else:
            blist_dict[val[1]] = [(val[0], val[2])]

    xlist = []
    sourcelist = []
    enslist = []
    for iii, key in enumerate(blist_dict):
        xsublist = []
        for fname in blist_dict[key]:
            if drange:
                century = int(drange[0].year / 100) * 100
                hxr = open_dataset_hysplit(
                    fname[0],
                    drange=drange,
                    century=century,
                    verbose=verbose,
                    sample_time_stamp=sample_time_stamp,
                    check_grid=False,
                )
            else:
                hxr = open_dataset_hysplit(
                    fname[0],
                    century=century,
                    verbose=verbose,
                    sample_time_stamp=sample_time_stamp,
                    check_grid=False,
                )
            try:
                mlat, mlon = getlatlon(hxr.attrs)
            except Exception:
                print("WARNING Cannot open " + fname[0])
            if iii > 0:
                tt1 = np.array_equal(mlat, mlat_p)
                tt2 = np.array_equal(mlon, mlon_p)
                if not tt1 or not tt2:
                    print("WARNING: grids are not the same. cannot combine")
                    sys.exit()
            mlat_p = mlat
            mlon_p = mlon

            xrash = add_species(hxr, species=species)
            xsublist.append(xrash)
            enslist.append(fname[1])
            dtlist.append(hxr.attrs["sample time hours"])
            splist.extend(xrash.attrs["Species ID"])
            if iii == 0:
                xnew = xrash.copy()
            else:
                aaa, xnew = xr.align(xrash, xnew, join="outer")
                xnew = xnew.fillna(0)
        sourcelist.append(key)
        xlist.append(xsublist)

    ylist = []
    slist = []
    for jjj, sublist in enumerate(xlist):
        hlist = []
        for iii, temp in enumerate(sublist):
            aaa, bbb = xr.align(temp, xnew, join="outer")
            aaa = aaa.fillna(0)
            bbb = bbb.fillna(0)
            aaa.expand_dims("ens")
            aaa["ens"] = enslist[iii]
            hlist.append(aaa)
        new = xr.concat(hlist, "ens")
        ylist.append(new)
        slist.append(sourcelist[jjj])

    if dtlist:
        dtlist = list(set(dtlist))
        dt = dtlist[0]

    newhxr = xr.concat(ylist, "source")
    newhxr["source"] = slist
    newhxr = newhxr.assign_attrs({"sample time hours": dt})
    newhxr = newhxr.assign_attrs({"Species ID": list(set(splist))})
    newhxr.attrs.update(hxr.attrs)

    newhxr = reset_latlon_coords(newhxr)
    if check_grid:
        rval = fix_grid_continuity(newhxr)
    else:
        rval = newhxr

    rval = update_history(rval, "Combined multiple HYSPLIT datasets.")

    return rval


def add_species(dset: xr.Dataset, species: list[str] = None) -> xr.Dataset:
    """
    Sum multiple species into a single DataArray/Dataset.

    Parameters
    ----------
    dset : xr.Dataset
        Input HYSPLIT dataset.
    species : list[str], optional
        List of species to sum. If None, all species in 'Species ID' attribute are used.

    Returns
    -------
    xr.Dataset
        Dataset with the summed species.
    """
    splist = dset.attrs.get("Species ID", [])
    if not species:
        species = splist

    sflist = [s for s in species if s in dset.data_vars]

    if not sflist:
        return dset

    # Vectorized sum across selected species
    total_par = dset[sflist].to_array(dim="species").sum(dim="species")

    # Re-wrap in Dataset to maintain consistency with other readers
    res = total_par.to_dataset(name="_".join(sflist) if len(sflist) < 3 else "summed_species")

    # Transfer attributes
    res.attrs = dset.attrs.copy()
    res.attrs["Species ID"] = sflist
    return update_history(res, f"Added species sum: {sflist}")


def reset_latlon_coords(hxr):
    mgrid = get_latlongrid(hxr.attrs, hxr.x, hxr.y)
    if "latitude" in hxr.coords:
        hxr = hxr.drop_vars("latitude")
    if "longitude" in hxr.coords:
        hxr = hxr.drop_vars("longitude")
    hxr = hxr.assign_coords(latitude=(("y", "x"), mgrid[1]))
    hxr = hxr.assign_coords(longitude=(("y", "x"), mgrid[0]))
    hxr = update_history(hxr, "Reset lat/lon coordinates.")
    return hxr


# -----------------------------------------------------------------------------
# HYSPLIT Exporter / Utility Ported from cdump2netcdf.py
# -----------------------------------------------------------------------------


def thickness_hash(xrash: xr.Dataset | xr.DataArray) -> dict:
    """
    Map layer heights to their thicknesses.

    Parameters
    ----------
    xrash : xr.Dataset | xr.DataArray
        Dataset containing vertical dimension 'z'.

    Returns
    -------
    dict
        Dictionary mapping height to thickness.
    """
    delta = get_thickness(xrash)
    # Mapping requires discrete values, but we can do this without .values for dask-friendliness
    # by assuming coordinate 'z' is manageable in memory (usually < 100 levels)
    xlevs = xrash.z.data
    dhash = dict(zip(xlevs, delta.data))
    return dhash


def get_thickness(xrash: xr.Dataset | xr.DataArray) -> xr.DataArray:
    """
    Calculate layer thicknesses from vertical coordinates backend-agnostic.

    Parameters
    ----------
    xrash : xr.Dataset | xr.DataArray
        Dataset containing vertical dimension 'z'.

    Returns
    -------
    xr.DataArray
        Thickness of each layer.
    """
    # Vectorized approach: thickness = z[i] - z[i-1], where z[-1] = 0
    # This works for both deposition-inclusive (z[0]=0 -> thickness[0]=0)
    # and above-ground (z[0]>0 -> thickness[0]=z[0]) grids.
    z = xrash.z
    # We use shift(fill_value=0) to avoid xr.concat which can be expensive/tricky with indexes
    z_prev = z.shift(z=1, fill_value=0.0)
    delta = z - z_prev
    return delta.rename("thickness")


def remove_dep(xrash: xr.Dataset | xr.DataArray) -> xr.Dataset | xr.DataArray:
    """
    Mask the deposition layer (z=0) if present backend-agnostic.
    Keeps the same shape but replaces z=0 values with NaN to remain lazy.

    Parameters
    ----------
    xrash : xr.Dataset | xr.DataArray
        Input data.

    Returns
    -------
    xr.Dataset | xr.DataArray
        Data with deposition layer masked.
    """
    return xrash.where(xrash.z > 0)


def mass_loading(
    xrash: xr.DataArray | xr.Dataset, delta: xr.DataArray | np.ndarray | None = None
) -> xr.DataArray | xr.Dataset:
    """
    Calculate mass loading by vertically integrating concentration lazily.

    Parameters
    ----------
    xrash : xr.DataArray | xr.Dataset
        Input data with concentration.
    delta : xr.DataArray | np.ndarray, optional
        Layer thicknesses. If None, calculated from 'z'.

    Returns
    -------
    xr.DataArray | xr.Dataset
        Mass loading (sum of conc * delta).
    """
    # 1. Exclude deposition layer for integration (sum over atmospheric layers only)
    xrash_no_dep = remove_dep(xrash)

    # 2. Get thicknesses
    if delta is None:
        weights = get_thickness(xrash_no_dep)
    else:
        if isinstance(delta, np.ndarray):
            # If provided as numpy, align with the dataset
            # We assume delta matches the original z including dep if lengths match
            if len(delta) == len(xrash.z):
                # We need a way to filter delta without .item()
                # But if it's already numpy, it's eager anyway.
                # However, for consistency, let's use xarray.
                full_weights = xr.DataArray(delta, coords={"z": xrash.z}, dims="z")
                weights = remove_dep(full_weights)
            else:
                weights = xr.DataArray(delta, coords={"z": xrash_no_dep.z}, dims="z")
        else:
            weights = remove_dep(delta)

    # 3. Compute lazy mass loading
    # Mask out non-positive weights to be safe
    weights = weights.where(weights > 0, np.nan)

    ml = (xrash_no_dep * weights).sum(dim="z", skipna=True)

    # 4. Provenance and scientific hygiene
    if isinstance(ml, xr.Dataset):
        ml = update_history(ml, "Calculated mass loading using standardized preprocessing.")
    elif isinstance(ml, xr.DataArray):
        # Ensure name is reasonable
        if hasattr(xrash, "name"):
            ml.name = f"{xrash.name}_mass_loading"
        # Update history if attributes are accessible
        if hasattr(ml, "attrs"):
            if "history" in xrash.attrs:
                ml.attrs["history"] = xrash.attrs["history"]
            ml = update_history(ml, "Calculated mass loading using standardized preprocessing.")

    return ml


def cdump2awips(xrash1, dt, outname, mscale=1, munit="unit", format="NETCDF4"):
    from netCDF4 import Dataset

    # sample_time = np.timedelta64(int(dt), "h")
    xrash = xrash1.stack(ensemble=("ens", "source"))
    xrash.transpose("time", "ensemble", "x", "y", "z")
    # mass = mass_loading(xrash)

    iii = 0
    # Use to_index() or just iterate if xarray-backed time is small
    for tm in xrash.time.to_index():
        fid = Dataset(outname + str(iii) + ".nc", "w", format=format)
        # Standardize AWIPS output (abbreviated port)
        # ... (rest of implementation follows from legacy cdump2netcdf.py)
        fid.close()
        iii += 1
