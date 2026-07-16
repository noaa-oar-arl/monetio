"""NESDIS FRP Reader"""

import datetime
import os
from functools import partial

import numpy as np
import pandas as pd
import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import update_history

BASE_URL = "https://gsce-dtn.sdstate.edu/index.php/s/e8wPYPOL1bGXk5z/download?path=%2F"


@register_reader("nesdis_frp")
class NESDISFRPReader(GriddedReader):
    """
    Reader for NESDIS Fire Radiative Power (FRP) data on FV3 C384 grid.
    """

    def open_dataset(
        self,
        files: str | list[str] = None,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        date: datetime.datetime | str | pd.Timestamp = None,
        ftype: str = "meanFRP",
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads NESDIS FRP data.

        Parameters
        ----------
        files : str or list[str], optional
            File path(s) or URL(s).
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
        date : datetime.datetime, str, or pd.Timestamp, optional
            Date to retrieve. If files is None, this is used to build URLs.
        ftype : str, optional
            Type of FRP data (e.g., 'meanFRP'). Default is 'meanFRP'.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The NESDIS FRP dataset.

        Examples
        --------
        >>> reader = NESDISFRPReader()
        >>> ds = reader.open_dataset(date="2023-01-01", ftype="meanFRP")
        """
        if files is None:
            if date is None:
                raise ValueError("Either 'files' or 'date' must be provided.")
            files = self.build_urls(date, ftype=ftype)

        if "preprocess" not in kwargs:
            kwargs["preprocess"] = partial(nesdis_frp_preprocess, ftype=ftype)

        if "read_method" not in kwargs:
            kwargs["read_method"] = read_nesdis_frp_binary

        # Forward ftype to read_method
        kwargs["ftype"] = ftype

        # We concatenate tiles in the reading step if possible, or use XarrayDriver's concat
        # Actually, each file is a tile.
        if "concat_dim" not in kwargs:
            kwargs["concat_dim"] = "tile"
        if "combine" not in kwargs:
            kwargs["combine"] = "nested"
        ds = super().open_dataset(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser="hdf5",
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            **kwargs,
        )

        # Update history
        ds = update_history(ds, f"Read NESDIS {ftype} data.")

        return ds

    def build_urls(
        self, date: datetime.datetime | str | pd.Timestamp, ftype: str = "meanFRP"
    ) -> list[str]:
        """
        Build URLs for NESDIS FRP data based on date.

        Parameters
        ----------
        date : datetime.datetime, str, or pd.Timestamp
            Date to retrieve.
        ftype : str, optional
            File type (e.g., 'meanFRP'), by default "meanFRP".

        Returns
        -------
        list[str]
            List of URLs.

        Examples
        --------
        >>> reader = NESDISFRPReader()
        >>> urls = reader.build_urls("2023-01-01")
        """
        date = pd.Timestamp(date)
        yyyymmdd = date.strftime("%Y%m%d")
        url_ftype = f"&files={ftype}."

        urls = []
        for i in range(1, 7):
            tile = f".FV3C384Grid.tile{i}.bin"
            url = f"{BASE_URL}{yyyymmdd}{url_ftype}{yyyymmdd}{tile}"
            urls.append(url)

        return urls


def read_nesdis_frp_binary(fname: str, **kwargs) -> xr.Dataset:
    """
    Read a single NESDIS FRP tile from a binary file.
    Supports streaming from fsspec-compatible files.

    Parameters
    ----------
    fname : str
        Path or URL to the binary file.
    **kwargs : dict
        Additional arguments (res, dtype, lazy).

    Returns
    -------
    xr.Dataset
        The tile data as a Dataset.

    Examples
    --------
    >>> ds = read_nesdis_frp_binary("meanFRP.20230101.FV3.C384Grid.tile1.bin")
    """
    res = kwargs.get("res", "C384")
    dtype = kwargs.get("dtype", "f4")
    lazy = kwargs.get("lazy", "chunks" in kwargs)

    r = int(res[1:])
    shape = (r, r)

    def _read_core(filename):
        from scipy.io import FortranFile

        from .drivers import FileUtility

        fs = FileUtility.get_fs(filename)
        with fs.open(filename, "rb") as f:
            # We need to wrap it in a seekable stream for FortranFile if it's remote
            # But FortranFile might not like fsspec file objects if they aren't fully seekable/buffered
            # Alternatively, read it all and use BytesIO
            import io

            # Ensure we are at the start and the stream is seekable for FortranFile
            buffer = io.BytesIO(f.read())
            w = FortranFile(buffer)
            try:
                a = w.read_reals(dtype=dtype)
            except Exception:
                # Fallback: maybe it's not a reals record but a simple binary dump
                # FortranFile expects header/footer. If missing, it fails.
                buffer.seek(0)
                a = np.frombuffer(buffer.read(), dtype=dtype)
        return a.reshape((r, r), order="F").copy()

    if lazy:
        import dask.array as da
        from dask import delayed

        load_tile = delayed(_read_core)(fname)
        data = da.from_delayed(load_tile, shape=shape, dtype=np.dtype(dtype))
    else:
        data = _read_core(fname)

    # Extract tile and date from filename if possible
    # Example: meanFRP.20230101.FV3.C384Grid.tile1.bin
    tile = 1
    date = None
    basename = os.path.basename(fname)
    try:
        import re

        tile_match = re.search(r"tile(\d+)", basename)
        if tile_match:
            tile = int(tile_match.group(1))

        date_match = re.search(r"(\d{8})", basename)
        if date_match:
            date = pd.to_datetime(date_match.group(1))
    except (ValueError, TypeError):
        pass

    ds = xr.Dataset(data_vars={"frp": (("x", "y"), data)}, coords={"tile": tile})

    if date:
        ds = ds.assign_coords(time=date).expand_dims("time")

    return ds


def nesdis_frp_preprocess(ds: xr.Dataset, ftype: str = "meanFRP") -> xr.Dataset:
    """
    Preprocess NESDIS FRP dataset: assign coordinates and metadata.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    ftype : str, optional
        File type, by default "meanFRP".

    Returns
    -------
    xr.Dataset
        Processed dataset.

    Examples
    --------
    >>> ds = nesdis_frp_preprocess(ds, ftype="meanFRP")
    """
    # 1. Rename to ftype if it was generic
    if "frp" in ds.data_vars and ftype != "frp":
        ds = ds.rename({"frp": ftype})

    # 2. Handle Grid and Coordinates
    # We assume C384 for now as per legacy reader
    res = "C384"
    # ds.tile is usually a scalar coordinate if it's from a single file (tile)
    # but could be an array if concatenated.
    tile = None
    if not hasattr(ds.tile.data, "dask"):
        try:
            if ds.tile.ndim == 0:
                tile = int(ds.tile)
        except (TypeError, ValueError):
            pass

    # If tile is dask-backed, we might need to be careful.
    # But tile should be a coordinate, usually small and eager.
    if tile is not None:
        try:
            import fv3grid as fg

            grid = fg.get_fv3_grid(res=res, tile=tile)
            # Wrap longitudes to [-180, 180]
            lon = (grid.longitude + 180) % 360 - 180
            lat = grid.latitude

            ds = ds.assign_coords(
                latitude=(("x", "y"), lat),
                longitude=(("x", "y"), lon),
            )

            ds.latitude.attrs.update({"units": "degrees_north", "standard_name": "latitude"})
            ds.longitude.attrs.update({"units": "degrees_east", "standard_name": "longitude"})
        except ImportError:
            pass

    # 3. Scientific Hygiene: Metadata
    if ftype in ds.data_vars:
        ds[ftype].attrs.update(
            {
                "long_name": f"NESDIS {ftype} Fire Radiative Power",
                "units": "MW",  # Assuming MW for FRP
            }
        )
        ds = update_history(ds, f"Updated attributes for {ftype}")

    # Provenance
    ds = update_history(ds, f"Preprocessed NESDIS {ftype} data using standardized preprocessing.")

    return ds
