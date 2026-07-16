"""NESDIS EDR VIIRS Reader"""

import datetime
import os
from functools import partial

import numpy as np
import pandas as pd
import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import update_history


@register_reader("nesdis_edr_viirs")
class NESDISEDRVIIRSReader(GriddedReader):
    """
    Reader for NESDIS EDR VIIRS gridded AOD data.
    Available via FTP.
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
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str = None,
        resolution: str = "high",
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads NESDIS EDR VIIRS data.

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
        dates : pd.DatetimeIndex, list, datetime, or str, optional
            Dates to retrieve. If files is None, this is used to build URLs.
        resolution : str, optional
            'high' (0.10 deg) or 'low' (0.25 deg). Default is 'high'.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The NESDIS EDR VIIRS dataset.

        Examples
        --------
        >>> reader = NESDISEDRVIIRSReader()
        >>> ds = reader.open_dataset(date="2023-01-01", resolution="high")
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files = self.build_urls(dates, resolution=resolution)

        if "preprocess" not in kwargs:
            kwargs["preprocess"] = partial(nesdis_edr_viirs_preprocess, resolution=resolution)

        if "read_method" not in kwargs:
            kwargs["read_method"] = read_nesdis_edr_binary

        # Forward resolution to read_method
        kwargs["resolution"] = resolution

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
        ds = update_history(ds, "Read NESDIS EDR VIIRS data.")

        return ds

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        resolution: str = "high",
    ) -> list[str]:
        """
        Build FTP URLs for NESDIS EDR VIIRS data based on dates.

        Parameters
        ----------
        dates : pd.DatetimeIndex, list, datetime, or str
            Dates to retrieve.
        resolution : str, optional
            'high' or 'low', by default "high".

        Returns
        -------
        list[str]
            List of FTP URLs.

        Examples
        --------
        >>> reader = NESDISEDRVIIRSReader()
        >>> urls = reader.build_urls("2023-01-01", resolution="high")
        """
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        server = "ftp.star.nesdis.noaa.gov"
        base_dir = "/pub/smcd/jhuang/npp.viirs.aerosol.data/edraot550"

        urls = []
        for d in dates:
            year = d.strftime("%Y")
            yyyymmdd = d.strftime("%Y%m%d")

            if resolution in {"high", "h", "0.10"}:
                filename = f"npp_aot550_edr_gridded_0.10_{yyyymmdd}.high.bin.gz"
            else:
                filename = f"npp_aot550_edr_gridded_0.25_{yyyymmdd}.high.bin.gz"

            url = f"ftp://{server}{base_dir}/{year}/{filename}"
            urls.append(url)
        return urls


def read_nesdis_edr_binary(fname: str, **kwargs) -> xr.Dataset:
    """
    Read NESDIS EDR VIIRS binary data into an xarray.Dataset.
    Supports streaming from fsspec-compatible files (including .gz).

    Parameters
    ----------
    fname : str
        Path or URL to the binary file.
    **kwargs : dict
        Additional arguments (resolution, lazy).

    Returns
    -------
    xr.Dataset
        The dataset containing AOD.

    Examples
    --------
    >>> ds = read_nesdis_edr_binary("npp_aot550_edr_gridded_0.10_20230101.high.bin.gz", resolution="high")
    """
    resolution = kwargs.get("resolution", "high")
    # XarrayDriver might pop 'lazy' and set 'chunks'.
    lazy = kwargs.get("lazy", "chunks" in kwargs)

    if resolution in {"high", "h", "0.10"}:
        nlat, nlon = 1800, 3600
    else:
        nlat, nlon = 720, 1440

    def _read_core(filename):
        from .drivers import FileUtility

        fs = FileUtility.get_fs(filename)
        # Using compression='infer' to handle .gz files automatically
        with fs.open(filename, compression="infer") as f:
            # Binary file contains 2 layers (AOD and something else), first is AOD.
            # Using np.frombuffer on the stream is efficient.
            data = np.frombuffer(f.read(), dtype="<f4")
            # Reshape and take first layer
            return data.reshape(2, nlat, nlon)[0, :, :].copy()

    if lazy:
        import dask.array as da
        from dask import delayed

        load_binary = delayed(_read_core)(fname)
        aot = da.from_delayed(load_binary, shape=(nlat, nlon), dtype="<f4")
    else:
        aot = _read_core(fname)

    ds = xr.Dataset(data_vars={"aod_550": (("y", "x"), aot)})

    # Extract time from filename if possible
    # Example: npp_aot550_edr_gridded_0.10_20230101.high.bin.gz
    basename = os.path.basename(fname)
    try:
        # Split by underscore and find the date part (8 digits)
        import re

        match = re.search(r"(\d{8})", basename)
        if match:
            date_str = match.group(1)
            date = pd.to_datetime(date_str)
            ds = ds.assign_coords(time=date).expand_dims("time")
    except (ValueError, TypeError):
        pass

    return ds


def nesdis_edr_viirs_preprocess(ds: xr.Dataset, resolution: str = "high") -> xr.Dataset:
    """
    Preprocess NESDIS EDR VIIRS dataset: generate coordinates and metadata.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    resolution : str, optional
        'high' or 'low', by default "high".

    Returns
    -------
    xr.Dataset
        Processed dataset.

    Examples
    --------
    >>> ds = nesdis_edr_viirs_preprocess(ds, resolution="high")
    """
    if resolution in {"high", "h", "0.10"}:
        nlat, nlon = 1800, 3600
    else:
        nlat, nlon = 720, 1440

    # Generate lat/lon coords
    # Centers of the 0.10 or 0.25 degree cells
    lons = np.linspace(-179.875, 179.875, nlon)
    lats = np.linspace(-89.875, 89.875, nlat)

    # Lazy coordinate generation
    lon1d = xr.DataArray(lons, dims=("x",), name="longitude")
    lat1d = xr.DataArray(lats, dims=("y",), name="latitude")
    lat2d, lon2d = xr.broadcast(lat1d, lon1d)

    ds = ds.assign_coords(
        latitude=lat2d.assign_attrs({"units": "degrees_north", "standard_name": "latitude"}),
        longitude=lon2d.assign_attrs({"units": "degrees_east", "standard_name": "longitude"}),
    )

    # Mask invalid values
    # Binary uses -999.9 for missing.
    ds["aod_550"] = ds["aod_550"].where(ds["aod_550"] > -900)

    # Metadata
    ds.aod_550.attrs.update(
        {
            "long_name": "Aerosol Optical Thickness at 550nm",
            "units": "1",
            "standard_name": "atmosphere_optical_thickness_due_to_ambient_aerosol",
        }
    )

    # Re-order dimensions to (time, y, x) for consistency if time exists
    if "time" in ds.dims:
        ds = ds.transpose("time", "y", "x")

    # Provenance
    ds = update_history(
        ds, "Preprocessed NESDIS EDR VIIRS binary data using standardized preprocessing."
    )

    return ds
