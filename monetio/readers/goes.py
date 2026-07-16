"""GOES Reader"""

import datetime

import numpy as np
import pandas as pd
import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import add_time_coord, standardize_satellite_coords, update_history


@register_reader("goes")
class GOESReader(GriddedReader):
    """
    Reader for GOES-R Series (GOES-16, 17, 18) ABI data.
    Supports local files and S3 (via obstore or s3fs).
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
        satellite: str = "16",
        product: str = "ABI-L2-AODF",
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads GOES data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
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
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str], optional
            Dates to retrieve. If files is None, this is used to build URLs.
        satellite : str, optional
            Satellite identifier (e.g., '16', '17', '18'). Default is '16'.
        product : str, optional
            GOES product (e.g., 'ABI-L2-AODF'). Default is 'ABI-L2-AODF'.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The GOES dataset.
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files = self.build_urls(dates, satellite=satellite, product=product)

        if "preprocess" not in kwargs:
            kwargs["preprocess"] = goes_preprocess

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"
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
        ds = update_history(ds, f"Read GOES-{satellite} {product} data.")

        return ds

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Apply GOES-specific naming conventions and metadata standardization.

        The heavy lifting is done in ``goes_preprocess`` which is passed as
        the ``preprocess`` callback to the driver.  This method applies any
        final dataset-level harmonization after files have been merged.

        Parameters
        ----------
        ds : xr.Dataset
            Input dataset (already preprocessed per-file).

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        return super().harmonize(ds)

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        satellite: str = "16",
        product: str = "ABI-L2-AODF",
    ) -> list[str]:
        """
        Build S3 URLs for GOES data based on dates.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        satellite : str, optional
            Satellite identifier ('16', '17', '18').
        product : str, optional
            GOES product.

        Returns
        -------
        List[str]
            List of S3 URLs.
        """
        from .drivers import FileUtility

        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        bucket = f"noaa-goes{satellite}"
        fs = FileUtility.get_fs(f"s3://{bucket}")

        urls = []
        for d in dates:
            # GOES S3 structure: <product>/<year>/<day_of_year>/<hour>/
            prefix = f"{bucket}/{product}/{d.strftime('%Y/%j/%H')}/"
            try:
                found = fs.ls(prefix)
                if not found:
                    continue

                # Find the file closest to the requested time
                file_dates = [
                    pd.to_datetime(f.split("_")[-1].split(".")[0][1:], format="%Y%j%H%M%S%f")
                    for f in found
                ]
                idx = np.argmin([abs(fd - d) for fd in file_dates])
                urls.append(f"s3://{found[idx]}")
            except (FileNotFoundError, ValueError):
                continue

        return sorted(list(set(urls)))


def goes_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess GOES dataset: calculate grid and standardize coordinates.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Processed dataset.

    Examples
    --------
    >>> ds = reader.open_dataset(files)
    >>> ds = goes_preprocess(ds)
    """
    # 1. Standardize dimensions and coordinates
    ds = standardize_satellite_coords(ds)

    # 2. Add time coordinate if not present
    if "time" not in ds.coords:
        ds = add_time_coord(ds, time_attr="time_coverage_start")

    # 3. Calculate Latitude/Longitude (Lazy)
    if "latitude" not in ds.coords and "goes_imager_projection" in ds.variables:
        ds = _add_goes_latlon(ds)

    # Update history
    ds = update_history(ds, "Preprocessed GOES data.")

    return ds


def _add_goes_latlon(ds: xr.Dataset) -> xr.Dataset:
    """
    Calculate latitude and longitude for GOES data lazily using standardized routines.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset containing 'x', 'y' and 'goes_imager_projection'.

    Returns
    -------
    xr.Dataset
        Dataset with 'latitude' and 'longitude' coordinates added.
    """
    from pyproj import CRS

    proj_var = ds.goes_imager_projection
    proj_dict = proj_var.attrs.copy()

    # Ensure all attributes are scalars
    for k, v in proj_dict.items():
        if isinstance(v, list | np.ndarray):
            proj_dict[k] = v[0]

    crs = CRS.from_cf(proj_dict)
    # Use PROJ string for the worker function to ensure it is picklable for Dask
    proj_srs = crs.to_proj4()
    ds.attrs["projection"] = crs.to_wkt()

    satellite_height = proj_var.perspective_point_height

    # 1. Multiply by satellite height lazily (convert radians to meters)
    x_m = ds.x * satellite_height
    y_m = ds.y * satellite_height

    # 2. Broadcast to 2D (y, x) lazily
    # Note: GOES data variables usually have dimensions (y, x)
    if ds.chunks:
        # Match coordinate chunking to data variables to maintain laziness.
        # This ensures that derived 2D coordinates are also lazy when the dataset is.
        x_m = x_m.chunk({d: ds.chunks[d] for d in x_m.dims if d in ds.chunks})
        y_m = y_m.chunk({d: ds.chunks[d] for d in y_m.dims if d in ds.chunks})

    y_2d, x_2d = xr.broadcast(y_m, x_m)

    def _proj_inv(xv: np.ndarray, yv: np.ndarray, p_srs: str) -> tuple[np.ndarray, np.ndarray]:
        """
        Element-wise inverse projection kernel using pyproj.Transformer.

        Parameters
        ----------
        xv : np.ndarray
            X coordinates in meters.
        yv : np.ndarray
            Y coordinates in meters.
        p_srs : str
            PROJ4 projection string.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            (latitude, longitude) as float32 NumPy arrays.
        """
        from functools import lru_cache

        from pyproj import Transformer

        @lru_cache(maxsize=1)
        def _get_transformer(srs):
            return Transformer.from_crs(srs, "EPSG:4326", always_xy=True)

        # Ensure p_srs is a string if it came as a Dask scalar/array
        if isinstance(p_srs, np.ndarray | np.generic):
            p_srs = p_srs.item()
        if hasattr(p_srs, "decode"):
            p_srs = p_srs.decode()

        # GOES projection is geostationary. Transformer is more efficient than Proj.
        # We use a cached transformer from the input SRS to WGS84 (EPSG:4326).
        transformer = _get_transformer(p_srs)
        lon, lat = transformer.transform(xv, yv)

        # Handle out of disk values (GOES specific)
        lon = np.where(lon < 400, lon, np.nan)
        lat = np.where(lat < 100, lat, np.nan)
        return lat.astype(np.float32), lon.astype(np.float32)

    # 3. Apply projection lazily
    lat, lon = xr.apply_ufunc(
        _proj_inv,
        x_2d,
        y_2d,
        proj_srs,
        dask="parallelized",
        output_dtypes=[np.float32, np.float32],
        output_core_dims=[(), ()],
    )

    ds = ds.assign_coords(
        latitude=lat.assign_attrs({"units": "degrees_north", "standard_name": "latitude"}),
        longitude=lon.assign_attrs({"units": "degrees_east", "standard_name": "longitude"}),
    )

    # Update history
    ds = update_history(
        ds, "Optimized GOES coordinate generation using standardized preprocessing."
    )

    return ds
