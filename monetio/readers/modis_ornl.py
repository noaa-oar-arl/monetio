"""MODIS ORNL Reader"""

from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from .base import GriddedReader, _ensure_time_dimension, register_reader
from .sat_utils import update_history

try:
    from suds.client import Client

    HAS_SUDS = True
except ImportError:
    HAS_SUDS = False

DEFAULT_WSDL = "https://modis.ornl.gov/cgi-bin/MODIS/soapservice/MODIS_soapservice.wsdl"


@register_reader("modis_ornl")
class MODISORNLReader(GriddedReader):
    """
    Reader for MODIS data from ORNL web service.
    """

    def open_dataset(
        self,
        date: pd.Timestamp | str = None,
        product: str = "MOD12A2H",
        band: str = "Lai_500m",
        quality_control: Any | None = None,
        latitude: float = 0,
        longitude: float = 0,
        kmAboveBelow: int = 100,
        kmLeftRight: int = 100,
        files: str | list[str] = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads MODIS data from ORNL.

        Parameters
        ----------
        date : pd.Timestamp or str
            Date to retrieve.
        product : str, optional
            MODIS product.
        band : str, optional
            Product band.
        quality_control : optional
            Quality control filter.
        latitude : float, optional
            Center latitude.
        longitude : float, optional
            Center longitude.
        kmAboveBelow : int, optional
            Kilometers above/below center.
        kmLeftRight : int, optional
            Kilometers left/right center.
        **kwargs : dict
            Additional arguments.

        Returns
        -------
        xr.Dataset
            The MODIS ORNL dataset.

        Examples
        --------
        >>> reader = MODISORNLReader()
        >>> ds = reader.open_dataset(date='2020-01-01', product='MOD15A2H', band='Lai_500m')
        """
        if not HAS_SUDS:
            raise ImportError(
                "Please install a suds client (pip install suds-jurko or suds-community)"
            )

        date = pd.to_datetime(date)
        ds = _get_single_retrieval(
            date,
            product=product,
            band=band,
            quality_control=quality_control,
            lat=latitude,
            lon=longitude,
            kmAboveBelow=kmAboveBelow,
            kmLeftRight=kmLeftRight,
        )

        # Update history
        ds = update_history(ds, f"Read MODIS ORNL {product} {band} data.")
        ds = _ensure_time_dimension(ds)

        return ds


def _nearest(items: pd.DatetimeIndex, pivot: pd.Timestamp) -> pd.Timestamp:
    """
    Find the nearest date in a list.

    Parameters
    ----------
    items : pd.DatetimeIndex
        List of available dates.
    pivot : pd.Timestamp
        Target date.

    Returns
    -------
    pd.Timestamp
        The nearest date.
    """
    return min(items, key=lambda x: abs(x - pivot))


def _get_single_retrieval(
    date: pd.Timestamp,
    product: str,
    band: str,
    quality_control: Any | None,
    lat: float,
    lon: float,
    kmAboveBelow: int,
    kmLeftRight: int,
) -> xr.Dataset:
    """
    Retrieve a single MODIS subset from ORNL and return as xr.Dataset.

    Parameters
    ----------
    date : pd.Timestamp
        Target date.
    product : str
        MODIS product ID.
    band : str
        Product band.
    quality_control : Optional[Any]
        Quality control filter.
    lat : float
        Latitude.
    lon : float
        Longitude.
    kmAboveBelow : int
        Kilometers above/below.
    kmLeftRight : int
        Kilometers left/right.

    Returns
    -------
    xr.Dataset
        Dataset containing retrieved data and metadata.
    """
    client = Client(DEFAULT_WSDL)

    # Get available dates
    date_list = client.service.getdates(lat, lon, product)
    available_dates = pd.to_datetime(date_list, format="A%Y%j")

    # Find nearest date
    target_date = _nearest(available_dates, date)
    date_str = target_date.strftime("A%Y%j")

    # Fetch subset
    data = client.service.getsubset(
        lat, lon, product, band, date_str, date_str, kmAboveBelow, kmLeftRight
    )

    # Extract metadata
    metadata = {
        "server": DEFAULT_WSDL,
        "product": product,
        "band": band,
        "latitude": lat,
        "longitude": lon,
        "nrows": int(data.nrows),
        "ncols": int(data.ncols),
        "cellsize": data.cellsize,
        "scale": data.scale,
        "units": data.units,
        "yllcorner": data.yllcorner,
        "xllcorner": data.xllcorner,
        "date_int": int(target_date.strftime("%Y%j")),
    }

    # Parse data
    subset_data = data.subset[0].split(",")[5:]
    grid_data = np.array([float(x) for x in subset_data]).reshape(
        metadata["nrows"], metadata["ncols"]
    )

    # Apply scaling
    if metadata["scale"] != 1.0:
        grid_data = grid_data * metadata["scale"]

    ds = _make_xarray_dataset(grid_data, metadata)

    return ds


def _make_xarray_dataset(grid_data: np.ndarray, metadata: dict) -> xr.Dataset:
    """
    Create an xarray Dataset from raw data and metadata.

    Parameters
    ----------
    grid_data : np.ndarray
        2D grid data.
    metadata : dict
        Metadata dictionary.

    Returns
    -------
    xr.Dataset
        The formatted dataset.
    """
    band = metadata["band"]
    # We use expand_dims later to add the time dimension to the data variables
    ds = xr.Dataset(data_vars={band: (("y", "x"), grid_data)})
    ds = ds.expand_dims("time")
    ds = ds.assign_coords({"time": [pd.to_datetime(str(metadata["date_int"]), format="%Y%j")]})

    # Add lat/lon
    lon, lat = _get_latlon(
        metadata["xllcorner"],
        metadata["yllcorner"],
        metadata["cellsize"],
        metadata["ncols"],
        metadata["nrows"],
    )
    ds = ds.assign_coords(
        latitude=(("y", "x"), lat.data, {"units": "degrees_north", "standard_name": "latitude"}),
        longitude=(("y", "x"), lon.data, {"units": "degrees_east", "standard_name": "longitude"}),
    )

    ds[band].attrs.update(
        {"units": metadata["units"], "long_name": band, "product": metadata["product"]}
    )
    ds.attrs["server"] = metadata["server"]

    # Update history
    ds = update_history(ds, "Created xarray Dataset from MODIS ORNL subset.")

    return ds


def _get_latlon(
    xll: float, yll: float, cell_width: float, nx: int, ny: int
) -> tuple[xr.DataArray, xr.DataArray]:
    """
    Generate latitude and longitude coordinates lazily.

    Parameters
    ----------
    xll : float
        X coordinate of lower left corner (meters).
    yll : float
        Y coordinate of lower left corner (meters).
    cell_width : float
        Cell width (meters).
    nx : int
        Number of columns.
    ny : int
        Number of rows.

    Returns
    -------
    tuple[xr.DataArray, xr.DataArray]
        (longitude, latitude) 2D DataArrays.
    """
    from pyproj import Proj

    # Generate 1D coordinates
    x = np.linspace(xll, xll + cell_width * nx, nx)
    y = np.linspace(yll, yll + cell_width * ny, ny)

    # Use broadcast for lazy 2D expansion
    xda = xr.DataArray(x, dims="x")
    yda = xr.DataArray(y, dims="y")
    y_2d, x_2d = xr.broadcast(yda, xda)

    def _proj_inv(xv: np.ndarray, yv: np.ndarray) -> tuple:
        """Element-wise inverse projection wrapper."""
        sinu = Proj("+proj=sinu +a=6371007.181 +b=6371007.181 +units=m +R=6371007.181")
        return sinu(xv, yv, inverse=True)

    lon, lat = xr.apply_ufunc(
        _proj_inv,
        x_2d,
        y_2d,
        dask="parallelized",
        output_dtypes=[float, float],
        output_core_dims=[(), ()],
    )

    # Original code flipped y orientation
    return lon.isel(y=slice(None, None, -1)), lat.isel(y=slice(None, None, -1))
