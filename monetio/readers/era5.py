"""ERA5 (ECMWF Reanalysis v5) readers for files and the public ARCO archive."""

import datetime

import pandas as pd
import xarray as xr

from .base import GriddedReader, _scientific_hygiene, register_reader
from .sat_utils import standardize_satellite_coords, update_history

ARCO_ERA5_STORE = "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"

ARCO_VARIABLES = {
    "temperature_2m": "2m_temperature",
    "dewpoint_temperature_2m": "2m_dewpoint_temperature",
    "u_wind_10m": "10m_u_component_of_wind",
    "v_wind_10m": "10m_v_component_of_wind",
    "surface_pressure": "surface_pressure",
    "mean_sea_level_pressure": "mean_sea_level_pressure",
    "total_precipitation": "total_precipitation",
    "temperature": "temperature",
    "u_wind": "u_component_of_wind",
    "v_wind": "v_component_of_wind",
    "specific_humidity": "specific_humidity",
    "relative_humidity": "relative_humidity",
    "geopotential": "geopotential",
}

ICECHUNK_VARIABLES = {
    "temperature_2m": ("single", "t2m"),
    "dewpoint_temperature_2m": ("single", "d2m"),
    "u_wind_10m": ("single", "u10"),
    "v_wind_10m": ("single", "v10"),
    "surface_pressure": ("single", "sp"),
    "mean_sea_level_pressure": ("single", "msl"),
    "total_precipitation": ("single", "tp"),
    "temperature": ("pressure", "t"),
    "u_wind": ("pressure", "u"),
    "v_wind": ("pressure", "v"),
    "specific_humidity": ("pressure", "q"),
    "relative_humidity": ("pressure", "r"),
    "geopotential": ("pressure", "z"),
    "geopotential_500hPa": ("500hPa", "z"),
}


@register_reader("era5")
class ERA5Reader(GriddedReader):
    """
    Reader for ERA5 (ECMWF Reanalysis v5) data.
    """

    def open_dataset(
        self,
        files: str | list[str] | None = None,
        source: str = "files",
        dates: pd.DatetimeIndex
        | list[datetime.datetime]
        | datetime.datetime
        | str
        | slice
        | None = None,
        variables: str | list[str] | None = None,
        levels: int | list[int] | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        include_era5t: bool = False,
        layout: str = "spatial",
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        **kwargs,
    ) -> xr.Dataset:
        """
        Read local NetCDF/GRIB2 files, or selected data from Google's public ARCO
        ERA5 Zarr archive.

        Parameters
        ----------
        files : str or list[str]
            Local/remote file path(s) or glob pattern. Required for ``source='files'``.
        source : str
            ``"files"`` for files, ``"arco"`` for Google's public Zarr archive,
            or ``"aws"`` for the public AWS Icechunk archive.
        dates : date, sequence, or slice, optional
            Requested time(s) for ARCO. Required when reading ARCO.
        variables : str or list[str], optional
            ARCO variable names, using Monetio names such as ``temperature_2m``.
            Required when reading ARCO.
        levels : int or list[int], optional
            Pressure levels in hPa to select from ARCO pressure-level variables.
        bbox : tuple, optional
            Geographic extent ``(west, south, east, north)`` in degrees.
        include_era5t : bool
            Permit dates from the preliminary ERA5T portion of the public archive.
        layout : str
            AWS Icechunk chunking layout: ``"spatial"`` or ``"temporal"``.
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
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The ERA5 dataset.
        """
        if source.lower() == "arco":
            return self._open_arco(
                dates=dates,
                variables=variables,
                levels=levels,
                bbox=bbox,
                include_era5t=include_era5t,
                use_dask=use_dask,
                **kwargs,
            )
        if source.lower() == "aws":
            return self._open_icechunk(
                dates=dates,
                variables=variables,
                levels=levels,
                bbox=bbox,
                layout=layout,
                use_dask=use_dask,
                **kwargs,
            )
        if source.lower() != "files":
            raise ValueError("source must be 'files', 'arco', or 'aws'")
        if files is None:
            raise ValueError("files is required when source='files'")

        paths = [files] if isinstance(files, str) else list(files)
        if "engine" not in kwargs and all(
            str(path).lower().split("?", 1)[0].endswith((".grib", ".grb", ".grib2", ".grb2"))
            for path in paths
        ):
            kwargs["engine"] = "grib2io"
        parser = "grib2" if kwargs.get("engine") == "grib2io" else "hdf5"
        if "preprocess" not in kwargs:
            kwargs["preprocess"] = era5_preprocess
        ds = super().open_dataset(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser=virtualizarr_parser or parser,
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            **kwargs,
        )

        # Update history
        ds = update_history(ds, "Read ERA5 data.")

        return ds

    def _open_arco(
        self,
        *,
        dates,
        variables,
        levels,
        bbox,
        include_era5t,
        use_dask,
        **kwargs,
    ) -> xr.Dataset:
        if dates is None or variables is None:
            raise ValueError("source='arco' requires both dates and variables")
        requested = [variables] if isinstance(variables, str) else list(variables)
        unknown = [name for name in requested if name not in ARCO_VARIABLES]
        if unknown:
            raise ValueError(f"Unsupported ARCO ERA5 variable(s): {', '.join(unknown)}")

        storage_options = kwargs.pop("storage_options", {"token": "anon"})
        kwargs.pop("chunks", None)
        kwargs.pop("consolidated", None)
        try:
            ds = xr.open_zarr(
                ARCO_ERA5_STORE,
                consolidated=True,
                chunks={} if use_dask else None,
                storage_options=storage_options,
                zarr_format=2,
                **kwargs,
            )
        except ImportError as exc:
            if "gcsfs" in str(exc).lower() or "zarr" in str(exc).lower():
                raise ImportError(
                    "Reading ERA5 from ARCO requires the optional dependencies: "
                    "pip install 'monetio[cloud]'"
                ) from exc
            raise
        time_values = pd.DatetimeIndex(ds.time.values)
        archive_start = time_values.min()
        archive_end = time_values.max()
        final_era5 = pd.Timestamp("2026-06-30 23:00")
        final_era5t = pd.Timestamp("2026-10-17 23:00")
        if isinstance(dates, slice):
            start = archive_start if dates.start is None else pd.Timestamp(dates.start)
            end = archive_end if dates.stop is None else pd.Timestamp(dates.stop)
        else:
            chosen_dates = (
                [dates] if isinstance(dates, str | datetime.datetime | pd.Timestamp) else dates
            )
            normalized = pd.DatetimeIndex(pd.to_datetime(chosen_dates))
            start, end = normalized.min(), normalized.max()
        if start < archive_start:
            raise ValueError(
                f"Requested start date {start} precedes the available ARCO data ({archive_start})"
            )
        allowed_end = final_era5t if include_era5t else final_era5
        if end > min(allowed_end, archive_end):
            label = "ERA5T" if include_era5t else "final ERA5"
            raise ValueError(
                f"Requested end date {end} exceeds the available {label} data "
                f"({min(allowed_end, archive_end)}); set include_era5t=True for preliminary data"
            )

        ds = _normalize_era5_time(ds).sel(time=dates)
        selected = [ARCO_VARIABLES[name] for name in requested]
        missing = [name for name in selected if name not in ds.data_vars]
        if missing:
            ds.close()
            raise ValueError(f"Variables missing from the ARCO ERA5 archive: {', '.join(missing)}")
        ds = ds[selected]
        if levels is not None:
            if "level" not in ds.coords:
                raise ValueError("levels can only be selected for pressure-level variables")
            ds = ds.sel(level=levels)
        if bbox is not None:
            ds = _select_bbox(ds, bbox)
        ds = era5_preprocess(ds)
        return update_history(ds, "Read ERA5 from the Google ARCO public archive.")

    def _open_icechunk(
        self, *, dates, variables, levels, bbox, layout, use_dask, **kwargs
    ) -> xr.Dataset:
        """Read the public AWS Icechunk/Zarr v3 edition of ERA5."""
        if dates is None or variables is None:
            raise ValueError("source='aws' requires both dates and variables")
        if layout not in {"spatial", "temporal"}:
            raise ValueError("layout must be 'spatial' or 'temporal'")
        requested = [variables] if isinstance(variables, str) else list(variables)
        unknown = [name for name in requested if name not in ICECHUNK_VARIABLES]
        if unknown:
            raise ValueError(f"Unsupported AWS Icechunk ERA5 variable(s): {', '.join(unknown)}")
        try:
            import icechunk
            import pcodec  # noqa: F401  # registers the Zarr v3 PCodec codec
        except ImportError as exc:
            raise ImportError(
                "Reading ERA5 from AWS Icechunk requires the optional dependencies: "
                "pip install 'monetio[cloud]'"
            ) from exc

        region = kwargs.pop("region", "us-east-1")
        storage = icechunk.s3_storage(
            bucket="earthmover-icechunk-era5",
            prefix="icechunkV2",
            region=region,
            anonymous=True,
        )
        repo = icechunk.Repository.open(storage)
        session = repo.readonly_session("main")
        groups = {}
        for name in requested:
            group, variable = ICECHUNK_VARIABLES[name]
            groups.setdefault(group, []).append((name, variable))

        datasets = []
        try:
            for group, selections in groups.items():
                group_path = f"{group}/{layout}"
                ds = xr.open_dataset(
                    session.store,
                    engine="zarr",
                    group=group_path,
                    consolidated=False,
                    zarr_format=3,
                    chunks={} if use_dask else None,
                    **kwargs,
                )
                missing = [variable for _, variable in selections if variable not in ds.data_vars]
                if missing:
                    ds.close()
                    raise ValueError(
                        f"Variables missing from AWS Icechunk ERA5 group {group_path}: "
                        f"{', '.join(missing)}"
                    )
                rename = {variable: name for name, variable in selections if variable != name}
                datasets.append(ds[[variable for _, variable in selections]].rename(rename))
            ds = xr.merge(datasets, compat="no_conflicts")
            original_close = [dataset._close for dataset in datasets]

            def close_sessions():
                for close in original_close:
                    if close is not None:
                        close()

            ds.set_close(close_sessions)
        except Exception:
            for dataset in datasets:
                dataset.close()
            raise

        ds = _normalize_era5_time(ds).sel(time=dates)
        if levels is not None:
            level_coord = next(
                (
                    coord
                    for coord in ("level", "pressure_level", "isobaricInhPa")
                    if coord in ds.coords
                ),
                None,
            )
            if level_coord is None:
                ds.close()
                raise ValueError("levels can only be selected for pressure-level variables")
            ds = ds.sel({level_coord: levels})
        if bbox is not None:
            ds = _select_bbox(ds, bbox)
        ds = era5_preprocess(ds)
        return update_history(ds, "Read ERA5 from the AWS Icechunk public archive.")


def _select_bbox(ds: xr.Dataset, bbox: tuple[float, float, float, float]) -> xr.Dataset:
    """Select a latitude/longitude box from a regular ERA5 grid."""
    if len(bbox) != 4:
        raise ValueError("bbox must be (west, south, east, north)")
    west, south, east, north = bbox
    if south > north:
        raise ValueError("bbox south must be less than or equal to north")
    lon = ds.longitude
    lon_west, lon_east = west, east
    if float(lon.max()) > 180:
        lon_west, lon_east = west % 360, east % 360
    lon_mask = (
        (lon >= lon_west) & (lon <= lon_east)
        if lon_west <= lon_east
        else (lon >= lon_west) | (lon <= lon_east)
    )
    lat = ds.latitude
    return ds.where(lon_mask & (lat >= south) & (lat <= north), drop=True)


def _normalize_era5_time(ds: xr.Dataset) -> xr.Dataset:
    """Rename the AWS Icechunk archive's valid_time dimension to time."""
    if "time" not in ds.dims and "time" not in ds.coords and "valid_time" in ds.variables:
        ds = ds.rename({"valid_time": "time"})
    return ds


def era5_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess ERA5 dataset: standardize coordinates and metadata.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # 1. Standardize dimensions and coordinates
    # ERA5 uses 'latitude', 'longitude', 'time', 'level'.
    ds = standardize_satellite_coords(
        ds,
        lat_name="latitude",
        lon_name="longitude",
        y_dim=["latitude", "lat"],
        x_dim=["longitude", "lon"],
        z_dim=["level", "lev", "pressure_level"],
    )

    # 2. Expand 1D coords to 2D for UGRID/CF compliance in MONETIO if needed
    if "latitude" in ds.coords and ds["latitude"].ndim == 1:
        if "longitude" in ds.coords and ds["longitude"].ndim == 1:
            # Use lazy broadcasting
            lons, lats = xr.broadcast(ds.longitude, ds.latitude)
            # Re-assign as 2D coordinates
            ds = ds.assign_coords(longitude=lons, latitude=lats)

    # 3. Variable renaming to standard names if they exist
    # Common ERA5 variable mapping
    mapping = {
        "10m_u_component_of_wind": "u_wind_10m",
        "10m_v_component_of_wind": "v_wind_10m",
        "2m_temperature": "temperature_2m",
        "2m_dewpoint_temperature": "dewpoint_temperature_2m",
        "t2": "temperature_2m",
        "d2": "dewpoint_temperature_2m",
        "u10": "u_wind_10m",
        "v10": "v_wind_10m",
        "t2m": "temperature_2m",
        "d2m": "dewpoint_temperature_2m",
        "sp": "surface_pressure",
        "msl": "mean_sea_level_pressure",
        "mslp": "mean_sea_level_pressure",
        "tp": "total_precipitation",
        "blh": "pbl_height",
        "u": "u_wind",
        "v": "v_wind",
        "t": "temperature",
        "q": "specific_humidity",
        "r": "relative_humidity",
        "z": "geopotential",
    }
    rename_dict = {
        old: new for old, new in mapping.items() if old in ds.data_vars and new not in ds.data_vars
    }
    if rename_dict:
        ds = ds.rename(rename_dict)

    # 4. Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # Update history
    ds = update_history(ds, "Preprocessed ERA5 data.")

    return ds
