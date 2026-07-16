"""
EarthCARE (Earth Cloud, Aerosol and Radiation Explorer) Reader.
Primarily for ATLID (Atmospheric Lidar) L2 Aerosol Profiles (A-AER).
"""

import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import standardize_satellite_coords, update_history


@register_reader("earthcare")
class EarthCAREReader(GriddedReader):
    """
    Reader for EarthCARE L2 data (ATLID, MSI, etc.).
    """

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
        group: str | list[str] | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads EarthCARE data.

        Parameters
        ----------
        files : Union[str, List[str]]
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
        group : str or list of str, optional
            The NetCDF group(s) to open.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The EarthCARE dataset.
        """
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

        # Preprocessing
        ds = earthcare_preprocess(ds)

        ds = update_history(ds, "Read EarthCARE L2 data.")

        return ds


def earthcare_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess EarthCARE dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # 1. Standardize coordinates
    # EarthCARE JSG (Joint Standard Grid)
    ds = standardize_satellite_coords(
        ds,
        lat_name="latitude",
        lon_name="longitude",
        y_dim=["profile", "n_profile", "JSG_along_track"],
        z_dim=["range", "n_range", "JSG_vertical"],
    )

    # 2. Handle Time
    if "time" not in ds.coords and "time" not in ds.variables:
        for t_var in ["UTC_time", "time_utc"]:
            if t_var in ds.variables:
                ds = ds.rename({t_var: "time"})
                if ds["time"].ndim == 1:
                    ds = ds.set_coords("time")
                break

    ds = update_history(ds, "Preprocessed EarthCARE data.")

    return ds
