"""
CALIPSO (Cloud-Aerosol Lidar and Infrared Pathfinder Satellite Observations) Reader.
Primarily for CALIOP (Cloud-Aerosol LIdar with Orthogonal Polarization) L2 Aerosol Profiles.
"""

import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import standardize_satellite_coords, update_history


@register_reader("calipso")
class CALIPSOReader(GriddedReader):
    """
    Reader for CALIPSO/CALIOP L2 Aerosol Profile data.
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
        variable_dict: dict | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads CALIPSO data.

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
            The NetCDF/HDF group(s) to open.
        variable_dict : dict, optional
            Dictionary mapping variable names to processing options.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The CALIPSO dataset.
        """
        if "engine" not in kwargs:
            # CALIPSO files are often HDF4, which may need 'netcdf4' or 'h5netcdf'
            # if converted, but native HDF4 might need 'pynio' or similar.
            # Assuming standard monetio environment (netcdf4/h5netcdf).
            kwargs["engine"] = "netcdf4"

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
        ds = calipso_preprocess(ds, variable_dict=variable_dict)

        ds = update_history(ds, "Read CALIPSO L2 data.")

        return ds


def calipso_preprocess(ds: xr.Dataset, variable_dict: dict | None = None) -> xr.Dataset:
    """
    Preprocess CALIPSO dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    variable_dict : dict, optional
        Dictionary mapping variable names to processing options.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # 1. Standardize coordinates
    # CALIOP often uses 'fakeDimX' or similar for track, and 'fakeDimY' for vertical.
    # Dimensions are typically (profile, range_bin).
    ds = standardize_satellite_coords(
        ds,
        lat_name="Latitude",
        lon_name="Longitude",
        y_dim=["nray", "profile", "fakeDim0", "N_Profiles"],
        z_dim=["nbin", "range_bin", "fakeDim1", "N_Range_Bins"],
    )

    # 2. Handle Time
    if "Profile_Time" in ds.variables:
        # Profile_Time is often seconds since a reference
        # But for now, let's just make sure it's a coord
        if "time" not in ds.coords:
                ds = ds.rename({"Profile_Time": "time"})
                if ds["time"].ndim == 1:
                    ds = ds.set_coords("time")

    # 3. Apply scale factors if not already applied by engine
    # CALIOP variables often have scale_factor attributes
    if variable_dict:
        for var, options in variable_dict.items():
            if var in ds.variables:
                if "scale" in options:
                    ds[var] = ds[var] * options["scale"]

    ds = update_history(ds, "Preprocessed CALIPSO data.")

    return ds
