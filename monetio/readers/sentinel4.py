"""
Sentinel-4 Reader.
Sentinel-4 is an ESA/Copernicus mission looking over Europe from GEO (on MTG-S).
Data structure is expected to be similar to TROPOMI.
"""

import warnings

import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import apply_qa_mask, standardize_satellite_coords, update_history


@register_reader("sentinel4")
class Sentinel4Reader(GriddedReader):
    """
    Reader for Sentinel-4 L2 data.
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
        qa_threshold: float | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads Sentinel-4 data.

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
            The NetCDF group(s) to open. If None, common Sentinel-4 groups will be opened.
        qa_threshold : float, optional
            If provided, mask data where 'qa_value' is less than this threshold.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The Sentinel-4 dataset.
        """
        # Common groups in Sentinel-5P/TROPOMI, Sentinel-4 likely follows
        if group is None:
            groups = [
                "PRODUCT",
                "PRODUCT/SUPPORT_DATA/INPUT_DATA",
                "PRODUCT/SUPPORT_DATA/GEOLOCATIONS",
                "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS",
            ]
        elif isinstance(group, str):
            groups = [group]
        else:
            groups = group

        user_preprocess = kwargs.pop("preprocess", None)

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        dsets = []
        for g in groups:
            g_kwargs = kwargs.copy()
            g_kwargs["group"] = g
            try:
                ds_g = super().open_dataset(
                    files,
                    use_virtualizarr=use_virtualizarr,
                    virtualizarr_file=virtualizarr_file,
                    virtualizarr_parser="hdf5",
                    virtualizarr_backend=virtualizarr_backend,
                    icechunk_repo=icechunk_repo,
                    use_icechunk=use_icechunk,
                    icechunk_url=icechunk_url,
                    use_dask=use_dask,
                    **g_kwargs,
                )
                dsets.append(ds_g)
            except Exception as e:
                warnings.warn(f"Could not open group {g}: {e}")

        if not dsets:
            try:
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
            except Exception:
                raise RuntimeError("No Sentinel-4 groups could be opened.")
        else:
            ds = xr.merge(dsets, compat="no_conflicts")

        # Preprocessing
        ds = sentinel4_preprocess(ds, qa_threshold=qa_threshold)

        if user_preprocess:
            ds = user_preprocess(ds)

        ds = update_history(ds, "Read Sentinel-4 L2 data.")

        return ds


def sentinel4_preprocess(ds: xr.Dataset, qa_threshold: float | None = None) -> xr.Dataset:
    """
    Preprocess Sentinel-4 dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    qa_threshold : float, optional
        Quality value threshold for masking.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # 1. Standardize coordinates
    ds = standardize_satellite_coords(ds, lat_name="latitude", lon_name="longitude")

    # 2. Quality masking
    if qa_threshold is not None:
        ds = apply_qa_mask(ds, qa_var="qa_value", threshold=qa_threshold)

    ds = update_history(ds, "Preprocessed Sentinel-4 data.")

    return ds
