"""
TCCON (Total Carbon Column Observing Network) Reader.
"""

import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .drivers import XarrayDriver
from .sat_utils import update_history


@register_reader("tccon")
class TCCONReader(PointReader):
    """
    Reader for TCCON (Total Carbon Column Observing Network) GGG2020 data.
    """

    def __init__(self):
        # TCCON opens NetCDF directly with Xarray; it does not use the
        # whole-file pandas parser inherited by ordinary PointReader classes.
        self.driver = XarrayDriver(source=self._source_name)

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
        siteid: str | list[str] = None,
        as_xarray: bool = True,
        expand2d: bool = True,
        **kwargs,
    ) -> xr.Dataset | pd.DataFrame:
        """
        Reads TCCON data.

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
        siteid : Union[str, List[str]], optional
            Site identifier(s) to build URLs if files is None.
            Example: 'pasadena01', 'nyalesund01', 'parkfalls01'.
        **kwargs : dict
            Additional arguments passed to xr.open_mfdataset.

        Returns
        -------
        xr.Dataset or pd.DataFrame
            UGRID xarray dataset by default, or DataFrame if ``as_xarray=False``.
        """
        if files is None:
            if siteid is None:
                raise ValueError("Either 'files' or 'siteid' must be provided.")
            files = self.build_urls(siteid)

        if "engine" not in kwargs:
            kwargs["engine"] = "netcdf4"

        # TCCON GGG2020 is NetCDF, treat as PointReader for harmonization
        ds = xr.open_mfdataset(files, **kwargs)
        source_ds = ds

        # Harmonize
        ds = self.harmonize(ds)

        # Update history
        ds = update_history(ds, "Read TCCON GGG2020 data.")

        if not as_xarray:
            if use_dask:
                # Xarray already includes dimension coordinates as columns in
                # to_dask_dataframe(); reset_index() adds a spurious integer
                # ``index`` column that the eager to_dataframe path omits.
                df = ds.to_dask_dataframe()
                column_order = [
                    *[dim for dim in ds.dims if dim in ds.coords],
                    *ds.data_vars,
                    *[coord for coord in ds.coords if coord not in ds.dims],
                ]
                df = df[column_order]
            else:
                df = ds.to_dataframe().reset_index()
            df.attrs = dict(ds.attrs)
            source_ds.close()
            return df

        if use_dask:
            # Use the same point-table conversion as eager output, while
            # retaining Dask-backed columns and the source's UGRID layout.
            df = ds.to_dask_dataframe()
            column_order = [
                *[dim for dim in ds.dims if dim in ds.coords],
                *ds.data_vars,
                *[coord for coord in ds.coords if coord not in ds.dims],
            ]
            ds_out = self.to_xarray(df[column_order], expand2d=expand2d)
            ds_out.set_close(source_ds.close)
        else:
            # Preserve the historical eager default via pandas conversion.
            df = ds.to_dataframe().reset_index()
            df.attrs = dict(ds.attrs)
            source_ds.close()
            ds_out = self.to_xarray(df, expand2d=expand2d)
        ds_out = update_history(ds_out, "Converted TCCON data to UGRID xarray format.")
        return ds_out

    def build_urls(self, siteid: str | list[str]) -> list[str]:
        """
        Build URLs for TCCON data from Caltech's OSN storage.

        Parameters
        ----------
        siteid : Union[str, List[str]]
            Site identifier(s).

        Returns
        -------
        List[str]
            List of URLs.
        """
        if isinstance(siteid, str):
            siteids = [siteid]
        else:
            siteids = siteid

        base_url = "https://sdsc.osn.xsede.org/ini210004tommorrell/10.14291/tccon.ggg2020.{}.R0/"
        # Note: Some sites might be R1, but R0 is common.
        # This is a best-effort automated retrieval.

        urls = []
        import fsspec

        for sid in siteids:
            # We try to find the .nc file in the directory
            url_prefix = base_url.format(sid)
            try:
                fs = fsspec.filesystem("http")
                found = fs.glob(f"{url_prefix}*.nc")
                urls.extend(found)
            except Exception:  # noqa: BLE001 - the R0 layout is probed first and a listing failure falls through to the R1 layout below (categories: retrieval_failed, not_found)
                # Try R1 if R0 fails
                try:
                    url_prefix_r1 = base_url.replace(".R0/", ".R1/").format(sid)
                    found = fs.glob(f"{url_prefix_r1}*.nc")
                    urls.extend(found)
                except Exception:  # noqa: BLE001 - per-site listing is best-effort; a site unreachable under both layouts is skipped and the remaining sites still resolve (categories: retrieval_failed, not_found)
                    continue

        return urls

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Harmonize TCCON dataset.

        Parameters
        ----------
        ds : xr.Dataset
            Input dataset.

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        # 1. Rename coordinates to MONETIO standards
        rename_dict = {}
        if "lat_deg" in ds.variables:
            rename_dict["lat_deg"] = "latitude"
        if "long_deg" in ds.variables:
            rename_dict["long_deg"] = "longitude"

        if rename_dict:
            ds = ds.rename(rename_dict)

        # 2. Ensure standard coordinates are set
        coords = [c for c in ["latitude", "longitude", "time"] if c in ds.variables]
        if coords:
            ds = ds.set_coords(coords)

        # 3. Add siteid if available in global attributes
        if "site_name" in ds.attrs and "siteid" not in ds.variables:
            ds = ds.assign_coords(siteid=ds.attrs["site_name"])

        ds = update_history(ds, "Harmonized TCCON data.")

        return ds
