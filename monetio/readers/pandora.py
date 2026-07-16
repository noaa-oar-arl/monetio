"""Pandora Reader."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import xarray as xr

from .base import register_reader
from .geoms import GEOMSReader
from .sat_utils import update_history

if TYPE_CHECKING:
    pass


@register_reader("pandora")
class PandoraReader(GEOMSReader):
    """
    Pandora (Pandonia Global Network) Reader.
    Pandora data follows the GEOMS (Generic Earth Observation Metadata Standard).
    """

    def open_dataset(
        self,
        files: str | list[str] | None = None,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str | None = None,
        siteid: str | None = None,
        instrument: str | None = None,
        product: str | None = "no2",
        as_xarray: bool = True,
        **kwargs: Any,
    ) -> xr.Dataset | pd.DataFrame:
        """
        Retrieve and load Pandora data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File path, list of paths, or glob pattern.
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
            Dates to retrieve if files are not provided.
        siteid : str, optional
            Specific Pandora site name (e.g. 'BoulderCO').
        instrument : str, optional
            Specific instrument name (e.g. 'Pandora57s1').
        product : str, optional
            Product type (e.g. 'no2', 'o3', 'h2co', 'so2'), by default 'no2'.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        **kwargs : Any
            Additional arguments passed to the driver.

        Returns
        -------
        xr.Dataset | pd.DataFrame
            The processed Pandora dataset.
        """
        if files is None:
            if dates is None:
                raise ValueError("Must provide either 'files' or 'dates'.")
            files = self.build_urls(
                dates, siteid=siteid, instrument=instrument, product=product, **kwargs
            )

        if not files:
            if as_xarray:
                return xr.Dataset()
            return pd.DataFrame()

        ds = super().open_dataset(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser=virtualizarr_parser,
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            **kwargs,
        )

        # Apply harmonization explicitly as GEOMSReader.open_dataset calls geoms_preprocess
        # but doesn't call a reader-specific harmonize method from the base class properly
        # if not overridden in a specific way.
        ds = self.harmonize(ds)

        if not as_xarray:
            return ds.to_dataframe().reset_index()

        # Update history
        ds = update_history(ds, "Read Pandora data using standardized preprocessing.")

        return ds

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        siteid: str | None = None,
        instrument: str | None = None,
        product: str | None = "no2",
        **kwargs: Any,
    ) -> list[str]:
        """
        Construct Pandora URLs.
        Note: Pandora data is hosted on https://data.pandonia-global-network.org/

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to build URLs for.
        siteid : str, optional
            Specific site folder name (e.g. 'BoulderCO').
        instrument : str, optional
            Specific instrument name (e.g. 'Pandora57s1').
        product : str, optional
            Product type (e.g. 'no2', 'o3', 'h2co', 'so2'), by default 'no2'.

        Returns
        -------
        List[str]
            List of matching Pandora URLs.
        """
        if siteid is None or instrument is None:
            import warnings

            warnings.warn(
                "Pandora retrieval requires 'siteid' (folder name) and 'instrument'. "
                "See https://data.pandonia-global-network.org/ for options."
            )
            return []

        dates = pd.DatetimeIndex(np.atleast_1d(pd.to_datetime(dates)))

        # PGN Archive: https://data.pandonia-global-network.org/{site}/{instrument}/L2_geoms/
        base_url = f"https://data.pandonia-global-network.org/{siteid}/{instrument}/L2_geoms"

        import fsspec

        try:
            fs = fsspec.filesystem("https")
            all_files = fs.ls(base_url)

            urls = []
            for f in all_files:
                if not f.endswith(".h5"):
                    continue

                f_lower = f.lower()
                if product and f"{product.lower()}_" not in f_lower:
                    continue

                # Pandora filenames contain start and end times:
                # groundbased_uvvis.doas.directsun.no2_noaa.esrl057_rd.rnvs3.1.8_boulder.co_20180430t232018z_20221222t223120z_001.h5
                # Extract dates from filename
                parts = f_lower.split("_")
                try:
                    # Usually the last two parts before .h5 are dates
                    start_str = parts[-3].split("t")[0]
                    end_str = parts[-2].split("t")[0]

                    f_start = pd.to_datetime(start_str, format="%Y%m%d")
                    f_end = pd.to_datetime(end_str, format="%Y%m%d")

                    for d in dates:
                        if f_start <= d <= f_end:
                            urls.append(
                                f if f.startswith("http") else f"{base_url}/{f.split('/')[-1]}"
                            )
                            break
                except Exception:
                    # Fallback to simple string match
                    for d in dates:
                        if d.strftime("%Y%m%d") in f_lower:
                            urls.append(
                                f if f.startswith("http") else f"{base_url}/{f.split('/')[-1]}"
                            )
                            break

            return sorted(list(set(urls)))
        except Exception as e:
            import warnings

            warnings.warn(f"Failed to list Pandora files at {base_url}: {e}")
            return []

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Standardize Pandora/GEOMS variable names and units.

        Parameters
        ----------
        ds : xr.Dataset
            Input dataset.

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        # Call GEOMS/GriddedReader harmonization first
        # GEOMS Reader already converted GEOMS names to lowercase and replaced . with _
        # e.g. NO2.COLUMN_ABSORPTION.SOLAR -> no2_column_absorption_solar

        # Pandora-specific additions: map GEOMS names to MONETIO names
        rename_dict = {
            "no2_column_absorption_solar": "nitrogen_dioxide",
            "o3_column_absorption_solar": "ozone",
            "h2co_column_absorption_solar": "formaldehyde",
            "so2_column_absorption_solar": "sulfur_dioxide",
        }

        actual_rename = {}
        for k, v in rename_dict.items():
            if k in ds.variables and v not in ds.variables:
                actual_rename[k] = v

        if actual_rename:
            ds = ds.rename(actual_rename)

        # Map GEOMS metadata to standard MONETIO attributes
        for vn in ds.data_vars:
            if "VAR_UNITS" in ds[vn].attrs and "units" not in ds[vn].attrs:
                ds[vn].attrs["units"] = ds[vn].attrs["VAR_UNITS"]
            if "VAR_DESCRIPTION" in ds[vn].attrs and "long_name" not in ds[vn].attrs:
                ds[vn].attrs["long_name"] = ds[vn].attrs["VAR_DESCRIPTION"]

        return ds
