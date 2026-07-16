"""NDACC Reader."""

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


@register_reader("ndacc")
class NDACCReader(GEOMSReader):
    """
    NDACC (Network for the Detection of Atmospheric Composition Change) Reader.
    NDACC data follows the GEOMS (Generic Earth Observation Metadata Standard).
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
        as_xarray: bool = True,
        **kwargs: Any,
    ) -> xr.Dataset | pd.DataFrame:
        """
        Retrieve and load NDACC data.

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
            Specific NDACC site folder name (e.g. 'mauna.loa.hi').
        instrument : str, optional
            Specific instrument name (e.g. 'lidar', 'ftir', 'uvvis.doas').
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        **kwargs : Any
            Additional arguments passed to the driver.

        Returns
        -------
        Union[xr.Dataset, pd.DataFrame]
            The processed NDACC dataset.
        """
        if files is None:
            if dates is None:
                raise ValueError("Must provide either 'files' or 'dates'.")
            files = self.build_urls(dates, siteid=siteid, instrument=instrument, **kwargs)

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

        if not as_xarray:
            return ds.to_dataframe().reset_index()

        # Update history
        ds = update_history(ds, "Read NDACC data using standardized preprocessing.")

        return ds

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        siteid: str | None = None,
        instrument: str | None = None,
        **kwargs: Any,
    ) -> list[str]:
        """
        Construct NDACC URLs.
        Note: NDACC data is primarily hosted on the NASA LaRC DHF server.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to build URLs for.
        siteid : str, optional
            Specific site folder name (e.g. 'mauna.loa.hi').
        instrument : str, optional
            Specific instrument name (e.g. 'ftir').

        Returns
        -------
        List[str]
            List of matching NDACC URLs.
        """
        if siteid is None or instrument is None:
            import warnings

            warnings.warn(
                "NDACC retrieval requires 'siteid' (folder name) and 'instrument'. "
                "See https://www-air.larc.nasa.gov/pub/NDACC/PUBLIC/stations/ for options."
            )
            return []

        dates = pd.DatetimeIndex(np.atleast_1d(pd.to_datetime(dates)))

        # NDACC Public Archive: https://www-air.larc.nasa.gov/pub/NDACC/PUBLIC/stations/
        base_url = f"https://www-air.larc.nasa.gov/pub/NDACC/PUBLIC/stations/{siteid}/{instrument}"

        # We use fsspec to list files in the directory
        import fsspec

        try:
            fs = fsspec.filesystem("https")
            all_files = fs.ls(base_url)

            # Filter by dates in filename (usually YYYYMMDD or similar)
            # Filenames: h2o_ftir_maunaloa_20230101t120000z_001.h5
            urls = []
            for f in all_files:
                if not f.endswith((".h5", ".hdf", ".hdf5", ".nc")):
                    continue

                # Check if any requested date matches the filename
                # This is a broad match; can be refined.
                f_lower = f.lower()
                for d in dates:
                    d_str = d.strftime("%Y%m%d")
                    if d_str in f_lower:
                        urls.append(f if f.startswith("http") else f"{base_url}/{f.split('/')[-1]}")
                        break
            return urls
        except Exception as e:
            import warnings

            warnings.warn(f"Failed to list NDACC files at {base_url}: {e}")
            return []

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Standardize NDACC/GEOMS variable names.

        Parameters
        ----------
        ds : xr.Dataset
            Input dataset.

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        # Call GEOMS harmonization first
        ds = super().harmonize(ds)

        # NDACC-specific additions: map GEOMS names to MONETIO names
        rename_dict = {
            "o3_mixing_ratio_volume": "ozone",
            "co_mixing_ratio_volume": "carbon_monoxide",
            "no2_mixing_ratio_volume": "nitrogen_dioxide",
            "no_mixing_ratio_volume": "nitrogen_monoxide",
            "ch4_mixing_ratio_volume": "methane",
        }

        actual_rename = {}
        for k, v in rename_dict.items():
            if k in ds.variables and v not in ds.variables:
                actual_rename[k] = v

        if actual_rename:
            ds = ds.rename(actual_rename)

        return ds
