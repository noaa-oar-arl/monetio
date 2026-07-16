"""
AmeriFlux / FLUXNET Reader.
"""

import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .sat_utils import update_history


@register_reader("ameriflux")
class AmeriFluxReader(PointReader):
    """
    Reader for AmeriFlux BASE data (CSV).
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
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads AmeriFlux data.

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
        **kwargs : dict
            Additional arguments passed to PointReader.open_dataset.

        Returns
        -------
        xr.Dataset
            The AmeriFlux dataset.
        """
        # AmeriFlux BASE files are CSV with a header and -9999 as missing value.
        kwargs.setdefault("na_values", -9999)
        kwargs.setdefault("skiprows", 0)  # Adjust if there are comment lines

        # Use PointReader's open_dataset which handles CSV and to_xarray
        return super().open_dataset(
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

    def harmonize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmonize AmeriFlux DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            Input dataframe.

        Returns
        -------
        pd.DataFrame
            Harmonized dataframe.
        """
        # 1. Handle Timestamps
        # AmeriFlux uses TIMESTAMP_START and TIMESTAMP_END (YYYYMMDDHHMM)
        if "TIMESTAMP_START" in df.columns:
            df["time"] = pd.to_datetime(df["TIMESTAMP_START"], format="%Y%m%d%H%M")
        elif "TIMESTAMP" in df.columns:
            # Yearly or monthly might have shorter format
            ts_str = df["TIMESTAMP"].astype(str)
            if len(ts_str.iloc[0]) == 4:
                df["time"] = pd.to_datetime(ts_str, format="%Y")
            elif len(ts_str.iloc[0]) == 6:
                df["time"] = pd.to_datetime(ts_str, format="%Y%m")
            else:
                df["time"] = pd.to_datetime(ts_str)

        # 2. Rename coordinates if present (often in a separate metadata file,
        # but sometimes included or can be passed)
        # Note: BASE files themselves often DON'T have Lat/Lon in every row.
        # They are usually in the BADM (metadata) files.

        df = update_history(df, "Harmonized AmeriFlux data.")

        return super().harmonize(df)
