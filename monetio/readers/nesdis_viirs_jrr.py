"""NESDIS VIIRS JRR Reader"""

import datetime

import pandas as pd
import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import add_time_coord, standardize_satellite_coords, update_history

JRR_SPECS = {
    "AOD": {
        "rename": {"AOD550": "aod_550"},
        "metadata": {
            "aod_550": {
                "long_name": "Aerosol Optical Thickness at 550nm",
                "units": "1",
                "standard_name": "atmosphere_optical_thickness_due_to_ambient_aerosol",
            }
        },
        "qa_var": "AOD_Quality_Flag",
    },
    "ADP": {
        "rename": {},
        "metadata": {
            "Smoke": {
                "long_name": "Aerosol Detection Product: Smoke",
                "units": "1",
                "standard_name": "is_smoke",
            },
            "Dust": {
                "long_name": "Aerosol Detection Product: Dust",
                "units": "1",
                "standard_name": "is_dust",
            },
            "Ash": {
                "long_name": "Aerosol Detection Product: Ash",
                "units": "1",
                "standard_name": "is_ash",
            },
        },
        "qa_var": "ADP_Quality_Flag",
    },
}


@register_reader("nesdis_viirs_jrr")
@register_reader("viirs_jrr")
class VIIRSJRRReader(GriddedReader):
    """
    Reader for NESDIS VIIRS JRR (Joint Polar Satellite System Risk Reduction) products.
    Supports AOD, ADP, CloudMask, and others.
    Available on AWS Open Data.
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
        satellite: str = "snpp",
        product: str = "AOD",
        qa_threshold: float | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads NESDIS VIIRS JRR data.

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
            Satellite identifier: 'snpp', 'n20' (NOAA-20/J01), or 'n21' (NOAA-21/J02).
            Default is 'snpp'.
        product : str, optional
            JRR product: 'AOD' (default), 'ADP', 'CloudMask', 'CloudHeight', etc.
        qa_threshold : float, optional
            Quality threshold for masking, by default None.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The VIIRS JRR dataset.
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files = self.build_urls(dates, satellite=satellite, product=product)

        if "preprocess" not in kwargs:
            from functools import partial

            kwargs["preprocess"] = partial(
                viirs_jrr_preprocess, product=product, qa_threshold=qa_threshold
            )

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        if "concat_dim" not in kwargs:
            kwargs["concat_dim"] = "time"
        if "combine" not in kwargs:
            kwargs["combine"] = "nested"

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
        ds = update_history(ds, f"Read NESDIS VIIRS JRR {product} data from {satellite}.")

        return ds

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        satellite: str = "snpp",
        product: str = "AOD",
    ) -> list[str]:
        """
        Build S3 URLs for NESDIS VIIRS JRR data based on dates.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        satellite : str, optional
            Satellite identifier ('snpp', 'n20', 'n21', 'j01', 'j02').
        product : str, optional
            JRR product.

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

        sat_map = {
            "snpp": "noaa-nesdis-snpp-pds",
            "j01": "noaa-nesdis-n20-pds",
            "n20": "noaa-nesdis-n20-pds",
            "j02": "noaa-nesdis-n21-pds",
            "n21": "noaa-nesdis-n21-pds",
        }
        bucket = sat_map.get(satellite.lower())
        if not bucket:
            raise ValueError(f"Unknown satellite: {satellite}. Choose from {list(sat_map.keys())}")

        urls = []
        for d in dates.floor("D").unique():
            prefix = f"s3://{bucket}/VIIRS-JRR-{product}/{d.strftime('%Y/%m/%d')}/"
            # Use FileUtility to expand paths, which leverages obstore/fsspec
            try:
                found = FileUtility.expand_paths(f"{prefix}*.nc")
                urls.extend(found)
            except Exception:
                continue

        return sorted(urls)


def viirs_jrr_preprocess(
    ds: xr.Dataset, product: str = "AOD", qa_threshold: float | None = None
) -> xr.Dataset:
    """
    Preprocess VIIRS JRR dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    product : str, optional
        Product type, by default "AOD".
    qa_threshold : float, optional
        Quality threshold for masking, by default None.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    ds = standardize_satellite_coords(ds)
    ds = add_time_coord(ds, time_attr="time_coverage_start")

    spec = JRR_SPECS.get(product.upper(), {})

    # 1. Product-specific renaming
    rename_dict = {k: v for k, v in spec.get("rename", {}).items() if k in ds.data_vars}
    if rename_dict:
        ds = ds.rename(rename_dict)

    # 2. Attribute assignment
    for var, attrs in spec.get("metadata", {}).items():
        if var in ds.data_vars:
            ds[var].attrs.update(attrs)

    # 3. Quality Flagging (Lazy & Vectorized)
    qa_var = spec.get("qa_var")
    if qa_threshold is not None and qa_var in ds.variables:
        # Mask data variables where quality is below threshold
        # We assume higher is better or use exact match for bitmasks if needed.
        # For JRR, we use >= threshold logic similar to TROPOMI/MODIS.
        mask = ds[qa_var] >= qa_threshold
        # Keep qa_var and coords, mask data_vars
        qa = ds[qa_var].copy()
        ds = ds.where(mask)
        ds[qa_var] = qa

    # Update history
    ds = update_history(ds, f"Preprocessed NESDIS VIIRS JRR {product} data.")

    return ds


# Legacy Alias
VIIRSJRRAODReader = VIIRSJRRReader
