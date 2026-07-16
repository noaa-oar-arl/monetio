"""ICAP-MME Reader"""

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import xarray as xr

from .base import GriddedReader, _ensure_time_dimension, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

VALID_FILETYPES = ("MMC", "C4", "MME")
VALID_DATA_VARS = (
    "modeaod550",
    "dustaod550",
    "pm",
    "seasaltaod550",
    "smokeaod550",
    "totaldustaod550",
)

ICAP_DEFAULT_BASE_URL = "https://nrlgodae1.nrlmry.navy.mil/ftp/outgoing/nrl/ICAP-MME/"


@register_reader("icap_mme")
class ICAPMMEReader(GriddedReader):
    """
    Reader for ICAP-MME (International Cooperative for Aerosol Prediction - Multi Model Ensemble) data.
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
        dates: pd.DatetimeIndex | list[datetime] | datetime | str | None = None,
        product: str = "MMC",
        data_var: str = "dustaod550",
        base_url: str = ICAP_DEFAULT_BASE_URL,
        download: bool = False,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Retrieve and load ICAP-MME data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File paths or URLs to read. If None, uses `dates` and `product` to discover files.
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
            Dates to retrieve if `files` is not provided.
        product : str, optional
            ICAP product (e.g., 'MMC', 'C4', 'MME'), by default 'MMC'.
        data_var : str, optional
            Data variable (e.g., 'dustaod550'), by default 'dustaod550'.
        base_url : str, optional
            Root URL used to construct ICAP file paths, by default
            "https://nrlgodae1.nrlmry.navy.mil/".
        download : bool, optional
            Whether to download files to local directory, by default False.
        **kwargs : Any
            Additional arguments passed to the driver.

        Returns
        -------
        xr.Dataset
            The loaded ICAP-MME dataset.

        Examples
        --------
        >>> reader = ICAPMMEReader()
        >>> ds = reader.open_dataset(dates="2024-02-01", product="C4")
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")

            # Validate parameters before building URLs
            if product.upper() not in VALID_FILETYPES:
                raise ValueError(
                    f"Invalid input for 'product': '{product}'. Must be one of {VALID_FILETYPES}."
                )
            if data_var.lower() not in VALID_DATA_VARS:
                raise ValueError(
                    f"Invalid input for 'data_var': '{data_var}'. Must be one of {VALID_DATA_VARS}."
                )

            urls, fnames = build_urls(
                dates,
                filetype=product,
                data_var=data_var,
                base_url=base_url,
            )
            if download:
                files = []
                for url, fname in zip(urls, fnames):
                    files.append(str(retrieve(url, fname, download=True)))
            else:
                files = urls

        # ICAP files are standard NetCDF, often h5netcdf compatible.
        if "concat_dim" not in kwargs:
            kwargs["concat_dim"] = "time"
        if "combine" not in kwargs:
            kwargs["combine"] = "nested"

        # Use XarrayDriver to open (Lazy by default)
        ds = self.driver.open(
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

        ds = self.harmonize(ds)
        ds = _ensure_time_dimension(ds)

        # Update history
        ds = update_history(ds, "Read ICAP-MME data.")

        return ds


def build_urls(
    dates: pd.DatetimeIndex | list[datetime] | datetime | str,
    filetype: str = "MMC",
    data_var: str = "dustaod550",
    base_url: str = ICAP_DEFAULT_BASE_URL,
    verbose: bool = True,
) -> tuple[list[str], list[str]]:
    """
    Construct ICAP-MME URLs and filenames for the given dates.

    Parameters
    ----------
    dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
        Dates to build URLs for.
    filetype : str, optional
        ICAP product type (MMC, C4, MME), by default "MMC".
    data_var : str, optional
        Data variable name, by default "dustaod550".
    base_url : str, optional
        Root URL used to construct ICAP file paths, by default
        "https://nrlgodae1.nrlmry.navy.mil/".
    verbose : bool, optional
        Whether to print status messages, by default True.

    Returns
    -------
    Tuple[List[str], List[str]]
        A tuple containing a list of URLs and a list of filenames.

    Examples
    --------
    >>> urls, fnames = build_urls("2024-02-01", filetype="C4")
    """
    from collections.abc import Iterable

    if isinstance(dates, Iterable) and not isinstance(dates, str):
        dates = pd.DatetimeIndex(dates)
    else:
        dates = pd.DatetimeIndex([dates])

    urls = []
    fnames = []
    if verbose:
        print("Building ICAP-MME URLs...")
    normalized_base_url = base_url.rstrip("/") + "/"

    for dt in dates:
        fname = "icap_{}_{}_{}.nc".format(
            dt.strftime(r"%Y%m%d%H"), filetype.upper(), data_var.lower()
        )
        url = normalized_base_url + dt.strftime(r"%Y/%Y%m/") + fname
        urls.append(url)
        fnames.append(fname)

    return urls, fnames


def retrieve(url: str, fname: str, download: bool = False, verbose: bool = True) -> str | Path:
    """
    Retrieve or download an ICAP-MME file.

    Parameters
    ----------
    url : str
        Source URL.
    fname : str
        Target filename.
    download : bool, optional
        Whether to download to a local file, by default False.
    verbose : bool, optional
        Whether to print status, by default True.

    Returns
    -------
    Union[str, Path]
        The path or URL to the file.

    Examples
    --------
    >>> path = retrieve("https://.../file.nc", "file.nc", download=True)
    """
    p = Path(fname).absolute()
    fs = FileUtility.get_fs(url)

    if not download:
        # Return URL for remote opening via fsspec
        return url
    else:
        if not p.is_file():
            if verbose:
                print(f"Downloading {url} to {p.as_posix()}")
            fs.get(url, str(p))
        else:
            if verbose:
                print(f"File Exists: {p.as_posix()}")
        return p
