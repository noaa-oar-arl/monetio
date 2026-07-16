"""PARDUMP Reader"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Union

import numpy as np
import pandas as pd
import xarray as xr

from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd


@register_reader("pardump")
class PardumpReader(PointReader):
    """
    Reader for HYSPLIT PARDUMP binary files.
    """

    fixed_location = False

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
        drange: tuple[datetime, datetime] | list[datetime] | None = None,
        century: int = 2000,
        verbose: bool = False,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load PARDUMP data.

        Parameters
        ----------
        files : Union[str, List[str]]
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
        drange : tuple of datetime, optional
            Date range to filter, by default None.
        century : int, optional
            Century for 2-digit years, by default 2000.
        verbose : bool, optional
            Print progress, by default False.
        as_xarray : bool, optional
            If True, return an xarray.Dataset, by default True.
        lazy : bool, optional
            If True, return a dask-backed object, by default False.
        **kwargs : Any
            Additional arguments passed to the driver.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded dataset.

        Examples
        --------
        >>> reader = PardumpReader()
        >>> ds = reader.open_dataset("PARDUMP.bin")
        """
        # Pass parameters to read_pardump via kwargs
        kwargs.update(
            {
                "drange": drange,
                "century": century,
                "verbose": verbose,
            }
        )

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
            read_method=read_pardump,
            as_xarray=as_xarray,
            lazy=lazy,
            **kwargs,
        )

    def harmonize(
        self, df: Union[pd.DataFrame, "dd.DataFrame"]
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Harmonize the PARDUMP dataset.

        Parameters
        ----------
        df : Union[pd.DataFrame, dd.DataFrame]
            Input dataframe.

        Returns
        -------
        Union[pd.DataFrame, dd.DataFrame]
            Harmonized dataframe.
        """
        # Rename to MONETIO consistency
        rename_dict = {"date": "time", "lat": "latitude", "lon": "longitude"}
        rename_dict = {k: v for k, v in rename_dict.items() if k in df.columns}
        if rename_dict:
            df = df.rename(columns=rename_dict)

        # Ensure siteid exists for to_xarray (trajectories usually use a unique ID)
        # For PARDUMP, we might use 'sorti' or generate a unique ID.
        # But 'siteid' is expected by PointReader.to_xarray.
        if "siteid" not in df.columns:
            if "sorti" in df.columns:
                df["siteid"] = df["sorti"].astype(str)
            else:
                df["siteid"] = "particle"

        # Update history
        df = update_history(df, "Harmonized PARDUMP data.")

        return super().harmonize(df)


def read_pardump(
    filename: str,
    drange: tuple[datetime, datetime] | list[datetime] | None = None,
    century: int = 2000,
    verbose: bool = False,
    sorti: list[int] | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Read a single HYSPLIT PARDUMP binary file.

    Parameters
    ----------
    filename : str
        Path to the PARDUMP file.
    drange : tuple of datetime, optional
        Date range to filter, by default None.
    century : int, optional
        Century for 2-digit years, by default 2000.
    verbose : bool, optional
        Whether to print verbose output, by default False.
    sorti : list of int, optional
        Filter by particle ID (sorti), by default None.
    **kwargs : Any
        Additional arguments (ignored).

    Returns
    -------
    pd.DataFrame
        The loaded particle data.
    """
    pdump = Pardump(filename)
    return pdump.read(drange=drange, century=century, verbose=verbose, sorti=sorti)


class Pardump:
    """
    Helper class to parse HYSPLIT PARDUMP binary format.
    """

    def __init__(self, fname: str = "PARINIT"):
        self.fname = fname
        tp1 = ">f4"  # Big-endian float32
        tp2 = ">i4"  # Big-endian int32
        tp3 = ">i8"  # Big-endian int64

        self.hdr_dt = np.dtype(
            [
                ("padding", tp2),
                ("parnum", tp2),
                ("pollnum", tp2),
                ("year", tp2),
                ("month", tp2),
                ("day", tp2),
                ("hour", tp2),
                ("minute", tp2),
            ]
        )

        self.pardt = np.dtype(
            [
                ("p1", tp2),
                ("p2", tp2),
                ("pmass", tp1),
                ("p3", tp3),
                ("lat", tp1),
                ("lon", tp1),
                ("ht", tp1),
                ("su", tp1),
                ("sv", tp1),
                ("sx", tp1),
                ("p4", tp3),
                ("age", tp2),
                ("dist", tp2),
                ("poll", tp2),
                ("mgrid", tp2),
                ("sorti", tp2),
            ]
        )

    def read(
        self,
        drange: tuple[datetime, datetime] | list[datetime] | None = None,
        verbose: bool = False,
        century: int = 2000,
        sorti: list[int] | None = None,
    ) -> pd.DataFrame:
        """
        Parse the PARDUMP file.

        Parameters
        ----------
        drange : tuple of datetime, optional
            Date range to filter, by default None.
        verbose : bool, optional
            Whether to print verbose output, by default False.
        century : int, optional
            Century for 2-digit years, by default 2000.
        sorti : list of int, optional
            Filter by particle ID (sorti), by default None.

        Returns
        -------
        pd.DataFrame
            The parsed particle data.
        """
        imax = 100000
        parframe_list = []

        fs = FileUtility.get_fs(self.fname)
        with fs.open(self.fname, "rb") as fpoint:
            # Read entire file content to avoid repeated I/O in loop
            # and to work better with remote filesystems.
            content = fpoint.read()
            offset = 0
            iii = 0

            while offset < len(content):
                # 1. Read Header
                # Each record is wrapped in Fortran-style padding (4-byte length markers)
                # The hdr_dt already includes the first padding.
                try:
                    hdata = np.frombuffer(content, dtype=self.hdr_dt, count=1, offset=offset)
                    offset += self.hdr_dt.itemsize
                except (ValueError, IndexError):
                    break

                if hdata.size == 0:
                    break

                year = hdata["year"][0]
                if year < 1000:
                    year += century

                try:
                    pdate = datetime(
                        int(year),
                        int(hdata["month"][0]),
                        int(hdata["day"][0]),
                        int(hdata["hour"][0]),
                        int(hdata["minute"][0]),
                    )
                except ValueError:
                    # Invalid date, possibly corrupt record or end of file
                    break

                parnum = hdata["parnum"][0]

                # 2. Read Particle Data
                # Particle data is count * pardt.itemsize
                data_size = parnum * self.pardt.itemsize
                if offset + data_size > len(content):
                    break

                data = np.frombuffer(content, dtype=self.pardt, count=parnum, offset=offset)
                offset += data_size

                # 3. Read Closing Padding (Fortran-style)
                offset += 4

                if verbose:
                    print(f"Record Header: {hdata}")
                    print(f"Date: {pdate} Particle count: {parnum}")

                testdate = True
                if drange is not None:
                    if pdate < drange[0] or pdate > drange[1]:
                        testdate = False

                if testdate:
                    # Standardize byte order to host
                    # newbyteorder on array was removed in NumPy 2.0; use view with new dtype
                    ndata = data.byteswap().view(data.dtype.newbyteorder("="))
                    par_frame = pd.DataFrame.from_records(ndata)

                    # Cleanup columns
                    drop_cols = ["p1", "p2", "p3", "p4", "su", "sv", "sx", "mgrid"]
                    par_frame = par_frame.drop(columns=[c for c in drop_cols if c in par_frame])

                    # Filter out invalid locations
                    par_frame = par_frame.loc[par_frame["lat"] != 0]

                    if sorti is not None:
                        par_frame = par_frame.loc[par_frame["sorti"].isin(sorti)]

                    par_frame["date"] = pdate
                    parframe_list.append(par_frame)

                iii += 1
                if drange is not None and pdate > drange[1]:
                    break

                if iii > imax:
                    if verbose:
                        print(f"Read PARDUMP limited to {imax} iterations. Stopping.")
                    break

        if not parframe_list:
            return pd.DataFrame()

        parframe_all = pd.concat(parframe_list, axis=0, ignore_index=True)

        # Set file name as an attribute if possible
        parframe_all.attrs["filename"] = self.fname

        return parframe_all
