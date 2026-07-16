"""HYTRAJ Reader"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    import dask.dataframe as dd

from .base import PointReader, register_reader
from .drivers import FileUtility
from .sat_utils import update_history


@register_reader("hytraj")
class HYTRAJReader(PointReader):
    """
    Reader for HYSPLIT trajectory (tdump) files.
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
        taglist: list[Any] | None = None,
        renumber: bool = False,
        as_xarray: bool = True,
        lazy: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | xr.Dataset | dd.DataFrame:
        """
        Reads HYSPLIT trajectory (tdump) files.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s) or glob pattern.
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
        taglist : list[Any], optional
            List of tags for each file, added as 'pid' column.
        renumber : bool, optional
            Whether to renumber trajectories across files, by default False.
        as_xarray : bool, optional
            Whether to return an xarray.Dataset, by default True.
        lazy : bool, optional
            Whether to return a dask-backed object, by default False.
        **kwargs : Any
            Additional arguments passed to the driver.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset]
            The loaded trajectory data.

        Examples
        --------
        >>> ds = reader.open_dataset("tdump.*", taglist=["run1", "run2"], renumber=True)
        """
        # Expand paths first to match tags and indices
        file_list = FileUtility.expand_paths(files)

        # Filter out reader-specific kwargs for driver
        driver_kwargs = {k: v for k, v in kwargs.items() if k not in ["taglist", "renumber"]}

        if taglist is not None or renumber:
            # If we need tagging or renumbering, we handle multi-file opening manually
            # to pass specific arguments to each read_hytraj_file call.
            from .drivers import PandasDriver

            if not isinstance(self.driver, PandasDriver):
                raise TypeError("HYTRAJReader requires a PandasDriver.")

            dsets = []
            for i, f in enumerate(file_list):
                tag = taglist[i] if taglist is not None and i < len(taglist) else None
                renumber_index = i if renumber else None

                # We use the driver.open for a single file to respect lazy settings
                ds_single = self.driver.open(
                    f,
                    read_method=read_hytraj_file,
                    lazy=lazy,
                    tag=tag,
                    renumber_index=renumber_index,
                    **driver_kwargs,
                )
                dsets.append(ds_single)

            if lazy:
                import dask.dataframe as dd

                df = dd.concat(dsets).reset_index(drop=True)
            else:
                df = pd.concat(dsets, ignore_index=True).reset_index(drop=True)

            if taglist is not None:
                df = update_history(df, f"Added tags from taglist: {taglist}")
            if renumber:
                df = update_history(df, "Renumbered trajectories for global uniqueness.")
        else:
            # Standard path
            df = self.driver.open(
                files,
                use_virtualizarr=use_virtualizarr,
                virtualizarr_file=virtualizarr_file,
                virtualizarr_parser=virtualizarr_parser,
                virtualizarr_backend=virtualizarr_backend,
                icechunk_repo=icechunk_repo,
                use_icechunk=use_icechunk,
                icechunk_url=icechunk_url,
                use_dask=use_dask,
                read_method=read_hytraj_file,
                lazy=lazy,
                **driver_kwargs,
            )

        df = self.harmonize(df)

        if as_xarray:
            # trajectories are moving locations, so fixed_location=False (default)
            # PointReader.to_xarray handles both Pandas and Dask DataFrames
            ds = self.to_xarray(df, **kwargs)

            # Set the intermediate time columns as coordinates
            time_coords = [
                c for c in ["year", "month", "day", "hour", "minute"] if c in ds.data_vars
            ]
            if time_coords:
                ds = ds.set_coords(time_coords)

            ds = update_history(ds, "Read HYTRAJ data.")
            return ds

        return df


def read_hytraj_file(
    filename: str, tag: Any = None, renumber_index: int | None = None, **kwargs: Any
) -> pd.DataFrame:
    """
    Read a single HYSPLIT trajectory (tdump) file.

    Parameters
    ----------
    filename : str
        Path to the tdump file.
    tag : Any, optional
        Tag to add to the 'pid' column, by default None.
    renumber_index : int, optional
        Index to prepend to 'traj_num' for uniqueness across files, by default None.
    **kwargs : Any
        Additional arguments.

    Returns
    -------
    pd.DataFrame
        Trajectory data.

    Examples
    --------
    >>> df = read_hytraj_file("tdump.txt", tag="run1")
    """
    fs = FileUtility.get_fs(filename)
    with fs.open(filename, "r") as f:
        # 1. Skip Meteorological info
        line1 = f.readline().strip()
        if not line1:
            return pd.DataFrame()
        try:
            n_met = int(re.split(r"\s+", line1)[0])
        except (ValueError, IndexError):
            return pd.DataFrame()

        for _ in range(n_met):
            f.readline()

        # 2. Skip Starting locations
        line_start = f.readline().strip()
        try:
            n_start = int(re.split(r"\s+", line_start)[0])
        except (ValueError, IndexError):
            return pd.DataFrame()

        for _ in range(n_start):
            f.readline()

        # 3. Get variable names
        var_line = f.readline().strip()
        var_parts = re.split(r"\s+", var_line)
        # n_vars = int(var_parts[0])
        variables = [v.lower() for v in var_parts[1:]]

        # 4. Read the data
        # Data format:
        # traj_num, met_grid, year, month, day, hour, minute, fhr, age, lat, lon, alt, [vars]
        heads = [
            "traj_num",
            "met_grid",
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "forecast_hour",
            "traj_age",
            "latitude",
            "longitude",
            "altitude",
        ] + variables

        # pd.read_csv accepts the file handle at current position
        df = pd.read_csv(f, header=None, sep=r"\s+", names=heads)

    if df.empty:
        return df

    # 5. Vectorized Time Construction
    # Handle 2-digit years
    years = df["year"].astype(int)
    years = np.where(years < 50, years + 2000, years + 1900)

    df["time"] = pd.to_datetime(
        {
            "year": years,
            "month": df["month"],
            "day": df["day"],
            "hour": df["hour"],
            "minute": df["minute"],
        }
    )

    if renumber_index is not None:
        # Prepend index to ensure global uniqueness across files
        df["traj_num"] = str(renumber_index) + "_" + df["traj_num"].astype(str)

    # Ensure consistent dtypes for merging
    df["siteid"] = df["traj_num"].astype(str)

    if tag is not None:
        df["pid"] = tag

    return df
