"""MERRA-2 Reader"""

import datetime

import pandas as pd
import xarray as xr

from .base import GriddedReader, _scientific_hygiene, register_reader
from .nasa_utils import setup_netrc
from .sat_utils import standardize_satellite_coords, update_history


@register_reader("merra2")
class MERRA2Reader(GriddedReader):
    """
    Reader for MERRA-2 (Modern-Era Retrospective analysis for Research and Applications, Version 2) data.
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
        product: str = "inst1_2d_asm_Nx",
        username: str | None = None,
        password: str | None = None,
        virtualizarr: str | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads MERRA-2 data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File path(s) or URL(s). If None, will try to build URLs using dates and product.
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
            Dates to retrieve. Used if files is None.
        product : str, optional
            MERRA-2 product short name, by default "inst1_2d_asm_Nx".
            Common products:
            - 'inst1_2d_asm_Nx': Instantaneous 2D atmospheric fields
            - 'tavg1_2d_slv_Nx': Time-averaged 2D surface fields
            - 'inst3_3d_asm_Np': Instantaneous 3D atmospheric fields (pressure levels)
            - 'inst3_3d_chm_Np': Instantaneous 3D chemical fields (pressure levels)
        username : str, optional
            NASA Earthdata username. If provided, will setup .netrc.
        password : str, optional
            NASA Earthdata password. If provided, will setup .netrc.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The MERRA-2 dataset.
        """
        if username and password:
            setup_netrc(username, password)

        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files = self.build_urls(dates, product=product)

        if "preprocess" not in kwargs:
            from functools import partial

            kwargs["preprocess"] = partial(merra2_preprocess, product=product)

        if virtualizarr is not None:
            kwargs["virtualizarr_file"] = virtualizarr
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
        ds = update_history(ds, f"Read MERRA-2 {product} data.")

        return ds

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        product: str = "inst1_2d_asm_Nx",
    ) -> list[str]:
        """
        Build OPeNDAP URLs for MERRA-2 data based on dates and product.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        product : str, optional
            MERRA-2 product short name.

        Returns
        -------
        List[str]
            List of OPeNDAP URLs.
        """
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        # Product mapping to GES DISC OPeNDAP paths
        # Format: (ShortName.Version, CollectionName, ServerNumber)
        prod_map = {
            "inst1_2d_asm_Nx": ("M2I1NXASM.5.12.4", "inst1_2d_asm_Nx", "4"),
            "inst1_2d_int_Nx": ("M2I1NXINT.5.12.4", "inst1_2d_int_Nx", "4"),
            "inst1_2d_lfo_Nx": ("M2I1NXLFO.5.12.4", "inst1_2d_lfo_Nx", "4"),
            "inst3_2d_gas_Nx": ("M2I3NXGAS.5.12.4", "inst3_2d_gas_Nx", "4"),
            "statD_2d_slv_Nx": ("M2SDNXSLV.5.12.4", "statD_2d_slv_Nx", "4"),
            "tavg1_2d_adg_Nx": ("M2T1NXADG.5.12.4", "tavg1_2d_adg_Nx", "4"),
            "tavg1_2d_aer_Nx": ("M2T1NXAER.5.12.4", "tavg1_2d_aer_Nx", "4"),
            "tavg1_2d_chm_Nx": ("M2T1NXCHM.5.12.4", "tavg1_2d_chm_Nx", "4"),
            "tavg1_2d_csp_Nx": ("M2T1NXCSP.5.12.4", "tavg1_2d_csp_Nx", "4"),
            "tavg1_2d_flx_Nx": ("M2T1NXFLX.5.12.4", "tavg1_2d_flx_Nx", "4"),
            "tavg1_2d_int_Nx": ("M2T1NXINT.5.12.4", "tavg1_2d_int_Nx", "4"),
            "tavg1_2d_lfo_Nx": ("M2T1NXLFO.5.12.4", "tavg1_2d_lfo_Nx", "4"),
            "tavg1_2d_lnd_Nx": ("M2T1NXLND.5.12.4", "tavg1_2d_lnd_Nx", "4"),
            "tavg1_2d_ocn_Nx": ("M2T1NXOCN.5.12.4", "tavg1_2d_ocn_Nx", "4"),
            "tavg1_2d_rad_Nx": ("M2T1NXRAD.5.12.4", "tavg1_2d_rad_Nx", "4"),
            "tavg1_2d_slv_Nx": ("M2T1NXSLV.5.12.4", "tavg1_2d_slv_Nx", "4"),
            "tavg3_2d_glc_Nx": ("M2T3NXGLC.5.12.4", "tavg3_2d_glc_Nx", "4"),
            "inst3_3d_asm_Np": ("M2I3NPASM.5.12.4", "inst3_3d_asm_Np", "5"),
            "inst3_3d_aer_Nv": ("M2I3NVAER.5.12.4", "inst3_3d_aer_Nv", "5"),
            "inst3_3d_asm_Nv": ("M2I3NVASM.5.12.4", "inst3_3d_asm_Nv", "5"),
            "inst3_3d_chm_Nv": ("M2I3NVCHM.5.12.4", "inst3_3d_chm_Nv", "5"),
            "inst3_3d_gas_Nv": ("M2I3NVGAS.5.12.4", "inst3_3d_gas_Nv", "5"),
            "inst6_3d_ana_Np": ("M2I6NPANA.5.12.4", "inst6_3d_ana_Np", "5"),
            "inst6_3d_ana_Nv": ("M2I6NVANA.5.12.4", "inst6_3d_ana_Nv", "5"),
            "tavg3_3d_mst_Ne": ("M2T3NEMST.5.12.4", "tavg3_3d_mst_Ne", "5"),
            "tavg3_3d_nav_Ne": ("M2T3NENAV.5.12.4", "tavg3_3d_nav_Ne", "5"),
            "tavg3_3d_trb_Ne": ("M2T3NETRB.5.12.4", "tavg3_3d_trb_Ne", "5"),
            "tavg3_3d_cld_Np": ("M2T3NPCLD.5.12.4", "tavg3_3d_cld_Np", "5"),
            "tavg3_3d_mst_Np": ("M2T3NPMST.5.12.4", "tavg3_3d_mst_Np", "5"),
            "tavg3_3d_odt_Np": ("M2T3NPODT.5.12.4", "tavg3_3d_odt_Np", "5"),
            "tavg3_3d_qdt_Np": ("M2T3NPQDT.5.12.4", "tavg3_3d_qdt_Np", "5"),
            "tavg3_3d_rad_Np": ("M2T3NPRAD.5.12.4", "tavg3_3d_rad_Np", "5"),
            "tavg3_3d_tdt_Np": ("M2T3NPTDT.5.12.4", "tavg3_3d_tdt_Np", "5"),
            "tavg3_3d_trb_Np": ("M2T3NPTRB.5.12.4", "tavg3_3d_trb_Np", "5"),
            "tavg3_3d_udt_Np": ("M2T3NPUDT.5.12.4", "tavg3_3d_udt_Np", "5"),
            "tavg3_3d_asm_Nv": ("M2T3NVASM.5.12.4", "tavg3_3d_asm_Nv", "5"),
            "tavg3_3d_cld_Nv": ("M2T3NVCLD.5.12.4", "tavg3_3d_cld_Nv", "5"),
            "tavg3_3d_mst_Nv": ("M2T3NVMST.5.12.4", "tavg3_3d_mst_Nv", "5"),
            "tavg3_3d_rad_Nv": ("M2T3NVRAD.5.12.4", "tavg3_3d_rad_Nv", "5"),
            "inst3_3d_chm_Np": ("M2I3NPCHM.5.12.4", "inst3_3d_chm_Np", "5"),
        }

        if product not in prod_map:
            raise ValueError(f"Unknown product: {product}. Available: {list(prod_map.keys())}")

        short_name, coll_name, server = prod_map[product]
        base_url = f"https://goldsmr{server}.gesdisc.eosdis.nasa.gov/opendap/MERRA2/{short_name}"

        urls = []
        for d in dates.floor("D").unique():
            # MERRA-2 filenames usually include the date and the product name
            # Example: MERRA2_400.inst1_2d_asm_Nx.20240101.nc4
            # The stream ID (400, 300, 200, 100) depends on the year.
            year = d.year
            if 1980 <= year <= 1991:
                stream = "100"
            elif 1992 <= year <= 2000:
                stream = "200"
            elif 2001 <= year <= 2010:
                stream = "300"
            else:
                stream = "400"

            date_str = d.strftime("%Y%m%d")
            url = f"{base_url}/{d.strftime('%Y/%m')}/MERRA2_{stream}.{coll_name}.{date_str}.nc4"
            urls.append(url)

        return urls


def merra2_preprocess(ds: xr.Dataset, product: str | None = None) -> xr.Dataset:
    """
    Preprocess MERRA-2 dataset: standardize coordinates and metadata.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    product : str, optional
        MERRA-2 product short name.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # 1. Pre-standardize coordinates to avoid losing them during dimension rename
    # if they share the same name.
    if "lat" in ds.coords and "latitude" not in ds.coords:
        ds = ds.assign_coords(latitude=ds.lat)
    if "lon" in ds.coords and "longitude" not in ds.coords:
        ds = ds.assign_coords(longitude=ds.lon)

    # 2. Standardize dimensions and coordinates
    # MERRA-2 typically uses 'lat', 'lon', 'time', 'lev'.
    ds = standardize_satellite_coords(
        ds,
        lat_name="latitude",
        lon_name="longitude",
        y_dim=["lat", "nlat", "y"],
        x_dim=["lon", "nlon", "x"],
        z_dim=["lev", "level", "layer"],
    )

    # 3. Expand 1D coords to 2D for UGRID/CF compliance in MONETIO if needed
    if "latitude" in ds.coords and ds["latitude"].ndim == 1:
        if "longitude" in ds.coords and ds["longitude"].ndim == 1:
            # Use lazy broadcasting
            lons, lats = xr.broadcast(ds.longitude, ds.latitude)
            # Ensure (y, x) order which is standard for gridded data in MONETIO
            if "y" in lons.dims and "x" in lons.dims:
                lons = lons.transpose("y", "x")
                lats = lats.transpose("y", "x")
            # Re-assign as 2D coordinates
            ds = ds.assign_coords(longitude=lons, latitude=lats)

    # 3. Variable renaming to standard names if they exist
    mapping = {
        "PS": "surface_pressure",
        "T": "temperature",
        "QV": "specific_humidity",
        "U": "u_wind",
        "V": "v_wind",
    }
    rename_dict = {
        old: new for old, new in mapping.items() if old in ds.variables and new not in ds.variables
    }
    if rename_dict:
        ds = ds.rename(rename_dict)

    # 4. Calculate Pressure (Lazy)
    ds = _add_merra2_pressure(ds)

    # 5. Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # Update history
    ds = update_history(ds, "Preprocessed MERRA-2 data using standardized preprocessing.")

    return ds


def _add_merra2_pressure(ds: xr.Dataset) -> xr.Dataset:
    """
    Calculate pressure levels lazily for MERRA-2 using ak and bk coefficients.
    p = ak + bk * surface_pressure

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Dataset with calculated pressure.
    """
    # Look for coefficients. Common names: ak, bk or ap, bp
    ak = ds.get("ak") if "ak" in ds.variables or "ak" in ds.coords else ds.get("ap")
    bk = ds.get("bk") if "bk" in ds.variables or "bk" in ds.coords else ds.get("bp")
    ps = ds.get("surface_pressure") if "surface_pressure" in ds.variables else ds.get("PS")

    if ak is not None and bk is not None and ps is not None:
        # p = ak + bk * ps
        # The calculation is fully lazy and backend-agnostic
        pres = ak + bk * ps

        ds["pres_pa_mid"] = pres.assign_attrs(
            {
                "units": "Pa",
                "long_name": "pressure",
                "standard_name": "air_pressure",
                "description": "Pressure calculated as ak + bk * surface_pressure",
            }
        )
        ds = update_history(ds, "Calculated 3D pressure lazily using ak and bk.")

    return ds
