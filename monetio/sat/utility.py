def write_array_tif(data, crs, transform, output_filename):
    """Write a tiff from a numpy array given the crs and transform

    Parameters
    ----------
    data : numpy array
        Description of parameter `data`.
    crs : type
        Description of parameter `crs`.
    transform : type
        Description of parameter `transform`.
    output_filename : type
        Description of parameter `output_filename`.

    Returns
    -------
    type
        Description of returned object.

    """
    import rasterio

    new_dataset = rasterio.open(
        output_filename,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs=crs,
        transform=transform,
    )
    new_dataset.write(data, 1)
    new_dataset.close()
    return None


def latlon_2modis_tile(lat, lon):
    """Find the latitude and longitude of a given modis tile

    Parameters
    ----------
    lat : float
        Description of parameter `lat`.
    lon : float
        Description of parameter `lon`.

    Returns
    -------
    (int, int)
        H and V

    """
    from pyproj import Proj

    # reference: https://code.env.duke.edu/projects/mget/wiki/SinusoidalMODIS
    p_modis_grid = Proj("+proj=sinu +R=6371007.181 +nadgrids=@null +wktext")
    x, y = p_modis_grid(lon, lat)
    # or the inverse, from x, y to lon, lat
    # lat, lon = p_modis_grid(x, y, inverse=True)
    tileWidth = 1111950.5196666666
    ulx = -20015109.354
    uly = -10007554.677
    H = int(x - ulx) / tileWidth
    V = 18 - (int(y - uly) / tileWidth)
    return int(V), int(H)


# def warp_to_wgs84(infile):
#     import os

#     crs = "+proj=longlat +ellps=WGS84 +datum=WGS84"
#     out_file = infile.replace(".tif", "_warped.tif")
#     convert_crs(infile, out_file, dst_crs=crs)
#     os.remove(infile)


# def convert_crs(in_file, out_file, dst_crs="EPSG:4326"):
#     """Short summary.

#     Parameters
#     ----------
#     in_file : type
#         Description of parameter `in_file`.
#     out_file : type
#         Description of parameter `out_file`.
#     dst_crs : type
#         Description of parameter `dst_crs`.

#     Returns
#     -------
#     type
#         Description of returned object.

#     """
#     import rasterio

#     # dst_crs = 'EPSG:4326'

#     with rasterio.open(in_file) as src:
#         transform, width, height = calculate_default_transform(  # not defined!
#             src.crs, dst_crs, src.width, src.height, *src.bounds, resolution=(0.004, 0.004)
#         )

#         kwargs = src.meta.copy()
#         kwargs.update({"crs": dst_crs, "transform": transform, "width": width, "height": height})

#         with rasterio.open(out_file, "w", **kwargs) as dst:
#             for i in range(1, src.count + 1):
#                 reproject(  # not defined!
#                     source=rasterio.band(src, i),
#                     destination=rasterio.band(dst, i),
#                     src_transform=src.transform,
#                     src_crs=src.crs,
#                     dst_transform=transform,
#                     dst_crs=dst_crs,
#                     resampling=Resampling.nearest,  # not defined!
#                 )


# def merge_tile_data(files_to_merge, outname):
#     """merges all swath data for a particular day and time of day"""
#     import rasterio

#     src_files_to_mosaic = []
#     for fp in files_to_merge:
#         src = rasterio.open(fp)
#         src_files_to_mosaic.append(src)
#     mosaic, out_trans = merge(src_files_to_mosaic, nodata=0)  # not defined!
#     out_meta = src.meta.copy()
#     out_meta.update(
#         {
#             "driver": "GTiff",
#             "height": mosaic.shape[1],
#             "width": mosaic.shape[2],
#             "transform": out_trans,
#             "crs": crs,  # not defined!
#         }
#     )
#     with rasterio.open(outname, "w", **out_meta) as dest:
#         dest.write(mosaic)
