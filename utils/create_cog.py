############################################################
# Create Cloud-Optimized GeoTIFF                           #
# Copyright (c) 2024, Caleb Kelly                          #
# Author: Caleb Kelly  (2024)                              #
############################################################

import subprocess
from concurrent.futures import ProcessPoolExecutor

def create_cog(geotiff_path, overviews=['2', '4', '8', '16', '32', '64', '128', '256']):
    '''
    Create a Cloud-Optimized GeoTIFF (COG) from a GeoTIFF file

    Parameters
    ----------
    geotiff_path: Name/full path of the geotiff file
    overviews.  : The desired number of overviews

    Returns
    -------
    cog_path.   : Path to the COG

    Reference
    ---------
    [1] https://www.cogeo.org/developers-guide.html 
    [2] https://github.com/OSGeo/gdal/blob/master/swig/python/gdal-utils/osgeo_utils/samples/validate_cloud_optimized_geotiff.py
    [3] https://trac.osgeo.org/gdal/wiki/CloudOptimizedGeoTIFF#HowtoreaditwithGDAL

    Notes
    -----
    [1] We will use gdaladdo to add overviews. Overviews are coarse versions of the GeoTIFF for quick visualization
    [2] We will then run gdal_translate to create the COG. See Reference [3]
    [3] If we pass a list of GeoTIFFs to `create_cog`, it will call `create_cogs_parallel` to use multi-cores.
    '''
    if isinstance(geotiff_paths, str):
        geotiff_paths = [geotiff_paths]
    if len(geotiff_paths) > 1:
        return create_cogs_parallel(geotiff_paths)
    else:
        geotiff_path = geotiff_paths[0]
        cog_path = geotiff_path.replace('.tif', '_cog.tif')
        subprocess.run([
            'gdaladdo',
            '-r', 'average',
            geotiff_path,
            *overviews
        ])
        subprocess.run([
            'gdal_translate',
            geotiff_path,
            cog_path,
            '-co', 'TILED=YES',
            '-co', 'COPY_SRC_OVERVIEWS=YES',
            '-co', 'COMPRESS=LZW'
        ])
        return cog_path

def create_cogs_parallel(geotiff_paths):
    '''
    Run `create_cogs` in parallel if a list of GeoTIFFs is passed as input
    '''
    with ProcessPoolExecutor() as executor:
        cog_paths = list(executor.map(create_cog, geotiff_paths))
    return cog_paths
