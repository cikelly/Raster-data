import subprocess
from concurrent.futures import ProcessPoolExecutor

def create_cog(geotiff_path, overviews=['2', '4', '8', '16', '32', '64', '128', '256']):
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
    with ProcessPoolExecutor() as executor:
        cog_paths = list(executor.map(create_cog, geotiff_paths))
    return cog_paths
