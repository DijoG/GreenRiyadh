import os
import glob
import rasterio
from rasterio.features import shapes
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from shapely.ops import unary_union
import multiprocessing
import time
from tqdm import tqdm
from itertools import chain

class TicToc:
    """Enhanced timer with memory monitoring"""
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.start_time = None
        self.laps = []
    
    def tic(self):
        self.reset()
        self.start_time = time.time()
    
    def toc(self):
        return time.time() - self.start_time
    
    def lap(self, name=""):
        elapsed = self.toc()
        self.laps.append((name, elapsed))
        return elapsed
    
    def print_lap(self, message=""):
        elapsed = self.lap(message)
        ram = self.get_memory_usage()
        print(f"\n{message}: {elapsed:.2f}s | RAM: {ram}%")
        return elapsed
    
    def get_memory_usage(self):
        try:
            import psutil
            return f"{psutil.virtual_memory().percent}%"
        except:
            return "N/A"
            

def get_driver_from_path(path):
    """Robust format detection from file extension"""
    ext = os.path.splitext(path)[1].lower()
    driver_map = {
        '.gpkg': 'GPKG',
        '.geojson': 'GeoJSON',
        '.shp': 'ESRI Shapefile',
        '.fgb': 'FlatGeobuf'
    }
    driver = driver_map.get(ext)
    if not driver:
        raise ValueError(
            f"Unsupported file extension '{ext}'. "
            f"Supported: {', '.join(driver_map.keys())}"
        )
    return driver

def process_tile(file_path):
    """Process single tile with robust geometry handling"""
    try:
        with rasterio.open(file_path) as src:
            image = src.read(1)
            mask = image != src.nodata
            all_shapes = list(shapes(image, mask=mask, 
                                  transform=src.transform, 
                                  connectivity=8))

        polygons = []
        for geom, val in all_shapes:
            try:
                poly = Polygon(geom['coordinates'][0])
                if not poly.is_valid:
                    poly = poly.buffer(0)
                if poly.is_valid and poly.area > 0.01:  # Skip tiny polygons
                    polygons.append(poly)
            except:
                continue

        return gpd.GeoDataFrame({'geometry': polygons}, crs=src.crs) if polygons else None
    
    except Exception as e:
        print(f"\nTile error: {os.path.basename(file_path)} - {str(e)}")
        return None

def merge_in_chunks(gdf_list, tolerance=1.0, chunk_size=1000):
    """Memory-efficient merging with fail-safes"""
    if not gdf_list:
        return gpd.GeoDataFrame()
    
    # CRS validation
    if len({gdf.crs for gdf in gdf_list}) > 1:
        raise ValueError("Mixed CRS detected in input tiles")
    
    # 1. Simplify and filter geometries first
    simplified = []
    with tqdm(gdf_list, desc="Simplifying") as pbar:
        for gdf in gdf_list:
            try:
                simplified_gdf = gdf[gdf.geometry.area > 0.1].simplify(tolerance)
                simplified.append(simplified_gdf)
            except Exception as e:
                print(f"Skipping problematic tile: {str(e)}")
            pbar.update(1)
    
    if not simplified:
        return gpd.GeoDataFrame()
    
    # 2. Process in smaller chunks with progress
    chunk_size = chunk_size 
    all_geoms = list(chain.from_iterable(
        gdf.geometry.tolist() for gdf in simplified
    ))
    
    merged = None
    with tqdm(total=len(all_geoms), desc="Merging") as pbar:
        for i in range(0, len(all_geoms), chunk_size):
            chunk = all_geoms[i:i + chunk_size]
            try:
                chunk_union = unary_union(chunk)
                if merged is None:
                    merged = chunk_union
                else:
                    merged = merged.union(chunk_union)
                pbar.update(len(chunk))
            except Exception as e:
                print(f"\nChunk {i//chunk_size} failed: {str(e)}")
                continue
    
    # 3. Final processing with timeout
    if merged is None:
        return gpd.GeoDataFrame()
    
    with tqdm(desc="Finalizing", total=1) as pbar:
        try:
            result = gpd.GeoDataFrame(
                geometry=gpd.GeoSeries(merged).explode(index_parts=True),
                crs=gdf_list[0].crs
            )
            pbar.update(1)
            return result[result.geometry.is_valid & ~result.geometry.is_empty]
        except Exception as e:
            print(f"\nFinal processing failed: {str(e)}")
            return gpd.GeoDataFrame() 

def process_tiled_rasters_parallel(
    input_dir,
    output_path,
    num_processes=None,
    batch_size=50,
    tolerance=1.0,
    chunk_size=1000
):
    """
    Main function to process tiled rasters in parallel and merge them into a single vector file.
    
    Args:
        input_dir: Path to tiled rasters directory
        output_path: Path to save the merged vector output
        num_processes: Number of parallel processes (default: None meaning 10)
        batch_size: Number of tiles to process in each batch (default: 50)
        tolerance: Tolerance for merging geometries (default: 1.0)
        chunk_size: Size of chunks (number of patches) for merging (default: 1000)

    Returns:
        Merged vector file (gpkg, geojson, shp and fgb) saved at output_path.
    """
    timer = TicToc()
    timer.tic()
    
    # Input validation
    all_tiffs = glob.glob(os.path.join(input_dir, "*.tif"))
    if not all_tiffs:
        raise ValueError(f"No TIFF files found in: {input_dir}")
    
    # Output format validation
    try:
        driver = get_driver_from_path(output_path)
        print(f"Output format: {driver}")
    except ValueError as e:
        raise ValueError(f"Invalid output path: {str(e)}")
    
    # Parallel processing setup
    num_processes = min(
        num_processes or multiprocessing.cpu_count(),
        10  # Cap at 10 cores
    )

    print(f"\nProcessing {len(all_tiffs)} tiles ({num_processes} workers)")

    # Process in batches
    results = []
    with tqdm(total=len(all_tiffs), desc="Overall progress") as main_pbar:
        for i in range(0, len(all_tiffs), batch_size):
            batch = all_tiffs[i:i + batch_size]
            with multiprocessing.Pool(num_processes) as pool:
                batch_results = list(tqdm(
                    pool.imap_unordered(process_tile, batch),
                    total=len(batch),
                    desc=f"Batch {i//batch_size + 1}",
                    leave=False
                ))
                results.extend(batch_results)
                main_pbar.update(len(batch))
    
    timer.print_lap("Tile processing completed")

    # Filter and merge
    valid_results = [gdf for gdf in results if gdf is not None]
    if not valid_results:
        raise ValueError("No valid polygons generated")
    
    merged_gdf = merge_in_chunks(valid_results, tolerance, chunk_size)
    merged_gdf['AREA'] = merged_gdf.geometry.area.round(2)
    timer.print_lap("Spatial merging completed")

    # Save output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with tqdm(desc="Saving output", total=1) as pbar:
        merged_gdf.to_file(output_path, driver=driver)
        pbar.update(1)
    
    print(timer.get_summary())
    print(f"\nSuccessfully saved {len(merged_gdf)} features to {output_path}")

if __name__ == "__main__":
    # Example usage:
    input_dir = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/12_Digitized_Geotechnical/GTM/tiles"
    output_path = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/12_Digitized_Geotechnical/GTM/wadi_clumpcleanarea_python.geojson"  # Supports .geojson, .shp, .gpkg and .fgb
    
    process_tiled_rasters_parallel(
        input_dir =  input_dir,
        output_path = output_path,
        num_processes = 10,      # Adjust based on your CPU
        batch_size = 100,        # Reduce if memory-constrained
        tolerance = 2.0,         # Adjust (1.0 is default)
        chunk_size = 1000         # Adjust (1000 is default)
    )

# Recommendation:
# Hardware	 Tiles	 num_processes	 batch_size  chunk_size (avoiding memory pikes)
# 4-core 	   100	 3	             20          100
# 16-core 	 1,000	 10	             100         100
# 32-core  >10,000	 20	             200         500/1000
