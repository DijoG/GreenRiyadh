import geopandas as gpd
from shapely.geometry import Polygon
import itertools
import concurrent.futures
import pandas as pd
import os

def create_tiles(polygon, num_tiles_x, num_tiles_y):
    """Divides a polygon's bounding box into a grid of tiles."""
    minx, miny, maxx, maxy = polygon.total_bounds
    width = maxx - minx
    height = maxy - miny
    tile_width = width / num_tiles_x
    tile_height = height / num_tiles_y
    tiles = []
    for i in range(num_tiles_x):
        for j in range(num_tiles_y):
            xmin = minx + i * tile_width
            xmax = minx + (i + 1) * tile_width
            ymin = miny + j * tile_height
            ymax = miny + (j + 1) * tile_height
            tile = Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])
            tiles.append(tile)
    return gpd.GeoDataFrame({'geometry': tiles}, crs=polygon.crs)

def process_tile(tile, large_polygon):
    """Processes a single tile by intersecting it with the large polygon."""
    tile_bbox = tile.geometry.bounds
    print(f"Processing tile with bounding box: {tile_bbox}")  # Added print statement
    possible_intersections = large_polygon.cx[tile_bbox[0]:tile_bbox[2], tile_bbox[1]:tile_bbox[3]]
    intersection = gpd.overlay(possible_intersections, gpd.GeoDataFrame({'geometry': [tile.geometry]}, crs=large_polygon.crs), how='intersection')
    return intersection

def tile_intersect(large_polygon_path, small_polygon_path, num_tiles_x=10, num_tiles_y=10, output_path="intersection_result.shp", use_parallel=True):
    """
    Intersects a large polygon with a smaller polygon using tile processing.

    Args:
        large_polygon_path (str): Path to the large polygon shapefile (.shp).
        small_polygon_path (str): Path to the smaller polygon shapefile (.shp).
        num_tiles_x (int): Number of tiles to divide the smaller polygon into along the x-axis.
        num_tiles_y (int): Number of tiles to divide the smaller polygon into along the y-axis.
        output_path (str): Path to save the resulting intersection shapefile (.shp).
        use_parallel (bool): Whether to use parallel processing for the tile intersections.
    """
    try:
        
        large_polygon = gpd.read_file(large_polygon_path) 
        print("Polygon 1 is successfully read")
        small_polygon = gpd.read_file(small_polygon_path)
        print("Polygon 2 is successfully read")

        # Ensure both GeoDataFrames have the same CRS
        if large_polygon.crs != small_polygon.crs:
            print("Warning: Coordinate Reference Systems do not match. Reprojecting small polygon to match large polygon.")
            small_polygon = small_polygon.to_crs(large_polygon.crs)

        tiles = create_tiles(small_polygon, num_tiles_x, num_tiles_y)
        intersected_parts = []

        if use_parallel:
            num_cores = os.cpu_count() - 2
            if num_cores <= 0:
                num_cores = 1  # Ensure at least one core is used
            print(f"Using parallel processing with {num_cores} cores.")
            with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
                futures = [executor.submit(process_tile, tile, large_polygon) for _, tile in tiles.iterrows()]
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if not result.empty:
                        intersected_parts.append(result)
        else:
            print("Using sequential processing.")
            for _, tile in tiles.iterrows():
                intersection = process_tile(tile, large_polygon)
                if not intersection.empty:
                    intersected_parts.append(intersection)

        if intersected_parts:
            final_intersection = gpd.GeoDataFrame(pd.concat(intersected_parts, ignore_index=True), crs=large_polygon.crs)
            print(f"Number of intersected features: {len(final_intersection)}")
            final_intersection.to_file(output_path)  # geopandas automatically infers the driver from the file extension
            print(f"Intersection results saved to: {output_path}")
        else:
            print("No intersections found.")

    except FileNotFoundError:
        print("Error: One or both of the input file paths are incorrect.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # --- Example Usage ---
    large_polygon_file = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/12_Digitized_Geotechnical/GTM/Wadis_bed_buff.shp"        
    small_polygon_file = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/12_Digitized_Geotechnical/GTM/SOIL_GEODIG/GEOTECHNICAL_Smooth_200.shp"
    output_file = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/12_Digitized_Geotechnical/Wadis_bed_SEL.shp"

    # Adjust the number of tiles based on your data and system
    num_x_tiles = 10
    num_y_tiles = 10

    # Run the tile intersection
    tile_intersect(large_polygon_file, small_polygon_file, num_x_tiles, num_y_tiles, output_file, use_parallel=True)
