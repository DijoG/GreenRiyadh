import rasterio
import numpy as np
from scipy.ndimage import convolve
import time

def calculate_slope_horn_tiled(input_raster_path, output_raster_path, resolution, tile_size=512):
    """
    Calculates slope from a large DEM using Horn's formula with tiling.
    """
    start_time = time.time()
    processing_time = 0.0  # Initialize processing_time here
    print(f"Starting tiled slope calculation for: {input_raster_path}")

    try:
        with rasterio.open(input_raster_path) as src:
            rows = src.height
            cols = src.width
            profile = src.profile
            dtype = np.float32

            output_profile = profile.copy()
            output_profile.update(dtype=rasterio.float32, count=1)

            with rasterio.open(output_raster_path, 'w', **output_profile) as dst:
                print(f"Processing {rows}x{cols} raster in {tile_size}x{tile_size} tiles...")

                for row_start in range(0, rows, tile_size):
                    row_end = min(row_start + tile_size, rows)
                    for col_start in range(0, cols, tile_size):
                        col_end = min(col_start + tile_size, cols)

                        # Read tile with buffer
                        buffer = 1
                        tile_row_start = max(0, row_start - buffer)
                        tile_row_end = min(rows, row_end + buffer)
                        tile_col_start = max(0, col_start - buffer)
                        tile_col_end = min(cols, col_end + buffer)

                        window = rasterio.windows.Window(tile_col_start, tile_row_start,
                                                         tile_col_end - tile_col_start,
                                                         tile_row_end - tile_row_start)
                        tile_elevation = src.read(1, window=window).astype(dtype)

                        # Apply Horn's formula
                        dzdx_kernel = np.array([[-1, 0, 1], [-2, 0, 2], [-1, 0, 1]]) / (8 * resolution)
                        dzdy_kernel = np.array([[1, 2, 1], [0, 0, 0], [-1, -2, -1]]) / (8 * resolution)
                        dzdx_tile = convolve(tile_elevation, dzdx_kernel, mode='constant', cval=np.nan)
                        dzdy_tile = convolve(tile_elevation, dzdy_kernel, mode='constant', cval=np.nan)
                        slope_rad_tile = np.arctan(np.sqrt(dzdx_tile**2 + dzdy_tile**2))
                        slope_deg_tile = np.degrees(slope_rad_tile)

                        # Define the write window (original tile boundaries)
                        write_window = rasterio.windows.Window(col_start, row_start,
                                                               col_end - col_start,
                                                               row_end - row_start)

                        # Define the slice of the *processed* tile to write
                        write_row_start = buffer if row_start > 0 else 0
                        write_row_end = slope_deg_tile.shape[0] - buffer if row_end < rows else slope_deg_tile.shape[0]
                        write_col_start = buffer if col_start > 0 else 0
                        write_col_end = slope_deg_tile.shape[1] - buffer if col_end < cols else slope_deg_tile.shape[1]

                        data_to_write = slope_deg_tile[write_row_start:write_row_end,
                                                       write_col_start:write_col_end]

                        dst.write(data_to_write, 1, window=write_window)

                        print(f"Processed tile: row {row_start}-{row_end}, col {col_start}-{col_end}", end='\r')

                print("\nTiled processing complete.")

    except rasterio.RasterioIOError as e:
        print(f"Error opening raster file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    end_time = time.time()
    processing_time = end_time - start_time
    print(f"Total processing time: {processing_time:.2f} seconds.")

# Example usage:
input_dem_file = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/17_DEMfromALS/DEMfromALS_1m.tif"
output_slope_file = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/17_DEMfromALS/DEMfromALS_1m_SLOPE.tif"
dem_resolution = 1.0
tile_size_pixels = 512

calculate_slope_horn_tiled(input_dem_file, output_slope_file, dem_resolution, tile_size_pixels)