import os
import numpy as np
import rasterio
from rasterio.windows import Window
from scipy.ndimage import generic_filter, label
import time
from tqdm import tqdm
import tempfile
import shutil

def distance_patches(input_path, distance, tile_size, write_to=True):
    """
    Python implementation of distance-based clumping.

    Args:
        input_path: Path to raster file
        distance: fical window size, distance (pixel) within to find patches (clumps)
        tile_size: x and y (width and length) of a tile in pixel
        write_to: whether to write patched tiles (tif) into 'tiles' directory created at the end of input path (default True) or not

    Output:
        Renamed raster tiles (in tiles dierctory) and info.
    """
    # Open the raster
    with rasterio.open(input_path) as src:
        profile = src.profile
        raster = src.read(1)
        transform = src.transform
        
    # Create null raster (0=null, 1=non-null)
    null_r = (raster == 0).astype(np.int8)
    
    # Get base output name
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_dir = os.path.dirname(input_path)
    
    # Process in tiles if the raster is large
    if isinstance(tile_size, int):
        tile_size = (tile_size, tile_size)
    
    height, width = raster.shape
    if (height * width) > (tile_size[0] * tile_size[1] * 4):
        print("Processing in tiles...")
        
        # Create output directory for tiles if writing
        if write_to:
            tile_dir = os.path.join(output_dir, f"{base_name}_tiles")
            os.makedirs(tile_dir, exist_ok=True)
        
        # Calculate number of tiles
        n_tiles_x = int(np.ceil(width / tile_size[1]))
        n_tiles_y = int(np.ceil(height / tile_size[0]))
        total_tiles = n_tiles_x * n_tiles_y
        
        # Create temporary directory for intermediate results
        temp_dir = tempfile.mkdtemp()
        temp_files = []
        
        try:
            # Process each tile
            results = []
            with tqdm(total=total_tiles, desc="Processing tiles") as pbar:
                for i in range(n_tiles_x):
                    for j in range(n_tiles_y):
                        # Calculate window bounds
                        xoff = i * tile_size[1]
                        yoff = j * tile_size[0]
                        win_width = min(tile_size[1], width - xoff)
                        win_height = min(tile_size[0], height - yoff)
                        
                        # Add overlap for focal operations
                        overlap = distance
                        x1 = max(0, xoff - overlap)
                        y1 = max(0, yoff - overlap)
                        x2 = min(width, xoff + win_width + overlap)
                        y2 = min(height, yoff + win_height + overlap)
                        
                        # Read data with overlap
                        window = Window(x1, y1, x2 - x1, y2 - y1)
                        tile_data = raster[y1:y2, x1:x2]
                        null_tile = null_r[y1:y2, x1:x2]
                        
                        # Process tile
                        result_tile = process_tile(null_tile, tile_data, distance)
                        
                        # Crop back to original tile size (without overlap)
                        result_tile = result_tile[
                            (yoff-y1):(yoff-y1)+win_height, 
                            (xoff-x1):(xoff-x1)+win_width
                        ]
                        
                        # Save temporary result
                        temp_file = os.path.join(temp_dir, f"tile_{i}_{j}.npy")
                        np.save(temp_file, result_tile)
                        temp_files.append(temp_file)
                        
                        # Write final tile if requested
                        if write_to:
                            out_path = os.path.join(
                                tile_dir,
                                f"{base_name}_CLUMP_{distance}_tile_{i:03d}_{j:03d}.tif"
                            )
                            
                            profile.update({
                                'width': win_width,
                                'height': win_height,
                                'transform': rasterio.windows.transform(window, transform),
                                'dtype': result_tile.dtype
                            })
                            
                            with rasterio.open(out_path, 'w', **profile) as dst:
                                dst.write(result_tile, 1)
                        
                        pbar.update(1)
            
            # Merge results
            print("Merging tiles...")
            final_r = np.zeros_like(raster)
            with tqdm(total=total_tiles, desc="Assembling tiles") as pbar:
                for i in range(n_tiles_x):
                    for j in range(n_tiles_y):
                        xoff = i * tile_size[1]
                        yoff = j * tile_size[0]
                        win_width = min(tile_size[1], width - xoff)
                        win_height = min(tile_size[0], height - yoff)
                        
                        temp_file = os.path.join(temp_dir, f"tile_{i}_{j}.npy")
                        tile_data = np.load(temp_file)
                        
                        final_r[yoff:yoff+win_height, xoff:xoff+win_width] = tile_data
                        pbar.update(1)
            
            # Write final mosaic
            if write_to:
                out_path = os.path.join(
                    output_dir,
                    f"{base_name}_CLUMP_{distance}_FULL.tif"
                )
                
                profile.update({
                    'width': width,
                    'height': height,
                    'transform': transform,
                    'dtype': final_r.dtype
                })
                
                with rasterio.open(out_path, 'w', **profile) as dst:
                    dst.write(final_r, 1)
        
        finally:
            # Clean up temporary files
            shutil.rmtree(temp_dir)
    
    else:
        print("Processing as single raster...")
        # Process entire raster at once
        final_r = process_tile(null_r, raster, distance)
        
        # Write output if requested
        if write_to:
            out_path = os.path.join(
                output_dir,
                f"{base_name}_CLUMP_{distance}.tif"
            )
            
            profile.update({
                'dtype': final_r.dtype
            })
            
            with rasterio.open(out_path, 'w', **profile) as dst:
                dst.write(final_r, 1)
    
    # Get unique pixel values
    unique_vals = np.unique(final_r[~np.isnan(final_r)])
    
    # Prepare output
    result = {
        'raster_vals': unique_vals,
        'tiles_processed': total_tiles if 'total_tiles' in locals() else 1,
        'out_path': out_path if write_to else None,
        'processing_time': time.time() - start_time
    }
    
    print("Processing complete!")
    return result

def process_tile(null_tile, original_tile, distance):
    """Process a single tile"""
    # Define footprint for focal operation
    footprint = np.ones((distance, distance), dtype=np.int8)
    
    # Calculate focal sum for null cells
    focal_sum = generic_filter(
        null_tile,
        np.sum,
        footprint=footprint,
        mode='constant',
        cval=0
    )
    
    # Identify patches using Queen's case (8-connectivity)
    labeled, _ = label(focal_sum > 0, structure=np.ones((3, 3)))
    
    # Add original values
    result = labeled + original_tile
    
    return result

# Example usage
if __name__ == "__main__":
    input_path = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/12_Digitized_Geotechnical/GTM/DEM_Wadis_cm.tif"
    distance = 3
    start = time.time()
    result = distance_patches(input_path, distance, 1000)
    end = time.time()
    print(f"Processing completed in {end-start:.2f} seconds")
    print(f"Unique values: {result['raster_vals']}")
