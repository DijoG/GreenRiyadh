import os
import numpy as np
import rasterio
from rasterio.windows import Window
from pathlib import Path
from tqdm import tqdm
import tempfile
import shutil
import sys

def validate_and_prepare_paths(input_path: str, output_dir: str) -> tuple:
    """Validate input path and prepare output directory structure."""
    try:
        input_path = Path(input_path.strip()).absolute()
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        output_dir = Path(output_dir.strip()).absolute()
        tiles_dir = output_dir / "tiles"
        tiles_dir.mkdir(parents=True, exist_ok=True)

        return input_path, output_dir, tiles_dir
    except Exception as e:
        print(f"Path preparation failed: {str(e)}", file=sys.stderr)
        raise

def save_tile_as_geotiff(data, output_path, template_meta, x, y, width, height, transform):
    """Save a tile as a GeoTIFF file."""
    profile = template_meta.copy()
    profile.update({
        'height': height,
        'width': width,
        'transform': transform,
        'driver': 'GTiff'
    })
    
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(data, 1)

def tile_raster_with_overlap(
    input_path: str,
    output_dir: str = "output",
    tile_size: int = 1000,
    overlap: int = 100
):
    """
    Tile a raster file into smaller chunks with overlap and save them.
    
    Args:
        input_path: Path to input raster file
        output_dir: Directory to save output tiles
        tile_size: Size of each tile (square)
        overlap: Number of pixels to overlap between tiles

    Returns:
        Tiles (tif) saved in the specified output directory with overlap.
    """
    try:
        # Validate and prepare paths
        input_path, output_dir, tiles_dir = validate_and_prepare_paths(
            input_path, output_dir
        )
        
        temp_dir = Path(tempfile.mkdtemp())

        with rasterio.open(str(input_path)) as src:
            # Configure output profile
            profile = src.profile.copy()
            profile.update(
                compress='lzw',
                tiled=True,
                blockxsize=256,
                blockysize=256
            )

            # Calculate tile grid with overlap
            tile_grid = []
            for y in range(0, src.height, tile_size - overlap):
                for x in range(0, src.width, tile_size - overlap):
                    # Adjust for edges
                    x_off = max(0, x - overlap if x > 0 else 0)
                    y_off = max(0, y - overlap if y > 0 else 0)
                    
                    width = min(tile_size + (overlap if x > 0 else 0), 
                               src.width - x_off)
                    height = min(tile_size + (overlap if y > 0 else 0), 
                                src.height - y_off)
                    
                    tile_grid.append((y_off, x_off, width, height))

            # Process and save tiles
            with tqdm(total=len(tile_grid), desc="Creating overlapping tiles") as pbar:
                for y, x, width, height in tile_grid:
                    # Create window for current tile with overlap
                    win = Window(
                        col_off=x,
                        row_off=y,
                        width=width,
                        height=height
                    )
                    
                    # Read tile data
                    tile = src.read(1, window=win)
                    
                    # Save temporary file
                    temp_path = temp_dir / f"tile_{y}_{x}.npy"
                    np.save(temp_path, tile)
                    
                    # Save permanent tile
                    tile_path = tiles_dir / f"tile_{y:04d}_{x:04d}.tif"
                    tile_transform = rasterio.windows.transform(win, src.transform)
                    
                    save_tile_as_geotiff(
                        tile,
                        tile_path,
                        profile,
                        x, y, 
                        width, height,
                        tile_transform
                    )
                    
                    pbar.update(1)

        print(f"\nSuccess! Overlapping tiles saved to: {tiles_dir}")
        print(f"Total tiles created: {len(tile_grid)}")
        print(f"Tile size: {tile_size}px with {overlap}px overlap")

    except Exception as e:
        print(f"\nError during processing: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'temp_dir' in locals():
            shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    tile_raster_with_overlap(
        input_path = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/12_Digitized_Geotechnical/GTM/DEM_Wadis_cm1.tif",
        output_dir = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/12_Digitized_Geotechnical/GTM",     
        tile_size = 2000,
        overlap = 20
    )