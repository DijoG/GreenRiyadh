from shapely.geometry import Polygon
import rasterio
import geopandas as gpd
import numpy as np
import cv2
from rasterio.features import geometry_mask
from rasterio.transform import Affine
import matplotlib.pyplot as plt

def raster_to_array(path, aoi_path=None, target_crs="EPSG:32638"):
    """Read raster and ensure proper orientation"""
    with rasterio.open(path) as src:
        # Read the image and flip it vertically if needed
        img = src.read(1)
        
        # Check if the image is stored with Y-axis inverted (typical for geospatial data)
        if src.transform.e < 0:  # Negative Y resolution indicates top-left origin
            img = np.flipud(img)  # Flip vertically to get correct orientation
        
        # Get the transform and update if we flipped the image
        transform = src.transform
        if src.transform.e < 0:
            height = src.height
            transform = Affine(
                transform.a, transform.b, transform.c,
                transform.d, -transform.e, transform.f + transform.e * height
            )
        
        # Handle AOI masking if provided
        if aoi_path:
            aoi = gpd.read_file(aoi_path).to_crs(target_crs)
            mask = geometry_mask(
                aoi.geometry,
                transform=transform,
                invert=True,
                out_shape=img.shape
            )
            img[~mask] = 0
            
    return img, transform, target_crs

def detect_tree_crowns(img, transform, crs, brightness_low, brightness_high, min_area, sobel_thresh):
    """Detect tree crowns with proper coordinate handling"""
    # Thresholding
    binary = np.where((img >= brightness_low) & (img <= brightness_high), 1, 0).astype(np.uint8)
    
    # Edge detection
    sobel = cv2.Sobel(img, cv2.CV_64F, 1, 1, ksize=3)
    edges = np.where(np.abs(sobel) > sobel_thresh, 1, 0).astype(np.uint8)
    
    # Find contours - using RETR_EXTERNAL to get only outer contours
    contours, _ = cv2.findContours(
        cv2.bitwise_and(binary, edges),
        cv2.RETR_EXTERNAL,
        cv2.CHAIN_APPROX_SIMPLE
    )
    
    # Convert contours to properly oriented polygons
    polygons = []
    height = img.shape[0]  # We need this for coordinate correction
    
    for contour in contours:
        if cv2.contourArea(contour) >= min_area:
            # Convert image coordinates to geographic coordinates
            # Note: We don't need to flip Y here because we already corrected the image orientation
            coords = [transform * (pt[0][0], pt[0][1]) for pt in contour]
            polygon = Polygon(coords)
            polygons.append(polygon)
    
    return gpd.GeoDataFrame(geometry=polygons, crs=crs)

def run_pipeline(input_raster, 
                 aoi_shapefile, 
                 output_geojson,
                 brightness_low=30,
                 brightness_high=70,
                 min_area=15,
                 sobel_thresh=0.05):
    print("Processing raster...")
    img, transform, crs = raster_to_array(
        input_raster,
        aoi_shapefile,
        target_crs="EPSG:32638"
    )
    
    print("Detecting tree crowns...")
    crowns = detect_tree_crowns(
        img,
        transform,
        crs,
        brightness_low=brightness_low,  
        brightness_high=brightness_high,  
        min_area=min_area,         
        sobel_thresh=sobel_thresh    
    )
    
    print(f"Saving results to {output_geojson}")
    crowns.to_file(output_geojson, driver="GeoJSON")
    print("Processing complete!")

if __name__ == "__main__":
    run_pipeline(
        input_raster = "D:/Gergo/KFP/KFP01.tif",
        aoi_shapefile = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/21_SupportiveFiles/KingFahadPlaza.shp",
        output_geojson = "D:/Gergo/KFP/KFP01_TreeCrowns_Finalllllll.geojson",
        brightness_low = 20,
        brightness_high = 65,
        min_area = 20,
        sobel_thresh = 0.08
    )

######
# For darker trees (common in urban areas):
# brightness_low=20, brightness_high=60

# For lighter trees:
# brightness_low=40, brightness_high=90

# For larger crowns:
# min_area=25  # pixels

# For smaller crowns:
# min_area=10  # pixels

# For sharper edges:
#sobel_thresh=0.08

# For softer edges:
#sobel_thresh=0.03


