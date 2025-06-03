import os
from osgeo import ogr

def merge2gpkg(input_folder, output_gpkg):
    """
    Merges multiple spatial files (SHP, GeoJSON, GPKG) into a single GeoPackage.
    
    Args:
        input_folder (str): Path to folder containing input files
        output_gpkg (str): Path for output GeoPackage
    """
    # Create output GeoPackage
    driver = ogr.GetDriverByName("GPKG")
    if os.path.exists(output_gpkg):
        driver.DeleteDataSource(output_gpkg)
    out_ds = driver.CreateDataSource(output_gpkg)
    
    # Supported extensions and their OGR drivers
    supported_formats = {
        '.shp': 'ESRI Shapefile',
        '.geojson': 'GeoJSON',
        '.gpkg': 'GPKG'
    }
    
    # Process all supported files in directory
    for file in os.listdir(input_folder):
        ext = os.path.splitext(file)[1].lower()
        
        if ext in supported_formats:
            file_path = os.path.join(input_folder, file)
            src_ds = ogr.Open(file_path)
            
            # Handle multi-layer formats (like GPKG)
            for i in range(src_ds.GetLayerCount()):
                layer = src_ds.GetLayerByIndex(i)
                base_name = os.path.splitext(file)[0]
                
                # Create unique layer name (append number if multiple layers from one file)
                layer_name = f"{base_name}_{i}" if src_ds.GetLayerCount() > 1 else base_name
                
                # Copy layer to output
                out_ds.CopyLayer(layer, layer_name)
            
            src_ds = None  # Close input
    
    out_ds = None  # Close output
    print(f"Success! Merged output created at: {output_gpkg}")

# Example usage:
merge2gpkg(
    input_folder=r"...\Green Cover\1_1_NDVI",
    output_gpkg=r"...\Green Cover\merged.gpkg")

# gpkg to gdb conversion:
# !!!!! in QGIS Python console (requies GDAL with FileGDB support) !!!!!:
# import os
# os.system('ogr2ogr -f "GPKG" ".../Green Cover/merged.gdb" ".../Green Cover/merged.gpkg"')
