
import os
from osgeo import ogr

def gpkg_to_gpkg(input_folder, output_gpkg):
    """Converts multiple GPKGs to a single merged GPKG (no ESRI needed)"""
    # Create output GPKG
    driver = ogr.GetDriverByName("GPKG")
    if os.path.exists(output_gpkg):
        driver.DeleteDataSource(output_gpkg)
    out_ds = driver.CreateDataSource(output_gpkg)
    
    # Process each input GPKG
    for file in os.listdir(input_folder):
        if file.endswith(".gpkg"):
            gpkg_path = os.path.join(input_folder, file)
            src_ds = ogr.Open(gpkg_path)
            
            # Copy layers with unique names
            for i in range(src_ds.GetLayerCount()):
                layer = src_ds.GetLayerByIndex(i)
                out_ds.CopyLayer(layer, f"{os.path.splitext(file)[0]}_{layer.GetName()}")
    
    out_ds = None  # Close file
    print(f"Success! Output: {output_gpkg}")

# Example usage:
gpkg_to_gpkg(
    input_folder=r"...\Green Cover\1_1_NDVI",
    output_gpkg=r"...\Green Cover\NDVI.gpkg")

# gpkg to gdb conversion:
# !!!!! in QGIS Python console (requies GDAL with FileGDB support) !!!!!:
# import os
# os.system('ogr2ogr -f "GPKG" ".../Green Cover/NDVI.gdb" ".../Green Cover/NDVI.gpkg"')
