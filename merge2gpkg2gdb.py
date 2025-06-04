### 1) No ESRI needed
### various GIS files to gpkg conversion:
import os
from osgeo import ogr

def merge2gpkg(input_folder, output_gpkg):
    """
    Converts multiple GIS files (SHP, GeoJSON, GPKG) to a single GeoPackage
    Args:
        input_folder (str): Path to input folder containing GIS files
        output_gpkg (str): Path to output GeoPackage (.gpkg)
    Returns:
        str: Path to the created .gpkg
    """
    # Create output GeoPackage
    driver = ogr.GetDriverByName("GPKG")
    if os.path.exists(output_gpkg):
        driver.DeleteDataSource(output_gpkg)
    out_ds = driver.CreateDataSource(output_gpkg)
    
    # Supported input formats
    supported_extensions = ['.shp', '.geojson', '.gpkg']
    
    # Process each input file
    for file in os.listdir(input_folder):
        file_lower = file.lower()
        if any(file_lower.endswith(ext) for ext in supported_extensions):
            file_path = os.path.join(input_folder, file)
            src_ds = ogr.Open(file_path)
            
            # Handle different file types
            if file_lower.endswith('.gpkg'):
                # For GPKG, copy all layers
                for i in range(src_ds.GetLayerCount()):
                    layer = src_ds.GetLayerByIndex(i)
                    layer_name = layer.GetName()
                    out_ds.CopyLayer(layer, layer_name)
            else:
                # For SHP/GeoJSON, copy the single layer
                layer_name = os.path.splitext(file)[0]
                out_ds.CopyLayer(src_ds.GetLayer(), layer_name)
            
            src_ds = None  # Close input
    
    out_ds = None  # Close output
    print(f"Success! Output: {output_gpkg}")

# Example usage:
merge2gpkg(
    input_folder = r"D:\...\folder",
    output_gpkg = r"D:\...\folder\output.gpkg"
)

### 2) ESRI ArcMap or Pro needed (for FileGDB)
### gpkg to gdb conversion:
import arcpy
import os
 
def gpkg2gdb(input_gpkg, output_gdb):
    """
    Converts all feature classes in a GeoPackage to an ESRI File Geodatabase.
    Args:
        input_gpkg (str): Path to input GeoPackage (.gpkg)
        output_gdb (str): Path to output File Geodatabase (.gdb)
    Returns:
        str: path to the created .gdb
    """
    try:
        # Validate inputs
        if not arcpy.Exists(input_gpkg):
            raise ValueError(f"Input GeoPackage not found: {input_gpkg}")
        if not input_gpkg.endswith('.gpkg'):
            raise ValueError("Input must be a GeoPackage (.gpkg file)")
        # Create output GDB if it doesn't exist
        if not arcpy.Exists(output_gdb):
            arcpy.management.CreateFileGDB(
                out_folder_path=os.path.dirname(output_gdb),
                out_name=os.path.basename(output_gdb)
            )
            print(f"Created new GDB: {output_gdb}")
        # List all feature classes in the GeoPackage
        arcpy.env.workspace = input_gpkg
        feature_classes = arcpy.ListFeatureClasses()
        if not feature_classes:
            print("Warning: No feature classes found in the GeoPackage")
            return output_gdb
        # Convert each feature class
        for fc in feature_classes:
            out_name = os.path.splitext(fc)[0]  # Remove any file extensions
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=fc,
                out_path=output_gdb,
                out_name=out_name
            )
            print(f"Converted: {fc} -> {os.path.join(output_gdb, out_name)}")
        return output_gdb
    except arcpy.ExecuteError:
        print("ArcPy Error:", arcpy.GetMessages(2))
        raise
    except Exception as e:
        print("Error:", str(e))
        raise
 
# Example usage, input is the output.gpkg:
gpkg2gdb(
    input_gpkg = r"D:\...\folder\output.gpkg",
    output_gdb = r"D:\...\folder\output.gdb"
)
