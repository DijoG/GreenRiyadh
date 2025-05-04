import os
import tempfile
import geopandas as gpd
import pandas as pd
from warnings import warn

def align_attr_files(path_one, path_to, write_to=None, convert_polygons_to_points=True):
    """
    Aligns and merges two spatial datasets, with optional polygon-to-point conversion.
    
    Args:
        path_one: Path to first spatial file
        path_to: Path to second spatial file (structure will be matched to this)
        write_to: Output path (None to return without writing)
        convert_polygons_to_points: Whether to convert polygons to centroids when mixing with points
    
    Returns:
        GeoDataFrame with merged data
    """
    # Read input files
    try:
        gdf_one = gpd.read_file(path_one)
        gdf_to = gpd.read_file(path_to)
    except Exception as e:
        raise Exception(f"Failed to read input files. Check paths and file formats: {str(e)}")
    
    # Get geometry types
    geom_type_one = gdf_one.geometry.geom_type.unique()
    geom_type_to = gdf_to.geometry.geom_type.unique()
    
    # Handle geometry type conversion if requested
    if (convert_polygons_to_points and 
        len(geom_type_one) == 1 and len(geom_type_to) == 1 and
        ((geom_type_one[0] == 'Polygon' and geom_type_to[0] == 'Point') or 
         (geom_type_one[0] == 'Point' and geom_type_to[0] == 'Polygon'))):
        
        print("Converting polygon features to points (centroids)...")
        
        if geom_type_one[0] == 'Polygon':
            gdf_one.geometry = gdf_one.geometry.centroid
            geom_type_one = ['Point']
        else:
            gdf_to.geometry = gdf_to.geometry.centroid
            geom_type_to = ['Point']
    
    # Verify geometry types are identical after conversion
    if not (geom_type_one == geom_type_to):
        raise Exception(
            f"Geometry type mismatch:\n"
            f"File ONE contains: {geom_type_one}\n"
            f"File TO contains: {geom_type_to}\n"
            "Shapefiles require consistent geometry types."
        )
    
    # Check and match CRS
    if gdf_one.crs != gdf_to.crs:
        print("Transforming CRS of 'ONE' to match 'TO'...")
        gdf_one = gdf_one.to_crs(gdf_to.crs)
    
    # Add missing columns to ONE (fill with NA)
    missing_cols = set(gdf_to.columns) - set(gdf_one.columns)
    if missing_cols:
        print(f"Adding missing columns to 'ONE': {', '.join(missing_cols)}")
        for col in missing_cols:
            gdf_one[col] = None
    
    # Ensure column order matches TO (geometry column remains last in GeoPandas)
    gdf_one = gdf_one[gdf_to.columns]
    
    # Combine data
    merged = gpd.GeoDataFrame(pd.concat([gdf_one, gdf_to], ignore_index=True), crs=gdf_to.crs)
    
    # Prepare for shapefile output
    if write_to:
        # Create directory if needed
        os.makedirs(os.path.dirname(write_to), exist_ok=True)
        
        # Check for long field names if writing shapefile
        if write_to.lower().endswith('.shp'):
            long_fields = [col for col in merged.columns if len(col) > 10]
            if long_fields:
                warn(
                    f"These field names exceed 10 characters and may cause issues in shapefiles:\n"
                    f"{', '.join(long_fields)}\n"
                    "Consider using GeoPackage (.gpkg) format to preserve full field names.",
                    UserWarning
                )
        
        try:
            # Write using GDAL's OGR through Fiona to better handle field names
            merged.to_file(write_to)
            print(f"Successfully wrote output to: {write_to}")
            
            if write_to.lower().endswith('.shp'):
                print("Note: Shapefile companion files (.dbf, .shx, etc.) were also created.")
                
                # Verify output
                if os.path.exists(write_to):
                    output_info = gpd.read_file(write_to)
                    print("\nOutput field names:")
                    print(list(output_info.columns))
        
        except Exception as e:
            raise Exception(f"Failed to write output file: {str(e)}")
    
    return merged

# Example usage
if __name__ == "__main__":
    result = align_attr_files(
        path_one = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/13_ExceptionalTrees/FromShadeTrees400m2.shp",
        path_to = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/02_ExceptionalTrees/202502/20022025_ExceptionalTrees.geojson",
        write_to = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/13_ExceptionalTrees/MLplusET.shp"
    )