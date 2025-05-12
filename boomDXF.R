#' Convert AutoCAD DXF Files to GIS Formats
#'
#' A robust converter that transforms AutoCAD DXF files into standard GIS formats 
#' (GPKG, Shapefile) using GDAL's powerful backend. Handles coordinate systems, 
#' geometry conversion, and collection exploding automatically.
#'
#' @param input_file Path to input DXF file (required)
#' @param output_path Directory where output files will be saved (required)
#' @param CRS Coordinate Reference System as EPSG code (default: "EPSG:32638" - UTM Zone 38N)
#' @param extension Output format: ".gpkg" (default) or ".shp"
#' @param explode_collections Should multi-part geometries be split? (default: TRUE)
#'
#' @return Path to the created output file, or NULL if conversion failed
#' @export
#'
#' @details
#' Key Features:
#' - Converts DXF entities layer to clean GIS geometries
#' - Maintains coordinate reference system throughout conversion
#' - Handles complex geometry types (POLYHEDRALSURFACE, GEOMETRYCOLLECTION)
#' - Optionally explodes multi-part features into single geometries
#' - Provides feedback about conversion success/failure
#'
#' Technical Notes:
#' - Uses GDAL's ogr2ogr through the gdalUtilities package
#' - For DXF files, always converts to "CONVERT_TO_LINEAR" geometry type
#' - Automatically overwrites existing output files
boomDXF <- function(input_file, 
                    output_path, 
                    CRS = "EPSG:32638", 
                    extension = ".gpkg", 
                    explode_collections = TRUE) {
  
  # Validate inputs
  if (!file.exists(input_file)) stop("Input file not found: ", input_file)
  if (!dir.exists(output_path)) dir.create(output_path, recursive = TRUE)
  
  # Set output format and filename
  output_file = file.path(
    paste0(output_path, "/",
           tools::file_path_sans_ext(basename(input_file)), 
           ifelse(extension == ".shp", ".shp", ".gpkg")
    ))
  
  # GDAL conversion parameters
  params = list(
    src_datasource_name = input_file,
    dst_datasource_name = output_file,
    layer = "entities",
    nlt = "CONVERT_TO_LINEAR",  # Convert to linear geometries
    s_srs = CRS,                # Source CRS
    t_srs = CRS,                # Target CRS (same as source)
    overwrite = TRUE,           # Overwrite existing files
    quiet = TRUE                # Suppress GDAL output
  )
  
  if (explode_collections) params$explodecollections <- TRUE
  
  # Execute conversion
  tryCatch({
    do.call(gdalUtilities::ogr2ogr, params)
    message("Successfully created: ", output_file)
    return(output_file)
  }, error = function(e) {
    message("Conversion failed: ", e$message)
    return(NULL)
  })
}

# Example usage:
require(gdalUtilities)

# 1) Single file conversion to GeoPackage
boomDXF("D:/.../input.dxf", "D:/output_folder")

# 2) Batch convert all DXF files in a directory to Geopackages (highly recommended!) or Shapefiles (not recommended)
FILS <- list.files("D:/.../cad_data", pattern = ".dxf$", full.names = TRUE)
purrr::map(FILS, ~boomDXF(.x, "D:/.../cad_data/output_folder", extension = ".gpkg"))  
# or:
lapply(files, function(x) boomDXF(x, "D:/.../cad_data/output_folder", extension = ".gpkg"))
