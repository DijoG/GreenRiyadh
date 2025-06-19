#####
# 1)
#####

require(dplyr)

##> MET
MET <- sf::st_read(".../0_1_Riyadh Boundaries/02032025_Riyadh_METROPOLITAN.geojson") %>%
  sf::st_union() %>%
  sf::st_cast("POLYGON") %>%
  sf::st_sf() %>%
  dplyr::mutate(ID = 1)

##> DEV
DEV <- sf::st_read(".../0_General/0_1_Riyadh Boundaries/02032025_Riyadh_DEVELOPED.geojson") %>%
  sf::st_transform(sf::st_crs(MET)) %>%
  sf::st_union() %>%
  sf::st_cast("MULTIPOLYGON") %>%
  sf::st_sf() %>%
  dplyr::mutate(ID = 1)

##> URBAN
URB <- sf::st_read(".../0_General/0_1_Riyadh Boundaries/02032025_Riyadh_URBAN.geojson") %>%
  sf::st_transform(sf::st_crs(MET)) %>%
  sf::st_union() %>%
  sf::st_cast("MULTIPOLYGON") %>%
  sf::st_sf() %>%
  dplyr::mutate(ID = 1)

##> CC
CC2022 <- terra::rast(".../06_GIS-Data/09_LCC/LCC_2022/LCC_2022_4_1to1_CC.tif")

##> VC
VC2022 <- terra::rast(".../06_GIS-Data/09_LCC/LCC_2022/LCC_2022_4_2_1to1_VC.tif")

require(sf);require(terra);require(tidyverse)

##>>>>>>>>>>>
split_POLY_m <- function(poly, n_areas) {
  # Sample points within the polygon and prepare them for K-means clustering
  points = sf::st_sample(poly, size = n_areas*3)
  
  # Convert points to a tibble with "x" and "y" columns
  points = 
    do.call(rbind, sf::st_geometry(points)) %>%
    as_tibble() %>% 
    setNames(c("x", "y"))
  
  # Apply K-means clustering to divide points into the specified number of areas
  k_means = kmeans(points, centers = n_areas)
  
  # Create Voronoi polygons based on K-means cluster centers
  voronoi_polys = dismo::voronoi(k_means$centers, ext = poly)
  
  # Ensure the coordinate reference system matches the input polygon
  crs(voronoi_polys) = crs(poly)
  
  # Convert Voronoi polygons to sf format and intersect with the original polygon
  voronoi_sf = sf::st_as_sf(voronoi_polys)
  splitted_poly = sf::st_intersection(voronoi_sf, poly) 
  splitted_poly = splitted_poly %>%
    mutate(AREA = as.numeric(sf::st_area(.))) %>%
    dplyr::select(id, AREA)
  
  return(splitted_poly)
}
##>>>>>>>>>>>
step_by_step_m <- function(poly, LCCrast, output_dir, n_areas) {
  
  # Ensure 'poly' is an sf object
  if (!inherits(poly, "sf")) {
    stop("The 'poly' object must be an sf object.")
  }
  
  # Ensure 'LCCrast' is a raster object
  if (!inherits(LCCrast, "SpatRaster")) {
    stop("The 'LCCrast' object must be a SpatRaster.")
  }
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    cat("Created output directory:", output_dir, "\n")
  }
  
  polyin = split_POLY_m(poly, n_areas) 
  
  # Extract raster values for the current polygon
  for (p in 1:nrow(polyin)) {
    cat("_____ Computing polygon ", polyin$id[p], " _____\n")
    exactextractr::exact_extract(LCCrast, polyin[p,]) %>%
      map_df(~ .x) %>%
      group_by(value) %>%
      summarise(N = n()) %>%
      tidyr::drop_na() %>%
      mutate(Area = round(.35*.35*N, 2),
             ID = polyin$id[p]) %>%
      dplyr::select(value, ID, Area) %>%
      readr::write_csv(., file.path(output_dir, str_c("Row_00", p, ".csv")))
    }
}
##>>>>>>>>>>>>>>>>
step_by_step_fast <- function(poly, LCCrast, output_dir, n_areas) {
  # Pre-checks (unchanged)
  if (!inherits(poly, "sf")) stop("The 'poly' object must be an sf object.")
  if (!inherits(LCCrast, "SpatRaster")) stop("The 'LCCrast' object must be a SpatRaster.")
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
  # 1. Pre-calculate constants
  pixel_area = 0.35 * 0.35  
  
  # 2. Split polygons upfront
  polyin = split_POLY_m(poly, n_areas)
  
  # 3. Vectorized file naming
  file_names = sprintf("Row_%04d.csv", 1:nrow(polyin))  # Padded zeros
  
  # 4. Process polygons in optimized loop
  for (p in seq_len(nrow(polyin))) {
    cat("_____ Processing polygon", polyin$id[p], "_____\n")  # Simpler progress
    
    # Single exact_extract call per polygon
    exactextractr::exact_extract(LCCrast, polyin[p, ], include_cols = "id") %>%
      bind_rows() %>%
      group_by(value) %>%
      summarise(
        N = n(),
        Area = round(pixel_area * N, 2),
        ID = first(id)  # More efficient than mutate
      ) %>%
      drop_na() %>%
      select(value, ID, Area) %>%
      write_csv(file.path(output_dir, file_names[p]))
  }

}

# Run 
tictoc::tic()
step_by_step_fast(
  DEV,
  VC2022,
  output_dir = ".../WVLCC_CC_VC/MET_VC",
  500
)
tictoc::toc()

#####
# 2)
#####

update_poly <- function(polyPATHinFILE, polyPATHout, castTO, csvPATH, EPSG) {
  
  message("Reading polygon...")
  P = 
    sf::st_read(polyPATHinFILE) %>%
    sf::st_transform(EPSG) %>%
    sf::st_union() %>%
    sf::st_cast(castTO) %>%
    sf::st_sf() %>%
    dplyr::mutate(ID = 1)
  
  Dl =
    list.files(csvPATH, pattern = ".csv", full.names = T)
  
  require(readr)
  require(purrr)
  
  message("Getting vegetation data...")
  D = 
    map_df(Dl, ~ read_csv(.x, show_col_types = F)) %>%
    mutate(IDD = 1) %>%
    group_by(IDD) %>%
    summarise(VAREA = sum(Area)) %>%
    ungroup()
  
  message("Updating polygon...")
  Ratio = round(D$VAREA / as.numeric(sum(sf::st_area(P))) * 100, 2)
  
  Pout = 
    P %>%
    mutate(Ratio = Ratio)
  
  message("Writing updated polygon...")
  
  file_basename = tools::file_path_sans_ext(basename(polyPATHinFILE))
  output_filename = paste0(file_basename, "_R.geojson")
  full_output_path = file.path(polyPATHout, output_filename)
  
  sf::write_sf(Pout, full_output_path)
  message(paste0("Updated polygon written to: ", full_output_path))
}

# Test
update_poly(".../0_General/0_1_Riyadh Boundaries/URBAN.geojson",
            ".../5_KPI Mapping/5_1_Vegetation Cover",
            "MULTIPOLYGON",
            ".../WVLCC_CC_VC/URB_VC",
            32638)
