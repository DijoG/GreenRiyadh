# ----- Multi-pages PDF merger -----

#' Merge PDFs from multiple page-numbered directories into single PDFs
#'
#' @param path2dirs Character string. Path to parent directory containing 
#'                  numbered subdirectories (001, 002, 003, etc.)
#' @param outdir Character string. Path to output directory
#' @param pattern Character string. File pattern to match (default: ".pdf")
#' @param dir_prefix Character string. Prefix for directory names (default: "")
#' @param zero_pad Logical. Should directory numbers be zero-padded? (default: TRUE)
#' @param sort_dirs Logical. Sort directories numerically? (default: TRUE)
#'
#' @return Invisible list with merge results
#' @export
#'
#' @examples
#' \dontrun{
#' # If directories are named: 001, 002, 003, etc.
#' merge2one("path/to/parent", "path/to/output")
#' 
#' # If directories are named: page1, page2, page3, etc.
#' merge2one("path/to/parent", "path/to/output", dir_prefix = "page")
#' 
#' # If directories are not zero-padded: 1, 2, 3, etc.
#' merge2one("path/to/parent", "path/to/output", zero_pad = FALSE)
#' }
PDF_merge2one <- function(path2dirs, outdir, 
                      pattern = "\\.pdf$",
                      dir_prefix = "",
                      zero_pad = TRUE,
                      sort_dirs = TRUE) {
  
  # Load required packages
  if (!requireNamespace("pdftools", quietly = TRUE)) {
    stop("Please install package 'pdftools': install.packages('pdftools')")
  }
  
  if (!requireNamespace("stringr", quietly = TRUE)) {
    stop("Please install package 'stringr': install.packages('stringr')")
  }
  
  # Check if parent directory exists
  if (!dir.exists(path2dirs)) {
    stop("Parent directory not found: ", path2dirs)
  }
  
  # Create output directory if it doesn't exist
  if (!dir.exists(outdir)) {
    dir.create(outdir, recursive = TRUE)
    message("Created output directory: ", outdir)
  }
  
  # Get all subdirectories
  all_dirs = list.dirs(path2dirs, full.names = TRUE, recursive = FALSE)
  
  if (length(all_dirs) == 0) {
    stop("No subdirectories found in: ", path2dirs)
  }
  
  # Filter for numbered directories
  dir_names = basename(all_dirs)
  
  if (dir_prefix != "") {
    # Remove prefix if specified
    numbers = stringr::str_remove(dir_names, paste0("^", dir_prefix))
  } else {
    numbers = dir_names
  }
  
  # Check if directories are numeric
  if (zero_pad) {
    # For zero-padded numbers (001, 002, etc.)
    numeric_dirs = all_dirs[grepl("^\\d+$", numbers)]
    dir_numbers = as.numeric(numbers[grepl("^\\d+$", numbers)])
  } else {
    # For non-zero-padded numbers (1, 2, etc.)
    numeric_dirs = all_dirs[grepl("^\\d+$", numbers)]
    dir_numbers = as.numeric(numbers[grepl("^\\d+$", numbers)])
  }
  
  if (length(numeric_dirs) == 0) {
    stop("No numbered directories found. Check dir_prefix and zero_pad parameters.")
  }
  
  message("Found ", length(numeric_dirs), " numbered directories")
  
  # Sort directories numerically if requested
  if (sort_dirs) {
    numeric_dirs = numeric_dirs[order(dir_numbers)]
    dir_numbers = sort(dir_numbers)
  }
  
  # Get all PDF files from each directory
  pdf_lists = lapply(numeric_dirs, function(dir) {
    pdf_files = list.files(dir, pattern = pattern, full.names = TRUE)
    if (length(pdf_files) == 0) {
      warning("No PDF files found in: ", dir)
    }
    pdf_files
  })
  
  # Check that all directories have the same number of files
  file_counts = sapply(pdf_lists, length)
  if (length(unique(file_counts)) > 1) {
    warning("Directories have different numbers of PDF files: ", 
            paste(file_counts, collapse = ", "))
  }
  
  # Get expected number of output files (minimum count across directories)
  n_output = min(file_counts)
  message("Will create ", n_output, " merged PDF files")
  
  # Extract basenames for matching (from first directory)
  if (n_output > 0) {
    base_names = tools::file_path_sans_ext(basename(pdf_lists[[1]]))
  } else {
    stop("No PDF files found to merge")
  }
  
  # Initialize results tracking
  results = list(
    merged = character(0),
    skipped = character(0),
    errors = character(0)
  )
  
  # Create a matrix of file paths for easier processing
  file_matrix = matrix(NA, nrow = n_output, ncol = length(pdf_lists))
  colnames(file_matrix) = paste0("Page_", sprintf("%03d", dir_numbers))
  rownames(file_matrix) = base_names[1:n_output]
  
  # Fill the matrix
  for (i in seq_along(pdf_lists)) {
    if (length(pdf_lists[[i]]) >= n_output) {
      current_names = tools::file_path_sans_ext(basename(pdf_lists[[i]]))
      
      # Match files by name
      for (j in 1:n_output) {
        match_idx = which(current_names == base_names[j])
        if (length(match_idx) > 0) {
          file_matrix[j, i] = pdf_lists[[i]][match_idx[1]]
        }
      }
    }
  }
  
  # Merge files for each base name
  message("\nMerging PDFs...")
  pb = txtProgressBar(min = 0, max = n_output, style = 3)
  
  for (i in 1:n_output) {
    setTxtProgressBar(pb, i)
    
    name = base_names[i]
    file_paths = file_matrix[i, ]
    
    # Check for any NA values (missing files)
    if (any(is.na(file_paths))) {
      missing_pages = which(is.na(file_paths))
      results$skipped = c(results$skipped, name)
      warning("Skipping '", name, "': missing from pages ", 
              paste(missing_pages, collapse = ", "))
      next
    }
    
    tryCatch({
      # Output file path
      output_file = file.path(outdir, paste0(name, ".pdf"))
      
      # Combine PDFs in order (001, 002, 003, etc.)
      pdftools::pdf_combine(as.character(file_paths), output = output_file)
      
      results$merged = c(results$merged, name)
    }, error = function(e) {
      results$errors = c(results$errors, name)
      message("\nError merging '", name, "': ", e$message)
    })
  }
  
  close(pb)
  
  # Summary
  message("\n=== Merge Summary ===")
  message("Successfully merged: ", length(results$merged), " / ", n_output)
  message("Skipped: ", length(results$skipped))
  message("Errors: ", length(results$errors))
  
  if (length(results$skipped) > 0) {
    message("\nSkipped files:")
    for (name in results$skipped) {
      missing = which(is.na(file_matrix[name, ]))
      message("  ", name, " (missing pages: ", 
              paste(missing, collapse = ", "), ")")
    }
  }
  
  if (length(results$errors) > 0) {
    message("\nFiles with errors:")
    cat(paste("  ", results$errors, collapse = "\n"), "\n")
  }
  
  # Return invisible results
  invisible(list(
    merged = results$merged,
    skipped = results$skipped,
    errors = results$errors,
    file_matrix = file_matrix,
    directories = numeric_dirs,
    dir_numbers = dir_numbers,
    summary = list(
      total_attempted = n_output,
      successfully_merged = length(results$merged),
      failed = length(results$skipped) + length(results$errors),
      directories_used = length(numeric_dirs)
    )
  ))
}

# For flexible directory matching ()
PDF_merge2one_flex <- function(path2dirs, outdir, 
                           pattern = "\\.pdf$",
                           dir_pattern = "^\\d+$",  # Regex for directory names
                           sort_by = c("numeric", "name", "none")) {
  
  sort_by = match.arg(sort_by)
  
  # Load required packages
  if (!requireNamespace("pdftools", quietly = TRUE)) {
    stop("Please install package 'pdftools': install.packages('pdftools')")
  }
  
  if (!requireNamespace("stringr", quietly = TRUE)) {
    stop("Please install package 'stringr': install.packages('stringr')")
  }
  
  # Get all subdirectories matching pattern
  all_items = list.files(path2dirs, full.names = TRUE)
  all_dirs = all_items[file.info(all_items)$isdir]
  
  if (length(all_dirs) == 0) {
    stop("No subdirectories found in: ", path2dirs)
  }
  
  # Filter by directory name pattern
  dir_names = basename(all_dirs)
  matching_idx = grepl(dir_pattern, dir_names)
  numeric_dirs = all_dirs[matching_idx]
  dir_names = dir_names[matching_idx]
  
  if (length(numeric_dirs) == 0) {
    stop("No directories matching pattern '", dir_pattern, "' found.")
  }
  
  # Sort directories
  if (sort_by == "numeric") {
    # Try to extract numeric part for sorting
    nums = as.numeric(stringr::str_extract(dir_names, "\\d+"))
    if (all(!is.na(nums))) {
      numeric_dirs = numeric_dirs[order(nums)]
      dir_names = dir_names[order(nums)]
    } else {
      numeric_dirs = numeric_dirs[order(dir_names)]
      dir_names = sort(dir_names)
    }
  } else if (sort_by == "name") {
    numeric_dirs = numeric_dirs[order(dir_names)]
    dir_names = sort(dir_names)
  }
  # else "none" - keep as is
  
  message("Found ", length(numeric_dirs), " matching directories")
  message("Directories (in order): ", paste(dir_names, collapse = ", "))
  
  # Call the main function
  merge2one(path2dirs, outdir, pattern = pattern, 
            dir_prefix = "", zero_pad = FALSE, sort_dirs = FALSE)
}

## ----- EXAMPLE USAGE -----
renamefrom <- list.files("D:/TMO_02/MergePDF/data/001", pattern = "\\.pdf$", full.names = T)
renameto <- stringr::str_remove(renamefrom, "f_P_")
file.rename(renamefrom, renameto)

# Basic usage - directories named 001, 002, 003, etc. (YOU NEED THIS!)
result <- PDF_merge2one(
  path2dirs = "D:/TMO_02/MergePDF/data",
  outdir = "D:/TMO_02/MergePDF/output"
)

# If directories are not zero-padded (1, 2, 3, etc.)
result <- PDF_merge2one(
  path2dirs = "D:/TMO_02/MergePDF/data",
  outdir = "D:/TMO_02/MergePDF/output",
  zero_pad = FALSE
)

# If directories have prefix (page001, page002, etc.)
result <- PDF_merge2one(
  path2dirs = "D:/TMO_02/MergePDF/data",
  outdir = "D:/TMO_02/MergePDF/output",
  dir_prefix = "page"
)

# If directories are: page_1, page_2, page_3
result <- PDF_merge2one_flex(
  path2dirs = "D:/TMO_02/MergePDF/data",
  outdir = "D:/TMO_02/MergePDF/output",
  dir_pattern = "^page_\\d+$",
  sort_by = "numeric"
)

# If directories are: section1, section2, section3
result <- PDF_merge2one_flex(
  path2dirs = "D:/TMO_02/MergePDF/data",
  outdir = "D:/TMO_02/MergePDF/output",
  dir_pattern = "^section\\d+$",
  sort_by = "numeric"

)
