###------------------------------------------------------------
###------------------------------------------------------------
### REAL ARABIC character removal
###------------------------------------------------------------
###------------------------------------------------------------

require(tidyverse)

### 01 Function to check if a column contains Arabic characters:
contains_arabic <- function(column) {
  any(str_detect(column, "\\p{Arabic}"), na.rm = TRUE)
}

### 02 The beauty in it:
BEAUTIFY <- function(var) {
  
  # first step
  first =
    var %>%
    str_remove_all("\\p{Arabic}|[:symbol:]|[:punct:]") 
  
  # second step
  removables = c(str_extract_all(first, "[^\\p{Latin}]") %>%
                    unlist() %>%
                    unique(), 
                  str_extract_all(first, "[\\p{Latin}&&[^A-Za-z]]") %>%
                    unlist() %>%
                    unique())
  
  if (" " %in% removables) {
    removables = removables[-which(removables == " ")]
  }
  if (any(is.na(removables))) {
    removables = removables[-which(is.na(removables))]
  }
  
  # last step
  if (length(removables) > 0) {
    final =
      first %>%
      str_remove_all(str_c(removables, collapse = "|"))
  } else {
    final = first
  }
  
  # output
  return(final)
}

### 03 MAIN function:
#'@import package tidyverse, readxl and writexl
#'@param indirdata path to xlsx up to directory (NO FILE NAME and NO EXTENSION)
#'@export updated/cleaned xlsx stored in the indirdata directory with '_clean' tag
BEAUTIFILL <- function(indirdata) {
  
  fils = list.files(indirdata, pattern =".xlsx", full.names = T)
  fils = fils[which.min(nchar(fils))]
  outname = str_c(str_remove(str_split_i(fils, "/", -1), ".xlsx"), "_clean.xlsx")
  
  CP = 
    readxl::read_xlsx(fils) 
  
  arabic_columns = 
    CP %>%
    select(where(~ contains_arabic(.))) %>%
    names()
  
  CP = 
    CP %>%
    mutate(across(all_of(arabic_columns), BEAUTIFY))
  
  writexl::write_xlsx(CP, str_c(indirdata, "/", outname))
}

