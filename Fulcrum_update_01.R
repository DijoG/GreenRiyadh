##
require(tidyverse)
##

##
CTI <- sf::read_sf("D:/TMO/Fulcrum/10032025_CTI/tmo_combined_tree_inventory.geojson") %>%
  rename(Project = "_project")
CTI %>% names()
length(unique(CTI$`_record_id`))
CTI$Project %>% unique()

"b65e1249-2b9f-42e5-bf6e-1e7449d5cda6" %in% unique(CTI$`_record_id`)
##

## 1) GRP >---------------------------------------
GRP <- sf::read_sf("D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/000_000/D_Structure/0_General/0_2_Green Riyadh Project Boundaries/05112024_GRP_ARABIC.geojson")
names(GRP)
GRP$NAME_ENGLI %>% unique()
##

##
GRP <- sf::st_transform(GRP, crs = sf::st_crs(CTI))
CTI_updated <- sf::st_join(
  sf::st_make_valid(CTI["_record_id"]), 
  sf::st_make_valid(GRP["NAME_ENGLI"]), 
  left = TRUE) %>%
  rename(Project = NAME_ENGLI) %>%
  distinct(`_record_id`, .keep_all = TRUE)  
length(CTI_updated$"_record_id" %>% unique())

# Save the updated dataset if needed
write_csv2(CTI_updated %>% as.data.frame() %>%
    rename(id = "_record_id") %>%
    select(-geometry), "D:/TMO/Fulcrum/10032025_CTI/CTI_updated.csv")

## 2) DQ only >------------------------------------------
DQ <- sf::st_read("D:/TMO/shp/DQ/DQ.shp") %>%
  mutate(Project = "DQ")

#
CTI <- sf::st_transform(CTI, crs = sf::st_crs(DQ))
CTI_updated <- sf::st_join(
  sf::st_make_valid(CTI[c("_record_id", "Project")]), 
  sf::st_make_valid(DQ["Project"]), 
  left = TRUE) %>%
  distinct(`_record_id`, .keep_all = TRUE) %>%
  mutate(Project = ifelse(is.na(Project.x), Project.y, Project.x))

length(CTI_updated$"_record_id" %>% unique())
CTI_updated$Project.x %>% unique() == CTI$Project %>% unique()
table(CTI$Project)
table(CTI_updated$Project.x)
table(CTI_updated$Project.y)
table(CTI_updated$Project) == table(CTI$Project)
CTI_updated$Project.y

# Save the updated dataset if needed
write_csv2(CTI_updated %>% as.data.frame() %>%
             rename(id = "_record_id") %>%
             select(-geometry, -Project.x, -Project.y), "D:/TMO/Fulcrum/10032025_CTI/CTI_DQ_updated.csv")

##
require(tidyverse)
csv <- read_csv2("D:/TMO/Fulcrum/10032025_CTI/CTI_DQ_updated.csv")
table(csv$Project)

################
################
################

##
require(tidyverse)
##

## ML only data as exported from Fulcrum
MLO <- sf::read_sf("D:/TMO/Fulcrum/MLonly.csv")
MLO %>% names()
MLO$`_record_id`

write_csv2(MLO %>% as.data.frame() %>%
             rename(rid = "_record_id") %>%
             select(-geometry), "D:/TMO/Fulcrum/MLonly.csv")


