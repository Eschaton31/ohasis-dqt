##  GF Logsheet Controller -----------------------------------------------------

# update warehouse - lab data
ohasis$data_factory("lake", "lab_wide", "upsert", TRUE)

# update warehouse - dispensed data
ohasis$data_factory("lake", "disp_meds", "upsert", TRUE)

# update warehouse - Form ART / BC
ohasis$data_factory("warehouse", "form_art_bc", "upsert", TRUE)

# update warehouse - PrEP
ohasis$data_factory("warehouse", "form_prep", "upsert", TRUE)

# update warehouse - Form A
ohasis$data_factory("warehouse", "form_a", "upsert", TRUE)

# update warehouse - HTS Form
ohasis$data_factory("warehouse", "form_hts", "upsert", TRUE)

# update warehouse - HTS Form
ohasis$data_factory("warehouse", "form_cfbs", "upsert", TRUE)

# update warehouse - OHASIS IDs
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE)

# update earlies / latest art vistis
source("src/official/harp_tx/03_load_visits.R")

# define datasets
if (!exists('gf'))
   gf <- list()

##  Google Drive Endpoint ------------------------------------------------------

path         <- list()
path$primary <- "~/DQT/Data Factory/DSA - GF/"
path$report  <- paste0(path$primary, ohasis$ym, "/")

# create folders if not exists
drive_folders <- list(
   c(path$primary, ohasis$ym),
   c(path$report, "Cleaning"),
   c(path$report, "Validation")
)
invisible(
   lapply(drive_folders, function(folder) {
      parent <- folder[1] # parent dir
      path   <- folder[2] # name of dir to be checked

      # get sub-folders
      dribble <- drive_ls(parent)

      # create folder if not exists
      if (nrow(dribble %>% filter(name == path)) == 0)
         drive_mkdir(paste0(parent, path))
   })
)

# get list of files in dir
gf$logsheet$gdrive$path <- path
rm(path, drive_folders)

##  Begin linkage of datasets --------------------------------------------------

# list of sites
gf$sites <- read_sheet("1y0i8l-HNieOIQ1QGIezVLltwut3y1FxHryvhno9GNb4") %>%
   distinct(FACI_ID, .keep_all = TRUE) %>%
   rename_at(
      .vars = vars(starts_with("DSA")),
      ~"DSA"
   ) %>%
   filter(DSA == TRUE) %>%
   mutate(WITH_DSA = 1)

source("src/official/dsa_gf/01_load_harp.R")
source("src/official/dsa_gf/02_data_logsheet.initial.R")