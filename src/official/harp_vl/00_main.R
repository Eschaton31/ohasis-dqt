##  HARP Registry Linkage Controller -------------------------------------------

# update warehouse - lab data
ohasis$data_factory("lake", "lab_wide", "upsert", TRUE)

# update warehouse - dispensed data
ohasis$data_factory("lake", "disp_meds", "upsert", TRUE)

# update warehouse - Form ART / BC
ohasis$data_factory("warehouse", "form_art_bc", "upsert", TRUE)

# update warehouse - OHASIS IDs
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE)

# define datasets
if (!exists('nhsss'))
   nhsss <- list()

##  Google Drive Endpoint ------------------------------------------------------

path         <- list()
path$primary <- "~/DQT/Data Factory/HARP VL/"
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
nhsss$harp_vl$gdrive$path <- path
rm(path, drive_folders)

##  Begin linkage of datasets --------------------------------------------------

nhsss$harp_vl$corr <- load_corr(nhsss$harp_vl$gdrive$path, ohasis$ym)

# optional to reload/process ml dataset
update <- input(
   prompt  = "Would you like to re-process a quarterly dataset for VL masterlists?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (update == "1") {
   source("src/official/harp_vl/01_load_ml.R")
}
rm(update)

# processing proper
source("src/official/harp_vl/02_data_initial.R")