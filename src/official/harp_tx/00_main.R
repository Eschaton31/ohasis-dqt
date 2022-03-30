##  HARP Tx Registry Linkage Controller ----------------------------------------

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
path$primary <- "~/DQT/Data Factory/HARP Tx/"
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
nhsss$harp_tx$gdrive$path <- path
rm(path, drive_folders)

##  Begin linkage of art registry ----------------------------------------------

source("src/official/harp_tx/01_load_corrections.R")
source("src/official/harp_tx/02_load_harp.R")
source("src/official/harp_tx/03_load_visits.R")
source("src/official/harp_tx/04_data_reg.initial.R")
source("src/official/harp_tx/05_data_reg.convert.R")
source("src/official/harp_tx/06_data_reg.final.R")

##  PII Deduplication ----------------------------------------------------------

# check if deduplications are to be run
dedup <- input(
   prompt  = "Do you want to run the deduplication?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (dedup == "1") {
   source("src/official/harp_tx/07_dedup_new.R")
   source("src/official/harp_tx/08_dedup_old.R")
   source("src/official/harp_tx/09_dedup_dx.R")
}
rm(dedup)

##  Begin linkage of outcomes dataset ------------------------------------------

source("src/official/harp_tx/11_data_outcome.initial.R")
source("src/official/harp_tx/12_data_outcome.convert.R")
source("src/official/harp_tx/13_data_outcome.final.R")


##  Finalize dataset -----------------------------------------------------------

# check if final dataset is to be completed
complete <- input(
   prompt  = "Do you want to finalize the dataset?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (complete == "1") {
   source("src/official/harp_tx/14_output.R")

   # TODO: Place these after pdf & ml conso
   source("src/official/harp_tx/15_archive.R")
   source("src/official/harp_tx/16_upload.R")
}
rm(complete)
