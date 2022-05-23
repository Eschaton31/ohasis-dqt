##  HARP Mortality Linkage Controller ------------------------------------------

# update warehouse - Form D / Form BC
ohasis$data_factory("warehouse", "form_d", "upsert", TRUE)

# update warehouse - OHASIS IDs
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE)

# define datasets
if (!exists("nhsss"))
   nhsss <- list()

##  Google Drive Endpoint ------------------------------------------------------

path         <- list()
path$primary <- "~/DQT/Data Factory/HARP Dead/"
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
nhsss$harp_dead$gdrive$path <- path
rm(path, drive_folders)

##  Begin linkage of datasets --------------------------------------------------

source("src/official/harp_dead/01_load_corrections.R")
source("src/official/harp_dead/02_load_harp.R")
source("src/official/harp_dead/03_data_initial.R")
source("src/official/harp_dead/04_data_convert.R")

##  PII Deduplication ----------------------------------------------------------

# check if deduplications are to be run
dedup <- input(
   prompt  = "Do you want to run the deduplication?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (dedup == "1") {
   source("src/official/harp_dead/05_dedup_new.R")
   source("src/official/harp_dead/06_dedup_old.R")

   # special daduplication
   # NOTE: run data_final first
   source("src/official/harp_dead/07_dedup_dx.R")
}
rm(dedup)

##  Finalize dataset -----------------------------------------------------------

# check if final dataset is to be completed
complete <- input(
   prompt  = "Do you want to finalize the dataset?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (complete == "1") {
   source("src/official/harp_dead/08_data_final.R")
   source("src/official/harp_dead/09_output.R")

   # TODO: Place these after pdf & ml conso
   source("src/official/harp_dead/10_archive.R")
   source("src/official/harp_dead/11_upload.R")
}
rm(complete)
