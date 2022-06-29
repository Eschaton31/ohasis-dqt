##  HARP Tx Linkage Controller -------------------------------------------------

# define datasets
if (!exists("nhsss"))
   nhsss <- list()

if (!("harp_tx" %in% names(nhsss)))
   nhsss$harp_tx <- new.env()

nhsss$harp_tx$wd <- file.path(getwd(), "src", "official", "harp_tx")

##  Begin linkage of art registry ----------------------------------------------

source(file.path(nhsss$harp_tx$wd, "02_load_reqs.R"))
source(file.path(nhsss$harp_tx$wd, "04_data_reg.initial.R"))
source(file.path(nhsss$harp_tx$wd, "05_data_reg.convert.R"))
source(file.path(nhsss$harp_tx$wd, "06_data_reg.final.R"))

##  PII Deduplication ----------------------------------------------------------

# check if deduplications are to be run
dedup <- input(
   prompt  = "Do you want to run the deduplication?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (dedup == "1") {
   source(file.path(nhsss$harp_tx$wd, "07_dedup_new.R"))
   source(file.path(nhsss$harp_tx$wd, "08_dedup_old.R"))
   source(file.path(nhsss$harp_tx$wd, "09_dedup_dx.R"))
}
rm(dedup)

##  Begin linkage of outcomes dataset ------------------------------------------

source(file.path(nhsss$harp_tx$wd, "11_data_outcome.initial.R"))
source(file.path(nhsss$harp_tx$wd, "12_data_outcome.convert.R"))
source(file.path(nhsss$harp_tx$wd, "13_data_outcome.final.R"))

##  Finalize dataset -----------------------------------------------------------

# check if final dataset is to be completed
complete <- input(
   prompt  = "Do you want to finalize the dataset?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (complete == "1") {
   source(file.path(nhsss$harp_tx$wd, "14_output.R"))

   # TODO: Place these after pdf & ml conso
   source(file.path(nhsss$harp_tx$wd, "15_archive.R"))
   source(file.path(nhsss$harp_tx$wd, "16_upload.R"))
}
rm(complete)

dedup_by(
   "16B0tEmFxG1Y0kf1y8qhMkjTuOC6CnarkX0fAtZjivuM",
   "reclink",
   23,
   26,
   1,
   1418
)