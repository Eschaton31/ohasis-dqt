##  HARP Tx Linkage Controller -------------------------------------------------

# define datasets
if (!exists("nhsss"))
   nhsss <- list()

if (!("harp_dead" %in% names(nhsss)))
   nhsss$harp_dead <- new.env()

nhsss$harp_dead$wd <- file.path(getwd(), "src", "official", "harp_dead")

##  Begin linkage of datasets --------------------------------------------------

source(file.path(nhsss$harp_dead$wd, "01_load_reqs.R"))
source(file.path(nhsss$harp_dead$wd, "02_data_initial.R"))
source(file.path(nhsss$harp_dead$wd, "03_data_convert.R"))

##  PII Deduplication ----------------------------------------------------------

# check if deduplications are to be run
dedup <- input(
   prompt  = "Do you want to run the deduplication?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (dedup == "1") {
   source(file.path(nhsss$harp_dead$wd, "x1_dedup_new.R"))
   source(file.path(nhsss$harp_dead$wd, "x2_dedup_old.R"))

   # special daduplication
   # NOTE: run data_final first
   source(file.path(nhsss$harp_dead$wd, "x3_dedup_dx.R"))
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
   source(file.path(nhsss$harp_dead$wd, "04_data_final.R"))
   source(file.path(nhsss$harp_dead$wd, "05_output.R"))

   # TODO: Place these after pdf & ml conso
   source(file.path(nhsss$harp_dead$wd, "06_archive.R"))
   source(file.path(nhsss$harp_dead$wd, "07_upload.R"))
}
rm(complete)
