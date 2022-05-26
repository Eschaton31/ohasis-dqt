##  PrEP Linkage Controller ----------------------------------------------------

# define datasets
if (!exists("nhsss"))
   nhsss <- list()

if (!("prep" %in% names(nhsss)))
   nhsss$prep <- new.env()

nhsss$prep$wd <- file.path(getwd(), "src", "official", "prep")

##  Begin linkage of art registry ----------------------------------------------

source(file.path(nhsss$prep$wd, "01_load_reqs.R"))
source(file.path(nhsss$prep$wd, "02_load_visits.R"))
source(file.path(nhsss$prep$wd, "03_data_reg.initial.R"))
source(file.path(nhsss$prep$wd, "05_data_reg.convert.R"))
source(file.path(nhsss$prep$wd, "06_data_reg.final.R"))

##  PII Deduplication ----------------------------------------------------------

# check if deduplications are to be run
dedup <- input(
   prompt  = "Do you want to run the deduplication?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (dedup == "1") {
   source(file.path(nhsss$prep$wd, "07_dedup_new.R"))
   source(file.path(nhsss$prep$wd, "08_dedup_old.R"))
   source(file.path(nhsss$prep$wd, "09_dedup_dx.R"))
}
rm(dedup)

##  Begin linkage of outcomes dataset ------------------------------------------

source(file.path(nhsss$prep$wd, "11_data_outcome.initial.R"))
source(file.path(nhsss$prep$wd, "12_data_outcome.convert.R"))
source(file.path(nhsss$prep$wd, "13_data_outcome.final.R"))

##  Finalize dataset -----------------------------------------------------------

# check if final dataset is to be completed
complete <- input(
   prompt  = "Do you want to finalize the dataset?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (complete == "1") {
   source(file.path(nhsss$prep$wd, "14_output.R"))

   # TODO: Place these after pdf & ml conso
   source(file.path(nhsss$prep$wd, "15_archive.R"))
   source(file.path(nhsss$prep$wd, "16_upload.R"))
}
rm(complete)