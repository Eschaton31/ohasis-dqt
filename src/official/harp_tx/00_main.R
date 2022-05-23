##  Generate pre-requisites and endpoints --------------------------------------

# define datasets
if (!exists('nhsss'))
   nhsss <- list()

if (!("harp_tx" %in% names(nhsss)))
   nhsss$harp_tx <- new.env()

local(envir = nhsss$harp_tx, {
   gdrive      <- list()
   gdrive$path <- gdrive_endpoint("HARP Tx", ohasis$ym)
   corr        <- gdrive_correct(gdrive$path, ohasis$ym)

   tables           <- list()
   tables$lake      <- c("lab_wide", "disp_meds")
   tables$warehouse <- c("form_art_bc", "id_registry")
})

# run through all tables
local(envir = nhsss$harp_tx, invisible({
   lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
   lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
}))

##  Begin linkage of art registry ----------------------------------------------

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
