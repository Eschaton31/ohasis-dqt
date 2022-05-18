##  HARP Registry Linkage Controller -------------------------------------------

# update warehouse - Form A
ohasis$data_factory("warehouse", "form_a", "upsert", TRUE)

# update warehouse - HTS Form
ohasis$data_factory("warehouse", "form_hts", "upsert", TRUE)

# update warehouse - OHASIS IDs
ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE)

# define datasets
if (!exists('nhsss'))
   nhsss <- list()

##  Generate pre-requisites and endpoints --------------------------------------

nhsss$harp_dx$gdrive$path <- gdrive_endpoint("HARP Dx", ohasis$ym)
nhsss$harp_vl$corr        <- gdrive_correct(nhsss$harp_vl$gdrive$path, ohasis$ym)

##  Begin linkage of datasets --------------------------------------------------

source("src/official/harp_dx/02_load_harp.R")
source("src/official/harp_dx/03_data_initial.R")
source("src/official/harp_dx/04_data_convert.R")

##  PII Deduplication ----------------------------------------------------------

# check if deduplications are to be run
dedup <- input(
   prompt  = "Do you want to run the deduplication?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (dedup == "1") {
   source("src/official/harp_dx/05_dedup_new.R")
   source("src/official/harp_dx/06_dedup_old.R")
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
   source("src/official/harp_dx/07_data_final.R")
   source("src/official/harp_dx/08_output.R")

   # TODO: Place these after pdf & ml conso
   source("src/official/harp_dx/09_archive.R")
   source("src/official/harp_dx/10_upload.R")
}
rm(complete)

##   Consolidate Confirmatories ------------------------------------------------

source("src/official/harp_dx/12_pdf_saccl.R")

# TODO: Add import of SACCL Confirmatory data based on `pdf_results`
# TODO: Add processing of crcl pdf

# TODO: Add download of Form A and pair with Result
# TODO: Add rename to `idnum_labcode.pdf` and upload to Form A folder (cloud)
# TODO: Check if file exists before combining
