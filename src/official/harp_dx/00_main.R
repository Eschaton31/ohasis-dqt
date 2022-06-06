##  HARP Registry Linkage Controller -------------------------------------------

# define datasets
if (!exists("nhsss"))
   nhsss <- list()

if (!("harp_dx" %in% names(nhsss)))
   nhsss$harp_dx <- new.env()

nhsss$harp_dx$wd <- file.path(getwd(), "src", "official", "harp_dx")

##  Begin linkage of dx registry -----------------------------------------------

source(file.path(nhsss$harp_dx$wd, "01_load_reqs.R"))
source(file.path(nhsss$harp_dx$wd, "03_data_initial.R"))
source(file.path(nhsss$harp_dx$wd, "04_data_convert.R"))

##  PII Deduplication ----------------------------------------------------------

# check if deduplications are to be run
dedup <- input(
   prompt  = "Do you want to run the deduplication?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (dedup == "1") {
   source(file.path(nhsss$harp_dx$wd, "05_dedup_new.R"))
   source(file.path(nhsss$harp_dx$wd, "06_dedup_old.R"))
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
   source(file.path(nhsss$harp_dx$wd, "07_data_final.R"))
   source(file.path(nhsss$harp_dx$wd, "08_output.R"))

   # TODO: Place these after pdf & ml conso
   source(file.path(nhsss$harp_dx$wd, "09_archive.R"))
   source(file.path(nhsss$harp_dx$wd, "10_upload.R"))
}
rm(complete)

##   Consolidate Confirmatories ------------------------------------------------

   source(file.path(nhsss$harp_dx$wd, "12_pdf_saccl.R"))

# TODO: Add import of SACCL Confirmatory data based on `pdf_results`
# TODO: Add processing of crcl pdf

# TODO: Add download of Form A and pair with Result
# TODO: Add rename to `idnum_labcode.pdf` and upload to Form A folder (cloud)
# TODO: Check if file exists before combining
