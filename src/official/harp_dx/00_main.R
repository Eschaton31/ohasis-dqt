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

##  Google Drive Endpoint ------------------------------------------------------

path         <- list()
path$primary <- "~/DQT/Data Factory/HARP Dx/"
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
nhsss$harp_dx$gdrive$path <- path
rm(path, drive_folders)

##  Begin linkage of datasets --------------------------------------------------

source("src/official/harp_dx/01_load_corrections.R")
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

TIMESTAMP <- "2022-03-07 17:49:00"
import    <- nhsss$harp_dx$pdf_saccl$data %>%
   mutate_all(~as.character(.)) %>%
   left_join(
      y  = nhsss$harp_dx$corr$pdf_results %>%
         select(
            LABCODE,
            REC_ID
         ),
      by = "LABCODE"
   )

px_confirm <- import %>%
   mutate(
      FACI_ID      = "130023",
      SUB_FACI_ID  = "130023_001",
      CONFIRM_TYPE = "1",
      CREATED_AT   = TIMESTAMP,
      CREATED_BY   = "1300000001",
      FINAL_RESULT = case_when(
         REMARKS == "Duplicate" ~ "Duplicate",
         TRUE ~ FINAL_RESULT
      ),
      REMARKS      = case_when(
         REMARKS == "Duplicate" ~ "Client already has a previous confirmatory record.",
         FINAL_RESULT == "Negative" ~ "Laboratory evidence suggests no presence of HIV Antibodies at the time of testing.",
         TRUE ~ REMARKS
      ),
      SIGNATORY_1  = if_else(SIGNATORY_1 == "NULL", NA_character_, SIGNATORY_1),
      DATE_CONFIRM = as.character(DATE_CONFIRM),
      DATE_CONFIRM = if_else(!is.na(DATE_CONFIRM), glue("{DATE_CONFIRM} 00:00:00"), NA_character_),
      DATE_RELEASE = as.character(DATE_RELEASE),
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SUB_FACI_ID,
      CONFIRM_TYPE,
      CONFIRM_CODE = LABCODE,
      SOURCE       = SOURCE_FACI,
      SUB_SOURCE   = SOURCE_SUB_FACI,
      FINAL_RESULT,
      REMARKS,
      SIGNATORY_1,
      SIGNATORY_2,
      SIGNATORY_3,
      DATE_CONFIRM,
      DATE_RELEASE,
      CREATED_AT,
      CREATED_BY
   ) %>%
   distinct(REC_ID, .keep_all = TRUE)

px_test_hiv <- import %>%
   mutate(
      TEST_TYPE     = "33",
      TEST_NUM      = "1",
      FACI_ID       = "130023",
      SUB_FACI_ID   = "130023_001",
      CREATED_AT    = TIMESTAMP,
      CREATED_BY    = "1300000001",
      SPECIMEN_TYPE = case_when(
         SPECIMEN_TYPE == "SERUM" ~ "1"
      ),
      RESULT        = substr(FINAL_RESULT_33, 1, 1)
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SUB_FACI_ID,
      TEST_TYPE,
      TEST_NUM,
      DATE_PERFORM = T3_DATE,
      RESULT,
      CREATED_AT,
      CREATED_BY
   ) %>%
   distinct(REC_ID, .keep_all = TRUE)


db_conn     <- ohasis$conn("db")
# table_space <- Id(schema = "ohasis_interim", table = "px_confirm")
# dbxUpsert(
#    db_conn,
#    table_space,
#    import,
#    "REC_ID"
# )
table_space <- Id(schema = "ohasis_interim", table = "px_test")
dbxUpsert(
   db_conn,
   table_space,
   px_test_hiv,
   c("REC_ID", "TEST_TYPE", "TEST_NUM")
)