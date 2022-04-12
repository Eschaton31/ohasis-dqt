shell("cls")
################################################################################
# Project name: > OHASIS Data Wrangling
# Author(s):    > Palo, John Benedict
# Date:         > 2022-01-27
# Description:  > This program creates the various OHASIS extracted data sets.
#
# Updates:      > See Changelog
#
# Input:        > OHASIS SQL Tables
#               > HARP Datasets
#
# Notes:        >
################################################################################

rm(list = ls())
Sys.setenv(TZ = "Asia/Hong_Kong")
options(
   # browser = Sys.getenv("BROWSER"),
   browser             = function(url) {
      if (grepl('^https?:', url)) {
         if (!.Call('.jetbrains_processBrowseURL', url)) {
            browseURL(url, .jetbrains$ther_old_browser)
         }
      } else {
         .Call('.jetbrains_showFile', url, url)
      }
   },
   help_type           = "html",
   RStata.StataPath    = Sys.getenv("STATA_PATH"),
   RStata.StataVersion = as.integer(Sys.getenv("STATA_VER"))
)

##  Load Environment Variables -------------------------------------------------

# check if OS is Windows, if not use the mounted Rlib volumne
if (Sys.info()['sysname'] == "Linux")
   .libPaths("/dqt/Rlib")

source("src/dependencies/libraries.R")
source("src/dependencies/functions.R")
source("src/dependencies/classes.R")

##  Load credentials and authentications ---------------------------------------

# Google
# trigger auth on purpose --> store a token in the specified cache
options(
   gargle_oauth_cache = ".secrets",
   gargle_oauth_email = "nhsss@doh.gov.ph",
   gargle_oob_default = TRUE
)
drive_auth(cache = ".secrets")
gs4_auth(cache = ".secrets")

# Dropbox
# trigger auth on purpose --> store a token in the specified cache
if (!file.exists(".secrets/hivregistry.nec@gmail.com.RDS")) {
   token <- drop_auth(new_user = TRUE)
   saveRDS(token, ".secrets/hivregistry.nec@gmail.com.RDS")
   rm('token')
} else {
   drop_auth(rdstoken = ".secrets/hivregistry.nec@gmail.com.RDS")
}

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB()

########

nhsss$harp_tx$official$new_reg %>%
   left_join(
      y  = nhsss$harp_dx$official$new %>% select(confirm_date, idnum),
      by = "idnum"
   ) %>%
   filter(year(artstart_date) > year(confirm_date))

# gcheckin
write_dta(
   nhsss$harp_tx$official$new_reg,
   "H:/20220329_reg-art_2022-02.dta"
)

write_dta(
   nhsss$harp_tx$outcome.converted$data,
   "H:/20220329_onart_2022-02 (prev vs curr).dta"
)

write_dta(
   nhsss$harp_tx$official$new_outcome,
   "H:/20220329_onart_2022-02.dta"
)

stata(r"(
u "H:/20220329_reg-art_2022-02.dta", clear
format_compress
form *date* %tdCCYY-NN-DD
sa "H:/20220329_reg-art_2022-02.dta", replace

u "H:/20220329_onart_2022-02 (prev vs curr).dta", clear
format_compress
form *date* %tdCCYY-NN-DD
sa "H:/20220329_onart_2022-02 (prev vs curr).dta", replace

u "H:/20220329_onart_2022-02.dta", clear
format_compress
form *date* %tdCCYY-NN-DD
sa "H:/20220329_onart_2022-02.dta", replace
)")

########


# run registry
source("src/official/harp_dx/00_main.R")

#######
db_conn    <- ohasis$conn("db")
px_confirm <- dbReadTable(db_conn, Id(schema = "ohasis_interim", table = "px_confirm"))
dbDisconnect(db_conn)

ei      <- get_ei("2022.03")
encoded <- ei %>%
   filter(Form %in% c("Form A", "HTS Form"),
          !is.na(`Record ID`)) %>%
   mutate(,
      `Encoder` = stri_replace_first_fixed(encoder, "2022.03_", "")
   ) %>%
   select(
      `Facility ID`,
      `Facility Name`,
      `Page ID`,
      `Record ID`,
      `ID Type`,
      `Identifier`,
      `Issues`,
      `Validation`,
      `Encoder`
   )

results <- nhsss$harp_dx$pdf_saccl$data %>%
   mutate(
      FILENAME_FORM = NA_character_,
      FINAL_RESULT  = case_when(
         FORM == "*Computer" ~ "Duplicate",
         REMARKS == "Duplicate" ~ "Duplicate",
         TRUE ~ FINAL_RESULT
      ),
      FINAL_RESULT  = case_when(
         FORM == "*Computer" ~ "Duplicate",
         REMARKS == "Duplicate" ~ "Duplicate",
         TRUE ~ FINAL_RESULT
      ),
      LABCODE       = case_when(
         FILENAME == 'SACCLHIV - D22-03-02610.pdf' ~ 'D22-03-02610',
         FILENAME == 'SACCLHIV - D22-03-02636D.pdf' ~ 'D22-03-02636',
         FILENAME == 'SACCLHIV - D22-03-02652D.pdf' ~ 'D22-03-02652',
         FILENAME == 'SACCLHIV - D22-03-03306.pdf' ~ 'D22-03-03306',
         FILENAME == 'SACCLHIV - D22-03-03316.pdf' ~ 'D22-03-03316',
         TRUE ~ LABCODE
      ),
      FULLNAME      = case_when(
         FILENAME == 'SACCLHIV - D22-03-02610.pdf' ~ 'FERANGCO, BERNABE L.',
         FILENAME == 'SACCLHIV - D22-03-02636D.pdf' ~ 'ESPIRITU, CEVIR N.',
         FILENAME == 'SACCLHIV - D22-03-02652D.pdf' ~ 'SUCGANG, DANDEE R.',
         FILENAME == 'SACCLHIV - D22-03-03306.pdf' ~ 'BUSTILLO, ALJON G.',
         FILENAME == 'SACCLHIV - D22-03-03316.pdf' ~ 'SERAD , JOEL B.',
         TRUE ~ FULLNAME
      )
   ) %>%
   distinct(LABCODE, .keep_all = TRUE) %>%
   select(
      FILENAME_PDF  = FILENAME,
      LABCODE,
      FULLNAME_PDF  = FULLNAME,
      BIRTHDATE_PDF = BDATE,
      FILENAME_FORM,
      FINAL_INTERPRETATION,
      FINAL_RESULT
   )

# match with pdf results
match <- results %>%
   # fuzzyjoin::stringdist_full_join(
   full_join(
   # inner_join(
      y      = encoded %>% mutate(only = 1),
      # by     = c("FULLNAME_PDF" = "Identifier"),
      by     = c("LABCODE" = "Page ID"),
      method = "osa"
   ) %>%
   mutate(
      LABCODE = if_else(is.na(LABCODE), `Record ID`, LABCODE)
   ) %>%
   distinct(LABCODE, .keep_all = TRUE) %>%
   anti_join(
      y  = px_confirm %>% select(LABCODE = CONFIRM_CODE),
      by = "LABCODE"
   )

nrow(encoded)
nrow(results)
nrow(match)
write_clip(
   match %>%
      mutate(
         `For Import` = NA_character_,
         `PATIENT_ID` = NA_character_,
      ) %>%
      select(
         `FILENAME_PDF`,
         `LABCODE`,
         `FULLNAME_PDF`,
         `BIRTHDATE_PDF`,
         `FILENAME_FORM`,
         `FINAL_INTERPRETATION`,
         `FINAL_RESULT`,
         `REC_ID` = `Record ID`,
         `PATIENT_ID`,
         `For Import`,
         `Identifier`
      )
)
write_xlsx(try, "H:/Feb 2022 Form A EI.xlsx")
#########

# insert some clients
# artstart   <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "art_first")) %>%
#    collect()
# artslatest <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "art_last")) %>%
#    collect()
artdrop  <- googlesheets4::read_sheet("1aCYXbgCxSoDQuKNnqaan_DKi4aO_jB2rgYDtR8sI0Ec")
artclean <- googlesheets4::read_sheet("1zxpOANQNQp6-enERbByvJtqYLrb_ayklTLhGYV8uigg")
outclean <- googlesheets4::read_sheet("1OYbesaeaAGRbDmdLtIF3l6sllV3rhcqO1s2j4CejdhA")

db_conn  <- ohasis$conn("db")
registry <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "registry")) %>%
   collect()
items    <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "inventory_product")) %>%
   collect()
dbDisconnect(db_conn)

# clean <- read_xlsx("H:/20220322_corr-dat_reg-art_2022-01.xlsx", sheet = "sex") %>%
#    mutate(
#       art_id = as.integer(art_id),
#       # n_first = nchar(first),
#       # n_last  = nchar(last),
#       # n_name  = n_first + n_last,
#       # suffix = case_when(
#       #    art_id == 990 ~ "Jr.",
#       #    TRUE ~ NA_character_
#       # ),
#       # birthdate = as.Date(birthdate),
#    ) %>%
#    # mutate_at(
#    #    .vars = vars(first, middle, last, suffix),
#    #    ~toupper(.)
#    # ) %>%
#    # filter(n_name >= 3) %>%
#    select(
#       art_id,
#       # corr_first  = first,
#       # corr_middle = middle,
#       # corr_last   = last,
#       # corr_suffix = suffix,
#       # corr_birthdate = birthdate
#       corr_sex = sex
#    )
# write_dta(clean, "H:/20220323_corr-dat-sex_reg-art_2022-01.dta")

# df <- read_xlsx("C:/Users/johnb/Downloads/artstart_registry.xlsx", col_types = "text")
df    <- read_dta("H:/_R/library/hiv_tx/data/20220323_reg-art_2022-01.dta")
onart <- read_dta("H:/_R/library/hiv_tx/data/20220319_onart_2022-01.dta")

dups_data <- df %>%
   select(-CENTRAL_ID, -REC_ID) %>%
   mutate(
      art_id = as.integer(art_id),
      idnum  = as.integer(idnum),
      age    = as.integer(age)
   ) %>%
   left_join(
      y  = registry %>% select(CENTRAL_ID, PATIENT_ID),
      by = "PATIENT_ID"
   ) %>%
   left_join(
      y  = onart %>% select(art_id, latest_ffupdate, latest_regimen, latest_nextpickup, hub),
      by = "art_id"
   ) %>%
   arrange(desc(latest_nextpickup), desc(latest_ffupdate)) %>%
   group_by(CENTRAL_ID) %>%
   mutate(
      ct            = n(),
      corr_artstart = min(artstart_date, na.rm = TRUE),
      corr_ffup     = max(latest_ffupdate, na.rm = TRUE),
      corr_pickup   = max(latest_nextpickup, na.rm = TRUE),
      corr_regimen  = first(latest_regimen, na.rm = TRUE),
      corr_hub      = first(hub, na.rm = TRUE),
   ) %>%
   ungroup() %>%
   filter(ct > 1) %>%
   anti_join(
      y  = artdrop,
      by = "art_id"
   )

reg_art <- .cleaning_list(reg_art, artclean, "ART_ID", "integer")
reg_art <- .cleaning_list(reg_art, outclean, "ART_ID", "integer")
review %>%
   filter(art_id == 43563) %>%
   select(middle)

reg_art %>%
   left_join(
      y  = dups_data %>% select(art_id, starts_with('corr')),
      by = "art_id"
   ) %>%
   filter(artstart_date > corr_artstart)
# filter(latest_ffupdate < corr_ffup)

View(
   reg_art %>%
      filter_at(
         .vars           = vars(first, last, birthdate),
         .vars_predicate = all_vars(!is.na(.))
      ) %>%
      get_dupes(first, last, birthdate) %>%
      group_by(first, last, birthdate) %>%
      mutate(
         # generate a group id to identify groups of duplicates
         group_id    = cur_group_id(),

         group_idnum = paste(collapse = ",", sort(idnum)),
      ) %>%
      ungroup() %>%
      filter(
         (group_idnum %in% c("4802,7284", "36505,51705", "5506,94164", "13959,13067", "13067,13959", "17724,47340", "6130,6333", "8645,66800", "380,1830"))
      ),
   'pii dup'
)
# update reg-art central_id
reg_art <- df %>%
   select(-CENTRAL_ID, -REC_ID) %>%
   mutate(
      art_id = as.integer(art_id),
      idnum  = as.numeric(idnum),
      age    = as.numeric(age)
   ) %>%
   left_join(
      y  = registry %>% select(CENTRAL_ID, PATIENT_ID),
      by = "PATIENT_ID"
   ) %>%
   anti_join(
      y  = artdrop,
      by = "art_id"
   ) %>%
   # filter(!is.na(CENTRAL_ID)) %>%
   left_join(
      y  = onart %>% select(art_id, latest_ffupdate, latest_regimen, latest_nextpickup, hub),
      by = "art_id"
   )

View(
   reg_art %>%
      filter(!is.na(CENTRAL_ID)) %>%
      get_dupes(CENTRAL_ID) %>%
      group_by(CENTRAL_ID) %>%
      mutate(dupgrp = cur_group_id()) %>%
      ungroup() %>%
      select(
         art_id,
         dupgrp,
         idnum,
         confirmatory_code,
         px_code,
         uic,
         first,
         middle,
         last,
         suffix,
         age,
         birthdate,
         sex,
         initials,
         philsys_id,
         philhealth_no,
         artstart_date,
         PATIENT_ID
      ),
   'cid dupes'
)
db_conn <- ohasis$conn("db")
lw_conn <- ohasis$conn("lw")

# form bc
query <- glue(r"(
SELECT CASE
           WHEN reg.CENTRAL_ID IS NULL THEN rec.PATIENT_ID
           WHEN reg.CENTRAL_ID IS NOT NULL THEN reg.CENTRAL_ID
           END AS CENTRAL_ID,
       rec.REC_ID,
       rec.VISIT_DATE
FROM ohasis_warehouse.form_art_bc rec
         LEFT JOIN ohasis_warehouse.id_registry reg ON rec.PATIENT_ID = reg.PATIENT_ID
WHERE ART_RECORD = 'ART' AND VISIT_DATE < '{ohasis$next_date}';
   )")
.log_info("Downloading dataset.")
rs      <- dbSendQuery(lw_conn, query)
form_bc <- dbFetch(rs)
dbClearResult(rs)


# query       <- dbSendQuery(lw_conn, "SELECT * FROM ohasis_warehouse.id_registry")
# id_registry <- dbFetch(query)
# dbClearResult(query)

# dbxDelete(db_conn, Id(schema = "ohasis_interim", table = "registry"), batch_size = 10000)
# dbxInsert(db_conn, Id(schema = "ohasis_interim", table = "registry"), id_registry %>% select(-SNAPSHOT), batch_size = 10000)

dbDisconnect(db_conn)
dbDisconnect(lw_conn)
# for_insert <- df %>%
#    filter(PATIENT_ID == "") %>%
#    select(-CENTRAL_ID, -REC_ID) %>%
#    mutate(
#       art_id = as.integer(art_id),
#       idnum  = as.integer(idnum),
#    ) %>%
#    left_join(
#       y  = registry %>% select(CENTRAL_ID, PATIENT_ID),
#       by = "PATIENT_ID"
#    ) %>%
#    anti_join(
#       y  = artdrop,
#       by = "art_id"
#    ) %>%
#    left_join(
#       y  = onart %>% select(art_id, latest_ffupdate, latest_regimen, latest_nextpickup),
#       by = "art_id"
#    ) %>%
#    rename(VISIT_DATE = latest_ffupdate) %>%
#    mutate(
#       hub = toupper(hub),
#       # hub = if_else(!is.na(artstart_hub), artstart_hub, hub),
#    ) %>%
#    left_join(
#       y          = ohasis$ref_faci %>%
#          distinct(FACI_CODE, FACI_ID) %>%
#          select(hub = FACI_CODE, FACI_ID),
#       by         = "hub",
#       na_matches = "never"
#    ) %>%
#    mutate(
#       CREATED_BY = "1300000000",
#       CREATED_AT = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
#    ) %>%
#    mutate_if(
#       .predicate = is.character,
#       ~str_squish(.) %>% if_else(. == "", NA_character_, .)
#    )

for_insert <- dups_data %>%
   rename(VISIT_DATE = latest_ffupdate) %>%
   mutate(
      VISIT = paste(sep = "-",
                    stri_pad_left(year(VISIT_DATE), 4, "0"),
                    stri_pad_left(month(VISIT_DATE), 2, "0"),
                    "01"
      ) %>% as.Date()
   ) %>%
   left_join(
      y  = form_bc %>%
         mutate(
            VISIT = paste(sep = "-",
                          stri_pad_left(year(VISIT_DATE), 4, "0"),
                          stri_pad_left(month(VISIT_DATE), 2, "0"),
                          "01"
            ) %>% as.Date()
         ) %>%
         select(-VISIT_DATE),
      by = c("CENTRAL_ID", "VISIT")
   ) %>%
   filter(
      is.na(REC_ID),
      VISIT_DATE < "2021-07-01"
   ) %>%
   mutate(
      # hub = toupper(hub),
      hub = toupper(hub),
      # hub = if_else(!is.na(artstart_hub), artstart_hub, hub),
   ) %>%
   left_join(
      y          = ohasis$ref_faci %>%
         distinct(FACI_CODE, FACI_ID) %>%
         select(hub = FACI_CODE, FACI_ID),
      by         = "hub",
      na_matches = "never"
   ) %>%
   mutate(
      CREATED_BY = "1300000000",
      CREATED_AT = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.) %>% if_else(. == "", NA_character_, .)
   )

for_insert <- reg_art %>%
   rename(VISIT_DATE = latest_ffupdate) %>%
   # left_join(
   #    y  = form_bc,
   #    by = c("CENTRAL_ID", "VISIT_DATE")
   # ) %>%
   anti_join(
      y  = form_bc %>% select(CENTRAL_ID),
      by = "CENTRAL_ID"
   ) %>%
   # filter(
   #    # is.na(REC_ID),
   #    VISIT_DATE < "2021-07-01"
   # ) %>%
   mutate(
      hub = toupper(hub)
   ) %>%
   left_join(
      y          = ohasis$ref_faci %>%
         distinct(FACI_CODE, FACI_ID) %>%
         select(hub = FACI_CODE, FACI_ID),
      by         = "hub",
      na_matches = "never"
   ) %>%
   mutate(
      CREATED_BY = "1300000000",
      CREATED_AT = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.) %>% if_else(. == "", NA_character_, .)
   )

# pxid
# for (i in seq_len(nrow(for_insert))) {
#    for_insert[i, "PATIENT_ID"] <- oh_px_id(db_conn, for_insert[i,]$FACI_ID)
# }

# rec_id
for_insert <- for_insert %>%
   mutate(
      # birthdate     = excel_numeric_to_date(as.numeric(birthdate)),
      # artstart_date = excel_numeric_to_date(as.numeric(artstart_date)),
      # REC_ID = paste0(format(Sys.time(), "%Y%m%d%H"), stri_pad_left(row_number(), 4, "0"), "_1300000001"),
      REC_ID = paste0("2022032314", stri_pad_left(row_number(), 4, "0"), "_1300000001"),
      nchar  = nchar(REC_ID)
   )

px_info <- for_insert %>%
   mutate(
      sex = case_when(
         sex == "MALE" ~ "1",
         sex == "FEMALE" ~ "2"
      )
   ) %>%
   select(
      REC_ID,
      PATIENT_ID,
      CONFIRMATORY_CODE = confirmatory_code,
      UIC               = uic,
      PHILHEALTH_NO     = philhealth_no,
      SEX               = sex,
      BIRTHDATE         = birthdate,
      PATIENT_CODE      = px_code,
      CREATED_BY,
      CREATED_AT
   )

px_name <- for_insert %>%
   select(
      REC_ID,
      PATIENT_ID,
      FIRST  = first,
      MIDDLE = middle,
      LAST   = last,
      SUFFIX = suffix,
      CREATED_BY,
      CREATED_AT
   )

px_record <- for_insert %>%
   mutate(
      DISEASE = "101000",
      MODULE  = "3",
   ) %>%
   select(
      REC_ID,
      PATIENT_ID,
      FACI_ID,
      RECORD_DATE = VISIT_DATE,
      DISEASE,
      MODULE,
      CREATED_BY,
      CREATED_AT
   )

px_faci <- for_insert %>%
   mutate(
      SERVICE_TYPE = "101201",
      VISIT_TYPE   = "1",
      TX_STATUS    = "1",
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SERVICE_TYPE,
      VISIT_TYPE,
      TX_STATUS,
      CREATED_BY,
      CREATED_AT
   )

for_insert <- read_xlsx("H:/2022.03.27-px_medicine.xlsx")

px_medicine <- for_insert %>%
   mutate(
      days = (latest_nextpickup - VISIT_DATE) %>% as.integer()
   ) %>%
   separate(
      col  = "latest_regimen",
      into = c("ITEM_1", "ITEM_2", "ITEM_3"),
      sep  = "\\+"
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      ITEM_1,
      ITEM_2,
      ITEM_3,
      DISP_DATE = VISIT_DATE,
      NEXT_DATE = latest_nextpickup
   ) %>%
   pivot_longer(
      cols      = c(ITEM_1, ITEM_2, ITEM_3),
      names_to  = "DISP_NUM",
      values_to = "ITEM"
   ) %>%
   mutate(
      DISP_NUM = stri_replace_all_fixed(DISP_NUM, "ITEM_", "") %>% as.integer(),
      ITEM     = case_when(
         ITEM == "3TC/TDF/EFV" ~ "TDF/3TC/EFV",
         ITEM == "EFVSyr" ~ "EFVsyr",
         ITEM == "3TCSyr" ~ "3TCsyr",
         ITEM == "ABCSyr" ~ "ABCsyr",
         TRUE ~ ITEM
      )
   ) %>%
   filter(!is.na(ITEM)) %>%
   left_join(
      y  = items %>% select(MEDICINE = ITEM, ITEM = SHORT, TYPICAL_BATCH),
      by = "ITEM"
   ) %>%
   distinct(REC_ID, MEDICINE, DISP_NUM, .keep_all = TRUE) %>%
   mutate(
      PER_DAY    = case_when(
         TYPICAL_BATCH == 120 ~ 4,
         TYPICAL_BATCH == 60 ~ 2,
         TYPICAL_BATCH == 30 ~ 1,
         TRUE ~ 1
      ),
      DISP_TOTAL = as.integer(difftime(NEXT_DATE, DISP_DATE, units = "days")) * PER_DAY
   ) %>%
   select(-ITEM, -TYPICAL_BATCH) %>%
   left_join(
      y  = for_insert %>% select(REC_ID, CREATED_AT, CREATED_BY),
      by = "REC_ID"
   )

px_record <- px_medicine %>%
   select(
      REC_ID,
      UPDATED_BY = CREATED_BY,
      UPDATED_AT = CREATED_AT
   )


db_conn <- ohasis$conn("db")
dbxUpsert(db_conn, Id(schema = "ohasis_interim", table = "px_info"), px_info, "REC_ID", batch_size = 1000)
dbxUpsert(db_conn, Id(schema = "ohasis_interim", table = "px_name"), px_name, "REC_ID", batch_size = 1000)
dbxUpsert(db_conn, Id(schema = "ohasis_interim", table = "px_faci"), px_faci, "REC_ID", batch_size = 1000)
dbxUpsert(db_conn, Id(schema = "ohasis_interim", table = "px_record"), px_record, "REC_ID", batch_size = 1000)
dbxUpsert(db_conn, Id(schema = "ohasis_interim", table = "px_medicine"), px_medicine, "REC_ID", batch_size = 1000)
dbDisconnect(db_conn)

######### VSM data
dir   <- "H:/System/HARP/2_Treatment/Activities/Data Quality Review/DQR_2019-vsm/FacilityML - vsm"
years <- c(2015, 2016, 2017, 2018, 2019)

vsm_data <- list()
vsm_df   <- data.frame()
for (year in years) {
   reports <- file.path(dir, glue("vsm_{year}")) %>% list.files(full.names = TRUE)
   for (report in reports) {
      files <- list.files(reports, full.names = TRUE)
      for (file in files) {
         if (!stri_detect_fixed(file, "~")) {
            filename <- basename(file) %>% xfun::sans_ext()
            if (stri_detect_fixed(filename, "Report")) {
               sheets               <- excel_sheets(file)
               vsm_data[[filename]] <- sheets

               if ("HCT Daily Registry" %in% sheets) {
                  df     <- read_xlsx(file, sheet = "HCT Daily Registry", col_types = "text") %>%
                     remove_empty(which = "cols")
                  vsm_df <- bind_rows(vsm_df, df)
               }
            }
         }
      }
   }
}
