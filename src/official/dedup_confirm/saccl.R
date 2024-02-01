##  Load credentials and authentications ---------------------------------------

readRenviron(".Renviron")

# dependencies
source("src/dependencies/options.R")
source("src/dependencies/libraries.R")
source("src/dependencies/functions.R")
source("src/dependencies/full_tables.R")
source("src/dependencies/cloud.R")
source("src/dependencies/dedup.R")
source("src/dependencies/googlesheets.R")
source("src/dependencies/excel.R")

# accounts
source("src/dependencies/auth_acct.R")
# source("src/dependencies/gmail.R")

# classes
source("src/classes/Project.R")
source("src/classes/DB.R")

source("src/pipeline/pipeline.R", chdir = TRUE)
flow_register()

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB("2023", "10", "check harp", "2")

##  Set parameters -------------------------------------------------------------

log_info("Setting parameters.")
dedup_confirm        <- new.env()
dedup_confirm$params <- list(
   yr      = format(Sys.time(), "%Y"),
   mo      = format(Sys.time(), "%m"),
   dir     = file.path("R:/File requests/SACCL Submissions/logsheet/logsheet_hiv/deduplication", format(Sys.time(), "%Y")),
   pattern = "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
)
dedup_confirm$corr   <- read_sheet("1LLsUNwRfYycXaUWxQjD87YbniZszefy1My_t0_dAHEk", "SOURCE", col_types = "c")
dedup_confirm$query  <- list(
   prev_uploaded = "SELECT conf.REC_ID, rec.PATIENT_ID, conf.CONFIRM_CODE FROM ohasis_interim.px_record AS rec JOIN ohasis_interim.px_confirm AS conf ON rec.REC_ID = conf.REC_ID WHERE conf.CONFIRM_CODE IN (?)",
   id_reg        = "SELECT CENTRAL_ID, PATIENT_ID FROM ohasis_interim.registry",
   oh_dx         = r"(
SELECT i.PATIENT_ID,
       conf.CONFIRM_CODE AS labcode2,
       n.FIRST           AS firstname,
       n.MIDDLE          AS middle,
       n.LAST            AS last,
       n.SUFFIX          AS name_suffix,
       CASE i.SEX
           WHEN 1 THEN 'MALE'
           WHEN 2 THEN 'FEMALE'
           END           AS sex,
       i.BIRTHDATE       AS bdate,
       i.UIC             AS uic,
       i.PHILHEALTH_NO   AS philhealth,
       conf.DATE_CONFIRM AS confirm_date,
       conf.SOURCE,
       conf.SUB_SOURCE
FROM ohasis_interim.px_confirm AS conf
         JOIN ohasis_interim.px_name AS n ON conf.REC_ID = n.REC_ID
         JOIN ohasis_interim.px_info AS i ON conf.REC_ID = i.REC_ID
         JOIN ohasis_interim.px_record AS r ON conf.REC_ID = r.REC_ID
WHERE r.DELETED_AT IS NULL
  AND conf.FINAL_RESULT REGEXP 'Positive';
)"
)

##  Functions ------------------------------------------------------------------

read_saccl <- function(file) {
   data <- read_excel(file, col_types = "text", col_names = FALSE, .name_repair = "unique_quiet")
   data %<>%
      select(
         DATE_REQUEST    = 4,
         LABCODE         = 5,
         FULLNAME        = 6,
         BIRTHDATE       = 7,
         AGE             = 8,
         SEX             = 9,
         SPECIMEN_SOURCE = 10,
         SPECIMEN_TYPE   = 11,
      ) %>%
      mutate_at(
         .vars = vars(BIRTHDATE, DATE_REQUEST),
         ~excel_numeric_to_date(as.numeric(.))
      ) %>%
      mutate(
         SEX = case_when(
            SEX %in% c("M", "MALE", "Male") ~ "MALE",
            SEX %in% c("F", "FEMALE", "Female") ~ "FEMALE",
            TRUE ~ SEX
         ),
         AGE = as.numeric(AGE)
      )

   return(data)
}

data_from_files <- function(dir, pattern) {
   submission <- dir_info(dir, regexp = stri_c("EPI.*", pattern, ".*.xlsx"))

   submission %<>%
      filter(!str_detect(path, "~")) %>%
      mutate(
         submit_date = str_extract(path, pattern)
      )

   validated <- dir_info(dir, regexp = stri_c("validated_", pattern, ".*.xlsx"))
   validated %<>%
      filter(!str_detect(path, "~")) %>%
      mutate(
         submit_date = str_extract(path, pattern)
      )

   files <- submission %>%
      anti_join(
         y  = validated,
         by = join_by(submit_date)
      )

   if (nrow(files) == 0) {
      slackr_msg(
         stri_c(
            ">*ATTENTION!*\n",
            ">No new files for deduplication were found..\n",
            ">The next iteration will run on `",
            format(Sys.time() %m+% days(1), "%b %d, %Y @ %Y %X"), "`."
         ),
         mrkdwn  = "true",
         channel = "#data-pipeline"
      )
      stop()
   }

   data        <- lapply(files$path, read_saccl)
   names(data) <- files$submit_date

   all_data <- data %>%
      bind_rows(.id = "DATE_SACCL") %>%
      fullname_to_components(FULLNAME) %>%
      rename(
         FIRST  = FirstName,
         MIDDLE = MiddleName,
         LAST   = LastName
      )

   return(all_data)
}

##  Import patients first ------------------------------------------------------

local(envir = dedup_confirm, {
   log_info("Loading excel.")
   data           <- list()
   data$for_dedup <- data_from_files(params$dir, params$pattern) %>%
      mutate(
         FACI_ID     = "130023",
         SUB_FACI_ID = "130023_001",
         CREATED_BY  = "1300000048",
         CREATED_AT  = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      ) %>%
      mutate(
         FINAL_RESULT = "Pending",
         REMARKS      = "Test No. 2 not completed.",
         CONFIRM_TYPE = "1",
      ) %>%
      left_join(
         y  = corr %>%
            select(
               SPECIMEN_SOURCE = SOURCE,
               SOURCE          = SOURCE_FACI,
               SUB_SOURCE      = SOURCE_SUB_FACI,
            ),
         by = join_by(SPECIMEN_SOURCE)
      )

   log_info("Downloading OHASIS data.")
   conn               <- ohasis$conn("db")
   data$id_reg        <- dbxSelect(conn, query$id_reg)
   data$oh_dx         <- dbxSelect(conn, query$oh_dx)
   data$prev_uploaded <- dbxSelect(conn, query$prev_uploaded, params = list(data$for_dedup$LABCODE))
   dbDisconnect(conn)

   data$for_dedup %<>%
      left_join(
         y  = data$prev_uploaded %>%
            select(REC_ID, PATIENT_ID, CONFIRM_CODE),
         by = join_by(LABCODE == CONFIRM_CODE)
      )

   data$for_dedup %<>%
      filter(!is.na(PATIENT_ID)) %>%
      bind_rows(
         batch_px_ids(data$for_dedup %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "LABCODE")
      )

   data$for_dedup %<>%
      filter(!is.na(REC_ID)) %>%
      bind_rows(
         batch_rec_ids(data$for_dedup %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "LABCODE")
      )

   log_success("Done.")
})


##  Get the HARP Diagnosis Registry --------------------------------------------

log_info("Loading Positives.")
local(envir = dedup_confirm, {
   data$oh_dx %<>%
      select(-matches("CENTRAL_ID")) %>%
      get_cid(data$id_reg, PATIENT_ID) %>%
      select(-PATIENT_ID)

   data$harp_dx <- hs_data("harp_dx", "reg", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            idnum,
            labcode2,
            firstname,
            middle,
            last,
            name_suffix,
            sex,
            bdate,
            uic,
            philhealth,
            confirm_date,
            dxlab_standard,
            dx_region,
            dx_province,
            dx_muncity,
         )
      ) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      get_cid(data$id_reg, PATIENT_ID) %>%
      select(-PATIENT_ID) %>%
      dxlab_to_id(
         c("SOURCE", "SUB_SOURCE"),
         c("dx_region", "dx_province", "dx_muncity", "dxlab_standard")
      ) %>%
      select(-any_of(c("dx_region", "dx_province", "dx_muncity", "dxlab_standard")))

   data$positives <- data$oh_dx %>%
      anti_join(
         y  = data$harp_dx,
         by = join_by(CENTRAL_ID)
      ) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      bind_rows(data$harp_dx) %>%
      mutate_if(
         .predicate = is.character,
         ~clean_pii(.)
      ) %>%
      ohasis$get_faci(
         list(dxlab_standard = c("SOURCE", "SUB_SOURCE")),
         "name"
      )
})

##  Prepare datasets -----------------------------------------------------------

log_info("Preparing and cleaning datasets.")
local(envir = dedup_confirm, {
   reclink           <- list()
   reclink$for_match <- data$for_dedup %>%
      mutate_if(
         .predicate = is.character,
         ~clean_pii(.)
      ) %>%
      get_cid(data$id_reg, PATIENT_ID) %>%
      mutate(
         SUFFIX = NA_character_,
         name   = stri_c(
            sep = ", ",
            str_squish(stri_c(sep = " ", coalesce(LAST, ""), coalesce(SUFFIX, ""))),
            str_squish(stri_c(sep = " ", coalesce(FIRST, ""), coalesce(MIDDLE, "")))
         )
      ) %>%
      rename(
         sex         = SEX,
         bdate       = BIRTHDATE,
         firstname   = FIRST,
         middle      = MIDDLE,
         last        = LAST,
         name_suffix = SUFFIX
      ) %>%
      left_join(
         y  = data$positives %>%
            select(
               CENTRAL_ID,
               old_dx_labcode = labcode2,
               old_dx_lab     = dxlab_standard,
               old_dx_conf    = confirm_date
            ) %>%
            mutate(old_dx = 1),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate(
         old_dx = coalesce(old_dx, 0)
      )

   reclink$match_ref <- data$harp_dx %>%
      mutate(
         name = stri_c(
            sep = ", ",
            str_squish(stri_c(sep = " ", coalesce(last, ""), coalesce(name_suffix, ""))),
            str_squish(stri_c(sep = " ", coalesce(firstname, ""), coalesce(middle, "")))
         )
      )
})

##  Record Linkage -------------------------------------------------------------

log_info("Conducting probabilistic record linkage.")
local(envir = dedup_confirm, {
   reclink$matched <- quick_reclink(
      reclink$for_match %>% filter(old_dx == 0),
      reclink$match_ref,
      "REC_ID",
      "idnum",
      c("firstname", "middle", "last", "name", "bdate"),
      "name"
   )

   if (nrow(reclink$matched) > 0) {
      # only keep records with a distance of 90% and above
      reclink$matched %<>%
         filter(AVG_DIST >= 0.9)
   } else {
      reclink$matched <- tibble(
         idnum      = NA_integer_,
         CENTRAL_ID = NA_character_,
         REC_ID     = NA_character_
      )
   }
})

##  Finalize matched records ---------------------------------------------------

local(envir = dedup_confirm, {
   reclink$final <- reclink$for_match %>%
      left_join(
         y  = reclink$matched %>%
            select(REC_ID, idnum) %>%
            inner_join(
               y  = data$positives %>%
                  select(
                     idnum,
                     new_match_labcode = labcode2,
                     new_match_lab     = dxlab_standard,
                     new_match_conf    = confirm_date
                  ),
               by = join_by(idnum)
            ) %>%
            mutate(new_match = 1),
         by = join_by(REC_ID)
      ) %>%
      mutate(
         old_dx         = coalesce(old_dx, 0),
         new_match      = coalesce(new_match, 0),
         labcode2       = coalesce(old_dx_labcode, new_match_labcode),
         dxlab_standard = coalesce(old_dx_lab, new_match_lab),
         confirm_date   = coalesce(old_dx_conf, new_match_conf),

         final_match    = case_when(
            old_dx == 1 ~ 1,
            new_match == 1 ~ 1,
            TRUE ~ 0
         ),
         FINAL_RESULT   = if_else(
            condition = final_match == 1,
            true      = "Duplicate",
            false     = "Pending",
            missing   = "Pending"
         ),
         REMARKS        = case_when(
            final_match == 1 ~ stri_c("Client was previously confirmed on ", format(confirm_date, " %b %d, %Y"), " at ", dxlab_standard, " (", labcode2, ")."),
            final_match == 0 & REMARKS == "TEST NO. 2 NOT COMPLETED." ~ "No previous result found. Proceed to Test No. 2.",
            final_match == 0 & REMARKS == "TEST NO. 3 NOT COMPLETED." ~ "No previous result found. Proceed to Test No. 3.",
            TRUE ~ REMARKS
         ),

         UPDATED_BY     = "1300000000",
         UPDATED_AT     = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      )
})

##  Generate data to update heatlh records -------------------------------------

log_info("Creating tables for upsert.")
local(envir = dedup_confirm, {
   upsert <- list()

   if (nrow(reclink$matched %>% filter(!is.na(REC_ID))) > 0) {
      upsert$registry <- list(
         name = "registry",
         pk   = "PATIENT_ID",
         data = reclink$for_match %>%
            select(REC_ID, PATIENT_ID, CREATED_BY, CREATED_AT) %>%
            inner_join(
               y  = reclink$matched %>%
                  inner_join(
                     y  = data$harp_dx %>%
                        select(CENTRAL_ID, idnum),
                     by = join_by(idnum)
                  ),
               by = join_by(REC_ID)
            ) %>%
            select(
               CENTRAL_ID,
               PATIENT_ID,
               CREATED_BY,
               CREATED_AT,
            ) %>%
            mutate(
               UPDATED_BY = "1300000048",
               UPDATED_AT = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
            )
      )
   }

   log_info("Creating tables for upload.")
   upsert$px_record <- list(
      name = "px_record",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = reclink$final %>%
         mutate(
            DISEASE = "101000",
            MODULE  = "2"
         ) %>%
         select(
            REC_ID,
            PATIENT_ID,
            FACI_ID,
            SUB_FACI_ID,
            RECORD_DATE = DATE_REQUEST,
            DISEASE,
            MODULE,
            CREATED_BY,
            CREATED_AT,
         )
   )

   upsert$px_info <- list(
      name = "px_info",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = reclink$final %>%
         select(
            REC_ID,
            PATIENT_ID,
            CONFIRMATORY_CODE = LABCODE,
            SEX               = sex,
            BIRTHDATE         = bdate,
            CREATED_BY,
            CREATED_AT,
         ) %>%
         mutate(
            SEX = case_when(
               SEX == "MALE" ~ "1",
               SEX == "FEMALE" ~ "2",
            ),
         )
   )

   upsert$px_name <- list(
      name = "px_name",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = reclink$final %>%
         select(
            REC_ID,
            PATIENT_ID,
            FIRST  = firstname,
            MIDDLE = middle,
            LAST   = last,
            CREATED_BY,
            CREATED_AT,
         )
   )

   upsert$px_confirm <- list(
      name = "px_confirm",
      pk   = "REC_ID",
      data = reclink$final %>%
         select(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            CONFIRM_TYPE,
            CONFIRM_CODE = LABCODE,
            SOURCE,
            SUB_SOURCE,
            FINAL_RESULT,
            REMARKS,
            CREATED_BY,
            CREATED_AT,
         )
   )
})

##  Update live records --------------------------------------------------------

log_info("Updating live records with linked data.")
local(envir = dedup_confirm, {
   conn <- ohasis$conn("db")
   lapply(upsert, function(ref, conn) {
      log_info("Uploading {green(ref$name)}.")
      table_space <- Id(schema = "ohasis_interim", table = ref$name)
      dbxUpsert(conn, table_space, ref$data, ref$pk)
   }, conn)
   dbDisconnect(conn)
   rm(conn)
})
gc()

slackr_msg(
   stri_c(
      ">*SUCCESS!*\n",
      ">The SACCL Deduplication cron job was executed successfully.\n",
      ">The next iteration will run on `",
      format(Sys.time() %m+% days(1), "%b %d, %Y @ %Y %X"), "`."
   ),
   mrkdwn  = "true",
   channel = "#data-pipeline"
)

## Results into the dropbox location -------------------------------------------

excel_data <- dedup_confirm$reclink$final %>%
   select(LABCODE, FINAL_RESULT, REMARKS, dxlab_standard, labcode2, confirm_date) %>%
   left_join(
      y  = dedup_confirm$data$for_dedup %>%
         select(-FINAL_RESULT, -REMARKS),
      by = join_by(LABCODE)
   ) %>%
   mutate(
      FINDINGS = case_when(
         FINAL_RESULT == "Duplicate" ~ "Duplicate",
         TRUE ~ "No existing positive result found."
      ),
      SEX      = substr(SEX, 1, 1),
   ) %>%
   mutate_if(
      .predicate = is.POSIXct,
      ~as.Date(.)
   ) %>%
   arrange(FINDINGS, LABCODE) %>%
   select(
      DATE_SACCL,
      `Date Requested`             = DATE_REQUEST,
      `Laboratory Code`            = LABCODE,
      `Full Name`                  = FULLNAME,
      `DOB`                        = BIRTHDATE,
      `Age`                        = AGE,
      `S`                          = SEX,
      `Referring Lab`              = SPECIMEN_SOURCE,
      `Specimen Type`              = SPECIMEN_TYPE,
      `DOH-EB Findings`            = FINDINGS,
      `Existing Laboratory Code`   = labcode2,
      `Existing Laboratory Name`   = dxlab_standard,
      `Existing Confirmatory Date` = confirm_date,
   )

periods <- unique(excel_data$DATE_SACCL)
lapply(periods, function(period) {
   filename   <- stri_c("validated_", period, ".xlsx")
   filepath   <- file.path(dedup_confirm$params$dir, filename)
   sheet_data <- list(EB = excel_data %>% select(-DATE_SACCL))
   write_flat_file(sheet_data, filepath)
})
