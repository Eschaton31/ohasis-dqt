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

# accounts
source("src/dependencies/auth_acct.R")
source("src/dependencies/gmail.R")

source("src/pipeline/pipeline.R", chdir = TRUE)

##  Set parameters -------------------------------------------------------------

log_info("Setting parameters.")
dedup_confirm           <- new.env()
dedup_confirm$params$yr <- format(Sys.time(), "%Y")
dedup_confirm$params$mo <- format(Sys.time(), "%m")

##  Download records -----------------------------------------------------------

# download the list of pending rhivda for deduplication. all rhivda records with
# a PENDING result will be included in the query. also downloads the current set
# of central ids for matching with the hiv dx registry.
#
# NOTE: this section takes around 20-30secs at a ping of 110ms to the server.
log_info("Downloading data.")
local(envir = dedup_confirm, {
   query           <- list()
   query$for_dedup <- r"(
SELECT r.REC_ID,
       r.PATIENT_ID,
       i.UIC,
       i.BIRTHDATE,
       i.SEX,
       i.PHILHEALTH_NO,
       n.FIRST,
       n.MIDDLE,
       n.LAST,
       n.SUFFIX,
       conf.CONFIRM_CODE,
       conf.FINAL_RESULT,
       conf.REMARKS
FROM ohasis_interim.px_confirm AS conf
         JOIN ohasis_interim.px_name AS n ON conf.REC_ID = n.REC_ID
         JOIN ohasis_interim.px_info AS i ON conf.REC_ID = i.REC_ID
         JOIN ohasis_interim.px_record AS r ON conf.REC_ID = r.REC_ID
WHERE r.DELETED_AT IS NULL
  AND conf.FINAL_RESULT REGEXP 'Pending';
)"
   query$id_reg    <- r"(
SELECT CENTRAL_ID,
       PATIENT_ID
FROM ohasis_interim.registry
)"

   conn <- dbConnect(
      RMariaDB::MariaDB(),
      user     = "ohasis",
      password = "t1rh0uGCyN2sz6zk",
      host     = "192.168.193.232",
      port     = "3307",
      timeout  = -1,
      "ohasis_interim"
   )
   data <- lapply(query, dbGetQuery, conn = conn)
   dbDisconnect(conn)

   conn          <- dbConnect(
      RMariaDB::MariaDB(),
      user     = "ohasis",
      password = "t1rh0uGCyN2sz6zk",
      host     = "192.168.193.228",
      port     = "3307",
      timeout  = -1,
      "ohasis_lake"
   )
   data$ref_faci <- dbGetQuery(conn, "SELECT * FROM ref_faci")
   dbDisconnect(conn)
   rm(conn)
})

##  Get the HARP Diagnosis Registry --------------------------------------------

log_info("Loading HARP Dx registry.")
local(envir = dedup_confirm, {
   log_info("Checking for updated registry.")
   Sys.setenv(HARP_DX = "N:/DQT/library/harp_dx")
   ver_cloud <- hs_data("harp_dx", "reg", params$yr, params$mo)
   readRenviron(".Renviron")
   ver_local <- hs_data("harp_dx", "reg", params$yr, params$mo)

   if (basename(ver_cloud) != basename(ver_local)) {
      log_info("Downnloading new registry.")
      fs::file_copy(ver_cloud, file.path(Sys.getenv("HARP_DX"), basename(ver_cloud)))
   }

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
            age
         )
      ) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      get_cid(data$id_reg, PATIENT_ID) %>%
      dxlab_to_id(
         c("HARPDX_FACI", "HARPDX_SUB_FACI"),
         c("dx_region", "dx_province", "dx_muncity", "dxlab_standard")
      ) %>%
      mutate(
         HARPDX_SUB_FACI = coalesce(HARPDX_SUB_FACI, "")
      ) %>%
      left_join(
         y  = data$ref_faci %>%
            select(
               FACI_ID,
               SUB_FACI_ID,
               DX_LAB = FACI_NAME
            ),
         by = join_by(HARPDX_FACI == FACI_ID, HARPDX_SUB_FACI == SUB_FACI_ID)
      )
})

##  Prepare datasets -----------------------------------------------------------

log_info("Preparing and cleaning datasets.")
local(envir = dedup_confirm, {
   reclink           <- list()
   reclink$for_match <- data$for_dedup %>%
      get_cid(data$id_reg, PATIENT_ID) %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, UIC, PHILHEALTH_NO),
         ~str_squish(toupper(.))
      ) %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, UIC, PHILHEALTH_NO),
         ~case_when(
            . == "--" ~ NA_character_,
            . == "N/A" ~ NA_character_,
            . == "NULL" ~ NA_character_,
            TRUE ~ .
         )
      ) %>%
      mutate(
         SEX  = case_when(
            SEX == 1 ~ "MALE",
            SEX == 2 ~ "FEMALE",
            TRUE ~ NA_character_
         ),
         name = stri_c(
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
         y  = data$harp_dx %>%
            select(
               CENTRAL_ID,
               old_dx_labcode = labcode2,
               old_dx_lab     = DX_LAB,
               old_dx_conf    = confirm_date
            ) %>%
            mutate(old_dx = 1),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate(
         old_dx = coalesce(old_dx, 0)
      )

   reclink$match_ref <- data$harp_dx %>%
      mutate_at(
         .vars = vars(firstname, middle, last, name_suffix, uic, philhealth),
         ~str_squish(toupper(.))
      ) %>%
      mutate_at(
         .vars = vars(firstname, middle, last, name_suffix, uic, philhealth),
         ~case_when(
            . == "--" ~ NA_character_,
            . == "N/A" ~ NA_character_,
            . == "NULL" ~ NA_character_,
            TRUE ~ .
         )
      ) %>%
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
               y  = data$harp_dx %>%
                  select(
                     idnum,
                     new_match_labcode = labcode2,
                     new_match_lab     = DX_LAB,
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
            false     = FINAL_RESULT,
            missing   = FINAL_RESULT
         ),
         REMARKS        = case_when(
            final_match == 1 ~ stri_c("Client was previously confirmed on ", format(confirm_date, " %b %d, %Y"), " at ", dxlab_standard, " (", labcode2, ")."),
            final_match == 0 & REMARKS == "rHIVda Test No. 2 not completed." ~ "No previous result found. Proceed to rHIVda Test No. 2.",
            final_match == 0 & REMARKS == "rHIVda Test No. 3 not completed." ~ "No previous result found. Proceed to rHIVda Test No. 3.",
            TRUE ~ REMARKS
         ),

         UPDATED_BY     = "1300000000",
         UPDATED_AT     = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      )
})

##  Check if any records were updated ------------------------------------------

local(envir = dedup_confirm, {
   match_names <- intersect(names(reclink$for_match), names(reclink$final))
   same_rows   <- identical(select(reclink$for_match, all_of(match_names)),
                            select(reclink$final, all_of(match_names)))
   rm(match_names)
})
if (dedup_confirm$same_rows)
   log_warn("No new records matched.")

##  Generate data to update heatlh records -------------------------------------

if (!dedup_confirm$same_rows) {
   log_info("Creating tables for upsert.")
   local(envir = dedup_confirm, {
      upsert            <- list()
      upsert$registry   <- reclink$for_match %>%
         select(REC_ID, PATIENT_ID) %>%
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
            PATIENT_ID
         )
      upsert$px_confirm <- reclink$final %>%
         select(
            REC_ID,
            CONFIRM_CODE,
            FINAL_RESULT,
            REMARKS
         )

      upsert$px_record <- reclink$for_match %>%
         select(
            REC_ID,
            PATIENT_ID,
            UPDATED_BY,
            UPDATED_AT
         )
   })
}

##  Update live records --------------------------------------------------------

if (!dedup_confirm$same_rows) {
   log_info("Updating live records with linked data.")
   local(envir = dedup_confirm, {

      log_info("Uploading duplicates into {green('registry')}.")
      upload_dupes(upsert$registry)

      conn <- dbConnect(
         RMariaDB::MariaDB(),
         user     = "ohasis",
         password = "t1rh0uGCyN2sz6zk",
         host     = "192.168.193.232",
         port     = "3307",
         timeout  = -1,
         "ohasis_interim"
      )
      log_info("Updating {green('px_confirm')}.")
      dbxUpsert(conn, "px_confirm", upsert$px_confirm, "REC_ID")

      log_info("Updating {green('px_record')}.")
      dbxUpsert(conn, "px_record", upsert$px_record, c("REC_ID", "PATIENT_ID"))
      dbDisconnect(conn)
      rm(conn)
   })
}
gc()