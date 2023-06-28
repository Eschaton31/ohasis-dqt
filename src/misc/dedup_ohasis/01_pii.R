dedup_reqs <- function() {
   ohasis$db_checks <- ohasis$check_consistency()
   proceed          <- input(
      prompt  = "Do you want to update required tables?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   if (proceed == "1") {
      ohasis$data_factory("lake", "px_pii", "upsert", TRUE)
      ohasis$data_factory("warehouse", "id_registry", "upsert", TRUE)
   }
   return(TRUE)
}

dedup_download <- function() {
   # open connections
   lw_conn <- ohasis$conn("lw")
   db_conn <- ohasis$conn("db")

   # instatiate list
   dedup     <- list()
   dedup$pii <- tibble(REC_ID = NA_character_) %>%
      slice(0)
   if (file.exists(Sys.getenv("DEDUP_PII")))
      dedup$pii <- read_rds(Sys.getenv("DEDUP_PII"))

   # download latest records not found in previous copy
   min <- "1900-01-01 00:00:00"
   max <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   if (nrow(dedup$pii) > 0)
      min <- format(max(dedup$pii$SNAPSHOT, na.rm = TRUE), "%Y-%m-%d %H:%M:%S")

   # central id reference
   log_info("Downloading {green('id_registry')}.")
   dedup$id_registry <- dbTable(
      lw_conn,
      "ohasis_warehouse",
      "id_registry"
   ) %>%
      select(-SNAPSHOT) %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~as.character(.)
      )

   # deleted records reference
   log_info("Downloading {green('deleted')}.")
   dedup$deleted <- dbTable(
      db_conn,
      "ohasis_interim",
      "px_record",
      raw_where = TRUE,
      where     = "DELETED_AT IS NOT NULL",
      cols      = "REC_ID"
   )

   # download data based on limits (min, max)
   download <- input(
      prompt  = "Do you want to download {green('new PIIs')}?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   if (download == "1") {
      log_info("Downloading {green('pii')}.")
      new_data <- tracked_select(
         lw_conn,
         r"(
SELECT pii.REC_ID,
       COALESCE(serv.SERVICE_FACI, pii.FACI_ID)         AS FACI_ID,
       COALESCE(serv.SERVICE_SUB_FACI, pii.SUB_FACI_ID) AS SUB_FACI_ID,
       pii.PATIENT_ID,
       pii.FIRST,
       pii.MIDDLE,
       pii.LAST,
       pii.SUFFIX,
       pii.UIC,
       pii.CONFIRMATORY_CODE,
       pii.PATIENT_CODE,
       pii.BIRTHDATE,
       pii.PHILSYS_ID,
       pii.PHILHEALTH_NO,
       pii.CLIENT_EMAIL,
       pii.CLIENT_MOBILE,
       pii.SEX,
       pii.PERM_PSGC_REG,
       pii.PERM_PSGC_PROV,
       pii.PERM_PSGC_MUNC,
       pii.CURR_PSGC_REG,
       pii.CURR_PSGC_PROV,
       pii.CURR_PSGC_MUNC,
       pii.DELETED_AT,
       pii.SNAPSHOT
FROM ohasis_lake.px_pii AS pii
         LEFT JOIN ohasis_lake.px_faci_info AS serv ON pii.REC_ID = serv.REC_ID
WHERE pii.DELETED_AT IS NULL
  AND (pii.SNAPSHOT BETWEEN ? AND ?)
      )",
         "PII Data",
         list(min, max)
      )

      # finalize data
      dedup$pii <- dedup$pii %>%
         # remove old version of record
         anti_join(select(new_data, REC_ID)) %>%
         # remove old data that were already deleted
         anti_join(dedup$deleted) %>%
         # append new data
         bind_rows(new_data)

      # write to local file for later use
      write_rds(dedup$pii, Sys.getenv("DEDUP_PII"))
   }

   # close connections
   dbDisconnect(lw_conn)
   dbDisconnect(db_conn)

   dedup$pii %<>%
      ohasis$get_addr(
         c(
            PERM_REG  = "PERM_PSGC_REG",
            PERM_PROV = "PERM_PSGC_PROV",
            PERM_MUNC = "PERM_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            CURR_REG  = "CURR_PSGC_REG",
            CURR_PROV = "CURR_PSGC_PROV",
            CURR_MUNC = "CURR_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      mutate_at(
         .vars = vars(ends_with("_REG"), ends_with("_PROV"), ends_with("_MUNC")),
         ~if_else(. == "UNKNOWN", NA_character_, ., .)
      ) %>%
      mutate(
         CLIENT_MOBILE = str_replace_all(CLIENT_MOBILE, "[^[:digit:]]", "")
      )

   return(dedup)
}

dedup_linelist <- function(dedup) {

   # columns to be included in the final linelist
   cols <- c(
      "FIRST",
      "MIDDLE",
      "LAST",
      "SUFFIX",
      "UIC",
      "CONFIRMATORY_CODE",
      "PATIENT_CODE",
      "BIRTHDATE",
      "PHILSYS_ID",
      "PHILHEALTH_NO",
      "CLIENT_EMAIL",
      "CLIENT_MOBILE",
      "SEX",
      get_names(dedup$pii, "PERM_"),
      get_names(dedup$pii, "CURR_")
   )

   # arrange descendingly based on latest record
   dedup$pii %<>%
      ungroup() %>%
      get_cid(dedup$id_registry, PATIENT_ID) %>%
      arrange(desc(SNAPSHOT))

   # get latest non-missing data from column
   for (col in cols) {
      .log_info("Getting latest data for {green(col)}.")
      col_name <- as.name(col)

      # remove values denoting missing data
      if (col %in% c("FIRST", "LAST", "CLIENT_EMAIL"))
         dedup$vars[[col]] <- dedup$pii %>%
            select(
               CENTRAL_ID,
               !!col_name
            ) %>%
            mutate(
               !!col_name := str_squish(toupper(!!col_name)),
               !!col_name := case_when(
                  !!col_name == "XXX" ~ NA_character_,
                  !!col_name == "N/A" ~ NA_character_,
                  !!col_name == "NA" ~ NA_character_,
                  !!col_name == "NULL" ~ NA_character_,
                  !!col_name == "NONE" ~ NA_character_,
                  nchar(!!col_name) == 1 ~ NA_character_,
                  TRUE ~ !!col_name
               )
            ) %>%
            filter(!is.na(!!col_name))
      else
         dedup$vars[[col]] <- dedup$pii %>%
            select(
               CENTRAL_ID,
               !!col_name
            ) %>%
            mutate(
               !!col_name := str_squish(toupper(!!col_name)),
               !!col_name := if_else(!!col_name == "", NA_character_, !!col_name, !!col_name),
            ) %>%
            filter(!is.na(!!col_name))

      # deduplicate based on central id
      dedup$vars[[col]] %<>%
         distinct(CENTRAL_ID, .keep_all = TRUE) %>%
         # rename columns for reshaping
         rename(
            DATA = 2
         ) %>%
         mutate(
            VAR = col
         ) %>%
         mutate_all(~as.character(.))
   }

   # append list of latest variablkes and reshape to created
   # final dataset/linelist
   .log_info("Consolidating variables.")
   dedup$linelist <- bind_rows(dedup$vars) %>%
      pivot_wider(
         id_cols     = CENTRAL_ID,
         names_from  = VAR,
         values_from = DATA
      )

   # load harp diagnosis
   log_info("Reloading HARP dataset.")
   dedup$dx <- hs_data("harp_dx", "reg", ohasis$yr, ohasis$mo) %>%
      read_dta(col_select = c(idnum, PATIENT_ID, labcode2)) %>%
      get_cid(dedup$id_registry, PATIENT_ID)

   log_info("Loading confirmatory data.")
   dedup$linelist %<>%
      left_join(
         y  = dedup$dx %>%
            select(
               CENTRAL_ID,
               labcode2
            ),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         CONFIRMATORY_CODE = coalesce(labcode2, CONFIRMATORY_CODE),
         SUFFIX            = NA_character_,
         BIRTHDATE         = if_else(!is.na(BIRTHDATE), as.Date(BIRTHDATE), NA_Date_)
      )

   # standardize for deduplication
   log_info("Dataset cleaning and preparation.")
   # profvis({
   # dedup$standard <- dedup_prep2(
   #    data         = dedup$linelist,
   #    name_f       = FIRST,
   #    name_m       = MIDDLE,
   #    name_l       = LAST,
   #    name_s       = SUFFIX,
   #    uic          = UIC,
   #    birthdate    = BIRTHDATE,
   #    code_confirm = CONFIRMATORY_CODE,
   #    code_px      = PATIENT_CODE,
   #    phic         = PHILHEALTH_NO,
   #    philsys      = PHILSYS_ID
   # )
   # })
   # profvis({
   dedup$standard <- dedup_prep(
      data         = dedup$linelist,
      name_f       = FIRST,
      name_m       = MIDDLE,
      name_l       = LAST,
      name_s       = SUFFIX,
      uic          = UIC,
      birthdate    = BIRTHDATE,
      code_confirm = CONFIRMATORY_CODE,
      code_px      = PATIENT_CODE,
      phic         = PHILHEALTH_NO,
      philsys      = PHILSYS_ID
   ) %>%
      mutate(row_id = row_number())

   dedup$num_linked <- dedup$id_registry %>%
      group_by(CENTRAL_ID) %>%
      summarise(
         NUM_LINKED = n()
      ) %>%
      ungroup()
   # })

   return(dedup)
}