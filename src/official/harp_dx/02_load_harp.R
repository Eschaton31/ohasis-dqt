##  Get the previous report's HARP Registry ------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")
db_conn <- ohasis$conn("db")
db_name <- "ohasis_warehouse"

# references for old dataset
old_dta       <- ohasis$get_data("harp_dx", ohasis$prev_yr, ohasis$prev_mo)
old_corr      <- nhsss$harp_dx$corr$harp_dx_old
old_tblname   <- "harp_dx_old"
old_tblspace  <- Id(schema = db_name, table = old_tblname)
old_tblschema <- dbplyr::in_schema(db_name, old_tblname)
oh_id_schema  <- dbplyr::in_schema(db_name, "id_registry")

legaci_ids <- c("rec_id", "central_id")

# check if registry is to be re-loaded
# TODO: add checking of latest version
reload <- input(
   prompt  = "How do you want to load the previous HARP Dx dataset?",
   options = c("1" = "reprocess", "2" = "download"),
   default = "1"
)
reload <- StrLeft(reload, 1) %>% toupper()

##  Re-process the dataset -----------------------------------------------------

# if Yes, re-process registry
if (reload == "1") {
   log_info("Re-processing the HARP Dx dataset from the previous reporting period.")
   if (dbExistsTable(lw_conn, old_tblspace))
      old_dataset <- tbl(lw_conn, old_tblschema) %>%
         select(-CENTRAL_ID) %>%
         left_join(
            y  = tbl(lw_conn, oh_id_schema) %>%
               select(CENTRAL_ID, PATIENT_ID),
            by = "PATIENT_ID"
         ) %>%
         select(CENTRAL_ID, PATIENT_ID) %>%
         collect() %>%
         right_join(
            y  = old_dta %>%
               read_dta() %>%
               select(-CENTRAL_ID) %>%
               # convert Stata string missing data to NAs
               mutate_if(
                  .predicate = is.character,
                  ~if_else(. == '', NA_character_, .)
               ),
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
         )
   else
      old_dataset <- old_dta %>%
         read_dta() %>%
         # convert Stata string missing data to NAs
         mutate_if(
            .predicate = is.character,
            ~if_else(. == '', NA_character_, .)
         ) %>%
         select(-CENTRAL_ID) %>%
         left_join(
            y  = tbl(lw_conn, oh_id_schema) %>%
               select(CENTRAL_ID, PATIENT_ID) %>%
               collect(),
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
         )

   if (!is.null(old_corr)) {
      log_info("Performing cleaning on the loaded dataset.")
      old_dataset <- .cleaning_list(old_dataset, old_corr, "IDNUM", "integer")
   }

   # remove legacy columns
   for (id in legaci_ids) {
      if (id %in% names(old_dataset))
         old_dataset %<>% select(-as.symbol(!!id))
   }
   rm(id, legaci_ids)

   log_info("Updating `harp_dx_old`.")
   # delete existing data, full refresh always
   if (dbExistsTable(lw_conn, old_tblspace))
      dbExecute(lw_conn, "DROP TABLE `ohasis_warehouse`.`harp_dx_old`;")

   # upload info
   ohasis$upsert(lw_conn, "warehouse", old_tblname, old_dataset, "PATIENT_ID")

   # TODO: add logs insert for harp_dx

   # assign to global environment
   nhsss$harp_dx$official$old <- old_dataset
   rm(old_dataset)

   slackr_msg(
      paste0(">`harp_dx_old` updated with the ", ohasis$prev_yr, ".", ohasis$prev_mo, " version."),
      mrkdwn = "true"
   )
}

##  Re-download the dataset ----------------------------------------------------

# if Yes, re-downlaod registry
if (reload == "2") {
   log_info("Downloading the HARP Dx dataset from the previous reporting period.")
   nhsss$harp_dx$official$old <- tbl(lw_conn, old_tblschema) %>%
      select(-CENTRAL_ID) %>%
      left_join(
         y  = tbl(lw_conn, oh_id_schema) %>%
            select(CENTRAL_ID, PATIENT_ID),
         by = "PATIENT_ID"
      ) %>%
      collect()
}
log_info("Closing connections.")
dbDisconnect(lw_conn)
dbDisconnect(db_conn)
log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))

# TODO: Add option to process locally