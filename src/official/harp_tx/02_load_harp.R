##  Get the previous report's HARP Registry ------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")
db_conn <- ohasis$conn("db")
db_name <- "ohasis_warehouse"

# references for old dataset
old_dta          <- ohasis$get_data("harp_tx-reg", ohasis$prev_yr, ohasis$prev_mo)
old_outcome      <- ohasis$get_data("harp_tx-outcome", ohasis$prev_yr, ohasis$prev_mo)
old_reg_corr     <- nhsss$harp_tx$corr$old_reg
old_outcome_corr <- nhsss$harp_tx$corr$old_outcome
old_tblname      <- "harp_tx_old"
old_tblspace     <- Id(schema = db_name, table = old_tblname)
old_tblschema    <- dbplyr::in_schema(db_name, old_tblname)
oh_id_schema     <- dbplyr::in_schema(db_name, "id_registry")

legaci_ids <- c("rec_id", "central_id")

# check if registry is to be re-loaded
# TODO: add checking of latest version
reload <- input(
   prompt  = "How do you want to load the previous HARP Tx dataset?",
   options = c("1" = "reprocess", "2" = "download"),
   default = "1"
)
reload <- StrLeft(reload, 1) %>% toupper()

##  Re-process the dataset -----------------------------------------------------

# if Yes, re-process registry
if (reload == "1") {
   .log_info("Re-processing the HARP Tx dataset from the previous reporting period.")

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

   if (!is.null(old_reg_corr)) {
      .log_info("Performing cleaning on the art registry dataset.")
      old_dataset <- .cleaning_list(old_dataset, old_reg_corr, "ART_ID", "integer")
   }

   # drop clients
   if ("anti_join" %in% names(nhsss$harp_tx$corr)) {
      old_dataset <- old_dataset %>%
         anti_join(
            y  = nhsss$harp_tx$corr$anti_join,
            by = "art_id"
         )
   }

   # remove legacy columns
   for (id in legaci_ids) {
      if (id %in% names(old_dataset))
         old_dataset %<>% select(-as.symbol(!!id))
   }
   rm(id, legaci_ids)

   .log_info("Updating `harp_tx_old`.")
   # delete existing data, full refresh always
   if (dbExistsTable(lw_conn, old_tblspace))
      dbExecute(lw_conn, "DROP TABLE `ohasis_warehouse`.`harp_tx_old`;")

   # upload info
   ohasis$upsert(lw_conn, "warehouse", old_tblname, old_dataset, "PATIENT_ID")

   # TODO: add logs insert for harp_tx

   # assign to global environment
   nhsss$harp_tx$official$old_reg <- old_dataset
   rm(old_dataset)

   slackr_msg(
      paste0(">`harp_tx_old` updated with the ", ohasis$prev_yr, ".", ohasis$prev_mo, " version."),
      mrkdwn = "true"
   )
}

##  Re-download the dataset ----------------------------------------------------

# if Yes, re-downlaod registry
if (reload == "2") {
   .log_info("Downloading the HARP Tx dataset from the previous reporting period.")
   nhsss$harp_tx$official$old_reg <- tbl(lw_conn, old_tblschema) %>%
      select(-CENTRAL_ID) %>%
      left_join(
         y  = tbl(lw_conn, oh_id_schema) %>%
            select(CENTRAL_ID, PATIENT_ID),
         by = "PATIENT_ID"
      ) %>%
      collect()
}
.log_info("Closing connections.")
dbDisconnect(lw_conn)
dbDisconnect(db_conn)

# load outcome dataset
nhsss$harp_tx$official$old_outcome <- old_outcome %>%
   read_dta() %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   )

# clean if any for cleaning found
if (!is.null(old_outcome_corr)) {
   .log_info("Performing cleaning on the outcome dataset.")
   nhsss$harp_tx$official$old_outcome <- .cleaning_list(nhsss$harp_tx$official$old_outcome, old_outcome_corr, "ART_ID", "integer")
}

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))

# TODO: Add option to process locally
