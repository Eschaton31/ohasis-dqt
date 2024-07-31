load_old_dta <- function(
   surv,
   corr = NULL,
   warehouse_table = NULL,
   id_col = NULL,
   dta_pid = NULL,
   remove_cols = NULL,
   remove_rows = NULL,
   id_registry = NULL,
   reload = NULL
) {
   # open connections
   log_info("Opening connections.")
   db_conn <- self$conn("lw")
   db_name <- "ohasis_warehouse"

   # references for old dataset
   old_tblspace  <- Id(schema = db_name, table = warehouse_table)
   old_tblschema <- dbplyr::in_schema(db_name, warehouse_table)
   oh_id_schema  <- dbplyr::in_schema(db_name, "id_registry")

   # check if dataset is to be re-loaded
   # TODO: add checking of latest version
   reload <- ifelse(
      !is.null(reload) && reload %in% c("1", "2"),
      reload,
      input(
         prompt  = "How do you want to load the previous dataset?",
         options = c("1" = "reprocess", "2" = "download"),
         default = "1"
      )
   )

   if (!is.null(id_registry))
      tbl_ids <- id_registry
   else
      tbl_ids <- tbl(db_conn, oh_id_schema) %>%
         select(CENTRAL_ID, PATIENT_ID) %>%
         collect()

   # if Yes, re-process registry
   if (reload == "1") {
      log_info("Re-processing the dataset from the previous reporting period.")

      old_dataset <- hs_data(surv,) %>%
         read_dta() %>%
         zap_labels() %>%
         # convert Stata string missing data to NAs
         mutate_if(
            .predicate = is.character,
            ~if_else(. == '', NA_character_, .)
         ) %>%
         select(-any_of(remove_cols)) %>%
         rename_all(
            ~case_when(
               . == dta_pid ~ "PATIENT_ID",
               TRUE ~ .
            )
         ) %>%
         left_join(
            y  = tbl_ids,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            )
         ) %>%
         relocate(CENTRAL_ID, .before = 1)

      if (!is.null(corr)) {
         log_info("Performing cleaning on the dataset.")
         old_dataset <- .cleaning_list(old_dataset, as.data.frame(corr), toupper(names(id_col)), id_col)
      }

      # drop clients
      if (!is.null(remove_rows)) {
         col         <- as.name(names(id_col))
         old_dataset <- old_dataset %>%
            mutate({{col}} := eval(parse(text = glue("as.{id_col}({names(id_col)})")))) %>%
            anti_join(
               y  = remove_rows %>%
                  mutate({{col}} := eval(parse(text = glue("as.{id_col}({names(id_col)})")))),
               by = names(id_col)
            )
      }

      log_info("Updating warehouse table.")
      # delete existing data, full refresh always
      if (dbExistsTable(db_conn, old_tblspace))
         dbExecute(db_conn, glue(r"(DROP TABLE `ohasis_warehouse`.`{warehouse_table}`;)"))

      # upload info
      # .self$upsert(db_conn, "warehouse", warehouse_table, old_dataset, "PATIENT_ID")
      self$upsert(db_conn, "warehouse", warehouse_table, old_dataset, names(id_col))
   }

   if (reload == "2") {
      log_info("Downloading the dataset from the previous reporting period.")
      old_dataset <- dbTable(
         db_conn,
         db_name,
         warehouse_table,
         join = list(
            "ohasis_warehouse.id_registry" = list(by = c("PATIENT_ID" = "PATIENT_ID"), cols = "CENTRAL_ID")
         )
      ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            )
         ) %>%
         relocate(CENTRAL_ID, .before = 1)
   }

   log_info("Closing connections.")
   dbDisconnect(db_conn)

   log_success("Done.")
   return(old_dataset)
}