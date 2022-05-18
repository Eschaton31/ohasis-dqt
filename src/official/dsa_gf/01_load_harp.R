##  Get HARP datasets ----------------------------------------------------------

if (!exists("nhsss"))
   nhsss <- list()


.log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")
db_conn <- ohasis$conn("db")

# ohasis ids
.log_info("Downloading OHASIS IDs.")
id_registry <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry")) %>%
   select(CENTRAL_ID, PATIENT_ID) %>%
   collect()

dbDisconnect(lw_conn)
dbDisconnect(db_conn)

.log_info("Loading HARP Dx Data.")
if (!("harp_dx" %in% names(nhsss)))
   nhsss$harp_dx$official$new <- ohasis$get_data("harp_dx", ohasis$yr, ohasis$mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      select(-CENTRAL_ID) %>%
      left_join(
         y  = id_registry,
         by = "PATIENT_ID"
      ) %>%
      mutate(
         CENTRAL_ID = if_else(
            condition = is.na(CENTRAL_ID),
            true      = PATIENT_ID,
            false     = CENTRAL_ID
         ),
         labcode2   = if_else(is.na(labcode2), labcode, labcode2)
      )

.log_info("Loading HARP Tx Data.")
if (!("harp_tx" %in% names(nhsss))) {
   nhsss$harp_tx$official$new_reg     <- ohasis$get_data("harp_tx-reg", ohasis$yr, ohasis$mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = id_registry,
         by = "PATIENT_ID"
      ) %>%
      mutate(
         CENTRAL_ID = if_else(
            condition = is.na(CENTRAL_ID),
            true      = PATIENT_ID,
            false     = CENTRAL_ID
         )
      )

   nhsss$harp_tx$official$new_outcome <- ohasis$get_data("harp_tx-outcome", ohasis$yr, ohasis$mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = nhsss$harp_tx$official$new_reg %>% select(art_id, CENTRAL_ID),
         by = "art_id"
      )
}

.log_success("Done!")