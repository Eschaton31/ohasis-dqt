dr       <- new.env()
dr$drive <- gdrive_endpoint("DSA - GF", "2022.07")
dr$corr  <- gdrive_correct(dr$drive, "2022.07")

local(envir = dr, {
   harp <- list()

   .log_info("Getting HARP Dx Dataset.")
   harp$dx <- ohasis$get_data("harp_dx", "2022", "07") %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
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
         ),
      )

   .log_info("Getting the new HARP Tx Datasets.")
   harp$tx$new_reg <- ohasis$get_data("harp_tx-reg", "2022", "07") %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
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
         ),
      )

   harp$tx$new_outcome <- ohasis$get_data("harp_tx-outcome", "2022", "07") %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = harp$tx$new_reg %>%
            select(art_id, CENTRAL_ID),
         by = "art_id"
      )

   .log_info("Getting HARP Dead Dataset.")
   harp$dead_reg <- ohasis$get_data("harp_dead", "2022", "07") %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = id_registry,
         by = "PATIENT_ID"
      ) %>%
      mutate(
         CENTRAL_ID       = if_else(
            condition = is.na(CENTRAL_ID),
            true      = PATIENT_ID,
            false     = CENTRAL_ID
         ),
         proxy_death_date = as.Date(ceiling_date(as.Date(paste(sep = '-', year, month, '01')), unit = 'month')) - 1,
         ref_death_date   = if_else(
            condition = is.na(date_of_death),
            true      = proxy_death_date,
            false     = date_of_death
         )
      )

   .log_info("Getting the new PrEP Datasets.")
   harp$prep$new_reg <- ohasis$get_data("prep-reg", "2022", "07") %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
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
         ),
      )

   harp$prep$new_outcome <- ohasis$get_data("prep-outcome", "2022", "07") %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = harp$prep$new_reg %>%
            select(prep_id, CENTRAL_ID),
         by = "prep_id"
      )
})

lw_conn        <- ohasis$conn("lw")
dr$ohasis_uic  <- dbTable(
   lw_conn,
   "ohasis_lake",
   "px_pii",
   cols      = c(
      "REC_ID",
      "PATIENT_ID",
      "UIC",
      "FACI_ID"
   ),
   join      = list(
      "ohasis_lake.px_faci_info" = list(by = c("REC_ID" = "REC_ID"), cols = c("REC_ID", "SERVICE_FACI"), type = "left")
   ),
   raw_where = TRUE,
   where     = r"(UIC IS NOT NULL)",
   name      = "pii"
) %>%
   distinct(
      PATIENT_ID,
      UIC,
      FACI_ID,
      SERVICE_FACI
   )
dr$id_registry <- dbTable(
   lw_conn,
   "ohasis_warehouse",
   "id_registry",
   cols = c("CENTRAL_ID", "PATIENT_ID")
)
dbDisconnect(lw_conn)
rm(lw_conn)

source("src/data_requests/20220816-retro_uic_gf/02_loghseet_psfi.R")
source("src/data_requests/20220816-retro_uic_gf/03_combine.R")
source("src/data_requests/20220816-retro_uic_gf/04_linked.R")

write_dta(
   format_stata(dr$linked),
   "H:/Data Requests/20220816_retro-uic/gf_uic_linked_2019-2021.dta"
)