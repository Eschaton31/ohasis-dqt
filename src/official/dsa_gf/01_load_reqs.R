##  Generate pre-requisites and endpoints --------------------------------------

check <- input(
   prompt  = glue("Check the {green('GDrive Endpoints')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Checking endpoints.")
   local(envir = gf, {
      gdrive      <- list()
      gdrive$path <- gdrive_endpoint("DSA - GF", ohasis$ym)
   })
}

check <- input(
   prompt  = glue("Re-download the {green('data corrections')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Downloading corrections list.")
   local(envir = gf, {
      .log_info("Getting corrections.")
      corr <- gdrive_correct(gdrive$path, ohasis$ym)
   })
}

check <- input(
   prompt  = glue("Re-download the {green('DSA-signed sites')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Downloading GF List of sites.")
   gf$sites <- read_sheet("1y0i8l-HNieOIQ1QGIezVLltwut3y1FxHryvhno9GNb4") %>%
      distinct(FACI_ID, .keep_all = TRUE) %>%
      rename_at(
         .vars = vars(starts_with("DSA")),
         ~"DSA"
      ) %>%
      filter(DSA == TRUE) %>%
      mutate(WITH_DSA = 1)
}

# run through all tables
check <- input(
   prompt  = glue("Update {green('data/forms')} to be used for consolidation?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Updating data lake and data warehouse.")
   local(envir = gf, invisible({
      tables           <- list()
      tables$lake      <- c("lab_wide", "disp_meds")
      tables$warehouse <- c("form_art_bc", "form_prep", "form_a", "form_hts", "form_cfbs", "id_registry")

      lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
      lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }))
}

##  Get the previous report's HARP Registry ------------------------------------

check <- input(
   prompt  = "Reload HARP Datasets?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Downloading OHASIS IDs.")
   lw_conn     <- ohasis$conn("lw")
   id_registry <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry")) %>%
      select(CENTRAL_ID, PATIENT_ID) %>%
      collect()
   dbDisconnect(lw_conn)

   local(envir = gf, {
      harp <- list()

      .log_info("Getting HARP Dx Dataset.")
      harp$dx <- ohasis$get_data("harp_dx", ohasis$yr, ohasis$mo) %>%
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

      .log_info("Getting HARP Tx Dataset.")
      harp$tx_reg <- ohasis$get_data("harp_tx-reg", ohasis$yr, ohasis$mo) %>%
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
   })

   gf$id_registry <- id_registry
   rm(id_registry, lw_conn)
}
rm(check)