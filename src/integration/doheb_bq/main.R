p_load(bigrquery)

update_version <- function(sys, ym) {
   version <- tibble(
      dataset             = sys,
      version             = ym,
      datetime_lastupdate = Sys.time()
   )

   versions <- bq_table(Sys.getenv("BQ_PROJECT"), Sys.getenv("BQ_DATASET"), "versions")
   bq_table_upload(versions, version)
}

upsert_bq <- function(sys, yr, mo) {
   ym   <- stri_c(yr, "-", stri_pad_left(mo, 2, "0"))
   data <- hs_data(sys, "outcome", yr, mo) %>%
      read_dta() %>%
      remove_pii() %>%
      mutate(
         newonart = if_else(
            artstart_year == as.numeric(yr) & artstart_month == as.numeric(mo),
            1,
            0, 0
         )
      ) %>%
      select(
         -any_of(c(
            "REC_ID",
            "concat_col",
            ""
         ))
      )

   sys <- case_when(
      sys == "harp_tx" ~ "onart",
      TRUE ~ sys
   )
   report <- stri_c(sys, "_", ym)
   table  <- bq_table(Sys.getenv("BQ_PROJECT"), Sys.getenv("BQ_DATASET"), report)

   if (bq_table_exists(table)) {
      bq_table_delete(table)
   }

   bq_table_create(table, data)
   bq_table_upload(table, data)
   update_version(sys, ym)
}

upsert_bq("harp_tx", 2023, 9)