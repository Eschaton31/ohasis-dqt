##  Download records -----------------------------------------------------------

download_data <- function(dir) {

   query   <- read_file(file.path(dir, "t1_for_dedup.sql"))
   db_conn <- ohasis$conn("db")

   data           <- list()
   data$for_dedup <- tracked_select(db_conn, query, "t1_for_dedup")
   data$id_reg    <- dbTable(db_conn, "ohasis_interim", "registry", cols = c("PATIENT_ID", "CENTRAL_ID"))
   dbDisconnect(db_conn)

   return(data)
}

##  Get the previous report's HARP Registry ------------------------------------

load_harp <- function(id_reg) {
   harp <- list()

   log_info("Getting HARP Dx Dataset.")
   harp$dx <- hs_data("harp_dx", "reg", ohasis$yr, ohasis$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            idnum,
            labcode2,
            transmit,
            sexhow,
            year,
            month,
            firstname,
            middle,
            last,
            name_suffix,
            sex,
            bdate,
            uic,
            philhealth,
            self_identity,
            self_identity_other,
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
      get_cid(id_reg, PATIENT_ID) %>%
      mutate(
         ref_report = as.Date(stri_c(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
      ) %>%
      dxlab_to_id(
         c("HARPDX_FACI", "HARPDX_SUB_FACI"),
         c("dx_region", "dx_province", "dx_muncity", "dxlab_standard")
      ) %>%
      ohasis$get_faci(
         list(DX_LAB = c("HARPDX_FACI", "HARPDX_SUB_FACI")),
         "name"
      )

   return(harp)
}

.init <- function(envir = parent.env(environment())) {
   p      <- envir
   p$data <- download_data(p$wd)
   p$harp <- load_harp(p$data$id_reg)
}
