# set year/month coverage
set_coverage <- function(yr = NULL, mo = NULL) {
   params    <- list()
   params$mo <- ifelse(!is.null(mo), mo, input(prompt = "What is the reporting month?", max.char = 2))
   params$mo <- stri_pad_left(params$mo, width = 2, pad = "0")
   params$yr <- ifelse(!is.null(yr), yr, input(prompt = "What is the reporting year?", max.char = 4))
   params$yr <- stri_pad_left(params$yr, width = 4, pad = "0")
   params$ym <- paste0(params$yr, ".", params$mo)

   params$min <- paste(sep = "-", params$yr, params$mo, "01")
   params$max <- params$min %>%
      as.Date() %>%
      ceiling_date(unit = "month") %m-%
      days(1) %>%
      as.character()

   params$prev_mo <- stri_pad_left(month(as.Date(params$min) %m-% months(1)), 2, "0")
   params$prev_yr <- stri_pad_left(year(as.Date(params$min) %m-% months(1)), 2, "0")

   return(params)
}

# take latest datasets based on reporting month
load_harp <- function(params = NULL) {
   harp    <- list()
   harp$dx <- hs_data("harp_dx", "reg", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            REC_ID,
            PATIENT_ID,
            year,
            month,
            idnum,
            firstname,
            middle,
            last,
            name_suffix,
            bdate,
            philhealth,
            labcode2,
            uic,
            sex,
            transmit,
            sexhow,
            confirmlab,
            confirm_date,
            self_identity,
            self_identity_other,
            gender_identity,
            age,
            dxlab_standard,
            dx_region,
            dx_province,
            dx_muncity,
            TEST_FACI,
            baseline_cd4_date,
            baseline_cd4_result,
            pregnant,
            rhivda_done,
            region,
            province,
            muncity
         )
      ) %>%
      left_join(
         y  = hs_data("harp_full", "reg", params$yr, params$mo) %>%
            read_dta(
               col_select = c(
                  idnum,
                  dead,
                  mort,
                  outcome
               )
            ),
         by = join_by(idnum)
      ) %>%
      left_join(
         y  = read_dta(hs_data("harp_vl", "naive_dx", params$yr, params$mo)),
         by = join_by(idnum)
      ) %>%
      mutate_if(
         .predicate = is.character,
         ~str_squish(.) %>%
            if_else(. == "", NA_character_, ., .)
      )

   harp$tx <- hs_data("harp_tx", "outcome", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            REC_ID,
            art_id,
            idnum,
            latest_ffupdate,
            latest_nextpickup,
            latest_regimen,
            hub,
            branch,
            realhub,
            realhub_branch,
            vlp12m,
            baseline_vl,
            vlp12m,
            vl_date,
            vl_result,
            sex,
            curr_age,
            onart,
            line,
            reg_line,
            artstart_date,
            outcome,
            art_reg
         )
      ) %>%
      left_join(
         y  = hs_data("harp_tx", "reg", params$yr, params$mo) %>%
            read_dta(
               col_select = c(
                  PATIENT_ID,
                  art_id,
                  first,
                  middle,
                  last,
                  suffix,
                  birthdate,
                  confirmatory_code,
                  px_code,
                  uic,
                  philhealth_no,
                  philsys_id,
                  baseline_cd4_date,
                  baseline_cd4_result,
               )
            ),
         by = join_by(art_id)
      ) %>%
      left_join(
         y  = hs_data("harp_tx", "outcome", params$prev_yr, params$prev_mo) %>%
            read_dta(
               col_select = c(
                  art_id,
                  latest_nextpickup,
                  outcome
               )
            ) %>%
            rename(
               previous_next_pickup = latest_nextpickup,
               previous_outcome     = outcome
            ),
         by = join_by(art_id)
      ) %>%
      left_join(
         y  = read_dta(hs_data("harp_vl", "naive_tx", params$yr, params$mo)),
         by = join_by(art_id)
      ) %>%
      mutate_if(
         .predicate = is.character,
         ~str_squish(.) %>%
            if_else(. == "", NA_character_, ., .)
      )

   harp$prep <- hs_data("prep", "outcome", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            REC_ID,
            prep_id,
            idnum,
            latest_ffupdate,
            latest_nextpickup,
            latest_regimen,
            faci,
            branch,
            sex,
            curr_age,
            onprep,
            prepstart_date,
            outcome
         )
      ) %>%
      left_join(
         y  = hs_data("prep", "reg", params$yr, params$mo) %>%
            read_dta(
               col_select = c(
                  PATIENT_ID,
                  prep_id,
                  first,
                  middle,
                  last,
                  suffix,
                  birthdate,
                  px_code,
                  uic,
                  philhealth_no,
                  philsys_id,
               )
            ),
         by = join_by(prep_id)
      ) %>%
      mutate_if(
         .predicate = is.character,
         ~str_squish(.) %>%
            if_else(. == "", NA_character_, ., .)
      )

   return(harp)
}

# take ohasis records for facilities
get_oh <- function(harp) {
   oh <- list()
   .log_info("Getting OHASIS dx records.")
   lw_conn   <- ohasis$conn("lw")
   dbname    <- "ohasis_warehouse"
   rec_ids   <- paste(collapse = "','", harp$dx$REC_ID)
   a         <- dbTable(
      lw_conn,
      dbname,
      "form_a",
      cols      = c(
         "REC_ID",
         "FACI_ID",
         "SUB_FACI_ID",
         "SERVICE_FACI",
         "SERVICE_SUB_FACI"
      ),
      where     = paste0("REC_ID IN ('", rec_ids, "')"),
      raw_where = TRUE
   )
   hts       <- dbTable(
      lw_conn,
      dbname,
      "form_hts",
      cols      = c(
         "REC_ID",
         "FACI_ID",
         "SUB_FACI_ID",
         "SERVICE_FACI",
         "SERVICE_SUB_FACI"
      ),
      where     = paste0("REC_ID IN ('", rec_ids, "')"),
      raw_where = TRUE
   )
   oh$dx     <- bind_rows(hts, a) %>% distinct_all()
   oh$tx     <- tracked_select(lw_conn, r"(
SELECT DISTINCT COALESCE(id.CENTRAL_ID, art.PATIENT_ID) AS CENTRAL_ID,
                CASE
                    WHEN SERVICE_FACI = '130000' THEN FACI_ID
                    WHEN SERVICE_FACI IS NULL THEN FACI_ID
                    ELSE SERVICE_FACI END               AS FACI_ID,
                SERVICE_SUB_FACI                        AS SUB_FACI_ID
FROM ohasis_warehouse.form_art_bc AS art
         LEFT JOIN ohasis_warehouse.id_registry AS id ON art.PATIENT_ID = id.PATIENT_ID;
   )", "OHASIS tx")
   oh$prep   <- tracked_select(lw_conn, r"(
SELECT DISTINCT COALESCE(id.CENTRAL_ID, prep.PATIENT_ID) AS CENTRAL_ID,
                CASE
                    WHEN SERVICE_FACI = '130000' THEN FACI_ID
                    WHEN SERVICE_FACI IS NULL THEN FACI_ID
                    ELSE SERVICE_FACI END               AS FACI_ID,
                SERVICE_SUB_FACI                        AS SUB_FACI_ID
FROM ohasis_warehouse.form_prep AS prep
         LEFT JOIN ohasis_warehouse.id_registry AS id ON art.PATIENT_ID = id.PATIENT_ID;
   )", "OHASIS PrEP")
   oh$id_reg <- dbTable(
      lw_conn,
      dbname,
      "id_registry",
      cols = c(
         "CENTRAL_ID",
         "PATIENT_ID"
      )
   )

   hts_where <- glue(r"(
   (YEAR(RECORD_DATE) >= 2021) OR
      (YEAR(DATE_CONFIRM) >= 2021) OR
      (YEAR(T3_DATE) >= 2021) OR
      (YEAR(T2_DATE) >= 2021) OR
      (YEAR(T1_DATE) >= 2021) OR
      (YEAR(T0_DATE) >= 2021)
   )")
   cbs_where <- glue(r"(
   (YEAR(RECORD_DATE) >= 2021) OR
      (YEAR(TEST_DATE) >= 2021)
   )")

   oh$forms           <- list()
   oh$forms$form_hts  <- dbTable(lw_conn, dbname, "form_hts", where = hts_where, raw_where = TRUE)
   oh$forms$form_a    <- dbTable(lw_conn, dbname, "form_a", where = hts_where, raw_where = TRUE)
   oh$forms$form_cfbs <- dbTable(lw_conn, dbname, "form_cfbs", where = cbs_where, raw_where = TRUE)
   dbDisconnect(lw_conn)

   return(oh)
}

.init <- function(envir = parent.env(environment()), yr = NULL, mo = NULL) {
   p        <- envir
   p$params <- set_coverage(yr, mo)
   p$harp   <- load_harp(p$params)
   p$oh     <- get_oh(p$harp)
}