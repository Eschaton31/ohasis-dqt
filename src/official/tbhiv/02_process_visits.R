##  Filter Initial Data & Remove Already Reported ------------------------------

standardize_visits <- function(form_art, coverage) {
   log_info("Standardizing visits.")
   curr_yr <- as.numeric(coverage$yr)
   data    <- form_art %>%
      # tag tbhiv info
      mutate(
         visit_yr         = year(VISIT_DATE),
         visit_mo         = month(VISIT_DATE),
         visit_cnt        = case_when(
            visit_yr == curr_yr ~ 1,
            TRUE ~ 0
         ),
         visit_scrn       = case_when(
            !is.na(TB_SCREEN) ~ 1,
            !is.na(TB_STATUS) ~ 1,
            year(LAB_XRAY_DATE) == curr_yr ~ 1,
            year(LAB_XPERT_DATE) == curr_yr ~ 1,
            year(LAB_DSSM_DATE) == curr_yr ~ 1,
            TRUE ~ 0
         ),
         visit_scrn_tri   = case_when(
            !is.na(TB_SCREEN) ~ 1,
            TRUE ~ 0
         ),
         visit_scrn_tb    = case_when(
            !is.na(TB_STATUS) ~ 1,
            TRUE ~ 0
         ),
         visit_scrn_labs  = case_when(
            year(LAB_XRAY_DATE) == curr_yr ~ 1,
            year(LAB_XPERT_DATE) == curr_yr ~ 1,
            year(LAB_DSSM_DATE) == curr_yr ~ 1,
            TRUE ~ 0
         ),
         visit_withtb     = case_when(
            StrLeft(TB_STATUS, 1) == '1' ~ 1,
            StrLeft(TB_STATUS, 1) == '0' ~ 0,
            # TRUE ~ 0
         ),
         visit_startedipt = case_when(
            StrLeft(TB_IPT_STATUS, 2) == '12' ~ 1,
            year(TB_IPT_START_DATE) == curr_yr ~ 1,
            # year(IPT_PROXY_START) == curr_yr ~ 1,
            # TRUE ~ 0
         ),
         visit_onipt      = case_when(
            StrLeft(TB_IPT_STATUS, 2) == '11' ~ 1,
            StrLeft(TB_IPT_STATUS, 2) == '12' ~ 1,
            StrLeft(TB_IPT_STATUS, 2) == '13' ~ 0,
            StrLeft(TB_IPT_STATUS, 1) == '0' ~ 0,
            # TRUE ~ 0
         ),
         visit_endipt     = case_when(
            StrLeft(TB_IPT_STATUS, 2) == '13' ~ 1, # new code
            StrLeft(TB_IPT_OUTCOME, 1) == '1' ~ 1, # new code
            StrLeft(TB_IPT_OUTCOME, 1) == '2' ~ 1, # new code
            # TRUE ~ 0
         ),
         visit_tbtx       = case_when(
            !is.na(TB_TX_OUTCOME) ~ 1,
            !is.na(TB_REGIMEN) ~ 1,
            StrLeft(TB_TX_STATUS, 2) == '11' ~ 1,
            StrLeft(TB_TX_STATUS, 2) == '12' ~ 1,
            StrLeft(TB_TX_STATUS, 2) == '13' ~ 0,
            StrLeft(TB_TX_STATUS, 1) == '0' ~ 0,
            # TRUE ~ 0
         ),
         visit_startedtx  = case_when(
            StrLeft(TB_TX_STATUS, 2) == '12' ~ 1,
            year(TB_TX_START_DATE) == curr_yr ~ 1,
            # year(TX_PROXY_START) == curr_yr ~ 1,
            # TRUE ~ 0
         ),
         visit_ontx       = case_when(
            StrLeft(TB_TX_STATUS, 2) == '11' ~ 1,
            StrLeft(TB_TX_STATUS, 2) == '12' ~ 1,
            !is.na(TB_REGIMEN) ~ 1,
            StrLeft(TB_TX_STATUS, 2) == '13' ~ 0,
            StrLeft(TB_TX_STATUS, 1) == '0' ~ 0,
            # TRUE ~ 0
         ),
         visit_endtx      = case_when(
            StrLeft(TB_TX_STATUS, 2) == '13' ~ 1,  # new code
            StrLeft(TB_TX_OUTCOME, 2) == '10' ~ 1, # new code
            StrLeft(TB_TX_OUTCOME, 2) == '11' ~ 1, # new code
            StrLeft(TB_TX_OUTCOME, 2) == '20' ~ 1, # new code
            # TRUE ~ 0
         ),
         visit_curetx     = case_when(
            StrLeft(TB_TX_OUTCOME, 2) == '11' ~ 1, # new code
            # TRUE ~ 0
         ),
         visit_failtx     = case_when(
            StrLeft(TB_TX_OUTCOME, 2) == '20' ~ 1, # new code
            # TRUE ~ 0
         ),
      ) %>%
      arrange(VISIT_DATE)

   return(data)
}

##  Group per coverage ---------------------------------------------------------

group_coverage <- function(harp_tx, standard, min, max) {

   .max <- function(..., def = 9999, na.rm = FALSE) {
      if (!is.infinite(x <- suppressWarnings(max(..., na.rm = na.rm)))) x else def
   }

   data <- harp_tx %>%
      left_join(
         y        = standard %>%
            filter(VISIT_DATE %within% interval(min, max)),
         by       = join_by(CENTRAL_ID),
         multiple = "all"
      ) %>%
      group_by(art_id) %>%
      summarise(
         visit         = .max(visit_cnt, na.rm = TRUE),
         screened      = .max(visit_scrn, na.rm = TRUE),
         screened_tri  = .max(visit_scrn_tri, na.rm = TRUE),
         screened_tb   = .max(visit_scrn_tb, na.rm = TRUE),
         screened_labs = .max(visit_scrn_labs, na.rm = TRUE),
         withtb        = .max(visit_withtb, na.rm = TRUE),
         startedipt    = .max(visit_startedipt, na.rm = TRUE),
         onipt         = .max(visit_onipt, na.rm = TRUE),
         endipt        = .max(visit_endipt, na.rm = TRUE),
         startedtx     = .max(visit_startedtx, na.rm = TRUE),
         ontx          = .max(visit_ontx, na.rm = TRUE),
         endtx         = .max(visit_endtx, na.rm = TRUE),
         curetx        = .max(visit_curetx, na.rm = TRUE),
         failtx        = .max(visit_failtx, na.rm = TRUE),
      ) %>%
      ungroup() %>%
      left_join(
         y  = harp_tx %>%
            select(
               art_id,
               idnum,
               artstart_date,
               sex,
               hub,
               branch,
               realhub,
               realhub_branch,
               curr_age,
               outcome,
               latest_ffupdate
            ),
         by = join_by(art_id)
      ) %>%
      mutate(
         hub    = if_else(branch == "SAIL-CALOOCAN", "SHP", hub, hub),
         branch = NA_character_
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         c(TX_FACI = "hub", TX_SUB_FACI = "branch")
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         c(REAL_FACI = "realhub", REAL_SUB_FACI = "realhub_branch")
      ) %>%
      mutate_at(
         .vars = vars(TX_FACI, REAL_FACI),
         ~if_else(. == "130000", NA_character_, ., .),
      ) %>%
      distinct(art_id, .keep_all = TRUE) %>%
      rename(ART_FACI_CODE = hub) %>%
      mutate(
         SEX       = case_when(
            sex == "MALE" ~ "M",
            sex == "FEMALE" ~ "F",
         ),
         onart     = case_when(
            outcome == "alive on arv" ~ 1,
            TRUE ~ 0
         ),
         final_new = case_when(
            artstart_date %within% interval(min, max) ~ 1,
            TRUE ~ 0
         ),
         rep_month = as.numeric(month(max)),
         curr_age  = floor(curr_age),
         Age_Band  = case_when(
            curr_age >= 0 & curr_age < 15 ~ '<15',
            curr_age >= 15 & curr_age < 25 ~ '15-24',
            curr_age >= 25 & curr_age < 35 ~ '25-34',
            curr_age >= 35 & curr_age < 50 ~ '35-49',
            curr_age >= 50 & curr_age < 1000 ~ '50+',
            TRUE ~ '(no data)'
         ),
      ) %>%
      ohasis$get_faci(
         list(TX_HUB = c("TX_FACI", "TX_SUB_FACI")),
         "name",
         c("TX_REG", "TX_PROV", "TX_MUNC")
      ) %>%
      ohasis$get_faci(
         list(REAL_HUB = c("REAL_FACI", "REAL_SUB_FACI")),
         "name",
         c("REAL_REG", "REAL_PROV", "REAL_MUNC")
      )

   return(data)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment())) {
   envir$data          <- list()
   envir$data$standard <- standardize_visits(envir$forms$form_art_bc, envir$coverage)

   log_info("Creating linelist - {green('Monthly')}.")
   envir$data$mo <- group_coverage(envir$harp$tx$new, envir$data$standard, envir$coverage$min$mo, envir$coverage$max)
   log_info("Creating linelist - {green('Quarterly')}.")
   envir$data$qr <- group_coverage(envir$harp$tx$new, envir$data$standard, envir$coverage$min$qr, envir$coverage$max)
   log_info("Creating linelist - {green('Semestral')}.")
   envir$data$sy <- group_coverage(envir$harp$tx$new, envir$data$standard, envir$coverage$min$sy, envir$coverage$max)
   log_info("Creating linelist - {green('Annual')}.")
   envir$data$yr <- group_coverage(envir$harp$tx$new, envir$data$standard, envir$coverage$min$yr, envir$coverage$max)
}