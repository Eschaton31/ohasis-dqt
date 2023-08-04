##  Filter Initial Data & Remove Already Reported ------------------------------

standardize_visits <- function(forms, coverage, itis_tpt = NULL, itis_drtb = NULL) {
   if (!is.null(itis_tpt)) {
      log_info("Processing ITIS TPT.")

      itis_tpt %<>%
         inner_join(forms$id_itis, join_by(ITIS_PATIENT_ID)) %>%
         get_cid(forms$id_reg, PATIENT_ID) %>%
         mutate(
            LAB_XPERT_RESULT = str_c(sep = "; ", XPERT_RESULT_MTB, XPERT_RESULT_RIF),
            LAB_XRAY_RESULT  = str_c(sep = "; ", X_RAY_PRESENTATION, X_RAY_EVOLUTION),
            TB_IPT_STATUS    = case_when(
               OUTCOME_STATUS == "ON TREATMENT" ~ "11_Ongoing IPT",
               OUTCOME_STATUS == "TREATMENT COMPLETED" ~ "13_Ended IPT",
               OUTCOME_STATUS == "CURED" ~ "13_Ended IPT",
               OUTCOME_STATUS == "COMPLETED" ~ "13_Ended IPT",
               OUTCOME_STATUS == "NOT EVALUATED" ~ "13_Ended IPT",
               OUTCOME_STATUS == "FAILED" ~ "13_Ended IPT",
               OUTCOME_STATUS == "DIAGNOSED" ~ "13_Ended IPT",
               OUTCOME_STATUS == "LOST TO FF-UP" ~ "13_Ended IPT",
            ),
            TB_IPT_STATUS    = if_else(is.na(DATE_STARTED_TX), NA_character_, TB_IPT_STATUS, TB_IPT_STATUS),
            TB_IPT_OUTCOME   = case_when(
               OUTCOME_STATUS == "ON TREATMENT" ~ "8888_Other",
               OUTCOME_STATUS == "TREATMENT COMPLETED" ~ "1_Completed",
               OUTCOME_STATUS == "CURED" ~ "1_Completed",
               OUTCOME_STATUS == "COMPLETED" ~ "1_Completed",
               OUTCOME_STATUS == "NOT EVALUATED" ~ "8888_Other",
               OUTCOME_STATUS == "FAILED" ~ "1_Completed",
               OUTCOME_STATUS == "LOST TO FF-UP" ~ "2_Stopped before target end",
            ),
            TB_IPT_OUTCOME   = if_else(is.na(DATE_STARTED_TX), NA_character_, TB_IPT_OUTCOME, TB_IPT_OUTCOME)
         ) %>%
         mutate_at(
            .vars = vars(ends_with("RESULT")),
            ~na_if(., "; ") %>%
               na_if("Not Done; N/A")
         ) %>%
         mutate_if(
            .predicate = is.Date,
            ~na_if(., as.Date("1970-01-01"))
         ) %>%
         mutate_if(
            .predicate = is.character,
            ~na_if(str_squish(.), "")
         ) %>%
         select(
            CENTRAL_ID,
            PATIENT_ID,
            VISIT_DATE        = REGISTRATION_DATE,
            LAB_XPERT_DATE    = XPERT_SPECIMEN_RELEASE_DATE,
            LAB_XPERT_RESULT,
            LAB_XRAY_DATE     = X_RAY_DATE,
            LAB_XRAY_RESULT,
            LAB_DSSM_DATE     = DSSM_RELEASE_DATE,
            LAB_DSSM_RESULT   = DSSM_RESULT,
            TB_IPT_START_DATE = DATE_STARTED_TX,
            TB_IPT_END_DATE   = TREATMENT_END_DATE,
            TB_IPT_STATUS,
            TB_IPT_OUTCOME
         ) %>%
         mutate(
            TB_SCREEN = "1_Yes",
            TB_STATUS = "0_No active TB"
         )
   }
   if (!is.null(itis_drtb)) {
      log_info("Processing ITIS DRTB.")

      itis_drtb %<>%
         inner_join(forms$id_itis, join_by(ITIS_PATIENT_ID)) %>%
         get_cid(forms$id_reg, PATIENT_ID) %>%
         mutate(
            LAB_XPERT_RESULT = str_c(sep = "; ", XPERT_RESULT_MTB, XPERT_RESULT_RIF),
            LAB_XRAY_RESULT  = str_c(sep = "; ", X_RAY_PRESENTATION, X_RAY_EVOLUTION),
            TB_TX_STATUS     = case_when(
               OUTCOME_STATUS == "ON TREATMENT" ~ "11_Ongoing Tx",
               OUTCOME_STATUS == "TREATMENT COMPLETED" ~ "13_Ended Tx",
               OUTCOME_STATUS == "CURED" ~ "13_Ended Tx",
               OUTCOME_STATUS == "COMPLETED" ~ "13_Ended Tx",
               OUTCOME_STATUS == "NOT EVALUATED" ~ "13_Ended Tx",
               OUTCOME_STATUS == "DIED" ~ "13_Ended Tx",
               OUTCOME_STATUS == "FAILED" ~ "13_Ended Tx",
               OUTCOME_STATUS == "DIAGNOSED" ~ "13_Ended Tx",
               OUTCOME_STATUS == "LOST TO FF-UP" ~ "13_Ended Tx",
            ),
            TB_TX_STATUS     = if_else(is.na(DATE_STARTED_TX), NA_character_, TB_TX_STATUS, TB_TX_STATUS),
            TB_TX_OUTCOME    = case_when(
               OUTCOME_STATUS == "ON TREATMENT" ~ "8888_Other",
               OUTCOME_STATUS == "TREATMENT COMPLETED" ~ "10_Not yet evaluated",
               OUTCOME_STATUS == "CURED" ~ "11_Cured",
               OUTCOME_STATUS == "NOT EVALUATED" ~ "10_Not yet evaluated",
               OUTCOME_STATUS == "DIED" ~ "20_Failed",
               OUTCOME_STATUS == "FAILED" ~ "20_Failed",
               OUTCOME_STATUS == "LOST TO FF-UP" ~ "8888_Other",
            ),
            TB_TX_OUTCOME    = if_else(is.na(DATE_STARTED_TX), NA_character_, TB_TX_OUTCOME, TB_TX_OUTCOME)
         ) %>%
         select(
            CENTRAL_ID,
            PATIENT_ID,
            VISIT_DATE       = REGISTRATION_DATE,
            LAB_XPERT_DATE   = XPERT_SPECIMEN_RELEASE_DATE,
            LAB_XPERT_RESULT,
            LAB_XRAY_DATE    = X_RAY_DATE,
            LAB_XRAY_RESULT,
            LAB_DSSM_DATE    = DSSM_RELEASE_DATE,
            LAB_DSSM_RESULT  = DSSM_RESULT,
            TB_TX_START_DATE = DATE_STARTED_TX,
            TB_TX_END_DATE   = TREATMENT_END_DATE,
            TB_REGIMEN       = TREATMENT_REGIMEN,
            TB_TX_STATUS,
            TB_TX_OUTCOME
         ) %>%
         mutate_at(
            .vars = vars(ends_with("RESULT")),
            ~na_if(., "; ") %>%
               na_if("Not Done; N/A")
         ) %>%
         mutate_if(
            .predicate = is.Date,
            ~na_if(., as.Date("1970-01-01"))
         ) %>%
         mutate_if(
            .predicate = is.character,
            ~na_if(str_squish(.), "")
         ) %>%
         mutate(
            TB_SCREEN = "1_Yes",
            TB_STATUS = "1_With active TB"
         )
   }

   log_info("Standardizing visits.")
   curr_yr <- as.numeric(coverage$yr)
   data    <- forms$form_art_bc %>%
      bind_rows(itis_tpt) %>%
      bind_rows(itis_drtb) %>%
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

   tx_visits <- harp_tx %>%
      left_join(
         y        = standard %>%
            filter(VISIT_DATE %within% interval(min, max)),
         by       = join_by(CENTRAL_ID),
         multiple = "all"
      ) %>%
      select(
         art_id,
         visit         = visit_cnt,
         screened      = visit_scrn,
         screened_tri  = visit_scrn_tri,
         screened_tb   = visit_scrn_tb,
         screened_labs = visit_scrn_labs,
         withtb        = visit_withtb,
         startedipt    = visit_startedipt,
         onipt         = visit_onipt,
         endipt        = visit_endipt,
         startedtx     = visit_startedtx,
         ontx          = visit_ontx,
         endtx         = visit_endtx,
         curetx        = visit_curetx,
         failtx        = visit_failtx,
      )

   flags <- c(
      "visit",
      "screened",
      "screened_tri",
      "screened_tb",
      "screened_labs",
      "withtb",
      "startedipt",
      "onipt",
      "endipt",
      "startedtx",
      "ontx",
      "endtx",
      "curetx",
      "failtx"
   )
   data  <- harp_tx %>% select(art_id)
   for (flag in flags) {
      flag <- as.name(flag)
      data %<>%
         left_join(
            y  = tx_visits %>%
               arrange(desc({{flag}})) %>%
               select(art_id, {{flag}}) %>%
               distinct(art_id, .keep_all = TRUE),
            by = join_by(art_id)
         )
   }

   data %<>%
      mutate_at(
         .vars = vars(all_of(flags)),
         ~as.integer(coalesce(., 9999))
      ) %>%
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

   itis_tpt            <- read_dta("H:/_R/library/itis_tpt/20230701_itis-tpt_2023-06.dta")
   itis_drtb           <- read_dta("H:/_R/library/itis_tbtx/20230701_itis-drtb_2023-06.dta")
   envir$data          <- list()
   envir$data$standard <- standardize_visits(envir$forms, envir$coverage, itis_tpt, itis_drtb)

   log_info("Creating linelist - {green('Monthly')}.")
   envir$data$mo <- group_coverage(envir$harp$tx$new, envir$data$standard, envir$coverage$min$mo, envir$coverage$max)
   log_info("Creating linelist - {green('Quarterly')}.")
   envir$data$qr <- group_coverage(envir$harp$tx$new, envir$data$standard, envir$coverage$min$qr, envir$coverage$max)
   log_info("Creating linelist - {green('Semestral')}.")
   envir$data$sy <- group_coverage(envir$harp$tx$new, envir$data$standard, envir$coverage$min$sy, envir$coverage$max)
   log_info("Creating linelist - {green('Annual')}.")
   envir$data$yr <- group_coverage(envir$harp$tx$new, envir$data$standard, envir$coverage$min$yr, envir$coverage$max)
}