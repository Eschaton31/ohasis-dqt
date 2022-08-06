##  HARP Tx Linkage Controller -------------------------------------------------

# define datasets
if (!exists("nhsss"))
   nhsss <- list()

if (!("tbhiv" %in% names(nhsss)))
   nhsss$tbhiv <- new.env()

nhsss$tbhiv$wd               <- file.path(getwd(), "src", "official", "tbhiv")
nhsss$tbhiv$coverage$curr_yr <- "2022"
nhsss$tbhiv$coverage$curr_mo <- "06"
nhsss$tbhiv$coverage$prev_yr <- "2022"
nhsss$tbhiv$coverage$prev_mo <- "03"

##  Begin linkage of art registry ----------------------------------------------

local(envir = nhsss$tbhiv, {
   lw_conn <- ohasis$conn("lw")
   forms   <- list()


   min <- ceiling_date(as.Date(paste(sep = "-", coverage$prev_yr, coverage$prev_mo, "01")), unit = "month")
   max <- ceiling_date(as.Date(paste(sep = "-", coverage$curr_yr, coverage$curr_mo, "01")), unit = "month") - 1

   coverage$min_date <- min
   coverage$max_date <- max

   min <- as.character(min)
   max <- as.character(max)

   .log_info("Downloading {green('Central IDs')}.")
   forms$id_registry <- dbTable(
      lw_conn,
      "ohasis_warehouse",
      "id_registry",
      cols = c("CENTRAL_ID", "PATIENT_ID")
   )

   .log_info("Downloading {green('ART Visits w/in the scope')}.")
   forms$form_art_bc <- dbTable(
      lw_conn,
      "ohasis_warehouse",
      "form_art_bc",
      cols      = c(
         "REC_ID",
         "PATIENT_ID",
         "VISIT_DATE",
         "LAB_XRAY_DATE",
         "LAB_XRAY_RESULT",
         "LAB_XPERT_DATE",
         "LAB_XPERT_RESULT",
         "LAB_DSSM_DATE",
         "LAB_DSSM_RESULT",
         "TB_SCREEN",
         "TB_STATUS",
         "TB_ACTIVE_ALREADY",
         "TB_TX_ALREADY",
         "TB_SITE_P",
         "TB_SITE_EP",
         "TB_DRUG_RESISTANCE",
         "TB_DRUG_RESISTANCE_OTHER",
         "TB_TX_STATUS",
         "TB_TX_STATUS_OTHER",
         "TB_REGIMEN",
         "TB_TX_START_DATE",
         "TB_TX_END_DATE",
         "TB_TX_OUTCOME",
         "TB_TX_OUTCOME_OTHER",
         "TB_IPT_STATUS",
         "TB_IPT_START_DATE",
         "TB_IPT_END_DATE",
         "TB_IPT_OUTCOME",
         "TB_IPT_OUTCOME_OTHER"
      ),
      where     = glue("(VISIT_DATE >= '{min}' AND VISIT_DATE <= '{max}')"),
      raw_where = TRUE
   )

   .log_success("Done.")
   dbDisconnect(lw_conn)
   rm(min, max, lw_conn)
})

local(envir = nhsss$tbhiv, {
   harp <- list()

   .log_info("Getting HARP Dx Dataset.")
   harp$dx <- ohasis$get_data("harp_dx", coverage$curr_yr, coverage$curr_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = forms$id_registry,
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
   harp$tx$new_reg <- ohasis$get_data("harp_tx-reg", coverage$curr_yr, coverage$curr_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = forms$id_registry,
         by = "PATIENT_ID"
      ) %>%
      mutate(
         CENTRAL_ID = if_else(
            condition = is.na(CENTRAL_ID),
            true      = PATIENT_ID,
            false     = CENTRAL_ID
         ),
      )

   harp$tx$new_outcome <- ohasis$get_data("harp_tx-outcome", coverage$curr_yr, coverage$curr_mo) %>%
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
})

.max <- function(..., def = 9999, na.rm = FALSE) {
   if (!is.infinite(x <- suppressWarnings(max(..., na.rm = na.rm)))) x else def
}

.min <- function(..., def = 9999, na.rm = FALSE) {
   if (!is.infinite(x <- suppressWarnings(min(..., na.rm = na.rm)))) x else def
}

nhsss$tbhiv$data <- nhsss$tbhiv$harp$tx$new_reg %>%
   select(-REC_ID, -PATIENT_ID) %>%
   left_join(
      y  = nhsss$tbhiv$forms$form_art_bc %>%
         left_join(
            y  = nhsss$tbhiv$forms$id_registry,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
         ),
      by = "CENTRAL_ID"
   ) %>%
   mutate(
      visit_yr         = year(VISIT_DATE),
      visit_mo         = month(VISIT_DATE),
      visit_cnt        = case_when(
         visit_yr == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
         TRUE ~ 0
      ),
      visit_scrn       = case_when(
         !is.na(TB_SCREEN) ~ 1,
         !is.na(TB_STATUS) ~ 1,
         year(LAB_XRAY_DATE) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
         year(LAB_XPERT_DATE) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
         year(LAB_DSSM_DATE) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
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
         year(LAB_XRAY_DATE) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
         year(LAB_XPERT_DATE) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
         year(LAB_DSSM_DATE) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
         TRUE ~ 0
      ),
      visit_withtb     = case_when(
         StrLeft(TB_STATUS, 1) == '1' ~ 1,
         StrLeft(TB_STATUS, 1) == '0' ~ 0,
         # TRUE ~ 0
      ),
      visit_startedipt = case_when(
         StrLeft(TB_IPT_STATUS, 2) == '12' ~ 1,
         year(TB_IPT_START_DATE) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
         # year(IPT_PROXY_START) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
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
      # visit_tbtx       = case_when(
      #    !is.na(TB_TX_OUTCOME) ~ 1,
      #    !is.na(TB_REGIMEN) ~ 1,
      #    StrLeft(TB_TX_STATUS, 2) == '11' ~ 1,
      #    StrLeft(TB_TX_STATUS, 2) == '12' ~ 1,
      #    StrLeft(TB_TX_STATUS, 2) == '13' ~ 0,
      #    StrLeft(TB_TX_STATUS, 1) == '0' ~ 0,
      #    # TRUE ~ 0
      # ),
      visit_startedtx  = case_when(
         StrLeft(TB_TX_STATUS, 2) == '12' ~ 1,
         year(TB_TX_START_DATE) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
         # year(TX_PROXY_START) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
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
      # onart_official   = if_else(is.na(onart_official), 0, onart_official)
   ) %>%
   arrange(VISIT_DATE) %>%
   group_by(art_id) %>%
   summarise(
      # art_id        = last(art_id),
      # onart_real    = .max((onart_official), na.rm = T),
      # onart         = .max((visit_onart), na.rm = T),
      visit         = .max((visit_cnt), na.rm = T),
      screened      = .max((visit_scrn), na.rm = T),
      screened_tri  = .max((visit_scrn_tri), na.rm = T),
      screened_tb   = .max((visit_scrn_tb), na.rm = T),
      screened_labs = .max((visit_scrn_labs), na.rm = T),
      withtb        = .max((visit_withtb), na.rm = T),
      startedipt    = .max((visit_startedipt), na.rm = T),
      onipt         = .max((visit_onipt), na.rm = T),
      endipt        = .max((visit_endipt), na.rm = T),
      startedtx     = .max((visit_startedtx), na.rm = T),
      ontx          = .max((visit_ontx), na.rm = T),
      endtx         = .max((visit_endtx), na.rm = T),
      curetx        = .max((visit_curetx), na.rm = T),
      failtx        = .max((visit_failtx), na.rm = T),
   ) %>%
   ungroup() %>%
   left_join(
      y  = nhsss$tbhiv$harp$tx$new_reg %>%
         select(
            art_id,
            sex,
            artstart_date,
            idnum
         ),
      by = "art_id"
   ) %>%
   left_join(
      y  = nhsss$tbhiv$harp$tx$new_outcome %>%
         select(
            art_id,
            hub,
            branch,
            curr_age,
            outcome,
            latest_ffupdate
         ),
      by = "art_id"
   ) %>%
   distinct_all() %>%
   # left_join(
   #   y  = ohasis$ref_faci_code %>%
   # 	 mutate(
   # 		FACI_CODE     = case_when(
   # 		   stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
   # 		   stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
   # 		   stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
   # 		   TRUE ~ FACI_CODE
   # 		),
   # 		SUB_FACI_CODE = if_else(
   # 		   condition = nchar(SUB_FACI_CODE) == 3,
   # 		   true      = NA_character_,
   # 		   false     = SUB_FACI_CODE
   # 		),
   # 		SUB_FACI_CODE = case_when(
   # 		   FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
   # 		   FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
   # 		   FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
   # 		   TRUE ~ SUB_FACI_CODE
   # 		),
   # 	 ) %>%
   # 	 select(
   # 		hub     = FACI_CODE,
   # 		branch  = SUB_FACI_CODE,
   # 		TX_FACI = FACI_NAME,
   # 		TX_REG  = FACI_NAME_REG,
   # 		TX_PROV = FACI_NAME_PROV,
   # 		TX_MUNC = FACI_NAME_MUNC,
   # 	 ) %>%
   # 	 distinct_all(),
   #   by = c("hub", "branch")
   # ) %>%
   distinct(art_id, .keep_all = TRUE) %>%
   left_join(
      y  = ohasis$ref_faci_code %>%
         filter(is.na(SUB_FACI_CODE) | SUB_FACI_CODE %in% c("TLY-ANGLO", "SHIP-MAKATI", "SAIL-MAKATI")) %>%
         select(
            hub               = FACI_CODE,
            ART_FACI_NAME     = FACI_NAME,
            ART_FACI_REGION   = FACI_NAME_REG,
            ART_FACI_PROVINCE = FACI_NAME_PROV,
            ART_FACI_MUNCITY  = FACI_NAME_MUNC,
         ) %>%
         distinct_all(),
      by = "hub"
   ) %>%
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
         artstart_date >= nhsss$tbhiv$coverage$min_date &
            artstart_date <= nhsss$tbhiv$coverage$max_date ~ 1,
         # year(artstart_date) == as.numeric(nhsss$tbhiv$coverage$curr_yr) ~ 1,
         TRUE ~ 0
      ),
      rep_month = as.numeric(nhsss$tbhiv$coverage$curr_mo),
      curr_age  = floor(curr_age),
      Age_Band  = case_when(
         curr_age >= 0 & curr_age < 15 ~ '<15',
         curr_age >= 15 & curr_age < 25 ~ '15-24',
         curr_age >= 25 & curr_age < 35 ~ '25-34',
         curr_age >= 35 & curr_age < 50 ~ '35-49',
         curr_age >= 50 & curr_age < 1000 ~ '50+',
         TRUE ~ '(no data)'
      ),
   )

write_dta(nhsss$tbhiv$data, "H:/System/HARP/TB-HIV/2022-2ndQr/TB-HIV Q2.dta")

df <- nhsss$tbhiv$data %>%
   mutate(
      visits    = if_else(visit == 1 & onart == 1, 1, 0, 0),
      screened  = if_else(visits == 1 & screened == 1, 1, 0, 0),
      final_new = if_else(screened == 1 & withtb != 1 & final_new == 1, 1, 0, 0),
      onipt     = if_else(final_new == 1 &
                             screened == 1 &
                             withtb != 1 &
                             onipt == 1, 1, 0, 0)
   ) %>%
   group_by(ART_FACI_CODE, ART_FACI_NAME, ART_FACI_REGION, ART_FACI_PROVINCE, ART_FACI_MUNCITY) %>%
   summarize(
      Visits            = sum(visits),
      `Screened for TB` = sum(screened),
      `New on ART`      = sum(final_new),
      `On IPT`          = sum(onipt),
   ) %>%
   ungroup() %>%
   adorn_totals()

write_xlsx(df, "H:/System/HARP/TB-HIV/2022-1stQr/2022 - Jan-Mar 1st Qr TB-HIV Indicator Report (Final).xlsx")