# process hts data
process_hts <- function(form_hts = data.frame(), form_a = data.frame(), form_cfbs = data.frame()) {
   # use hts form as base
   hts <- form_hts %>%
      mutate(
         FORM_VERSION = "HTS Form (v2021)",
      ) %>%
      bind_rows(
         # second priority - form a
         form_a %>%
            mutate(
               FORM_VERSION = if_else(is.na(FORM_VERSION), "Form A (v2017)", FORM_VERSION),
            ),
         # lastly - cfbs form
         form_cfbs %>%
            mutate(
               FORM_VERSION = "CFBS Form (v2020)",
            ) %>%
            rename(
               T0_DATE         = TEST_DATE,
               T0_RESULT       = TEST_RESULT,
               SERVICE_CONDOMS = SERVICE_GIVEN_CONDOMS,
               SERVICE_LUBES   = SERVICE_GIVEN_LUBES,
            ) %>%
            rename_at(
               .vars = vars(starts_with("RISK_")),
               ~stri_replace_first_fixed(., "RISK_", "EXPOSE_")
            )
      ) %>%
      distinct(REC_ID, .keep_all = TRUE) %>%
      # make simplified tagging for source form
      mutate(
         src = FORM_VERSION,
         src = stri_replace_first_fixed(src, "Form", ""),
         src = stri_replace_first_fixed(src, " (v", ""),
         src = stri_replace_first_fixed(src, ")", ""),
         src = tolower(stri_replace_all_fixed(src, " ", ""))
      ) %>%
      # test information
      mutate_at(
         .vars = vars(
            T0_RESULT,
            T1_RESULT,
            T2_RESULT,
            T3_RESULT,
            CONFIRM_RESULT,
            MODALITY,
            SCREEN_AGREED
         ),
         ~keep_code(.)
      ) %>%
      # results
      mutate(
         hts_date        = case_when(
            T0_DATE >= -25567 & interval(RECORD_DATE, T0_DATE) / years(1) <= -2 ~ as.Date(RECORD_DATE),
            T0_DATE >= -25567 & interval(RECORD_DATE, T0_DATE) / years(1) > -2 ~ as.Date(T0_DATE),
            !is.na(DATE_COLLECT) ~ as.Date(DATE_COLLECT),
            T1_DATE < RECORD_DATE ~ as.Date(T1_DATE),
            TRUE ~ RECORD_DATE
         ),
         hts_result      = case_when(
            CONFIRM_RESULT == 1 ~ "R",
            CONFIRM_RESULT == 2 ~ "NR",
            CONFIRM_RESULT == 3 ~ "IND",
            T3_RESULT == 1 ~ "R",
            T3_RESULT == 2 ~ "NR",
            T3_RESULT == 3 ~ "IND",
            T2_RESULT == 1 ~ "R",
            T2_RESULT == 2 ~ "NR",
            T1_RESULT == 1 ~ "R",
            T1_RESULT == 2 ~ "NR",
            T0_RESULT == 1 ~ "R",
            T0_RESULT == 2 ~ "NR",
            grepl("HIV-NR", toupper(CLINIC_NOTES)) ~ "NR",
            grepl("HIN NR", toupper(CLINIC_NOTES)) ~ "NR",
            grepl("HIV-NR", toupper(COUNSEL_NOTES)) ~ "NR",
            TRUE ~ "(no data)"
         ),
         hts_modality    = case_when(
            SCREEN_AGREED == 0 ~ "REACH",
            is.na(SCREEN_AGREED) & is.na(hts_result) ~ "REACH",
            CONFIRM_RESULT != 4 & is.na(MODALITY) ~ "FBT",
            src == "a2017" ~ "FBT",
            src == "cfbs2020" & !is.na(hts_result) ~ "CBS",
            src == "hts2021" &
               is.na(MODALITY) &
               !is.na(hts_result) ~ "FBT",
            FACI_ID == "130605" ~ "CBS",
            MODALITY == "101101" ~ "FBT",
            MODALITY == "101103" ~ "CBS",
            MODALITY == "101104" ~ "FBS",
            MODALITY == "101105" ~ "ST",
            MODALITY == "101304" ~ "REACH",
            TRUE ~ "(no data)"
         ),
         test_agreed     = case_when(
            SCREEN_AGREED == 0 ~ 0,
            SCREEN_AGREED == 1 ~ 1,
            !(hts_modality %in% c("REACH", "(no data)")) ~ 1,
            hts_result != "(no data)" ~ 1,
            TRUE ~ 0
         ),
         hts_client_type = case_when(
            StrLeft(CLIENT_TYPE, 1) == "1" ~ "Inpatient",
            hts_modality == "ST" ~ "ST",
            hts_modality %in% c("CBS", "FBS") ~ "CBS",
            StrLeft(CLIENT_TYPE, 1) == "3" ~ "CBS",
            StrLeft(CLIENT_TYPE, 1) == "7" ~ "PDL",
            StrLeft(CLIENT_TYPE, 1) == "2" ~ "Walk-in",
            StrLeft(CLIENT_TYPE, 1) == "4" ~ "Walk-in",
            TRUE ~ "Walk-in"
         )
      )

   data <- hts %>%
      # risk information
      mutate_at(
         .vars = vars(starts_with("EXPOSE_", ignore.case = FALSE) & !contains("DATE")),
         ~as.integer(keep_code(.))
      ) %>%
      mutate(
         risk_motherhashiv     = case_when(
            EXPOSE_HIV_MOTHER %in% c(1, 2) ~ "yes",
            EXPOSE_HIV_MOTHER == 0 ~ "no",
            TRUE ~ "(no data)"
         ),

         # sex with female
         recent_sexwithf       = floor(interval(EXPOSE_SEX_F_AV_DATE, RECORD_DATE) / months(1)),
         recent_sexwithf       = case_when(
            recent_sexwithf <= 1 ~ "p01m",
            recent_sexwithf <= 3 ~ "p03m",
            recent_sexwithf <= 6 ~ "p06m",
            recent_sexwithf <= 12 ~ "p12m",
            YR_LAST_F == year(hts_date) ~ "p12m",
            src == "cfbs2020" &
               (recent_sexwithf > 12 | is.na(recent_sexwithf)) &
               NUM_F_PARTNER > 0 ~ "p12m",
            src != "cfbs2020" &
               (recent_sexwithf > 12 | is.na(recent_sexwithf)) &
               NUM_F_PARTNER > 0 ~ "beyond_p12m",
            recent_sexwithf > 12 ~ "beyond_p12m",
            YR_LAST_F != year(hts_date) ~ "beyond_p12m",
            NUM_F_PARTNER == 0 ~ "none",
            TRUE ~ "(no data)"
         ),
         recent_sexwithf_nocdm = case_when(
            src == "hts2021" ~ floor(interval(EXPOSE_SEX_F_AV_NOCONDOM_DATE, RECORD_DATE) / months(1)),
            src == "cfbs2020" ~ floor(interval(EXPOSE_CONDOMLESS_VAGINAL_DATE, RECORD_DATE) / months(1)),
         ),
         recent_sexwithf_nocdm = case_when(
            recent_sexwithf_nocdm <= 1 ~ "p01m",
            recent_sexwithf_nocdm <= 3 ~ "p03m",
            recent_sexwithf_nocdm <= 6 ~ "p06m",
            recent_sexwithf_nocdm <= 12 ~ "p12m",
            recent_sexwithf_nocdm > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         # consolidate both categories
         recent_sexwithf_c     = case_when(
            recent_sexwithf == "p01m" | recent_sexwithf_nocdm == "p01m" ~ "p01m",
            recent_sexwithf == "p03m" | recent_sexwithf_nocdm == "p03m" ~ "p03m",
            recent_sexwithf == "p06m" | recent_sexwithf_nocdm == "p06m" ~ "p06m",
            recent_sexwithf == "p12m" | recent_sexwithf_nocdm == "p12m" ~ "p12m",
            recent_sexwithf == "beyond_p12m" | recent_sexwithf_nocdm == "beyond_p12m" ~ "beyond_p12m",
            recent_sexwithf == "none" | recent_sexwithf_nocdm == "none" ~ "none",
            recent_sexwithf == "(no data)" & recent_sexwithf_nocdm == "(no data)" ~ "(no data)",
         ),

         risk_sexwithf         = case_when(
            # form a
            src == "a2017" & EXPOSE_SEX_F_NOCONDOM == 0 ~ "none",
            src == "a2017" & EXPOSE_SEX_F_NOCONDOM == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_SEX_F_NOCONDOM == 2 ~ "yes-beyond_p12m",
            src == "a2017" & is.na(EXPOSE_SEX_F_NOCONDOM) ~ "(no data)",

            # hts form
            src == "hts2021" &
               EXPOSE_SEX_F == 0 &
               (EXPOSE_SEX_F_AV_NOCONDOM == 0 | is.na(EXPOSE_SEX_F_AV_NOCONDOM)) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               EXPOSE_SEX_F_AV_NOCONDOM == 0 &
               (EXPOSE_SEX_F == 0 | is.na(EXPOSE_SEX_F)) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               (EXPOSE_SEX_F == 1 | EXPOSE_SEX_F_AV_NOCONDOM == 1) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               (EXPOSE_SEX_F == 1 | EXPOSE_SEX_F_AV_NOCONDOM == 1) &
               !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            src == "hts2021" &
               (EXPOSE_SEX_F == 0 | EXPOSE_SEX_F_AV_NOCONDOM == 0) &
               !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            src == "hts2021" &
               is.na(EXPOSE_SEX_F) &
               is.na(EXPOSE_SEX_F_AV_NOCONDOM) &
               !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            src == "hts2021" &
               is.na(EXPOSE_SEX_F) &
               is.na(EXPOSE_SEX_F_AV_NOCONDOM) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ recent_sexwithf_c,

            # cfbs form
            src == "cfbs2020" &
               EXPOSE_CONDOMLESS_VAGINAL == 0 &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" & !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            src == "cfbs2020" & EXPOSE_CONDOMLESS_VAGINAL == 2 ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               is.na(EXPOSE_CONDOMLESS_VAGINAL) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ recent_sexwithf_c,
         ),
         risk_sexwithf_nocdm   = case_when(
            # form a
            src == "a2017" & EXPOSE_SEX_F_NOCONDOM == 0 ~ "none",
            src == "a2017" & EXPOSE_SEX_F_NOCONDOM == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_SEX_F_NOCONDOM == 2 ~ "yes-beyond_p12m",
            src == "a2017" & is.na(EXPOSE_SEX_F_NOCONDOM) ~ "(no data)",

            # hts form
            src == "hts2021" &
               EXPOSE_SEX_F_AV_NOCONDOM == 0 &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               EXPOSE_SEX_F_AV_NOCONDOM == 1 &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               EXPOSE_SEX_F_AV_NOCONDOM == 1 &
               !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            src == "hts2021" &
               EXPOSE_SEX_F_AV_NOCONDOM == 0 &
               !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            src == "hts2021" &
               is.na(EXPOSE_SEX_F_AV_NOCONDOM) &
               !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            src == "hts2021" &
               is.na(EXPOSE_SEX_F_AV_NOCONDOM) &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ recent_sexwithf_nocdm,

            # cfbs form
            src == "cfbs2020" &
               EXPOSE_CONDOMLESS_VAGINAL == 0 &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" & !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            src == "cfbs2020" & EXPOSE_CONDOMLESS_VAGINAL == 2 ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               is.na(EXPOSE_CONDOMLESS_VAGINAL) &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ recent_sexwithf_nocdm,
         ),


         # sex with male
         recent_sexwithm       = floor(interval(EXPOSE_SEX_M_AV_DATE, RECORD_DATE) / months(1)),
         recent_sexwithm       = case_when(
            recent_sexwithm <= 1 ~ "p01m",
            recent_sexwithm <= 3 ~ "p03m",
            recent_sexwithm <= 6 ~ "p06m",
            recent_sexwithm <= 12 ~ "p12m",
            YR_LAST_M == year(hts_date) ~ "p12m",
            src == "cfbs2020" &
               (recent_sexwithm > 12 | is.na(recent_sexwithm)) &
               NUM_M_PARTNER > 0 ~ "p12m",
            src != "cfbs2020" &
               (recent_sexwithm > 12 | is.na(recent_sexwithm)) &
               NUM_M_PARTNER > 0 ~ "beyond_p12m",
            recent_sexwithm > 12 ~ "beyond_p12m",
            YR_LAST_M != year(hts_date) ~ "beyond_p12m",
            NUM_M_PARTNER == 0 ~ "none",
            TRUE ~ "(no data)"
         ),
         recent_sexwithm_nocdm = case_when(
            src == "hts2021" ~ floor(interval(EXPOSE_SEX_M_AV_NOCONDOM_DATE, RECORD_DATE) / months(1)),
            src == "cfbs2020" ~ floor(interval(EXPOSE_CONDOMLESS_VAGINAL_DATE, RECORD_DATE) / months(1)),
         ),
         recent_sexwithm_nocdm = case_when(
            recent_sexwithm_nocdm <= 1 ~ "p01m",
            recent_sexwithm_nocdm <= 3 ~ "p03m",
            recent_sexwithm_nocdm <= 6 ~ "p06m",
            recent_sexwithm_nocdm <= 12 ~ "p12m",
            recent_sexwithm_nocdm > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         # consolidate both categories
         recent_sexwithm_c     = case_when(
            recent_sexwithm == "p01m" | recent_sexwithm_nocdm == "p01m" ~ "p01m",
            recent_sexwithm == "p03m" | recent_sexwithm_nocdm == "p03m" ~ "p03m",
            recent_sexwithm == "p06m" | recent_sexwithm_nocdm == "p06m" ~ "p06m",
            recent_sexwithm == "p12m" | recent_sexwithm_nocdm == "p12m" ~ "p12m",
            recent_sexwithm == "beyond_p12m" | recent_sexwithm_nocdm == "beyond_p12m" ~ "beyond_p12m",
            recent_sexwithm == "none" | recent_sexwithm_nocdm == "none" ~ "none",
            recent_sexwithm == "(no data)" & recent_sexwithm_nocdm == "(no data)" ~ "(no data)",
         ),

         risk_sexwithm         = case_when(
            # form a
            src == "a2017" & EXPOSE_SEX_M_NOCONDOM == 0 ~ "none",
            src == "a2017" & EXPOSE_SEX_M_NOCONDOM == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_SEX_M_NOCONDOM == 2 ~ "yes-beyond_p12m",
            src == "a2017" & is.na(EXPOSE_SEX_M_NOCONDOM) ~ "(no data)",

            # hts form
            src == "hts2021" &
               EXPOSE_SEX_M == 0 &
               (EXPOSE_SEX_M_AV_NOCONDOM == 0 | is.na(EXPOSE_SEX_M_AV_NOCONDOM)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               EXPOSE_SEX_M_AV_NOCONDOM == 0 &
               (EXPOSE_SEX_M == 0 | is.na(EXPOSE_SEX_M)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               (EXPOSE_SEX_M == 1 | EXPOSE_SEX_M_AV_NOCONDOM == 1) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               (EXPOSE_SEX_M == 1 | EXPOSE_SEX_M_AV_NOCONDOM == 1) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "hts2021" &
               (EXPOSE_SEX_M == 0 | EXPOSE_SEX_M_AV_NOCONDOM == 0) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "hts2021" &
               is.na(EXPOSE_SEX_M) &
               is.na(EXPOSE_SEX_M_AV_NOCONDOM) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "hts2021" &
               is.na(EXPOSE_SEX_M) &
               is.na(EXPOSE_SEX_M_AV_NOCONDOM) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ recent_sexwithm_c,

            # cfbs form
            src == "cfbs2020" &
               EXPOSE_CONDOMLESS_ANAL == 0 &
               (EXPOSE_M_SEX_ORAL_ANAL == 0 | is.na(EXPOSE_M_SEX_ORAL_ANAL)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               EXPOSE_M_SEX_ORAL_ANAL == 0 &
               (EXPOSE_CONDOMLESS_ANAL == 0 | is.na(EXPOSE_CONDOMLESS_ANAL)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               (EXPOSE_CONDOMLESS_ANAL == 1 | EXPOSE_M_SEX_ORAL_ANAL == 1) &
               recent_sexwithm_c %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            src == "cfbs2020" &
               (EXPOSE_CONDOMLESS_ANAL == 2 | EXPOSE_M_SEX_ORAL_ANAL == 2) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               (EXPOSE_CONDOMLESS_ANAL == 2 | EXPOSE_M_SEX_ORAL_ANAL == 2) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "cfbs2020" &
               (EXPOSE_CONDOMLESS_ANAL == 0 | EXPOSE_M_SEX_ORAL_ANAL == 0) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "cfbs2020" &
               is.na(EXPOSE_CONDOMLESS_ANAL) &
               is.na(EXPOSE_M_SEX_ORAL_ANAL) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "cfbs2020" &
               is.na(EXPOSE_CONDOMLESS_ANAL) &
               is.na(EXPOSE_M_SEX_ORAL_ANAL) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ recent_sexwithm_c,
         ),
         risk_sexwithm_nocdm   = case_when(
            # form a
            src == "a2017" & EXPOSE_SEX_M_NOCONDOM == 0 ~ "none",
            src == "a2017" & EXPOSE_SEX_M_NOCONDOM == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_SEX_M_NOCONDOM == 2 ~ "yes-beyond_p12m",
            src == "a2017" & is.na(EXPOSE_SEX_M_NOCONDOM) ~ "(no data)",

            # hts form
            src == "hts2021" &
               EXPOSE_SEX_M_AV_NOCONDOM == 0 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               EXPOSE_SEX_M_AV_NOCONDOM == 1 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               EXPOSE_SEX_M_AV_NOCONDOM == 1 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "hts2021" &
               EXPOSE_SEX_M_AV_NOCONDOM == 0 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "hts2021" &
               is.na(EXPOSE_SEX_M_AV_NOCONDOM) &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "hts2021" &
               is.na(EXPOSE_SEX_M_AV_NOCONDOM) &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ recent_sexwithm_nocdm,

            # cfbs form
            src == "cfbs2020" &
               EXPOSE_CONDOMLESS_ANAL == 0 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               EXPOSE_CONDOMLESS_ANAL == 1 &
               recent_sexwithm_nocdm %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            src == "cfbs2020" &
               EXPOSE_CONDOMLESS_ANAL == 2 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               EXPOSE_CONDOMLESS_ANAL == 2 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "cfbs2020" &
               EXPOSE_CONDOMLESS_ANAL == 0 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "cfbs2020" &
               is.na(EXPOSE_CONDOMLESS_ANAL) &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "cfbs2020" &
               is.na(EXPOSE_CONDOMLESS_ANAL) &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ recent_sexwithm_nocdm,
         ),

         # paid for sex / sex worker
         recent_payingforsex   = floor(interval(EXPOSE_SEX_PAYING_DATE, RECORD_DATE) / months(1)),
         recent_payingforsex   = case_when(
            recent_payingforsex <= 1 ~ "p01m",
            recent_payingforsex <= 3 ~ "p03m",
            recent_payingforsex <= 6 ~ "p06m",
            recent_payingforsex <= 12 ~ "p12m",
            recent_payingforsex > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_payingforsex     = case_when(
            src == "a2017" & EXPOSE_SEX_PAYING == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_SEX_PAYING == 2 ~ "yes-beyond_p12m",
            src == "a2017" & EXPOSE_SEX_PAYING == 0 ~ "none",
            src == "a2017" & is.na(EXPOSE_SEX_PAYING) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" &
               EXPOSE_SEX_PAYING == 1 &
               !(recent_payingforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_payingforsex),
            src == "hts2021" &
               EXPOSE_SEX_PAYING == 1 &
               recent_payingforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               EXPOSE_SEX_PAYING == 0 &
               recent_payingforsex %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(EXPOSE_SEX_PAYING) ~ "(no data)"
         ),

         # paid for sex / sex worker
         recent_paymentforsex  = floor(interval(EXPOSE_SEX_PAYMENT_DATE, RECORD_DATE) / months(1)),
         recent_paymentforsex  = case_when(
            recent_paymentforsex <= 1 ~ "p01m",
            recent_paymentforsex <= 3 ~ "p03m",
            recent_paymentforsex <= 6 ~ "p06m",
            recent_paymentforsex <= 12 ~ "p12m",
            recent_paymentforsex > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_paymentforsex    = case_when(
            src == "a2017" & EXPOSE_SEX_PAYMENT == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_SEX_PAYMENT == 2 ~ "yes-beyond_p12m",
            src == "a2017" & EXPOSE_SEX_PAYMENT == 0 ~ "none",
            src == "a2017" & is.na(EXPOSE_SEX_PAYMENT) ~ "(no data)",
            src == "hts2021" &
               EXPOSE_SEX_PAYMENT == 1 &
               !(recent_paymentforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_paymentforsex),
            src == "hts2021" &
               EXPOSE_SEX_PAYMENT == 1 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               EXPOSE_SEX_PAYMENT == 0 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(EXPOSE_SEX_PAYMENT) ~ "(no data)",
            src == "cfbs2020" &
               EXPOSE_SEX_PAYMENT == 2 &
               !(recent_paymentforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_paymentforsex),
            src == "cfbs2020" &
               EXPOSE_SEX_PAYMENT == 2 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               EXPOSE_SEX_PAYMENT == 1 &
               !(recent_paymentforsex %in% c("none", "(no data)", "beyond_p12m")) ~ paste0("yes-", recent_paymentforsex),
            src == "cfbs2020" &
               EXPOSE_SEX_PAYMENT == 1 &
               recent_paymentforsex %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            src == "cfbs2020" &
               EXPOSE_SEX_PAYMENT == 0 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               is.na(EXPOSE_SEX_PAYMENT) &
               recent_paymentforsex == "(no data)" ~ "(no data)",
         ),

         # sex w/ someone who has HIV
         recent_sexwithhiv     = floor(interval(EXPOSE_SEX_HIV_DATE, RECORD_DATE) / months(1)),
         recent_sexwithhiv     = case_when(
            recent_sexwithhiv <= 1 ~ "p01m",
            recent_sexwithhiv <= 3 ~ "p03m",
            recent_sexwithhiv <= 6 ~ "p06m",
            recent_sexwithhiv <= 12 ~ "p12m",
            recent_sexwithhiv > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_sexwithhiv       = case_when(
            src == "a2017" & EXPOSE_SEX_HIV == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_SEX_HIV == 2 ~ "yes-beyond_p12m",
            src == "a2017" & EXPOSE_SEX_HIV == 0 ~ "none",
            src == "a2017" & is.na(EXPOSE_SEX_HIV) ~ "(no data)",
            src == "hts2021" ~ "(no data)",
            src == "cfbs2020" &
               EXPOSE_SEX_HIV == 2 &
               !(recent_payingforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_payingforsex),
            src == "cfbs2020" &
               EXPOSE_SEX_HIV == 2 &
               recent_payingforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               EXPOSE_SEX_HIV == 0 &
               recent_payingforsex %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" & is.na(EXPOSE_SEX_HIV) ~ "(no data)"
         ),

         # shared injects / injecting drugs
         recent_injectdrug     = floor(interval(EXPOSE_DRUG_INJECT_DATE, RECORD_DATE) / months(1)),
         recent_injectdrug     = case_when(
            recent_injectdrug <= 1 ~ "p01m",
            recent_injectdrug <= 3 ~ "p03m",
            recent_injectdrug <= 6 ~ "p06m",
            recent_injectdrug <= 12 ~ "p12m",
            recent_injectdrug > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         recent_injectshare    = floor(interval(EXPOSE_NEEDLE_SHARE_DATE, RECORD_DATE) / months(1)),
         recent_injectshare    = case_when(
            recent_injectshare <= 1 ~ "p01m",
            recent_injectshare <= 3 ~ "p03m",
            recent_injectshare <= 6 ~ "p06m",
            recent_injectshare <= 12 ~ "p12m",
            recent_injectshare > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         # consolidate both categories
         recent_injectdrug_c   = case_when(
            recent_injectshare == "p01m" | recent_injectdrug == "p01m" ~ "p01m",
            recent_injectshare == "p03m" | recent_injectdrug == "p03m" ~ "p03m",
            recent_injectshare == "p06m" | recent_injectdrug == "p06m" ~ "p06m",
            recent_injectshare == "p12m" | recent_injectdrug == "p12m" ~ "p12m",
            recent_injectshare == "beyond_p12m" | recent_injectdrug == "beyond_p12m" ~ "beyond_p12m",
            recent_injectshare == "none" | recent_injectdrug == "none" ~ "none",
            recent_injectshare == "(no data)" & recent_injectdrug == "(no data)" ~ "(no data)",
         ),

         risk_injectdrug       = case_when(
            # form a
            src == "a2017" & EXPOSE_DRUG_INJECT == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_DRUG_INJECT == 2 ~ "yes-beyond_p12m",
            src == "a2017" & EXPOSE_DRUG_INJECT == 0 ~ "none",
            src == "a2017" & is.na(EXPOSE_DRUG_INJECT) ~ "(no data)",

            # hts form
            src == "hts2021" &
               EXPOSE_DRUG_INJECT == 1 &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            src == "hts2021" &
               EXPOSE_DRUG_INJECT == 1 &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               EXPOSE_DRUG_INJECT == 0 &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(EXPOSE_DRUG_INJECT) ~ "(no data)",

            # cfbs form
            src == "cfbs2020" &
               EXPOSE_DRUG_INJECT == 2 &
               (EXPOSE_NEEDLE_SHARE %in% c(0, 2) | is.na(EXPOSE_NEEDLE_SHARE)) &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            src == "cfbs2020" &
               EXPOSE_NEEDLE_SHARE == 2 &
               (EXPOSE_DRUG_INJECT %in% c(0, 2) | is.na(EXPOSE_DRUG_INJECT)) &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            src == "cfbs2020" &
               EXPOSE_DRUG_INJECT == 2 &
               (EXPOSE_NEEDLE_SHARE %in% c(0, 2) | is.na(EXPOSE_NEEDLE_SHARE)) &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               EXPOSE_NEEDLE_SHARE == 2 &
               (EXPOSE_DRUG_INJECT %in% c(0, 2) | is.na(EXPOSE_DRUG_INJECT)) &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               EXPOSE_DRUG_INJECT == 1 &
               !(recent_injectdrug_c %in% c("none", "(no data)", "beyond_p12m")) ~ paste0("yes-", recent_injectdrug_c),
            src == "cfbs2020" &
               EXPOSE_DRUG_INJECT == 1 &
               recent_injectdrug_c %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            src == "cfbs2020" &
               EXPOSE_DRUG_INJECT == 0 &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            src == "cfbs2020" &
               EXPOSE_DRUG_INJECT == 0 &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               is.na(EXPOSE_DRUG_INJECT) &
               recent_injectdrug_c == "(no data)" ~ "(no data)",
            src == "cfbs2020" &
               is.na(EXPOSE_DRUG_INJECT) &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
         ),

         # occupational exposure
         recent_needlestick    = floor(interval(EXPOSE_OCCUPATION_DATE, RECORD_DATE) / months(1)),
         recent_needlestick    = case_when(
            recent_needlestick <= 1 ~ "p01m",
            recent_needlestick <= 3 ~ "p03m",
            recent_needlestick <= 6 ~ "p06m",
            recent_needlestick <= 12 ~ "p12m",
            recent_needlestick > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_needlestick      = case_when(
            src == "a2017" & EXPOSE_OCCUPATION == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_OCCUPATION == 2 ~ "yes-beyond_p12m",
            src == "a2017" & EXPOSE_OCCUPATION == 0 ~ "none",
            src == "a2017" & is.na(EXPOSE_OCCUPATION) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" &
               EXPOSE_OCCUPATION == 1 &
               !(recent_needlestick %in% c("none", "(no data)")) ~ paste0("yes-", recent_needlestick),
            src == "hts2021" &
               EXPOSE_OCCUPATION == 1 &
               recent_needlestick %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               EXPOSE_OCCUPATION == 0 &
               recent_needlestick %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(EXPOSE_OCCUPATION) ~ "(no data)"
         ),

         # blood transfusion
         recent_bloodtransfuse = floor(interval(EXPOSE_BLOOD_TRANSFUSE_DATE, RECORD_DATE) / months(1)),
         recent_bloodtransfuse = case_when(
            recent_bloodtransfuse <= 1 ~ "p01m",
            recent_bloodtransfuse <= 3 ~ "p03m",
            recent_bloodtransfuse <= 6 ~ "p06m",
            recent_bloodtransfuse <= 12 ~ "p12m",
            recent_bloodtransfuse > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_bloodtransfuse   = case_when(
            src == "a2017" & EXPOSE_BLOOD_TRANSFUSE == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_BLOOD_TRANSFUSE == 2 ~ "yes-beyond_p12m",
            src == "a2017" & EXPOSE_BLOOD_TRANSFUSE == 0 ~ "none",
            src == "a2017" & is.na(EXPOSE_BLOOD_TRANSFUSE) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" &
               EXPOSE_BLOOD_TRANSFUSE == 1 &
               !(recent_bloodtransfuse %in% c("none", "(no data)")) ~ paste0("yes-", recent_bloodtransfuse),
            src == "hts2021" &
               EXPOSE_BLOOD_TRANSFUSE == 1 &
               recent_bloodtransfuse %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               EXPOSE_BLOOD_TRANSFUSE == 0 &
               recent_bloodtransfuse %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(EXPOSE_BLOOD_TRANSFUSE) ~ "(no data)"
         ),

         # chemsex & drugs
         recent_illicitdrug    = floor(interval(EXPOSE_ILLICIT_DRUGS_DATE, RECORD_DATE) / months(1)),
         recent_illicitdrug    = case_when(
            recent_illicitdrug <= 1 ~ "p01m",
            recent_illicitdrug <= 3 ~ "p03m",
            recent_illicitdrug <= 6 ~ "p06m",
            recent_illicitdrug <= 12 ~ "p12m",
            recent_illicitdrug > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_illicitdrug      = case_when(
            src == "a2017" ~ "(no data)",
            src == "hts2021" ~ "(no data)",
            src == "cfbs2020" &
               EXPOSE_ILLICIT_DRUGS == 2 &
               !(recent_illicitdrug %in% c("none", "(no data)")) ~ paste0("yes-", recent_illicitdrug),
            src == "cfbs2020" &
               EXPOSE_ILLICIT_DRUGS == 2 &
               recent_illicitdrug %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               EXPOSE_ILLICIT_DRUGS == 0 &
               recent_illicitdrug %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" & is.na(EXPOSE_ILLICIT_DRUGS) ~ "(no data)"
         ),

         # had sex under influence of drugs
         recent_chemsex        = floor(interval(EXPOSE_SEX_DRUGS_DATE, RECORD_DATE) / months(1)),
         recent_chemsex        = case_when(
            recent_chemsex <= 1 ~ "p01m",
            recent_chemsex <= 3 ~ "p03m",
            recent_chemsex <= 6 ~ "p06m",
            recent_chemsex <= 12 ~ "p12m",
            recent_chemsex > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_chemsex          = case_when(
            src == "a2017" ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" &
               EXPOSE_SEX_DRUGS == 1 &
               !(recent_chemsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_chemsex),
            src == "hts2021" &
               EXPOSE_SEX_DRUGS == 1 &
               recent_chemsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               EXPOSE_SEX_DRUGS == 0 &
               recent_chemsex %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(EXPOSE_SEX_DRUGS) ~ "(no data)"
         ),

         # tattoo
         risk_tattoo           = case_when(
            src == "a2017" & EXPOSE_TATTOO == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_TATTOO == 2 ~ "yes-beyond_p12m",
            src == "a2017" & EXPOSE_TATTOO == 0 ~ "none",
            src == "a2017" & is.na(EXPOSE_TATTOO) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" ~ "(no data)",
         ),

         # sti
         risk_sti              = case_when(
            src == "a2017" & EXPOSE_STI == 1 ~ "yes-p12m",
            src == "a2017" & EXPOSE_STI == 2 ~ "yes-beyond_p12m",
            src == "a2017" & EXPOSE_STI == 0 ~ "none",
            src == "a2017" & is.na(EXPOSE_STI) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" ~ "(no data)",
         ),
      ) %>%
      select(-starts_with("recent_", ignore.case = FALSE)) %>%
      mutate(
         # process reach types
         CBS_VENUE      = toupper(str_squish(HIV_SERVICE_ADDR)),
         ONLINE_APP     = case_when(
            grepl("GRINDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRNDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRINDER", CBS_VENUE) ~ "GRINDR",
            grepl("TWITTER", CBS_VENUE) ~ "TWITTER",
            grepl("FACEBOOK", CBS_VENUE) ~ "FACEBOOK",
            grepl("MESSENGER", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bFB\\b", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bGR\\b", CBS_VENUE) ~ "GRINDR",
         ),
         REACH_ONLINE   = if_else(!is.na(ONLINE_APP), "1_Yes", REACH_ONLINE, REACH_ONLINE),
         REACH_CLINICAL = if_else(
            condition = if_all(starts_with("REACH_"), ~is.na(.)) & hts_modality == "FBT",
            true      = "1_Yes",
            false     = REACH_CLINICAL,
            missing   = REACH_CLINICAL
         )
      ) %>%
      select(
         -any_of(
            c(
               "PRIME",
               "DISEASE",
               "HIV_SERVICE_TYPE",
               "src",
               "MODULE",
               "MODALITY",
               "CONFIRMATORY_CODE",
               "CHILDREN..50"
            )
         ),
         -starts_with("EXPOSE_")
      ) %>%
      left_join(
         y  = hts %>%
            select(
               REC_ID,
               starts_with("EXPOSE_")
            ),
         by = join_by(REC_ID)
      ) %>%
      relocate(any_of(names(hts)), .before = 1)

   hts_risk <- data %>%
      select(
         REC_ID,
         contains("risk", ignore.case = FALSE)
      ) %>%
      pivot_longer(
         cols = contains("risk", ignore.case = FALSE)
      ) %>%
      group_by(REC_ID) %>%
      summarise(
         risks = stri_c(collapse = ", ", unique(sort(value)))
      )

   data %<>%
      left_join(hts_risk, join_by(REC_ID)) %>%
      mutate(
         SEXUAL_RISK = case_when(
            str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "M+F",
            str_detect(risk_sexwithm, "yes") & !str_detect(risk_sexwithf, "yes") ~ "M",
            !str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "F",
         ),
         kap_unknown = if_else(coalesce(risks, "(no data)") == "(no data)", "(no data)", NA_character_),
         kap_msm     = if_else(SEX == "1_Male" & SEXUAL_RISK %in% c("M", "M+F"), "MSM", NA_character_),
         kap_heterom = if_else(SEX == "1_Male" & SEXUAL_RISK == "F", "Hetero Male", NA_character_),
         kap_heterof = if_else(SEX == "2_Female" & !is.na(SEXUAL_RISK), "Hetero Female", NA_character_),
         kap_pwid    = if_else(str_detect(risk_injectdrug, "yes"), "PWID", NA_character_),
         kap_pip     = if_else(str_detect(risk_paymentforsex, "yes"), "PIP", NA_character_),
         kap_pdl     = case_when(
            StrLeft(CLIENT_TYPE, 1) == "7" ~ "PDL",
            StrLeft(CLIENT_TYPE, 1) == "7" ~ "PDL",
         ),
      )

   return(data)
}

convert_hts <- function(hts_data, convert_type = c("nhsss", "name", "code")) {
   data <- hts_data %>%
      mutate(
         use_record_faci    = if_else(is.na(SERVICE_FACI), 1, 0, 0),
         SERVICE_FACI       = if_else(use_record_faci == 1, FACI_ID, SERVICE_FACI),

         PERM_PSGC_PROV     = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999900000", PERM_PSGC_PROV, PERM_PSGC_PROV),
         PERM_PSGC_MUNC     = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999999000", PERM_PSGC_MUNC, PERM_PSGC_MUNC),
         use_curr           = if_else(
            condition = !is.na(CURR_PSGC_MUNC) & (is.na(PERM_PSGC_MUNC) | StrLeft(PERM_PSGC_MUNC, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         PERMCURR_PSGC_REG  = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_REG,
            false     = PERM_PSGC_REG
         ),
         PERMCURR_PSGC_PROV = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_PROV,
            false     = PERM_PSGC_PROV
         ),
         PERMCURR_PSGC_MUNC = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_MUNC,
            false     = PERM_PSGC_MUNC
         ),


         SERVICE_CONDOMS    = as.numeric(SERVICE_CONDOMS),
         SERVICE_LUBES      = as.numeric(SERVICE_LUBES),
      ) %>%
      rename(
         CREATED                 = CREATED_BY,
         UPDATED                 = UPDATED_BY,
         HTS_PROVIDER_TYPE       = PROVIDER_TYPE,
         HTS_PROVIDER_TYPE_OTHER = PROVIDER_TYPE_OTHER,
      ) %>%
      select(
         -any_of(
            c(
               "PRIME",
               "DISEASE",
               "HIV_SERVICE_TYPE",
               "src",
               "MODULE",
               "MODALITY",
               "CONFIRMATORY_CODE",
               "use_curr"
            )
         )
      ) %>%
      ohasis$get_faci(
         list(REPORT_FACI = c("FACI_ID", "SUB_FACI_ID")),
         convert_type
      ) %>%
      ohasis$get_faci(
         list(HTS_FACI = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
         convert_type,
         c("HTS_REG", "HTS_PROV", "HTS_MUNC")
      ) %>%
      ohasis$get_faci(
         list(SPECIMEN_SOURCE_FACI = c("SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE")),
         convert_type
      ) %>%
      ohasis$get_faci(
         list(CONFIRM_LAB = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
         convert_type
      ) %>%
      ohasis$get_addr(
         c(
            PERM_REG  = "PERM_PSGC_REG",
            PERM_PROV = "PERM_PSGC_PROV",
            PERM_MUNC = "PERM_PSGC_MUNC"
         ),
         convert_type
      ) %>%
      ohasis$get_addr(
         c(
            CURR_REG  = "CURR_PSGC_REG",
            CURR_PROV = "CURR_PSGC_PROV",
            CURR_MUNC = "CURR_PSGC_MUNC"
         ),
         convert_type
      ) %>%
      ohasis$get_addr(
         c(
            PERMCURR_REG  = "PERMCURR_PSGC_REG",
            PERMCURR_PROV = "PERMCURR_PSGC_PROV",
            PERMCURR_MUNC = "PERMCURR_PSGC_MUNC"
         ),
         convert_type
      ) %>%
      ohasis$get_addr(
         c(
            BIRTH_REG  = "BIRTH_PSGC_REG",
            BIRTH_PROV = "BIRTH_PSGC_PROV",
            BIRTH_MUNC = "BIRTH_PSGC_MUNC"
         ),
         convert_type
      ) %>%
      ohasis$get_addr(
         c(
            CBS_REG  = "HIV_SERVICE_PSGC_REG",
            CBS_PROV = "HIV_SERVICE_PSGC_PROV",
            CBS_MUNC = "HIV_SERVICE_PSGC_MUNC"
         ),
         convert_type
      ) %>%
      ohasis$get_staff(c(CREATED_BY = "CREATED")) %>%
      ohasis$get_staff(c(UPDATED_BY = "UPDATED")) %>%
      ohasis$get_staff(c(HTS_PROVIDER = "SERVICE_BY")) %>%
      ohasis$get_staff(c(ANALYZED_BY = "SIGNATORY_1")) %>%
      ohasis$get_staff(c(REVIEWED_BY = "SIGNATORY_2")) %>%
      ohasis$get_staff(c(NOTED_BY = "SIGNATORY_3"))

   return(data)
}

deconstruct_hts <- function(hts) {
   tables <- c(
      "px_record",
      "px_name",
      "px_info",
      "px_profile",
      "px_addr",
      "px_contact",
      "px_faci",
      "px_form",
      "px_test",
      "px_ob",
      "px_occupation",
      "px_cfbs",
      "px_consent",
      "px_ofw",
      "px_expose_hist",
      "px_expose_profile",
      "px_test_reason",
      "px_prev_test",
      "px_med_profile",
      "px_staging",
      "px_reach",
      "px_other_service",
      "px_test_refuse",
      "px_linkage",
      "px_remarks"
   )

   conn <- ohasis$conn("db")

   # primary keys
   log_info("Obtaining {green('Primary Keys')}.")
   pks        <- lapply(tables, function(table) dbGetQuery(conn, glue("SHOW KEYS FROM ohasis_interim.{table} WHERE Key_name = 'PRIMARY'")))
   pks        <- lapply(pks, function(data) return(data$Column_name))
   names(pks) <- tables


   # columns
   log_info("Obtaining {green('Column Names')}.")
   cols        <- lapply(tables, function(table) dbGetQuery(conn, glue("SHOW COLUMNS FROM ohasis_interim.{table}")))
   cols        <- lapply(cols, function(data) return(data$Field))
   names(cols) <- tables

   dbDisconnect(conn)

   log_info("Creating tables using obtained schema.")
   data        <- lapply(tables, function(table, data, cols) {
      col_need      <- cols[[table]]
      col_not_found <- setdiff(col_need, names(data))

      schema <- data %>%
         mutate(
            !!!setNames(rep(NA_character_, length(col_not_found)), col_not_found)
         ) %>%
         select(any_of(col_need))

      return(schema)
   }, data = hts, cols = cols)
   names(data) <- tables

   log_info("Manually creating long tables.")
   # long tables
   data$px_addr <- hts %>%
      select(
         any_of(cols$px_addr),
         ends_with("_REG"),
         ends_with("_PROV"),
         ends_with("_MUNC"),
         ends_with("_ADDR")
      ) %>%
      rename_all(~str_replace(., "HIV_SERVICE", "SERVICE")) %>%
      pivot_longer(
         cols      = c(
            ends_with("_REG"),
            ends_with("_PROV"),
            ends_with("_MUNC"),
            ends_with("_ADDR")
         ),
         names_to  = "ADDR_DATA",
         values_to = "ADDR_VALUE"
      ) %>%
      mutate(
         ADDR_TYPE = str_extract(ADDR_DATA, "^[^_]*"),
         PIECE     = str_extract(ADDR_DATA, "_(?!.*_)(.*)", 1)
      ) %>%
      mutate(
         ADDR_TYPE = case_when(
            ADDR_TYPE == "CURR" ~ "1",
            ADDR_TYPE == "PERM" ~ "2",
            ADDR_TYPE == "BIRTH" ~ "3",
            ADDR_TYPE == "DEATH" ~ "4",
            ADDR_TYPE == "SERVICE" ~ "5",
            ADDR_TYPE == "HIV_SERVICE" ~ "5",
            TRUE ~ ADDR_TYPE
         ),
         PIECE     = case_when(
            PIECE == "ADDR" ~ "TEXT",
            TRUE ~ PIECE
         ),
      ) %>%
      select(-ADDR_DATA) %>%
      pivot_wider(
         names_from   = PIECE,
         values_from  = ADDR_VALUE,
         names_prefix = "ADDR_"
      ) %>%
      select(any_of(cols$px_addr))

   data$px_contact <- hts %>%
      select(
         any_of(cols$px_contact),
         CLIENT_MOBILE,
         CLIENT_EMAIL
      ) %>%
      pivot_longer(
         cols      = c(CLIENT_MOBILE, CLIENT_EMAIL),
         names_to  = "CONTACT_TYPE",
         values_to = "CONTACT"
      ) %>%
      mutate(
         CONTACT_TYPE = case_when(
            CONTACT_TYPE == "CLIENT_MOBILE" ~ "1",
            CONTACT_TYPE == "CLIENT_EMAIL" ~ "2",
            TRUE ~ CONTACT_TYPE
         )
      ) %>%
      select(any_of(cols$px_contact))

   data$px_expose_hist <- hts %>%
      select(
         any_of(cols$px_expose_hist),
         starts_with("EXPOSE_")
      ) %>%
      pivot_longer(
         cols      = starts_with("EXPOSE_"),
         names_to  = "EXPOSURE",
         values_to = "EXPOSE_VALUE"
      ) %>%
      mutate(
         EXPOSE_DATA = if_else(str_detect(EXPOSURE, "_DATE"), "DATE_LAST_EXPOSE", "IS_EXPOSED"),
         EXPOSURE    = str_replace(EXPOSURE, "^EXPOSE_", ""),
         EXPOSURE    = str_replace(EXPOSURE, "_DATE$", ""),
         EXPOSURE    = case_when(
            EXPOSURE == "HIV_MOTHER" ~ "120000",
            EXPOSURE == "SEX_M" ~ "217000",
            EXPOSURE == "SEX_M_AV" ~ "216000",
            EXPOSURE == "SEX_M_AV_NOCONDOM" ~ "216200",
            EXPOSURE == "SEX_F" ~ "227000",
            EXPOSURE == "SEX_F_AV" ~ "226000",
            EXPOSURE == "SEX_F_AV_NOCONDOM" ~ "226200",
            EXPOSURE == "SEX_PAYING" ~ "200010",
            EXPOSURE == "SEX_PAYMENT" ~ "200020",
            EXPOSURE == "SEX_DRUGS" ~ "200300",
            EXPOSURE == "DRUG_INJECT" ~ "301010",
            EXPOSURE == "BLOOD_TRANSFUSE" ~ "530000",
            EXPOSURE == "OCCUPATION" ~ "510000",
            TRUE ~ EXPOSURE
         )
      ) %>%
      pivot_wider(
         names_from  = EXPOSE_DATA,
         values_from = EXPOSE_VALUE,
      ) %>%
      select(any_of(cols$px_expose_hist)) %>%
      mutate(
         IS_EXPOSED = keep_code(IS_EXPOSED),
         IS_EXPOSED = if_else(!is.na(DATE_LAST_EXPOSE), "1", IS_EXPOSED, IS_EXPOSED),
         IS_EXPOSED = coalesce(IS_EXPOSED, "0"),
      )

   data$px_test <- hts %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         CREATED_BY,
         CREATED_AT,
         starts_with("T0_")
      ) %>%
      filter(!is.na(T0_RESULT) | !is.na(T0_DATE)) %>%
      rename(
         RESULT       = T0_RESULT,
         DATE_PERFORM = T0_DATE
      ) %>%
      mutate(
         TEST_TYPE = "10",
         TEST_NUM  = 1,
         RESULT    = case_when(
            RESULT == "Reactive" ~ "1",
            RESULT == "Non-reactive" ~ "2",
            TRUE ~ RESULT
         ),
      ) %>%
      select(any_of(cols$px_test))

   data$px_test_reason <- hts %>%
      select(
         any_of(cols$px_test_reason),
         starts_with("TEST_REASON")
      ) %>%
      pivot_longer(
         cols      = starts_with("TEST_REASON_"),
         names_to  = "REASON",
         values_to = "IS_REASON"
      ) %>%
      mutate(
         REASON_OTHER = if_else(str_detect(REASON, "OTHER_TEXT$"), IS_REASON, NA_character_),
         IS_REASON    = if_else(!is.na(REASON_OTHER), "1_Yes", IS_REASON, IS_REASON),
         REASON       = str_replace(REASON, "^TEST_REASON_", ""),
         REASON       = str_replace(REASON, "_TEXT$", ""),
         REASON       = case_when(
            REASON == "HIV_EXPOSE" ~ "1",
            REASON == "PHYSICIAN" ~ "2",
            REASON == "PEER_ED" ~ "8",
            REASON == "EMPLOY_OFW" ~ "3",
            REASON == "EMPLOY_LOCAL" ~ "4",
            REASON == "TEXT_EMAIL" ~ "9",
            REASON == "INSURANCE" ~ "5",
            REASON == "OTHER" ~ "8888",
            TRUE ~ REASON
         ),
         IS_REASON    = coalesce(keep_code(IS_REASON), "0"),
      ) %>%
      filter(IS_REASON == 1) %>%
      select(any_of(cols$px_test_reason))

   data$px_med_profile <- hts %>%
      select(
         any_of(cols$px_med_profile),
         starts_with("MED_")
      ) %>%
      pivot_longer(
         cols      = starts_with("MED_"),
         names_to  = "PROFILE",
         values_to = "IS_PROFILE"
      ) %>%
      mutate(
         PROFILE    = str_replace(PROFILE, "^MED_", ""),
         PROFILE    = case_when(
            PROFILE == "TB_PX" ~ "1",
            PROFILE == "STI" ~ "8",
            PROFILE == "HEP_B" ~ "3",
            PROFILE == "HEP_C" ~ "4",
            PROFILE == "PREP_PX" ~ "6",
            PROFILE == "PEP_PX" ~ "7",
            TRUE ~ PROFILE
         ),
         IS_PROFILE = coalesce(keep_code(IS_PROFILE), "0"),
      ) %>%
      filter(IS_PROFILE == 1) %>%
      select(any_of(cols$px_med_profile))

   data$px_reach <- hts %>%
      select(
         any_of(cols$px_reach),
         starts_with("REACH_")
      ) %>%
      pivot_longer(
         cols      = starts_with("REACH_"),
         names_to  = "REACH",
         values_to = "IS_REACH"
      ) %>%
      mutate(
         REACH    = str_replace(REACH, "^REACH_", ""),
         REACH    = case_when(
            REACH == "ONLINE" ~ "2",
            REACH == "INDEX_TESTING" ~ "3",
            REACH == "INDEX" ~ "3",
            REACH == "SSNT" ~ "4",
            REACH == "VENUE" ~ "5",
            TRUE ~ REACH
         ),
         IS_REACH = coalesce(keep_code(IS_REACH), "0"),
      ) %>%
      filter(IS_REACH == 1) %>%
      select(any_of(cols$px_reach))

   data$px_other_service <- hts %>%
      select(
         any_of(cols$px_other_service),
         starts_with("SERVICE_")
      ) %>%
      select(-SERVICE_TYPE) %>%
      pivot_longer(
         cols      = starts_with("SERVICE_"),
         names_to  = "SERVICE",
         values_to = "GIVEN"
      ) %>%
      mutate(
         OTHER_SERVICE = case_when(
            SERVICE == "SERVICE_CONDOMS" ~ GIVEN,
            SERVICE == "SERVICE_LUBES" ~ GIVEN,
            TRUE ~ NA_character_
         ),
         GIVEN         = if_else(!is.na(OTHER_SERVICE), "1_Yes", GIVEN, GIVEN),
         SERVICE       = str_replace(SERVICE, "^SERVICE_", ""),
         SERVICE       = case_when(
            SERVICE == "HIV_101" ~ "1013",
            SERVICE == "IEC_MATS" ~ "1004",
            SERVICE == "RISK_COUNSEL" ~ "1002",
            SERVICE == "PREP_REFER" ~ "5001",
            SERVICE == "SSNT_OFFER" ~ "5002",
            SERVICE == "SSNT_ACCEPT" ~ "5003",
            SERVICE == "CONDOMS" ~ "2001",
            SERVICE == "LUBES" ~ "2002",
            TRUE ~ SERVICE
         ),
         GIVEN         = coalesce(keep_code(GIVEN), "0"),
      ) %>%
      filter(GIVEN == 1) %>%
      select(any_of(cols$px_other_service))

   data$px_test_refuse <- hts %>%
      select(
         any_of(cols$px_test_refuse),
         starts_with("TEST_REFUSE_")
      ) %>%
      pivot_longer(
         cols      = starts_with("TEST_REFUSE_"),
         names_to  = "REASON",
         values_to = "IS_REASON"
      ) %>%
      mutate(
         REASON_OTHER = case_when(
            REASON == "TEST_REFUSE_CONDOMS" ~ IS_REASON,
            REASON == "TEST_REFUSE_LUBES" ~ IS_REASON,
            TRUE ~ NA_character_
         ),
         REASON_OTHER = if_else(str_detect(REASON, "OTHER_TEXT$"), IS_REASON, NA_character_),
         IS_REASON    = if_else(!is.na(REASON_OTHER), "1_Yes", IS_REASON, IS_REASON),
         REASON       = str_replace(REASON, "^TEST_REFUSE_", ""),
         REASON       = str_replace(REASON, "_TEXT$", ""),
         REASON       = case_when(
            REASON == "OTHER" ~ "8888",
            TRUE ~ REASON
         ),
         IS_REASON    = coalesce(keep_code(IS_REASON), "0"),
      ) %>%
      filter(IS_REASON == 1) %>%
      select(any_of(cols$px_test_refuse))

   data$px_remarks <- hts %>%
      select(
         any_of(cols$px_remarks),
         CLINIC_NOTES,
         COUNSEL_NOTES,
         SYMPTOMS
      ) %>%
      pivot_longer(
         cols      = c(
            CLINIC_NOTES,
            COUNSEL_NOTES,
            SYMPTOMS
         ),
         names_to  = "REMARK_TYPE",
         values_to = "REMARKS"
      ) %>%
      mutate(
         REMARK_TYPE = case_when(
            REMARK_TYPE == "CLINIC_NOTES" ~ "1",
            REMARK_TYPE == "COUNSEL_NOTES" ~ "2",
            REMARK_TYPE == "SYMPTOMS" ~ "10",
            TRUE ~ REMARK_TYPE
         ),
      ) %>%
      select(any_of(cols$px_remarks))

   log_info("Finalizing upload schema.")
   schema <- list()
   for (table in tables) {
      schema[[table]] <- list(
         name = table,
         pk   = pks[[table]],
         data = data[[table]]
      )
   }

   log_success("Done!")
   return(schema)
}
