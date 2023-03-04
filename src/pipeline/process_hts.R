# process hts data
process_hts <- function(form_hts = data.frame(), form_a = data.frame(), form_cfbs = data.frame()) {
   # use hts form as base
   data <- form_hts %>%
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
               T0_DATE   = TEST_DATE,
               T0_RESULT = TEST_RESULT,
            ) %>%
            rename_at(
               .vars = vars(starts_with("RISK_")),
               ~stri_replace_first_fixed(., "RISK_", "EXPOSE_")
            )
      ) %>%
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
         hts_date     = case_when(
            T0_DATE >= -25567 & interval(RECORD_DATE, T0_DATE) / years(1) <= -2 ~ as.Date(RECORD_DATE),
            T0_DATE >= -25567 & interval(RECORD_DATE, T0_DATE) / years(1) > -2 ~ as.Date(T0_DATE),
            !is.na(DATE_COLLECT) ~ as.Date(DATE_COLLECT),
            T1_DATE < RECORD_DATE ~ as.Date(T1_DATE),
            TRUE ~ RECORD_DATE
         ),
         hts_result   = case_when(
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
         hts_modality = case_when(
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
         test_agreed  = case_when(
            SCREEN_AGREED == 0 ~ 0,
            SCREEN_AGREED == 1 ~ 1,
            !(hts_modality %in% c("REACH", "(no data)")) ~ 1,
            hts_result != "(no data)" ~ 1,
            TRUE ~ 0
         )
      ) %>%
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
            src == "cmbs2020" &
               (recent_sexwithm > 12 | is.na(recent_sexwithm)) &
               NUM_M_PARTNER > 0 ~ "p12m",
            src != "cmbs2020" &
               (recent_sexwithm > 12 | is.na(recent_sexwithm)) &
               NUM_M_PARTNER > 0 ~ "beyond_p12m",
            recent_sexwithm > 12 ~ "beyond_p12m",
            YR_LAST_M != year(hts_date) ~ "beyond_p12m",
            NUM_M_PARTNER == 0 ~ "none",
            TRUE ~ "(no data)"
         ),
         recent_sexwithm_nocdm = case_when(
            src == "hts2021" ~ floor(interval(EXPOSE_SEX_M_AV_NOCONDOM_DATE, RECORD_DATE) / months(1)),
            src == "cmbs2020" ~ floor(interval(EXPOSE_CONDOMLESS_VAGINAL_DATE, RECORD_DATE) / months(1)),
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
      select(-starts_with("recent_", ignore.case = FALSE))
}