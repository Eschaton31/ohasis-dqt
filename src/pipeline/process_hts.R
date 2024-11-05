get_hts <- function(min, max) {

   con   <- ohasis$conn("lw")
   forms <- QB$new(con)
   forms$select("*")
   forms$from("ohasis_warehouse.form_hts")
   forms$where(function(query = QB$new(con)) {
      query$whereBetween('RECORD_DATE', c(min, max), "or")
      query$whereBetween('DATE_CONFIRM', c(min, max), "or")
      query$whereBetween('T0_DATE', c(min, max), "or")
      query$whereBetween('T1_DATE', c(min, max), "or")
      query$whereBetween('T2_DATE', c(min, max), "or")
      query$whereBetween('T3_DATE', c(min, max), "or")
      query$whereNested
   })
   form_hts <- forms$get()

   forms <- QB$new(con)
   forms$from("ohasis_warehouse.form_a")
   forms$where(function(query = QB$new(con)) {
      query$whereBetween('RECORD_DATE', c(min, max), "or")
      query$whereBetween('DATE_CONFIRM', c(min, max), "or")
      query$whereBetween('T0_DATE', c(min, max), "or")
      query$whereBetween('T1_DATE', c(min, max), "or")
      query$whereBetween('T2_DATE', c(min, max), "or")
      query$whereBetween('T3_DATE', c(min, max), "or")
      query$whereNested
   })
   form_a <- forms$get()

   forms <- QB$new(con)
   forms$from("ohasis_warehouse.form_cfbs")
   forms$where(function(query = QB$new(con)) {
      query$whereBetween('RECORD_DATE', c(min, max), "or")
      query$whereBetween('TEST_DATE', c(min, max), "or")
      query$whereNested
   })
   form_cfbs <- forms$get()
   dbDisconnect(con)

   return(list(hts = form_hts, a = form_a, cfbs = form_cfbs))
}

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

         # mot from dx processing

         # for mot
         motherisi1            = case_when(
            EXPOSE_HIV_MOTHER > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithf              = case_when(
            EXPOSE_SEX_F > 0 ~ 1,                      # HTS Form
            !is.na(EXPOSE_SEX_F_AV_DATE) ~ 1,          # HTS Form
            !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 1, # HTS Form
            EXPOSE_SEX_F_NOCONDOM > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithm              = case_when(
            EXPOSE_SEX_M > 0 ~ 1,                      # HTS Form
            !is.na(EXPOSE_SEX_M_AV_DATE) ~ 1,          # HTS Form
            !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 1, # HTS Form
            EXPOSE_SEX_M_NOCONDOM > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithpro            = case_when(
            EXPOSE_SEX_PAYING > 0 ~ 1,
            TRUE ~ 0
         ),
         regularlya            = case_when(
            EXPOSE_SEX_PAYMENT > 0 ~ 1,
            TRUE ~ 0
         ),
         injectdrug            = case_when(
            EXPOSE_DRUG_INJECT > 0 ~ 1,
            TRUE ~ 0
         ),
         chemsex               = case_when(
            EXPOSE_SEX_DRUGS > 0 ~ 1, # HTS Form
            TRUE ~ 0
         ),
         receivedbt            = case_when(
            EXPOSE_BLOOD_TRANSFUSE > 0 ~ 1,
            TRUE ~ 0
         ),
         sti                   = case_when(
            EXPOSE_STI > 0 ~ 1,
            TRUE ~ 0
         ),
         needlepri1            = case_when(
            EXPOSE_OCCUPATION > 0 ~ 1,
            TRUE ~ 0
         ),

         p10y                  = hts_date %m-% years(1),
         p10y                  = year(p10y),

         mot                   = 0,
         # m->m only
         mot                   = case_when(
            SEX == "1_Male" & EXPOSE_SEX_M_NOCONDOM == 1 ~ 1,
            SEX == "1_Male" & YR_LAST_M >= p10y ~ 1,
            SEX == "1_Male" & year(EXPOSE_SEX_M_AV_DATE) >= p10y ~ 1,          # HTS Form
            SEX == "1_Male" & year(EXPOSE_SEX_M_AV_NOCONDOM_DATE) >= p10y ~ 1, # HTS Form
            TRUE ~ mot
         ),

         # m->m+f
         mot                   = case_when(
            mot == 1 & EXPOSE_SEX_F_NOCONDOM == 1 ~ 2,
            mot == 1 & YR_LAST_F >= p10y ~ 2,
            mot == 1 & year(EXPOSE_SEX_F_AV_DATE) >= p10y ~ 2,          # HTS Form
            mot == 1 & year(EXPOSE_SEX_F_AV_NOCONDOM_DATE) >= p10y ~ 2, # HTS Form
            TRUE ~ mot
         ),

         # m->f only
         mot                   = case_when(
            SEX == "1_Male" &
               mot == 0 &
               EXPOSE_SEX_F_NOCONDOM == 1 ~ 3,
            SEX == "1_Male" &
               mot == 0 &
               YR_LAST_F >= p10y ~ 3,
            SEX == "1_Male" &
               mot == 0 &
               year(EXPOSE_SEX_F_AV_DATE) >= p10y ~ 3,          # HTS Form
            SEX == "1_Male" &
               mot == 0 &
               year(EXPOSE_SEX_F_AV_NOCONDOM_DATE) >= p10y ~ 3, # HTS Form
            TRUE ~ mot
         ),

         # f->m
         mot                   = case_when(
            SEX == "2_Female" & EXPOSE_SEX_M_NOCONDOM == 1 ~ 4,
            SEX == "2_Female" & YR_LAST_M >= p10y ~ 4,
            SEX == "2_Female" & year(EXPOSE_SEX_M_AV_DATE) >= p10y ~ 4,          # HTS Form
            SEX == "2_Female" & year(EXPOSE_SEX_M_AV_NOCONDOM_DATE) >= p10y ~ 4, # HTS Form
            TRUE ~ mot
         ),

         # ivdu
         mot                   = case_when(
            EXPOSE_DRUG_INJECT > 0 & StrLeft(PERM_PSGC_PROV, 4) == "0722" ~ 5,
            TRUE ~ mot
         ),

         # vertical
         mot                   = case_when(
            mot == 0 & motherisi1 == 1 ~ 6,
            TRUE ~ mot
         ),

         # m->m-f hx
         mot                   = case_when(
            SEX == "1_Male" &
               mot == 0 &
               NUM_M_PARTNER > 0 &
               is.na(YR_LAST_M) ~ 11,
            SEX == "1_Male" &
               mot == 0 &
               YR_LAST_M >= p10y ~ 11,
            SEX == "1_Male" &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_DATE) ~ 11,                           # HTS Form
            SEX == "1_Male" &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 11,                  # HTS Form
            SEX == "1_Male" & mot == 0 & EXPOSE_SEX_M > 0 ~ 11,             # HTS Form
            TRUE ~ mot
         ),

         # m->m+f hx
         mot                   = case_when(
            mot == 1 & NUM_F_PARTNER > 0 & is.na(YR_LAST_F) ~ 21,
            mot == 3 & NUM_M_PARTNER > 0 & is.na(YR_LAST_M) ~ 21,
            mot == 11 & NUM_F_PARTNER > 0 & is.na(YR_LAST_F) ~ 21,
            mot == 11 & YR_LAST_F >= p10y ~ 21,
            mot == 11 & !is.na(EXPOSE_SEX_F_AV_DATE) ~ 21,          # HTS Form,
            mot == 11 & !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 21, # HTS Form,
            mot == 11 & EXPOSE_SEX_F > 0 ~ 21,                      # HTS Form,
            TRUE ~ mot
         ),

         # m->f hx
         mot                   = case_when(
            SEX == "1_Male" &
               mot == 0 &
               NUM_F_PARTNER > 0 &
               is.na(YR_LAST_F) ~ 31,
            SEX == "1_Male" &
               mot == 0 &
               YR_LAST_F >= p10y ~ 31,
            SEX == "1_Male" &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_DATE) ~ 31,                           # HTS Form,
            SEX == "1_Male" &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 31,                  # HTS Form,
            SEX == "1_Male" & mot == 0 & EXPOSE_SEX_F > 0 ~ 31,             # HTS Form,
            TRUE ~ mot
         ),

         # f->m hx
         mot                   = case_when(
            SEX == "2_Female" &
               mot == 0 &
               NUM_M_PARTNER > 0 &
               is.na(YR_LAST_M) ~ 41,
            SEX == "2_Female" &
               mot == 0 &
               YR_LAST_M >= p10y ~ 41,
            SEX == "2_Female" &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_DATE) ~ 41,                    # HTS Form,
            SEX == "2_Female" &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 41,           # HTS Form,
            SEX == "2_Female" & mot == 0 & EXPOSE_SEX_M > 0 ~ 41,    # HTS Form,
            TRUE ~ mot
         ),

         # ivdu hx
         mot                   = case_when(
            injectdrug > 0 & StrLeft(PERM_PSGC_PROV, 4) == "0722" ~ 51,
            TRUE ~ mot
         ),

         # mtct
         mot                   = case_when(
            mot == 0 & AGE < 5 ~ 61,
            TRUE ~ mot
         ),

         # all else fails
         mot                   = case_when(
            SEX == "1_Male" & mot == 0 & NUM_M_PARTNER > 0 ~ 1,
            TRUE ~ mot
         ),
         mot                   = case_when(
            mot == 1 & NUM_F_PARTNER > 0 ~ 2,
            TRUE ~ mot
         ),

         # needlestick
         mot                   = case_when(
            mot == 0 & needlepri1 == 1 ~ 7,
            TRUE ~ mot
         ),

         # transfusion
         mot                   = case_when(
            mot == 0 & receivedbt == 1 ~ 8,
            TRUE ~ mot
         ),

         # no data
         mot                   = case_when(
            mot == 0 ~ 9,
            TRUE ~ mot
         ),

         # f->f
         mot                   = case_when(
            SEX == "2_Female" & mot == 0 & NUM_F_PARTNER > 0 ~ 10,
            SEX == "2_Female" &
               mot == 0 &
               !is.na(YR_LAST_F) > 0 ~ 10,
            SEX == "2_Female" &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_DATE) ~ 10,
            SEX == "2_Female" &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 10,
            SEX == "2_Female" & mot == 0 & EXPOSE_SEX_F > 0 ~ 10,
            TRUE ~ mot
         ),

         # # clean mot_09
         # mot                  = case_when(
         #    mot %in% c(7, 8) ~ 9,
         #    TRUE ~ mot
         # ),

         # final filtering of mot using risk_*
         mot                   = case_when(
            mot %in% c(7, 8, 9, 10) &
               SEX == "1_Male" &
               str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 22,
            mot %in% c(7, 8, 9, 10) &
               SEX == "1_Male" &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 12,
            mot %in% c(7, 8, 9, 10) &
               SEX == "1_Male" &
               !str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 32,
            mot %in% c(7, 8, 9, 10) &
               SEX == "2_Female" &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 42,
            mot %in% c(7, 8, 9, 10) & str_detect(risk_injectdrug, "^yes") ~ 52,
            TRUE ~ mot
         ),

         # transmit
         transmit              = case_when(
            mot %in% c(1, 2, 3, 4, 11, 12, 21, 22, 31, 32, 41, 42) ~ "SEX",
            mot %in% c(5, 51, 52) ~ "IVDU",
            mot %in% c(6, 61) ~ "PERINATAL",
            mot %in% c(8, 9, 10) ~ "UNKNOWN",
            mot == 7 ~ "OTHERS",
         ),

         # sexhow
         sexhow                = case_when(
            mot %in% c(1, 11, 12) ~ "HOMOSEXUAL",
            mot %in% c(2, 21, 22) ~ "BISEXUAL",
            mot %in% c(3, 4, 31, 32, 41, 42) ~ "HETEROSEXUAL",
         ),


         mot                   = labelled(
            mot,
            c(
               'M->M only'               = 1,
               'M->(M+F) sex'            = 2,
               'M->F only'               = 3,
               'F->M'                    = 4,
               'IVDU (Cebu province)'    = 5,
               'Vertical'                = 6,
               'Needlestick'             = 7,
               'Transfusion'             = 8,
               'M->M hx'                 = 11,
               'M->M hx'                 = 21,
               'M->(M+F) hx'             = 31,
               'F->M hx'                 = 41,
               'IVDU hx (Cebu province)' = 51,
               'Vertical (<5 y.o.)'      = 61,
               'No risk'                 = 9,
               'F->F only'               = 10,
               'M->(M+F) unreliable'     = 22,
               'M->M unreliable'         = 12,
               'M->F unreliable'         = 32,
               'F->M unreliable'         = 42,
               'IVDU unreliable'         = 52
            )
         )
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

   hts %<>%
      mutate_at(
         .vars = vars(
            MODULE,
            SEX,
            SELF_IDENT,
            CIVIL_STATUS,
            EDUC_LEVEL,
            LIVING_WITH_PARTNER,
            CLIENT_TYPE,
            PROVIDER_TYPE,
            T0_RESULT,
            PREV_TEST_RESULT,
            IS_PREGNANT,
            IS_STUDENT,
            IS_EMPLOYED,
            IS_OFW,
            SCREEN_AGREED,
            CLINICAL_PIC,
            WHO_CLASS,
            REFER_ART,
            REFER_CONFIRM,
            OFW_STATION,
            PREV_TESTED,
            SIGNATURE,
            VERBAL_CONSENT,
            OFW_STATION
         ),
         ~keep_code(.)
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
            REACH == "CLINICAL" ~ "1",
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

convert_dx <- function(hts_data, yr, mo) {
   if (missing(yr)) {
      yr <- format(Sys.time(), "%Y")
   }
   if (missing(mo)) {
      mo <- format(Sys.time(), "%m")
   }

   con         <- connect('ohasis-lw')
   corr_classd <- QB$new(con)$from("harp_dx.corr_classd")$get()
   dbDisconnect(con)

   params <- list(
      yr = yr,
      mo = mo
   )


   converted <- hts_data %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, PATIENT_CODE, UIC, PHILHEALTH_NO, PHILSYS_ID, CLIENT_MOBILE, CLIENT_EMAIL),
         ~clean_pii(.)
      ) %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~as.Date(.)
      ) %>%
      mutate_if(
         .predicate = is.Date,
         ~if_else(. <= -25567, NA_Date_, ., .)
      ) %>%
      rename(
         blood_extract_date    = DATE_COLLECT,
         specimen_receipt_date = DATE_RECEIVE,
         confirm_date          = DATE_CONFIRM,
      ) %>%
      mutate(
         # month of labcode/date received
         lab_month       = coalesce(
            str_extract(CONFIRM_CODE, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 2),
            stri_pad_left(month(specimen_receipt_date), 2, "0")
         ),

         # year of labcode/date received
         lab_year        = coalesce(
            stri_c("20", str_extract(CONFIRM_CODE, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 1)),
            stri_pad_left(year(specimen_receipt_date), 4, "0")
         ),

         # date variables
         visit_date      = RECORD_DATE,

         # date var for keeping
         report_date     = as.Date(stri_c(sep = "-", lab_year, lab_month, "01")),

         # name
         STANDARD_FIRST  = stri_trans_general(FIRST, "latin-ascii"),
         name            = str_squish(stri_c(LAST, ", ", FIRST, " ", MIDDLE, " ", SUFFIX)),

         # Permanent
         PERM_PSGC_PROV  = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999900000", PERM_PSGC_PROV, PERM_PSGC_PROV),
         PERM_PSGC_MUNC  = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999999000", PERM_PSGC_MUNC, PERM_PSGC_MUNC),
         use_curr        = if_else(
            condition = !is.na(CURR_PSGC_MUNC) & (is.na(PERM_PSGC_MUNC) | StrLeft(PERM_PSGC_MUNC, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         PERM_PSGC_REG   = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_REG,
            false     = PERM_PSGC_REG
         ),
         PERM_PSGC_PROV  = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_PROV,
            false     = PERM_PSGC_PROV
         ),
         PERM_PSGC_MUNC  = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_MUNC,
            false     = PERM_PSGC_MUNC
         ),

         # Age
         AGE             = coalesce(AGE, AGE_MO / 12),
         AGE_DTA         = calc_age(BIRTHDATE, visit_date),

         HTS_REC         = REC_ID,
         FORM_SORT       = if_else(REC_ID == HTS_REC, 1, 9999, 9999),

         p10y            = year(visit_date %m-% years(10)),
         CONFIRM_REMARKS = NA_character_
      ) %>%
      rename(
         TEST_FACI     = SERVICE_FACI,
         TEST_SUB_FACI = SERVICE_SUB_FACI,
      ) %>%
      mutate(
         # calculate distance from confirmatory date
         CD4_DATE                  = NA_Date_,
         CD4_CONFIRM               = NA_integer_,

         # baseline is within 182 days
         BASELINE_CD4              = NA_integer_,
         idnum                     = NA_integer_,

         # report date
         year                      = params$yr,
         month                     = params$mo,

         # Perm Region (as encoded)
         PERMONLY_PSGC_REG         = if_else(
            condition = use_curr == 0,
            true      = PERM_PSGC_REG,
            false     = NA_character_
         ),
         PERMONLY_PSGC_PROV        = if_else(
            condition = use_curr == 0,
            true      = PERM_PSGC_PROV,
            false     = NA_character_
         ),
         PERMONLY_PSGC_MUNC        = if_else(
            condition = use_curr == 0,
            true      = PERM_PSGC_MUNC,
            false     = NA_character_
         ),

         # tagging vars
         male                      = if_else(
            condition = StrLeft(SEX, 1) == "1",
            true      = 1,
            false     = 0
         ),
         female                    = if_else(
            condition = StrLeft(SEX, 1) == "2",
            true      = 1,
            false     = 0
         ),

         # confirmatory info
         test_done                 = case_when(
            str_detect(toupper(T3_KIT), "GEENIUS") ~ "GEENIUS",
            str_detect(toupper(T3_KIT), "STAT-PAK") ~ "STAT-PAK",
            str_detect(toupper(T3_KIT), "MP DIAGNOSTICS") ~ "WESTERN BLOT",
            AGE <= 1 ~ "PCR"
         ),
         rhivda_done               = if_else(
            condition = StrLeft(CONFIRM_TYPE, 1) == "2",
            true      = 1,
            false     = as.numeric(NA)
         ),
         sample_source             = substr(SPECIMEN_REFER_TYPE, 3, 3),

         # demographics
         pxcode                    = str_squish(stri_c(StrLeft(FIRST, 1), StrLeft(MIDDLE, 1), StrLeft(LAST, 1))),
         SEX                       = remove_code(stri_trans_toupper(SEX)),
         self_identity             = remove_code(stri_trans_toupper(SELF_IDENT)),
         self_identity             = case_when(
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            self_identity == "MALE" ~ "MALE",
            self_identity == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = stri_trans_toupper(SELF_IDENT_OTHER),
         self_identity_other_sieve = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),

         CIVIL_STATUS              = stri_trans_toupper(CIVIL_STATUS),
         nationalit                = case_when(
            toupper(NATIONALITY) == "PHILIPPINES" ~ "FILIPINO",
            toupper(NATIONALITY) != "PHILIPPINES" ~ "NON-FILIPINO",
            TRUE ~ "UNKNOWN"
         ),
         current_school_level      = if_else(
            condition = StrLeft(IS_STUDENT, 1) == "1",
            true      = EDUC_LEVEL,
            false     = NA_character_
         ),

         # occupation
         curr_work                 = if_else(
            condition = StrLeft(IS_EMPLOYED, 1) == "1",
            true      = stri_trans_toupper(WORK),
            false     = NA_character_
         ),
         prev_work                 = if_else(
            condition = StrLeft(IS_EMPLOYED, 1) == "0" | is.na(IS_EMPLOYED),
            true      = stri_trans_toupper(WORK),
            false     = NA_character_
         ),

         # clinical pic
         who_staging               = as.integer(keep_code(WHO_CLASS)),
         other_reason_test         = stri_trans_toupper(TEST_REASON_OTHER_TEXT),

         CLINICAL_PIC              = case_when(
            StrLeft(CLINICAL_PIC, 1) == "1" ~ "0_Asymptomatic",
            StrLeft(CLINICAL_PIC, 1) == "2" ~ "1_Symptomatic",
         ),

         OFW_STATION               = case_when(
            StrLeft(OFW_STATION, 1) == "1" ~ "1_On ship",
            StrLeft(OFW_STATION, 1) == "2" ~ "2_Land",
         ),

         REFER_TYPE                = case_when(
            StrLeft(REFER_TYPE, 1) == "1" ~ "1",
            StrLeft(REFER_TYPE, 1) == "2" ~ "1",
         )
      ) %>%
      # exposure history
      mutate_at(
         .vars = vars(starts_with("EXPOSE_") & !contains("DATE")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_,
         ) %>% as.integer()
      ) %>%
      # medical history
      mutate_at(
         .vars = vars(starts_with("MED_")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # test reason
      mutate_at(
         .vars = vars(starts_with("TEST_REASON") & !matches("_OTHER")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # mode of reach (HTS)
      mutate_at(
         .vars = vars(starts_with("REACH_")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # mode of reach (HTS)
      mutate_at(
         .vars = vars(starts_with("REFER")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # services provided (HTS)
      mutate_at(
         .vars = vars(starts_with("SERVICE_")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
      # mode of transmission
      mutate(
         # for mot
         motherisi1 = case_when(
            EXPOSE_HIV_MOTHER > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithf   = case_when(
            EXPOSE_SEX_F > 0 ~ 1,                      # HTS Form
            !is.na(EXPOSE_SEX_F_AV_DATE) ~ 1,          # HTS Form
            !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 1, # HTS Form
            EXPOSE_SEX_F_NOCONDOM > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithm   = case_when(
            EXPOSE_SEX_M > 0 ~ 1,                      # HTS Form
            !is.na(EXPOSE_SEX_M_AV_DATE) ~ 1,          # HTS Form
            !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 1, # HTS Form
            EXPOSE_SEX_M_NOCONDOM > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithpro = case_when(
            EXPOSE_SEX_PAYING > 0 ~ 1,
            TRUE ~ 0
         ),
         regularlya = case_when(
            EXPOSE_SEX_PAYMENT > 0 ~ 1,
            TRUE ~ 0
         ),
         injectdrug = case_when(
            EXPOSE_DRUG_INJECT > 0 ~ 1,
            TRUE ~ 0
         ),
         chemsex    = case_when(
            EXPOSE_SEX_DRUGS > 0 ~ 1, # HTS Form
            TRUE ~ 0
         ),
         receivedbt = case_when(
            EXPOSE_BLOOD_TRANSFUSE > 0 ~ 1,
            TRUE ~ 0
         ),
         sti        = case_when(
            EXPOSE_STI > 0 ~ 1,
            TRUE ~ 0
         ),
         needlepri1 = case_when(
            EXPOSE_OCCUPATION > 0 ~ 1,
            TRUE ~ 0
         ),

         mot        = 0,
         # m->m only
         mot        = case_when(
            male == 1 & EXPOSE_SEX_M_NOCONDOM == 1 ~ 1,
            male == 1 & YR_LAST_M >= p10y ~ 1,
            male == 1 & year(EXPOSE_SEX_M_AV_DATE) >= p10y ~ 1,          # HTS Form
            male == 1 & year(EXPOSE_SEX_M_AV_NOCONDOM_DATE) >= p10y ~ 1, # HTS Form
            TRUE ~ mot
         ),

         # m->m+f
         mot        = case_when(
            mot == 1 & EXPOSE_SEX_F_NOCONDOM == 1 ~ 2,
            mot == 1 & YR_LAST_F >= p10y ~ 2,
            mot == 1 & year(EXPOSE_SEX_F_AV_DATE) >= p10y ~ 2,          # HTS Form
            mot == 1 & year(EXPOSE_SEX_F_AV_NOCONDOM_DATE) >= p10y ~ 2, # HTS Form
            TRUE ~ mot
         ),

         # m->f only
         mot        = case_when(
            male == 1 & mot == 0 & EXPOSE_SEX_F_NOCONDOM == 1 ~ 3,
            male == 1 &
               mot == 0 &
               YR_LAST_F >= p10y ~ 3,
            male == 1 &
               mot == 0 &
               year(EXPOSE_SEX_F_AV_DATE) >= p10y ~ 3,          # HTS Form
            male == 1 &
               mot == 0 &
               year(EXPOSE_SEX_F_AV_NOCONDOM_DATE) >= p10y ~ 3, # HTS Form
            TRUE ~ mot
         ),

         # f->m
         mot        = case_when(
            female == 1 & EXPOSE_SEX_M_NOCONDOM == 1 ~ 4,
            female == 1 & YR_LAST_M >= p10y ~ 4,
            female == 1 & year(EXPOSE_SEX_M_AV_DATE) >= p10y ~ 4,          # HTS Form
            female == 1 & year(EXPOSE_SEX_M_AV_NOCONDOM_DATE) >= p10y ~ 4, # HTS Form
            TRUE ~ mot
         ),

         # ivdu
         mot        = case_when(
            EXPOSE_DRUG_INJECT > 0 & StrLeft(PERM_PSGC_PROV, 4) == "0722" ~ 5,
            TRUE ~ mot
         ),

         # vertical
         mot        = case_when(
            mot == 0 & motherisi1 == 1 ~ 6,
            TRUE ~ mot
         ),

         # m->m-f hx
         mot        = case_when(
            male == 1 &
               mot == 0 &
               NUM_M_PARTNER > 0 &
               is.na(YR_LAST_M) ~ 11,
            male == 1 &
               mot == 0 &
               YR_LAST_M >= p10y ~ 11,
            male == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_DATE) ~ 11,                     # HTS Form
            male == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 11,            # HTS Form
            male == 1 & mot == 0 & EXPOSE_SEX_M > 0 ~ 11,             # HTS Form
            TRUE ~ mot
         ),

         # m->m+f hx
         mot        = case_when(
            mot == 1 & NUM_F_PARTNER > 0 & is.na(YR_LAST_F) ~ 21,
            mot == 3 & NUM_M_PARTNER > 0 & is.na(YR_LAST_M) ~ 21,
            mot == 11 & NUM_F_PARTNER > 0 & is.na(YR_LAST_F) ~ 21,
            mot == 11 & YR_LAST_F >= p10y ~ 21,
            mot == 11 & !is.na(EXPOSE_SEX_F_AV_DATE) ~ 21,          # HTS Form,
            mot == 11 & !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 21, # HTS Form,
            mot == 11 & EXPOSE_SEX_F > 0 ~ 21,                      # HTS Form,
            TRUE ~ mot
         ),

         # m->f hx
         mot        = case_when(
            male == 1 &
               mot == 0 &
               NUM_F_PARTNER > 0 &
               is.na(YR_LAST_F) ~ 31,
            male == 1 &
               mot == 0 &
               YR_LAST_F >= p10y ~ 31,
            male == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_DATE) ~ 31,                     # HTS Form,
            male == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 31,            # HTS Form,
            male == 1 & mot == 0 & EXPOSE_SEX_F > 0 ~ 31,             # HTS Form,
            TRUE ~ mot
         ),

         # f->m hx
         mot        = case_when(
            female == 1 &
               mot == 0 &
               NUM_M_PARTNER > 0 &
               is.na(YR_LAST_M) ~ 41,
            female == 1 &
               mot == 0 &
               YR_LAST_M >= p10y ~ 41,
            female == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_DATE) ~ 41,              # HTS Form,
            female == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 41,     # HTS Form,
            female == 1 & mot == 0 & EXPOSE_SEX_M > 0 ~ 41,    # HTS Form,
            TRUE ~ mot
         ),

         # ivdu hx
         mot        = case_when(
            injectdrug > 0 & StrLeft(PERM_PSGC_PROV, 4) == "0722" ~ 51,
            TRUE ~ mot
         ),

         # mtct
         mot        = case_when(
            mot == 0 & AGE < 5 ~ 61,
            TRUE ~ mot
         ),

         # all else fails
         mot        = case_when(
            male == 1 & mot == 0 & NUM_M_PARTNER > 0 ~ 1,
            TRUE ~ mot
         ),
         mot        = case_when(
            mot == 1 & NUM_F_PARTNER > 0 ~ 2,
            TRUE ~ mot
         ),

         # needlestick
         mot        = case_when(
            mot == 0 & needlepri1 == 1 ~ 7,
            TRUE ~ mot
         ),

         # transfusion
         mot        = case_when(
            mot == 0 & receivedbt == 1 ~ 8,
            TRUE ~ mot
         ),

         # no data
         mot        = case_when(
            mot == 0 ~ 9,
            TRUE ~ mot
         ),

         # f->f
         mot        = case_when(
            female == 1 & mot == 0 & NUM_F_PARTNER > 0 ~ 10,
            female == 1 & mot == 0 & !is.na(YR_LAST_F) > 0 ~ 10,
            female == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_DATE) ~ 10,
            female == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 10,
            female == 1 & mot == 0 & EXPOSE_SEX_F > 0 ~ 10,
            TRUE ~ mot
         ),

         # # clean mot_09
         # mot                  = case_when(
         #    mot %in% c(7, 8) ~ 9,
         #    TRUE ~ mot
         # ),

         # final filtering of mot using risk_*
         mot        = case_when(
            mot %in% c(7, 8, 9, 10) &
               male == 1 &
               str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 22,
            mot %in% c(7, 8, 9, 10) &
               male == 1 &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 12,
            mot %in% c(7, 8, 9, 10) &
               male == 1 &
               !str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 32,
            mot %in% c(7, 8, 9, 10) &
               female == 1 &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 42,
            mot %in% c(7, 8, 9, 10) & str_detect(risk_injectdrug, "^yes") ~ 52,
            TRUE ~ mot
         ),

         # transmit
         transmit   = case_when(
            mot %in% c(1, 2, 3, 4, 11, 12, 21, 22, 31, 32, 41, 42) ~ "SEX",
            mot %in% c(5, 51, 52) ~ "IVDU",
            mot %in% c(6, 61) ~ "PERINATAL",
            mot %in% c(8, 9, 10) ~ "UNKNOWN",
            mot == 7 ~ "OTHERS",
         ),

         # sexhow
         sexhow     = case_when(
            mot %in% c(1, 11, 12) ~ "HOMOSEXUAL",
            mot %in% c(2, 21, 22) ~ "BISEXUAL",
            mot %in% c(3, 4, 31, 32, 41, 42) ~ "HETEROSEXUAL",
         ),
      ) %>%
      mutate(
         # cd4 tagging
         days_cd4_confirm     = interval(CD4_DATE, confirm_date) / days(1),
         cd4_is_baseline      = if_else(abs(days_cd4_confirm) <= 182, 1, 0, 0),

         CD4_RESULT           = NA_character_,
         CD4_DATE             = NA_Date_,
         CD4_DATE             = case_when(
            cd4_is_baseline == 0 ~ NA_Date_,
            is.na(CD4_RESULT) ~ NA_Date_,
            TRUE ~ CD4_DATE
         ),
         CD4_RESULT           = case_when(
            cd4_is_baseline == 0 ~ NA_character_,
            TRUE ~ CD4_RESULT
         ),
         CD4_RESULT           = parse_number(CD4_RESULT),
         baseline_cd4         = case_when(
            CD4_RESULT >= 500 ~ 1,
            CD4_RESULT >= 350 & CD4_RESULT < 500 ~ 2,
            CD4_RESULT >= 200 & CD4_RESULT < 350 ~ 3,
            CD4_RESULT >= 50 & CD4_RESULT < 200 ~ 4,
            CD4_RESULT < 50 ~ 5,
         ),

         # WHO Case Definition of advanced HIV classification
         # refined ahd
         ahd                  = case_when(
            who_staging %in% c(3, 4) ~ 1,
            AGE >= 5 & baseline_cd4 %in% c(4, 5) ~ 1,
            AGE < 5 ~ 1,
            !is.na(baseline_cd4) ~ 0
         ),
         baseline_cd4         = labelled(
            baseline_cd4,
            c(
               "1_500+ cells/μL"    = 1,
               "2_350-499 cells/μL" = 2,
               "3_200-349 cells/μL" = 3,
               "4_50-199 cells/μL"  = 4,
               "5_below 50"         = 5
            )
         ),

         # tb patient
         # class
         classd               = if_else(
            condition = !is.na(who_staging),
            true      = who_staging,
            false     = NA_integer_
         ) %>% as.numeric(),
         description_symptoms = stri_trans_toupper(SYMPTOMS),
         MED_TB_PX            = case_when(
            stri_detect_fixed(description_symptoms, "TB") ~ 1,
            TRUE ~ as.numeric(MED_TB_PX)
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (corr_classd %>% filter(as.numeric(class) == 3))$symptom)) ~ 3,
            MED_TB_PX == 1 ~ 3,
            TRUE ~ classd
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (corr_classd %>% filter(as.numeric(class) == 4))$symptom)) ~ 4,
            TRUE ~ classd
         ),

         # final class
         class                = case_when(
            classd %in% c(3, 4) ~ "AIDS",
            TRUE ~ "HIV"
         ),

         # new class for 2022
         class2022            = case_when(
            class == "AIDS" ~ "AIDS",
            ahd == 1 ~ "AIDS",
            TRUE ~ "HIV"
         ),

         # no data for stage of hiv
         nodata_hiv_stage     = if_else(
            if_all(c(who_staging, description_symptoms, MED_TB_PX, CLINICAL_PIC), ~is.na(.)),
            1,
            0,
            0
         ),

         # form (HTS)
         FORM_VERSION         = if_else(FORM_VERSION == " (vNA)", NA_character_, FORM_VERSION),

         # provider type (HTS)
         PROVIDER_TYPE        = as.integer(keep_code(PROVIDER_TYPE)),

         # other services (HTS)
         given_ssnt           = case_when(
            SERVICE_SSNT_ACCEPT == 1 ~ "Accepted",
            SERVICE_SSNT_OFFER == 1 ~ "Offered",
         ),

         # combi prev (HTS)
         SERVICE_CONDOMS      = if_else(SERVICE_CONDOMS == 0, NA_integer_, as.integer(SERVICE_CONDOMS), NA_integer_),
         SERVICE_LUBES        = if_else(SERVICE_LUBES == 0, NA_integer_, as.integer(SERVICE_LUBES), NA_integer_),
      ) %>%
      arrange(CENTRAL_ID, desc(cd4_is_baseline), days_cd4_confirm, CD4_DATE) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      ohasis$get_addr(
         c(
            region   = "PERM_PSGC_REG",
            province = "PERM_PSGC_PROV",
            muncity  = "PERM_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            region_c   = "CURR_PSGC_REG",
            province_c = "CURR_PSGC_PROV",
            muncity_c  = "CURR_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            region01   = "BIRTH_PSGC_REG",
            province01 = "BIRTH_PSGC_PROV",
            placefbir  = "BIRTH_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            region_p   = "PERMONLY_PSGC_REG",
            province_p = "PERMONLY_PSGC_PROV",
            muncity_p  = "PERMONLY_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            venue_region   = "HIV_SERVICE_PSGC_REG",
            venue_province = "HIV_SERVICE_PSGC_PROV",
            venue_muncity  = "HIV_SERVICE_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      # country names
      left_join(
         y  = ohasis$ref_country %>%
            select(COUNTRY_CODE, ocw_country = COUNTRY_NAME),
         by = join_by(OFW_COUNTRY == COUNTRY_CODE)
      ) %>%
      relocate(ocw_country, .before = OFW_COUNTRY) %>%
      # dxlab_standard
      mutate(
         use_specimen_source = is.na(TEST_FACI) & !is.na(SPECIMEN_SOURCE),
         TEST_FACI           = coalesce(if_else(use_specimen_source, SPECIMEN_SOURCE, TEST_FACI, TEST_FACI), ""),
         TEST_SUB_FACI       = coalesce(if_else(use_specimen_source, SPECIMEN_SUB_SOURCE, TEST_SUB_FACI, TEST_SUB_FACI), ""),
      ) %>%
      left_join(
         na_matches = "never",
         y          = read_sheet("1aOqYjx5wbc403xy-64YHJU6NzhEBRUu6Ldg59yDEUMw", "Sheet1", range = "A:D", col_types = "c") %>%
            select(
               TEST_FACI = HARP_FACI,
               pubpriv   = FINAL_PUBPRIV
            ) %>%
            distinct(TEST_FACI, .keep_all = TRUE) %>%
            mutate_all(~toupper(coalesce(., ""))),
         by         = join_by(TEST_FACI)
      ) %>%
      mutate(
         FORM_FACI_2        = TEST_FACI,
         FORM_FACI          = TEST_FACI,
         SUB_FORM_FACI      = TEST_SUB_FACI,
         diff_source_v_form = if_else(coalesce(FORM_FACI, "") != coalesce(SPECIMEN_SOURCE, "") & (sample_source == "R" | is.na(sample_source)), 1, 0, 0)
      ) %>%
      ohasis$get_faci(
         list(HTS_FACI = c("FORM_FACI", "SUB_FORM_FACI")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(SOURCE_FACI = c("SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE")),
         "name"
      ) %>%
      # confirmlab
      ohasis$get_faci(
         list(confirmlab = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
         "code",
         c("confirm_region", "confirm_province", "confirm_muncity")
      ) %>%
      ohasis$get_faci(
         list(dxlab_standard = c("TEST_FACI", "TEST_SUB_FACI")),
         "nhsss",
         c("dx_region", "dx_province", "dx_muncity")
      ) %>%
      rename(
         FORM_FACI = FORM_FACI_2
      ) %>%
      mutate(
         labcode2    = CONFIRM_CODE,
         confirm_rec = REC_ID,
      ) %>%
      # same vars as registry
      select(
         REC_ID,
         CENTRAL_ID,
         PATIENT_ID,
         idnum,
         confirm_rec,
         hts_rec                   = HTS_REC,
         form                      = FORM_VERSION,
         modality                  = hts_modality,          # HTS Form
         consent_test              = test_agreed,           # HTS Form
         labcode                   = CONFIRM_CODE,
         labcode2,
         year,
         month,
         uic                       = UIC,
         firstname                 = FIRST,
         middle                    = MIDDLE,
         last                      = LAST,
         name_suffix               = SUFFIX,
         bdate                     = BIRTHDATE,
         patient_code              = PATIENT_CODE,
         pxcode,
         age                       = AGE,
         age_months                = AGE_MO,
         sex                       = SEX,
         philhealth                = PHILHEALTH_NO,
         philsys_id                = PHILSYS_ID,
         mobile                    = CLIENT_MOBILE,
         email                     = CLIENT_EMAIL,
         muncity,
         province,
         region,
         muncity_c,
         province_c,
         region_c,
         muncity_p,
         province_p,
         region_p,
         ocw                       = IS_OFW,
         motherisi1,
         pregnant                  = IS_PREGNANT,
         tbpatient1                = MED_TB_PX,
         nationalit,
         civilstat                 = CIVIL_STATUS,
         self_identity,
         self_identity_other,
         gender_identity,
         nationality               = NATIONALITY,
         highest_educ              = EDUC_LEVEL,
         in_school                 = IS_STUDENT,
         current_school_level,
         with_partner              = LIVING_WITH_PARTNER,
         child_count               = CHILDREN,
         sexwithf,
         sexwithm,
         sexwithpro,
         regularlya,
         injectdrug,
         chemsex,
         receivedbt,
         sti,
         needlepri1,
         transmit,
         sexhow,
         mot,
         starts_with("risk_", ignore.case = FALSE),
         class,
         class2022,
         ahd,
         baseline_cd4,
         baseline_cd4_date         = CD4_DATE,
         baseline_cd4_result       = CD4_RESULT,
         confirm_date,
         confirmlab,
         confirm_region,
         confirm_province,
         confirm_muncity,
         confirm_result            = CONFIRM_RESULT,
         confirm_remarks           = CONFIRM_REMARKS,
         region01,
         province01,
         placefbir,
         curr_work,
         prev_work,
         ocw_based                 = OFW_STATION,
         ocw_country,
         age_sex                   = AGE_FIRST_SEX,
         age_inj                   = AGE_FIRST_INJECT,
         howmanymse                = NUM_M_PARTNER,
         yrlastmsex                = YR_LAST_M,
         howmanyfse                = NUM_F_PARTNER,
         yrlastfsex                = YR_LAST_F,
         past12mo_injdrug          = EXPOSE_DRUG_INJECT,
         past12mo_rcvbt            = EXPOSE_BLOOD_TRANSFUSE,
         past12mo_sti              = EXPOSE_STI,
         past12mo_sexfnocondom     = EXPOSE_SEX_F_NOCONDOM,
         past12mo_sexmnocondom     = EXPOSE_SEX_M_NOCONDOM,
         past12mo_sexprosti        = EXPOSE_SEX_PAYING,
         past12mo_acceptpayforsex  = EXPOSE_SEX_PAYMENT,
         past12mo_needle           = EXPOSE_OCCUPATION,
         past12mo_hadtattoo        = EXPOSE_TATTOO,
         history_sex_m             = EXPOSE_SEX_M,
         date_lastsex_m            = EXPOSE_SEX_M_AV_DATE,
         date_lastsex_condomless_m = EXPOSE_SEX_M_AV_NOCONDOM_DATE,
         history_sex_f             = EXPOSE_SEX_F,
         date_lastsex_f            = EXPOSE_SEX_F_AV_DATE,
         date_lastsex_condomless_f = EXPOSE_SEX_F_AV_NOCONDOM_DATE,
         prevtest                  = PREV_TESTED,
         prev_test_result          = PREV_TEST_RESULT,
         prev_test_faci            = PREV_TEST_FACI,
         prevtest_date             = PREV_TEST_DATE,
         clinicalpicture           = CLINICAL_PIC,
         recombyph1                = TEST_REASON_PHYSICIAN,
         recomby_peer_ed           = TEST_REASON_PEER_ED,   # HTS Form
         insurance1                = TEST_REASON_INSURANCE,
         recheckpr1                = TEST_REASON_RETEST,
         no_test_reason            = TEST_REASON_NO_REASON,
         possible_exposure         = TEST_REASON_HIV_EXPOSE,
         emp_local                 = TEST_REASON_EMPLOY_LOCAL,
         emp_abroad                = TEST_REASON_EMPLOY_OFW,
         other_reason_test,
         description_symptoms,
         who_staging,
         hx_hepb                   = MED_HEP_B,
         hx_hepc                   = MED_HEP_C,
         hx_cbs                    = MED_CBS_REACTIVE,
         hx_prep                   = MED_PREP_PX,
         hx_pep                    = MED_PEP_PX,
         hx_sti                    = MED_STI,
         reach_clinical            = REACH_CLINICAL,
         reach_online              = REACH_ONLINE,
         reach_it                  = REACH_INDEX_TESTING,
         reach_ssnt                = REACH_SSNT,
         reach_venue               = REACH_VENUE,
         refer_art                 = REFER_ART,
         refer_confirm             = REFER_CONFIRM,
         retest                    = REFER_RETEST,
         retest_in_mos             = RETEST_MOS,
         retest_in_wks             = RETEST_WKS,
         retest_date               = RETEST_DATE,
         given_hiv101              = SERVICE_HIV_101,
         given_iec_mats            = SERVICE_IEC_MATS,
         given_risk_reduce         = SERVICE_RISK_COUNSEL,
         given_prep_pep            = SERVICE_PREP_REFER,
         given_ssnt,
         provider_type             = PROVIDER_TYPE,
         provider_type_other       = PROVIDER_TYPE_OTHER,
         venue_region,
         venue_province,
         venue_muncity,
         venue_text                = HIV_SERVICE_ADDR,
         px_type                   = CLIENT_TYPE,
         referred_by               = REFER_TYPE,
         hts_date,
         t0_date                   = T0_DATE,
         t0_result                 = T0_RESULT,
         test_done,
         name,
         t1_date                   = T1_DATE,
         t1_kit                    = T1_KIT,
         t1_result                 = T1_RESULT,
         t2_date                   = T2_DATE,
         t2_kit                    = T2_KIT,
         t2_result                 = T2_RESULT,
         t3_date                   = T3_DATE,
         t3_kit                    = T3_KIT,
         t3_result                 = T3_RESULT,
         final_interpretation      = CONFIRM_RESULT,
         visit_date,
         blood_extract_date,
         specimen_receipt_date,
         rhivda_done,
         sample_source,
         dxlab_standard,
         pubpriv,
         dx_region,
         dx_province,
         dx_muncity,
         diff_source_v_form,
         SOURCE_FACI,
         HTS_FACI,
         # DUP_MUNC,
         FORM_FACI
      ) %>%
      # turn into codes
      mutate_at(
         .vars = vars(
            ocw,
            highest_educ,
            current_school_level,
            in_school,
            pregnant,
            with_partner,
            ocw_based,
            prev_test_result,
            clinicalpicture,
            prevtest,
            px_type,
            t1_result,
            t2_result,
            t3_result,
         ),
         ~as.integer(keep_code(.))
      ) %>%
      # remove codes
      mutate_at(
         .vars = vars(
            sex,
            civilstat,
            final_interpretation
         ),
         ~remove_code(.)
      ) %>%
      # fix test data
      mutate_at(
         .vars = vars(
            t1_result,
            t2_result,
            t3_result
         ),
         ~case_when(
            . == 1 ~ "Positive / Reactive",
            . == 2 ~ "Negative / Non-reactive",
            . == 3 ~ "Indeterminate",
            TRUE ~ NA_character_
         )
      ) %>%
      mutate(
         age_pregnant = if_else(
            condition = pregnant == 1,
            true      = age,
            false     = as.numeric(NA)
         ),
         age_vertical = if_else(
            condition = transmit == "PERINATAL",
            true      = age,
            false     = as.numeric(NA)
         ),
         age_unknown  = if_else(
            condition = transmit == "UNKNOWN",
            true      = age,
            false     = as.numeric(NA)
         ),
         pubpriv      = if_else(pubpriv == "0", NA_character_, as.character(pubpriv))
      ) %>%
      distinct_all() %>%
      mutate(
         confirm_date     = coalesce(confirm_date, as.Date(t3_date)),
         nodata_hiv_stage = if_else(
            is.na(ahd) &
               is.na(baseline_cd4) &
               ((coalesce(description_symptoms, "") == "" & clinicalpicture == 1) | is.na(clinicalpicture)) &
               is.na(tbpatient1) &
               is.na(who_staging) &
               class2022 == "HIV",
            1,
            0,
            0
         )
      )

   return(converted)
}

changes_dx_v_hts <- function(rec_ids, yr, mo) {
   dx   <- stri_c("harp_dx.reg_", yr, stri_pad_left(mo, 2, "0"))
   con  <- connect("ohasis-lw")
   hts  <- QB$new(con)$
      from("ohasis_warehouse.form_hts AS rec")$
      select("rec.*")$
      selectRaw("COALESCE(id.CENTRAL_ID, rec.PATIENT_ID) AS CENTRAL_ID")$
      leftJoin("ohasis_interim.registry AS id", "rec.PATIENT_ID", "=", "id.PATIENT_ID")$
      whereIn("REC_ID", rec_ids)$
      get()
   a    <- QB$new(con)$
      from("ohasis_warehouse.form_a AS rec")$
      select("rec.*")$
      selectRaw("COALESCE(id.CENTRAL_ID, rec.PATIENT_ID) AS CENTRAL_ID")$
      leftJoin("ohasis_interim.registry AS id", "rec.PATIENT_ID", "=", "id.PATIENT_ID")$
      whereIn("REC_ID", rec_ids)$
      get()
   cfbs <- QB$new(con)$
      from("ohasis_warehouse.form_cfbs AS rec")$
      select("rec.*")$
      selectRaw("COALESCE(id.CENTRAL_ID, rec.PATIENT_ID) AS CENTRAL_ID")$
      leftJoin("ohasis_interim.registry AS id", "rec.PATIENT_ID", "=", "id.PATIENT_ID")$
      whereIn("REC_ID", rec_ids)$
      get()
   dx   <- QB$new(con)$
      from(dx)$
      whereIn("REC_ID", rec_ids)$
      get()
   dbDisconnect(con)

   records <- process_hts(hts, a, cfbs)

   convert <- convert_dx(records)

   variables <- dx %>%
      summary.default %>%
      as.data.frame %>%
      group_by(Var1) %>%
      spread(key = Var2, value = Freq) %>%
      ungroup %>%
      mutate(
         format = case_when(
            Class == "Date" ~ "Date",
            TRUE ~ Mode
         )
      )

   check <- convert %>%
      select(-idnum) %>%
      left_join(
         y  = dx %>%
            select(REC_ID, idnum),
         by = join_by(REC_ID)
      ) %>%
      mutate_all(as.character) %>%
      pivot_longer(
         cols      = !matches("REC_ID"),
         values_to = "new_value",
         names_to  = "variable",
      ) %>%
      right_join(
         y  = dx %>%
            mutate(
               REC_ID = coalesce(hts_rec, REC_ID)
            ) %>%
            mutate_all(as.character) %>%
            pivot_longer(
               cols      = !matches("REC_ID"),
               values_to = "old_value",
               names_to  = "variable",
            ),
         by = join_by(REC_ID, variable)
      ) %>%
      mutate(
         period = stri_c(yr, ".", stri_pad_left(mo, 2, "0")),
      ) %>%
      left_join(
         y  = variables %>%
            select(variable = Var1, format),
         by = join_by(variable)
      ) %>%
      left_join(
         y  = dx %>%
            mutate(
               REC_ID = coalesce(hts_rec, REC_ID)
            ) %>%
            select(REC_ID, idnum),
         by = join_by(REC_ID)
      ) %>%
      select(
         period,
         idnum,
         variable,
         old_value,
         new_value,
         format
      ) %>%
      filter(coalesce(old_value, "") != coalesce(new_value, "")) %>%
      mutate(
         new_value = coalesce(new_value, "NULL")
      ) %>%
      filter(!(variable %in% c('final_interpretation', 'confirm_result', 'confirm_remarks')))

   return(check)
}

# recs  <- c('20240708104656A1300000044')
# check <- changes_dx_v_hts(recs, 2024, 8)
#
# check %>%
#    filter(coalesce(old_value, "") != coalesce(new_value, ""))