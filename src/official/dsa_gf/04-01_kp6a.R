##  prepare dataset for TX_CURR ------------------------------------------------

risk <- gf$harp$prep$new_outcome %>%
   select(
      prep_id,
      contains("risk", ignore.case = FALSE)
   ) %>%
   select(-ends_with("screen")) %>%
   pivot_longer(
      cols = contains("risk", ignore.case = FALSE)
   ) %>%
   group_by(prep_id) %>%
   summarise(
      risks = paste0(collapse = ", ", unique(sort(value)))
   ) %>%
   ungroup()

gf$linelist$kp6a <- gf$harp$prep$new_reg %>%
   select(
      REC_ID,
      CENTRAL_ID,
      prep_id,
      first,
      middle,
      last,
      suffix,
      uic,
      px_code,
      sex
   ) %>%
   left_join(
      y  = gf$harp$prep$new_outcome %>%
         select(-CENTRAL_ID, -REC_ID, -sex),
      by = "prep_id"
   ) %>%
   left_join(
      y  = risk,
      by = "prep_id"
   ) %>%
   left_join(
      y  = gf$forms$form_prep %>%
         select(
            REC_ID,
            RISK_CONDOMLESS_ANAL
         ),
      by = "REC_ID"
   ) %>%
   select(-HTS_REC) %>%
   left_join(
      y  = gf$forms$rec_link %>%
         select(
            REC_ID  = DESTINATION_REC,
            HTS_REC = SOURCE_REC
         ),
      by = "REC_ID"
   ) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   left_join(
      y  = gf$linelist$psfi_matched %>%
         select(
            CENTRAL_ID,
            reach_date,
            sextype_anal_receive,
            sextype_anal_insert,
            date_last_sex_msm
         ),
      by = "CENTRAL_ID"
   ) %>%
   left_join(
      y  = bind_rows(
         gf$forms$form_hts %>% mutate(FORM = "HTS"),
         gf$forms$form_a %>% mutate(FORM = "A"),
         gf$forms$form_cfbs %>% mutate(FORM = "CFBS")
      ) %>%
         mutate(
            RECORD_P6M       = RECORD_DATE %m-% months(6),
            RECORD_P12M      = RECORD_DATE %m-% months(12),
            SEX_ANAL_RECEIVE = case_when(
               RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
               RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
               RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
               RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
               StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
               StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
               StrLeft(RISK_CONDOMLESS_ANAL, 1) == "1" ~ "Y",
               StrLeft(RISK_CONDOMLESS_ANAL, 1) == "2" ~ "Y",
               StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == "1" ~ "Y",
               TRUE ~ "N"
            ),
            SEX_ANAL_INSERT  = case_when(
               RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
               RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
               RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
               RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
               StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
               StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
               StrLeft(RISK_CONDOMLESS_ANAL, 1) == "1" ~ "Y",
               StrLeft(RISK_CONDOMLESS_ANAL, 1) == "2" ~ "Y",
               StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == "1" ~ "Y",
               TRUE ~ "N"
            ),
            analp12m         = case_when(
               RECORD_P6M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "w/in 6m",
               RECORD_P6M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "w/in 6m",
               RECORD_P6M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "w/in 6m",
               RECORD_P6M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "w/in 6m",
               RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "w/in 12m",
               RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "w/in 12m",
               RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "w/in 12m",
               RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "w/in 12m",
               RECORD_P12M > EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ ">p12m",
               RECORD_P12M > EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ ">p12m",
               RECORD_P12M > EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ ">p12m",
               RECORD_P12M > EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ ">p12m",
               StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "1" ~ "w/in 12m",
               StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "2" ~ ">p12m",
               StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == "2" ~ ">p12m",
               StrLeft(RISK_CONDOMLESS_ANAL, 1) == "1" ~ "w/in 12m",
               StrLeft(RISK_CONDOMLESS_ANAL, 1) == "2" ~ ">p12m",
               StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == "1" ~ "w/in 12m",
               SEX_ANAL_INSERT == "Y" | SEX_ANAL_RECEIVE == "Y" ~ ">p12m",
               TRUE ~ "(no data)"
            ),
         ) %>%
         select(
            HTS_REC = REC_ID,
            FORM,
            analp12m,
            # date_last_sex_msm = DATE_LAST_M,
         ),
      by = "HTS_REC"
   ) %>%
   filter(
      prepstart_date >= as.Date(gf$coverage$min),
      !is.na(latest_regimen)
   ) %>%
   mutate(
      # sex variable (use registry if available)
      Sex              = StrLeft(sex, 1),
      Sex              = case_when(
         Sex == "M" ~ "Male",
         Sex == "F" ~ "Female",
         TRUE ~ Sex
      ),

      #
      Date_Start       = paste(
         sep = "-",
         year(prepstart_date),
         stri_pad_left(month(prepstart_date), 2, "0"),
         "01"
      ),
      Date_End         = as.character((as.Date(Date_Start) %m+% months(1)) - 1),


      # Age Band
      curr_age         = floor(curr_age),
      `Age Group`      = case_when(
         curr_age %in% seq(0, 14) ~ "01_<15",
         curr_age %in% seq(15, 17) ~ "02_15-17",
         curr_age %in% seq(18, 19) ~ "03_18-19",
         curr_age %in% seq(20, 24) ~ "04_20-24",
         curr_age %in% seq(25, 29) ~ "05_25-29",
         curr_age %in% seq(30, 34) ~ "06_30-34",
         curr_age %in% seq(35, 49) ~ "07_35-49",
         curr_age %in% seq(50, 54) ~ "08_50-54",
         curr_age %in% seq(55, 59) ~ "09_55-59",
         curr_age %in% seq(60, 64) ~ "10_60-64",
         curr_age %in% seq(65, 69) ~ "11_65-69",
         curr_age %in% seq(70, 74) ~ "12_70-74",
         curr_age %in% seq(75, 79) ~ "13_75-79",
         curr_age %in% seq(80, 1000) ~ "14_80+",
         TRUE ~ "99_(no data)"
      ),

      # KAP
      msm              = case_when(
         stri_detect_fixed(prep_risk_sexwithm, "yes") ~ 1,
         stri_detect_fixed(hts_risk_sexwithm, "yes") ~ 1,
         kp_msm == 1 ~ 1,
         TRUE ~ 0
      ),
      tgw              = if_else(
         condition = sex == "MALE" & self_identity == "FEMALE",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero           = if_else(
         condition = sex == self_identity & msm != 0,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      unknown          = if_else(
         condition = risks == "(no data)",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      `Key Population` = case_when(
         msm == 1 & tgw == 0 ~ "MSM",
         msm == 1 & tgw == 1 ~ "TGW",
         kp_pwid == 1 ~ "PWID",
         hetero == 1 & Sex == "Male" ~ "Hetero Male",
         hetero == 1 & Sex == "Female" ~ "Hetero Female",
         unknown == 1 ~ "(no data)",
         TRUE ~ "Others"
      ),

      # disaggregations
      ffup_to_pickup   = floor(difftime(latest_nextpickup, latest_ffupdate, units = "days")),
      `DISAG 1`        = if_else(
         condition = prep_type != "(no data)",
         true      = toupper(prep_type),
         false     = prep_type
      ),
      `DISAG 2`        = case_when(
         ffup_to_pickup %in% seq(0, 30) ~ "1 month of PrEP",
         ffup_to_pickup %in% seq(31, 90) ~ "3 months of PrEP",
         ffup_to_pickup %in% seq(91, 10000) ~ "3+ months of PrEP",
      ),
      RECORD_P6M       = reach_date %m-% months(6),
      RECORD_P12M      = reach_date %m-% months(12),
      `DISAG 3`        = case_when(
         # StrLeft(RISK_CONDOMLESS_ANAL, 1) %in% c("0", "2") & analp12m == "w/in 6m" ~ "1) Sexual Risk (anal sex) past 6 months",
         # StrLeft(RISK_CONDOMLESS_ANAL, 1) %in% c("0", "2") & analp12m == "w/in 12m" ~ "2) Sexual Risk (anal sex) past 12 months",
         # StrLeft(RISK_CONDOMLESS_ANAL, 1) %in% c("0", "2") & analp12m == ">p12m" ~ "3) Sexual Risk (anal sex) >12 months",
         # StrLeft(RISK_CONDOMLESS_ANAL, 1) == "4" ~ "0) Sexual Risk (anal sex) past 30 days",
         # StrLeft(RISK_CONDOMLESS_ANAL, 1) == "3" ~ "1) Sexual Risk (anal sex) past 6 months",
         # StrLeft(RISK_CONDOMLESS_ANAL, 1) == "1" ~ "2) Sexual Risk (anal sex) past 12 months",
         # StrLeft(RISK_CONDOMLESS_ANAL, 1) == "2" ~ "3) Sexual Risk (anal sex) >12 months",
         # is.na(RISK_CONDOMLESS_ANAL) & analp12m == "w/in 6m" ~ "1) Sexual Risk (anal sex) past 6 months",
         # is.na(RISK_CONDOMLESS_ANAL) & analp12m == "w/in 12m" ~ "2) Sexual Risk (anal sex) past 12 months",
         # is.na(RISK_CONDOMLESS_ANAL) & analp12m == ">p12m" ~ "3) Sexual Risk (anal sex) >12 months",
         prep_risk_sexwithm == "yes-p01m" | hts_risk_sexwithm == "yes-p01m" ~ "1) Sexual Risk (anal sex) past 6 months",
         prep_risk_sexwithm == "yes-p03m" | hts_risk_sexwithm == "yes-p03m" ~ "1) Sexual Risk (anal sex) past 6 months",
         prep_risk_sexwithm == "yes-p06m" | hts_risk_sexwithm == "yes-p06m" ~ "1) Sexual Risk (anal sex) past 6 months",
         prep_risk_sexwithm == "yes-p12m" | hts_risk_sexwithm == "yes-p12m" ~ "2) Sexual Risk (anal sex) past 12 months",
         prep_risk_sexwithm == "yes-beyond_p12m" | hts_risk_sexwithm == "yes-beyond_p12m" ~ "3) Sexual Risk (anal sex) >12 months",
         date_last_sex_msm >= RECORD_P6M ~ "1) Sexual Risk (anal sex) past 6 months",
         date_last_sex_msm >= RECORD_P12M ~ "2) Sexual Risk (anal sex) past 12 months",
         date_last_sex_msm < RECORD_P12M ~ "3) Sexual Risk (anal sex) >12 months",
         sextype_anal_receive == "Y" | sextype_anal_insert == "Y" ~ "2) Sexual Risk (anal sex) past 12 months",
         TRUE ~ "(no data)"
      ),
      `DISAG 4`        = NA_character_,
      `DISAG 5`        = NA_character_,
      `Data Source`    = "OHASIS"
   ) %>%
   left_join(
      y  = ohasis$ref_faci_code %>%
         mutate(
            FACI_CODE     = case_when(
               stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
               stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
               stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
               TRUE ~ FACI_CODE
            ),
            SUB_FACI_CODE = if_else(
               condition = nchar(SUB_FACI_CODE) == 3,
               true      = NA_character_,
               false     = SUB_FACI_CODE
            ),
            SUB_FACI_CODE = case_when(
               FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
               FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
               FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
               TRUE ~ SUB_FACI_CODE
            ),
         ) %>%
         select(
            FACI_ID,
            faci                = FACI_CODE,
            branch              = SUB_FACI_CODE,
            `Site/Organization` = FACI_NAME,
            `Site City`         = FACI_NAME_MUNC,
            `Site Province`     = FACI_NAME_PROV,
            `Site Region`       = FACI_NAME_REG,
         ) %>%
         distinct_all(),
      by = c("faci", "branch")
   ) %>%
   left_join(
      y  = gf$sites %>%
         filter(!is.na(FACI_ID)) %>%
         select(
            GF_FACI    = FACI_ID,
            LS_SUBTYPE = `Clinic Type`
         ) %>%
         distinct_all() %>%
         add_row(GF_FACI = "130605", LS_SUBTYPE = "CBO") %>%
         bind_rows(
            ohasis$ref_faci %>%
               filter(
                  !(FACI_NHSSS_REG %in% c("CARAGA", "ARMM", "1", "2", "5", "8"))
               ) %>%
               inner_join(
                  y  = gf$forms$service_art %>% select(FACI_ID),
                  by = "FACI_ID"
               ) %>%
               anti_join(
                  y  = gf$sites %>%
                     filter(!is.na(FACI_ID)) %>%
                     select(FACI_ID),
                  by = "FACI_ID"
               ) %>%
               select(GF_FACI = FACI_ID)
         ) %>%
         distinct_all() %>%
         mutate(
            site_gf_2022 = 1,
            LS_SUBTYPE   = if_else(
               condition = is.na(LS_SUBTYPE),
               true      = "Treatment Hub",
               false     = LS_SUBTYPE,
               missing   = LS_SUBTYPE
            )
         ) %>%
         mutate(
            LS_SUBTYPE = if_else(
               condition = GF_FACI %in% c("130001", "070021"),
               true      = "TLY",
               false     = LS_SUBTYPE,
               missing   = LS_SUBTYPE
            )
         ) %>%
         distinct_all() %>%
         rename(FACI_ID = GF_FACI),
      by = "FACI_ID"
   ) %>%
   rename(
      `Logsheet Subtype` = LS_SUBTYPE,
   ) %>%
   arrange(reach_date, `DISAG 3`) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE)
rm(risk)
