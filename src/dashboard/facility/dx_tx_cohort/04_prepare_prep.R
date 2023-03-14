get_faci_ids <- function(harp, oh, data) {
   data$prep <- harp$prep %>%
      get_cid(oh$id_reg, PATIENT_ID) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         c(PREP_FACI = "faci", PREP_SUB_FACI = "branch")
      ) %>%
      mutate_at(
         .vars = vars(PREP_FACI),
         ~if_else(. == "130000", NA_character_, ., .),
      ) %>%
      distinct(prep_id, .keep_all = TRUE)

   return(data)
}

# attach facility names
add_faci_info <- function(data) {
   data$prep %<>%
      mutate(CURR_FACI = PREP_FACI) %>%
      ohasis$get_faci(
         list(PREP_HUB = c("PREP_FACI", "PREP_SUB_FACI")),
         "name",
         c("PREP_REG", "PREP_PROV", "PREP_MUNC")
      )

   return(data)
}

# disaggregations
gen_disagg <- function(data, params) {
   data$prep %<>%
      mutate(
         # sex
         sex              = coalesce(StrLeft(sex, 1), "(no data)"),

         # KAP
         msm              = case_when(
            sex == "M" & stri_detect_fixed(prep_risk_sexwithm, "yes") ~ 1,
            sex == "M" & stri_detect_fixed(hts_risk_sexwithm, "yes") ~ 1,
            sex == "M" & kp_msm == 1 ~ 1,
            TRUE ~ 0
         ),
         tgw              = if_else(
            condition = sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         hetero           = case_when(
            sex == "M" &
               !grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) &
               grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) ~ 1,
            sex == "F" &
               grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) &
               !grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) ~ 1,
            TRUE ~ 0
         ),
         pwid             = case_when(
            str_detect(prep_risk_injectdrug, "yes") ~ 1,
            str_detect(hts_risk_injectdrug, "yes") ~ 1,
            kp_pwid == 1 ~ 1,
            TRUE ~ 0
         ),
         unknown          = if_else(
            condition = risks == "(no data)",
            true      = 1,
            false     = 0,
            missing   = 0
         ),

         # kap
         sex              = case_when(
            sex == "M" ~ "Male",
            sex == "F" ~ "Female",
            TRUE ~ "(no data)"
         ),
         kap_type         = case_when(
            msm == 1 & tgw == 0 ~ "MSM",
            msm == 1 & tgw == 1 ~ "MSM-TGW",
            hetero == 0 & pwid == 1 ~ "PWID",
            sex == "Male" ~ "Other Males",
            sex == "Female" ~ "Other Females",
            TRUE ~ "Other"
         ),

         # dx age
         curr_age_c1      = gen_agegrp(curr_age, "harp"),
         curr_age_c2      = gen_agegrp(curr_age, "5yr"),

         # sex
         sex              = case_when(
            StrLeft(toupper(sex), 1) == "M" ~ "Male",
            StrLeft(toupper(sex), 1) == "F" ~ "Female",
            TRUE ~ "(no data)"
         ),

         mortality        = if_else(outcome == "dead", 1, 0, 0),
         plhiv            = if_else(mortality == 0, 1, 0, 0),
         everonprep       = if_else(!is.na(prepstart_date), 1, 0, 0),
         everonprep_plhiv = if_else(everonprep == 1 & mortality == 0, 1, 0, 0),
      )

   return(data)
}

tag_indicators <- function(data) {
   data %<>%
      mutate(
         # tag specific indicators
         prep_screen     = 1,
         prep_elig       = if_else(
            condition = prep_screen == 1 & eligible == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         prep_ineligible = if_else(
            condition = prep_screen == 1 & coalesce(eligible, 0) == 0,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
      )

   return(data)
}

remove_cols <- function(data, oh) {
   data$prep %<>%
      select(
         -ends_with("PSGC_MUNC"),
         -ends_with("PSGC_PROV"),
         -any_of(c(
            "hub",
            "branch",
            "realhub",
            "realhub_branch",
            "previous_ffupdate",
            "previous_nextpickup",
            "previous_regimen",
            "art_reg",
            "PATIENT_ID",
            "artlen_days",
            "artlen_months",
            "diff",
            "startpickuplen_months",
            "ltfulen_month"
         ))
      ) %>%
      left_join(
         y        = oh$prep %>%
            mutate(ORIG_FACI = FACI_ID) %>%
            ohasis$get_faci(
               list(HUB_NAME = c("FACI_ID", "SUB_FACI_ID")),
               "name",
               c("HUB_REG", "HUB_PROV", "HUB_MUNC")
            ) %>%
            distinct(CENTRAL_ID, HUB_NAME, .keep_all = TRUE),
         by       = join_by(CENTRAL_ID),
         multiple = "all"
      ) %>%
      rename(
         FACI_ID        = ORIG_FACI,
         CURR_PREP_HUB  = PREP_HUB,
         CURR_PREP_REG  = PREP_REG,
         CURR_PREP_PROV = PREP_PROV,
         CURR_PREP_MUNC = PREP_MUNC
      )

   return(data)
}

.init <- function(envir = parent.env(environment())) {
   p           <- envir
   p$data      <- get_faci_ids(p$harp, p$oh, p$data)
   p$data      <- add_faci_info(p$data)
   p$data      <- gen_disagg(p$data, p$params)
   p$data      <- remove_cols(p$data, p$oh)
   p$data$prep <- tag_indicators(p$data$prep)
}