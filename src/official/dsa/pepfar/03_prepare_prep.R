##  prepare dataset for tx indicators ------------------------------------------

tag_indicators <- function(data, coverage) {
   data %<>%
      mutate(
         # tag specific indicators
         PREP_SCREEN     = if_else(
            condition = prep_first_screen %within% interval(coverage$min, coverage$max),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         PREP_ELIG       = if_else(
            condition = PREP_SCREEN == 1 & eligible == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         PREP_INELIGIBLE = if_else(
            condition = PREP_SCREEN == 1 & coalesce(eligible, 0) == 0,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         PREP_CURR       = if_else(
            condition = !is.na(latest_regimen) & latest_ffupdate %within% interval(coverage$min, coverage$max),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         PREP_CT         = if_else(
            condition = PREP_CURR == 1 & prepstart_date < coverage$min,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         PREP_NEW        = if_else(
            condition = prepstart_date %within% interval(coverage$min, coverage$max),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
      )

   return(data)
}

generate_disagg <- function(data, coverage) {
   data %<>%
      mutate(
         # sex variable (use registry if available)
         Sex             = coalesce(str_left(sex, 1), "(no data)"),

         # KAP
         msm             = case_when(
            Sex == "M" & stri_detect_fixed(prep_risk_sexwithm, "yes") ~ 1,
            Sex == "M" & stri_detect_fixed(hts_risk_sexwithm, "yes") ~ 1,
            Sex == "M" & kp_msm == 1 ~ 1,
            TRUE ~ 0
         ),
         tgw             = if_else(
            condition = sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         hetero          = case_when(
            Sex == "M" &
               !grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) &
               grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) ~ 1,
            Sex == "F" &
               grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) &
               !grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) ~ 1,
            TRUE ~ 0
         ),
         pwid            = case_when(
            str_detect(prep_risk_injectdrug, "yes") ~ 1,
            str_detect(hts_risk_injectdrug, "yes") ~ 1,
            kp_pwid == 1 ~ 1,
            TRUE ~ 0
         ),
         unknown         = if_else(
            condition = risks == "(no data)",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         `KP Population` = case_when(
            msm == 1 & tgw == 0 ~ "MSM",
            msm == 1 & tgw == 1 ~ "TGW",
            pwid == 1 ~ "PWID",
            Sex == "F" ~ "(not included)",
            unknown == 1 ~ "(no data)",
            TRUE ~ "Non-MSM"
         ),

         # Age Band
         curr_age        = floor(calc_age(birthdate, latest_ffupdate)),
         Age_Band        = case_when(
            curr_age >= 0 & curr_age < 5 ~ "01_0-4",
            curr_age >= 5 & curr_age < 10 ~ "02_5-9",
            curr_age >= 10 & curr_age < 15 ~ "03_10-14",
            curr_age >= 15 & curr_age < 20 ~ "04_15-19",
            curr_age >= 20 & curr_age < 25 ~ "05_20-24",
            curr_age >= 25 & curr_age < 30 ~ "06_25-29",
            curr_age >= 30 & curr_age < 35 ~ "07_30-34",
            curr_age >= 35 & curr_age < 40 ~ "08_35-39",
            curr_age >= 40 & curr_age < 45 ~ "09_40-44",
            curr_age >= 45 & curr_age < 50 ~ "10_45-49",
            curr_age >= 50 & curr_age < 55 ~ "11_50-54",
            curr_age >= 55 & curr_age < 60 ~ "12_55-59",
            curr_age >= 60 & curr_age < 65 ~ "13_60-64",
            curr_age >= 65 & curr_age < 1000 ~ "14_65+",
            TRUE ~ "99_(no data)"
         ),
         `DATIM Age`     = if_else(
            condition = curr_age < 15,
            true      = "<15",
            false     = ">=15",
            missing   = "(no data)"
         ),
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code %>% distinct(FACI_CODE, SUB_FACI_CODE, .keep_all = TRUE),
         c(FACI_ID = "faci", SUB_FACI_ID = "branch")
      ) %>%
      left_join(
         y  = coverage$sites %>%
            select(
               FACI_ID,
               starts_with("site_")
            ) %>%
            distinct_all(),
         by = join_by(FACI_ID)
      ) %>%
      ohasis$get_faci(
         list(`Site/Organization` = c("FACI_ID", "SUB_FACI_ID")),
         "name",
         c("Site Region", "Site Province", "Site City")
      )

   return(data)
}

.init <- function(envir = parent.env(environment())) {
   p               <- envir
   p$linelist$prep <- tag_indicators(p$harp$prep$new, p$coverage) %>%
      generate_disagg(p$coverage)
}