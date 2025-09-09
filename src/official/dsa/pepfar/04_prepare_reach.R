##  prepare dataset for kp_prev & hts indicators -------------------------------

prepare_hts <- function(forms, harp, coverage) {
   data <- process_hts(forms$form_hts, forms$form_a, forms$form_cfbs) %>%
      filter(
         (date_confirm %within% interval(coverage$min, coverage$max) & confirm_result %in% c(1, 2, 3)) |
            hts_date %within% interval(coverage$min, coverage$max)
      ) %>%
      get_cid(forms$id_reg, patient_id) %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~null_dates(., "POSIXct")
      ) %>%
      mutate_if(
         .predicate = is.Date,
         ~null_dates(., "Date")
      ) %>%
      mutate(
         hts_priority = case_when(
            confirm_result %in% c(1, 2, 3) ~ 1,
            hts_result != "(no data)" & src %in% c("hts2021", "a2017") ~ 2,
            hts_result != "(no data)" & hts_modality == "fbt" ~ 3,
            hts_result != "(no data)" & hts_modality == "cbs" ~ 4,
            hts_result != "(no data)" & hts_modality == "fbs" ~ 5,
            hts_result != "(no data)" & hts_modality == "st" ~ 6,
            TRUE ~ 9999
         )
      ) %>%
      select(-any_of(c("transmit", "sexhow", "idnum"))) %>%
      left_join(
         y  = harp$dx %>%
            select(
               central_id,
               idnum,
               transmit,
               sexhow,
               confirm_date,
               ref_report,
               harpdx_birthdate  = bdate,
               harpdx_sex        = sex,
               harpdx_self_ident = self_identity,
               harpdx_faci,
               harpdx_sub_faci
            ),
         by = join_by(central_id)
      ) %>%
      left_join(
         y  = harp$tx$new %>%
            select(
               central_id,
               art_id,
               artstart_date
            ),
         by = join_by(central_id)
      )

   return(data)
}

consolidate_risks <- function(data) {
   risk <- data %>%
      select(
         rec_id,
         contains("risk", ignore.case = FALSE)
      ) %>%
      pivot_longer(
         cols = contains("risk", ignore.case = FALSE)
      ) %>%
      group_by(rec_id) %>%
      summarise(
         risks = stri_c(collapse = ", ", unique(sort(value)))
      )

   return(risk)
}

clean_hts <- function(data, risk) {
   data %<>%
      select(-matches("risks")) %>%
      left_join(y = risk, by = join_by(rec_id)) %>%
      mutate(
         # tag if central to be used
         use_harpdx        = if_else(
            condition = !is.na(idnum),
            true      = 1,
            false     = 0,
            missing   = 0
         ),

         # old confirmed
         old_dx            = case_when(
            confirm_date >= as.Date(pepfar$coverage$min) ~ 0,
            ref_report < as.Date(pepfar$coverage$min) ~ 1,
            TRUE ~ 0
         ),

         # tag those without form faci
         use_record_faci   = if_else(
            condition = is.na(service_faci),
            true      = 1,
            false     = 0
         ),

         # tag which test to be used
         final_faci        = case_when(
            old_dx == 0 & !is.na(harpdx_faci) ~ harpdx_faci,
            use_record_faci == 1 ~ faci_id,
            TRUE ~ service_faci
         ),
         final_sub_faci    = case_when(
            old_dx == 0 & !is.na(harpdx_faci) ~ harpdx_sub_faci,
            use_record_faci == 1 & faci_id == "130000" ~ specimen_sub_source,
            !(service_faci %in% c("130001", "130605", "040200")) ~ NA_character_,
            nchar(service_sub_faci) == 6 ~ NA_character_,
            TRUE ~ service_sub_faci
         ),

         hts_tst_result    = case_when(
            hts_result == "R" ~ "Reactive",
            hts_result == "NR" ~ "Non-reactive",
            hts_result == "IND" ~ "Indeterminate",
            is.na(hts_result) ~ "(no data)",
            TRUE ~ hts_result
         ),

         final_test_result = case_when(
            old_dx == 1 & !is.na(idnum) ~ "Confirmed: Known Pos",
            old_dx == 0 & !is.na(idnum) ~ "Confirmed: Positive",
            confirm_result == 1 ~ "Confirmed: Positive",
            confirm_result == 2 ~ "Confirmed: Negative",
            confirm_result == 3 ~ "Confirmed: Indeterminate",
            hts_modality == "FBT" ~ paste0("Tested: ", hts_tst_result),
            hts_modality == "FBS" ~ paste0("Tested: ", hts_tst_result),
            hts_modality == "CBS" ~ paste0("CBS: ", hts_tst_result),
            hts_modality == "ST" ~ paste0("Self-Testing: ", hts_tst_result),
         ),
      )

   return(data)
}

tag_indicators <- function(data) {
   confirm_data <- data %>%
      mutate(
         final_confirm_date = case_when(
            old_dx == 0 & hts_priority == 1 ~ as.Date(coalesce(date_confirm, t3_date, t2_date, t1_date)),
            old_dx == 1 ~ confirm_date,
            TRUE ~ NA_Date_
         )
      ) %>%
      filter(!is.na(final_confirm_date)) %>%
      select(central_id, final_confirm_date) %>%
      distinct(central_id, .keep_all = TRUE)

   data %<>%
      left_join(y = confirm_data, by = join_by(central_id)) %>%
      mutate(
         # tag specific indicators
         kp_prev        = 1,
         hts_tst        = if_else(
            condition = hts_tst_result != "(no data)",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         hts_tst_pos    = if_else(
            condition = final_test_result == "Confirmed: Positive",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         hts_tst_verify = if_else(
            condition = hts_modality %in% c("CBS", "ST") & final_confirm_date >= hts_date,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         tx_new_verify  = if_else(
            condition = hts_modality %in% c("CBS", "ST") & artstart_date >= hts_date,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         prep_offer     = if_else(
            condition = hts_result %in% c("IND", "NR") & keep_code(service_prep_refer) == "1",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
      )

   return(data)
}

generate_disagg <- function(data) {
   data %<>%
      mutate(
         # sex
         Sex             = coalesce(str_left(coalesce(harpdx_sex, remove_code(sex)), 1), "(no data)"),

         # kap
         msm             = case_when(
            use_harpdx == 1 &
               Sex == "M" &
               sexhow %in% c("HOMOSEXUAL", "BISEXUAL") ~ 1,
            use_harpdx == 0 &
               Sex == "M" &
               grepl("yes-", risk_sexwithm) ~ 1,
            TRUE ~ 0
         ),
         tgw             = case_when(
            use_harpdx == 1 &
               msm == 1 &
               harpdx_self_ident %in% c("FEMALE", "OTHERS") ~ 1,
            use_harpdx == 0 &
               msm == 1 &
               keep_code(self_ident) %in% c("2", "3") ~ 1,
            TRUE ~ 0
         ),
         hetero          = case_when(
            use_harpdx == 1 & sexhow == "HETEROSEXUAL" ~ 1,
            use_harpdx == 0 &
               Sex == "M" &
               !grepl("yes-", risk_sexwithm) &
               grepl("yes-", risk_sexwithf) ~ 1,
            use_harpdx == 0 &
               Sex == "F" &
               grepl("yes-", risk_sexwithm) &
               !grepl("yes-", risk_sexwithf) ~ 1,
            TRUE ~ 0
         ),
         pwid            = case_when(
            use_harpdx == 1 & transmit == "IVDU" ~ 1,
            use_harpdx == 0 & grepl("yes-", risk_injectdrug) ~ 1,
            TRUE ~ 0
         ),
         sw              = case_when(
            stri_detect_fixed(risk_paymentforsex, "yes") ~ 1,
            TRUE ~ 0
         ),
         unknown         = case_when(
            transmit == "unknown" ~ 1,
            risks == "(no data)" ~ 1,
            TRUE ~ 0
         ),
         `KP Population` = case_when(
            msm == 1 & tgw == 0 ~ "MSM",
            msm == 1 & tgw == 1 ~ "TGW",
            pwid == 1 ~ "PWID",
            Sex == "F" ~ "(not included)",
            unknown == 1 ~ "(no data)",
            TRUE ~ "Non-MSM"
         ),

         # for aiha
         # `KP Population` = case_when(
         #    pwid == 1 ~ "pwid",
         #    msm == 1 & tgw == 0 ~ "msm",
         #    msm == 1 & tgw == 1 ~ "tgw",
         #    Sex == "F" & sw == 1 ~ "fsw",
         #    unknown == 1 ~ "(no data)",
         #    Sex == "F" ~ "Non-KP Female",
         #    Sex == "M" ~ "Non-KP Male",
         #    TRUE ~ "Non-msm"
         # ),

         # Age Band
         curr_age        = calc_age(coalesce(harpdx_birthdate, birthdate), hts_date),
         curr_age        = if_else(curr_age <= 0 & !is.na(age), age, curr_age, curr_age),
         curr_age        = floor(coalesce(curr_age, age)),
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
      left_join(
         y  = coverage$sites %>%
            select(
               faci_id,
               starts_with("site_")
            ) %>%
            distinct_all(),
         by = join_by(final_faci == faci_id)
      ) %>%
      ohasis$get_faci(
         list(`Site/Organization` = c("final_faci", "final_sub_faci")),
         "name",
         c("Site Region", "Site Province", "Site City")
      ) %>%
      mutate(
         confirm_result = case_when(
            confirm_result == 1 ~ "1_Positive",
            confirm_result == 2 ~ "2_Negative",
            confirm_result == 3 ~ "3_Indeterminate",
            confirm_result == 4 ~ "4_Pending",
            confirm_result == 5 ~ "5_Duplicate",
         )
      ) %>%
      ohasis$get_faci(
         list(specimen_source_faci = c("specimen_source", "specimen_sub_source")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(confirm_lab = c("confirm_faci", "confirm_sub_faci")),
         "name"
      ) %>%
      get_addr(
         c(
            perm_reg  = "perm_reg",
            perm_prov = "perm_prov",
            perm_munc = "perm_munc"
         ),
         "name"
      ) %>%
      get_addr(
         c(
            curr_reg  = "curr_reg",
            curr_prov = "curr_prov",
            curr_munc = "curr_munc"
         ),
         "name"
      ) %>%
      get_addr(
         c(
            birth_reg  = "birth_reg",
            birth_prov = "birth_prov",
            birth_munc = "birth_munc"
         ),
         "name"
      ) %>%
      get_addr(
         c(
            cbs_reg  = "hiv_service_reg",
            cbs_prov = "hiv_service_prov",
            cbs_munc = "hiv_service_munc"
         ),
         "name"
      ) %>%
      rename(
         created = created_by,
         updated = updated_by,
         deleted = deleted_by,
      ) %>%
      ohasis$get_staff(c(created_by = "created")) %>%
      ohasis$get_staff(c(updated_by = "updated")) %>%
      ohasis$get_staff(c(deleted_by = "deleted")) %>%
      ohasis$get_staff(c(hts_provider = "provider_id")) %>%
      generate_gender_identity(sex, self_ident, self_ident_other, gender_identity) %>%
      rename(
         hts_provider_type       = provider_type,
         hts_provider_type_other = provider_type_other,
      ) %>%
      select(
         -faci_id,
         -sub_faci_id,
         -service_faci,
         -any_of(c(
            "modality",
            "use_record_faci",
            "idnum",
            "perm_addr",
            "curr_addr",
            "birth_addr",
            "first",
            "middle",
            "last",
            "suffix",
            "client_email",
            "client_mobile",
            "uic",
            "philhealth_no",
            "patient_code",
            "philsys_id",
            "confirmatory_code",
            "snapshot",
            "prime",
            "patient_id",
            "record_date",
            "disease",
            "deleted_at",
            "deleted_by",
            "birthdate",
            "hiv_service_type",
            "gender_affirm_therapy",
            "hiv_service_addr",
            "src",
            "module"
         )),
         -c(
            starts_with("signature", ignore.case = FALSE),
            ends_with("sub_faci", ignore.case = FALSE),
            ends_with("msm", ignore.case = FALSE),
            ends_with("tgw", ignore.case = FALSE),
            ends_with("fsw", ignore.case = FALSE),
            ends_with("pwid", ignore.case = FALSE),
            ends_with("genpop", ignore.case = FALSE),
            ends_with("_NA", ignore.case = FALSE),
            starts_with("signatory_", ignore.case = FALSE)
         )
      ) %>%
      mutate(
         rt_agreed       = NA_character_,
         rt_specimen     = NA_character_,
         rt_result       = NA_character_,
         rt_vl_requested = NA_character_,
         rt_vl_date      = NA_Date_,
         rt_vl_result    = NA_character_,
         rita_result     = NA_character_,
      ) %>%
      mutate(
         `CBS Region`   = if_else(
            hts_modality %in% c("CBS", "FBS", "ST"),
            cbs_reg,
            NA_character_,
            NA_character_
         ),
         `CBS Province` = if_else(
            hts_modality %in% c("CBS", "FBS", "ST"),
            cbs_prov,
            NA_character_,
            NA_character_
         ),
         `CBS City`     = if_else(
            hts_modality %in% c("CBS", "FBS", "ST"),
            cbs_munc,
            NA_character_,
            NA_character_
         )
      )

   return(data)
}

.init <- function(envir = parent.env(environment())) {
   p     <- envir
   reach <- prepare_hts(p$forms, p$harp, p$coverage)
   risks <- consolidate_risks(reach)

   p$linelist$reach <- clean_hts(reach, risks) %>%
      tag_indicators() %>%
      generate_disagg()

   p$linelist$reach %<>%
      left_join(
         y  = hs_data("harp_dx", "reg", p$coverage$curr$yr, p$coverage$curr$mo) %>%
            read_dta(col_select = c(any_of(c('PATIENT_ID', 'patient_id')), confirm_date)) %>%
            rename_all(tolower) %>%
            get_cid(p$forms$id_reg, patient_id) %>%
            select(-patient_id) %>%
            rename(harp_confirm_date = confirm_date),
         by = join_by(central_id)
      ) %>%
      left_join(
         y  = hs_data("harp_tx", "reg", p$coverage$curr$yr, p$coverage$curr$mo) %>%
            read_dta(col_select = c(any_of(c('PATIENT_ID', 'patient_id')), artstart_date)) %>%
            rename_all(tolower) %>%
            get_cid(p$forms$id_reg, patient_id) %>%
            select(-patient_id) %>%
            rename(art_start_date = artstart_date),
         by = join_by(central_id)
      ) %>%
      left_join(
         y  = hs_data("prep", "outcome", p$coverage$curr$yr, p$coverage$curr$mo) %>%
            read_dta(col_select = c(any_of(c('PATIENT_ID', 'patient_id')), prepstart_date)) %>%
            rename_all(tolower) %>%
            get_cid(p$forms$id_reg, patient_id) %>%
            select(-patient_id) %>%
            rename(prep_start_date = prepstart_date),
         by = join_by(central_id)
      )
}