source("src/official/dsa/protects-upscale/01_load_reqs.R")

prep_gf <- read_dta(hs_data("prep", "reg", 2024, 3)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   select(-any_of(c("prep_reg", "prep_prov", "prep_munc", "prep_faci"))) %>%
   left_join(
      y  = read_dta(
         hs_data("prep", "outcome", 2024, 3),
         col_select = c(
            prep_id,
            kp_pdl,
            kp_sw,
            kp_tg,
            kp_pwid,
            kp_msm,
            kp_ofw,
            kp_partner,
            kp_other,
            prepstart_date,
            prep_plan,
            prep_type,
            latest_ffupdate,
            latest_nextpickup,
            latest_regimen,
            faci,
            branch
         )
      ) %>%
         faci_code_to_id(
            ohasis$ref_faci_code,
            list(FACI_ID = "faci", SUB_FACI_ID = "branch")
         ) %>%
         mutate(
            PREP_FACI     = FACI_ID,
            PREP_SUB_FACI = SUB_FACI_ID
         ) %>%
         select(-any_of(c("prep_reg", "prep_prov", "prep_munc"))) %>%
         ohasis$get_faci(
            list(prep_faci = c("FACI_ID", "SUB_FACI_ID")),
            "name",
         ),
      by = join_by(prep_id)
   ) %>%
   left_join(
      y  = prep %>%
         select(prep_id, prepstart_hub = site_name),
      by = join_by(prep_id)
   ) %>%
   left_join(sites %>% select(PREP_FACI = FACI_ID, PREP_SUB_FACI = FACI_CODE, site_gf_2024), join_by(PREP_FACI, PREP_SUB_FACI)) %>%
   mutate(
      keep          = case_when(
         site_gf_2024 == 1 ~ 1,
         CENTRAL_ID %in% testing$CENTRAL_ID ~ 1,
         TRUE ~ 1
      ),

      birthdate     = case_when(
         prep_id == 399337 ~ as.Date("2004-08-20"),
         prep_id == 429548 ~ as.Date("1998-07-27"),
         prep_id == 432344 ~ as.Date("2002-07-08"),
         TRUE ~ birthdate
      ),

      sex           = str_to_title(sex),
      self_identity = str_to_title(self_identity),
   ) %>%
   filter(keep == 1) %>%
   select(-keep, -site_gf_2024) %>%
   mutate(
      curr_age = calc_age(birthdate, latest_ffupdate)
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~na_if(., "")
   ) %>%
   filter(!is.na(prepstart_date)) %>%
   select(
      CENTRAL_ID,
      prep_id,
      idnum,
      art_id,
      mort_id,
      uic,
      birthdate,
      sex,
      curr_age,
      self_identity,
      self_identity_other,
      gender_identity,
      perm_reg,
      perm_prov,
      perm_munc,
      curr_reg,
      curr_prov,
      curr_munc,
      hts_form,
      hts_modality,
      hts_result,
      hts_date,
      prep_hts_date,
      prep_risk_sexwithf,
      prep_risk_sexwithf_nocdm,
      prep_risk_sexwithm,
      prep_risk_sexwithm_nocdm,
      prep_risk_injectdrug,
      prep_risk_chemsex,
      prep_risk_sextransaction,
      prep_risk_sexwithplhiv_novl,
      prep_risk_sexunknownhiv,
      prep_risk_avgsexweek,
      hts_risk_motherhashiv,
      hts_risk_sexwithf,
      hts_risk_sexwithf_nocdm,
      hts_risk_sexwithm,
      hts_risk_sexwithm_nocdm,
      hts_risk_payingforsex,
      hts_risk_paymentforsex,
      hts_risk_sexwithhiv,
      hts_risk_injectdrug,
      hts_risk_needlestick,
      hts_risk_bloodtransfuse,
      hts_risk_illicitdrug,
      hts_risk_chemsex,
      hts_risk_tattoo,
      hts_risk_sti,
      kp_pdl,
      kp_sw,
      kp_tg,
      kp_pwid,
      kp_msm,
      kp_ofw,
      kp_partner,
      kp_other,
      prepstart_hub,
      prepstart_date,
      prep_faci,
      prep_plan,
      prep_type,
      latest_ffupdate,
      latest_nextpickup,
   ) %>%
   mutate_if(
      .predicate = is.Date,
      ~if_else(. < -25567, NA_Date_, ., .)
   ) %>%
   mutate(
      prep_plan = case_when(
         prep_plan == "(not on prep)" ~ "free",
         prep_plan == "(no data)" ~ "free",
         TRUE ~ prep_plan
      ),
      prep_type = case_when(
         prep_type == "(not on prep)" ~ "daily",
         prep_type == "(no data)" ~ "daily",
         TRUE ~ prep_type
      )
   )

write_clip(names(prep_gf))

variables <- read_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "prep")
dict      <- data_dictionary(prep_gf, variables)

write_sheet(dict, "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "try")

