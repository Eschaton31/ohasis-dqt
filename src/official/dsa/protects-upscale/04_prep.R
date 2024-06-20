source("src/official/dsa/protects-upscale/01_load_reqs.R")

prep_gf <- read_dta(hs_data("prep", "reg", 2024, 5)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   select(-any_of(c("prep_reg", "prep_prov", "prep_munc", "prep_faci", 'REC_ID'))) %>%
   left_join(
      y  = read_dta(
         hs_data("prep", "outcome", 2024, 5),
         col_select = c(
            REC_ID,
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
            PREP_SUB_FACI = SUB_FACI_ID,
         ) %>%
         select(-any_of(c("prep_reg", "prep_prov", "prep_munc"))) %>%
         ohasis$get_faci(
            list(prep_faci = c("FACI_ID", "SUB_FACI_ID")),
            "name",
         ),
      by = join_by(prep_id)
   ) %>%
   left_join(
      y  = prep_start %>%
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
      REC_ID,
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
      latest_regimen,
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


con       <- ohasis$conn("lw")
rec_ids   <- prep_gf$REC_ID
rec_ids   <- rec_ids[!is.na(rec_ids)]
form_prep <- QB$new(con)$
   from("ohasis_warehouse.form_prep")$
   whereIn("REC_ID", rec_ids)$
   get()
dbDisconnect(con)
con       <- ohasis$conn("db")
prep_disc <- QB$new(con)$
   from("ohasis_interim.px_prep_discontinue")$
   whereIn("REC_ID", rec_ids)$
   get()
prep_disc %<>%
   mutate(
      DISC_REASON = case_when(
         DISC_REASON == 1 ~ "prep_cost",
         DISC_REASON == 2 ~ "seroconvert",
         DISC_REASON == 3 ~ "ineligible",
         DISC_REASON == 4 ~ "reduced_risk",
         DISC_REASON == 6 ~ "difficult_regimen",
         DISC_REASON == 7 ~ "stigma",
         DISC_REASON == 8888 ~ "other",
      ),
   ) %>%
   rename(
      disc = IS_DISC,
   ) %>%
   mutate(
      disc = case_when(
         disc == 1 ~ "Yes",
         disc == 0 ~ "No",
      )
   ) %>%
   pivot_wider(
      id_cols     = REC_ID,
      names_from  = DISC_REASON,
      values_from = c(disc, DISC_OTHER)
   ) %>%
   select(
      REC_ID,
      disc_reduced_risk,
      disc_seroconvert,
      disc_ineligible,
      disc_prep_cost,
      disc_difficult_regimen,
      disc_other,
      disc_other_text = DISC_OTHER_other,
   )
dbDisconnect(con)

prep_gf %<>%
   left_join(
      y  = form_prep %>%
         select(
            REC_ID,
            CREATED = CREATED_BY,
            CREATED_AT,
            UPDATED = UPDATED_BY,
            UPDATED_AT,
         ) %>%
         ohasis$get_staff(c(CREATED_BY = "CREATED")) %>%
         ohasis$get_staff(c(UPDATED_BY = "UPDATED")) %>%
         distinct(REC_ID, .keep_all = TRUE),
      by = join_by(REC_ID)
   ) %>%
   left_join(
      y  = prep_disc,
      by = join_by(REC_ID)
   ) %>%
   relocate(CREATED_BY, CREATED_AT, UPDATED_BY, UPDATED_AT, .after = REC_ID) %>%
   relocate(CENTRAL_ID, .before = 1)


write_clip(names(prep_gf))

variables <- read_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "prep")
dict      <- data_dictionary(prep_gf, variables)

write_sheet(dict, "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "final-prep")

