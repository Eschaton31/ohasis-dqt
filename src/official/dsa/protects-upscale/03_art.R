source("src/official/dsa/protects-upscale/01_load_reqs.R")

tx_gf <- tx_out %>%
   left_join(sites %>% select(ART_FACI = FACI_ID, ART_SUB_FACI = FACI_CODE, site_gf_2024), join_by(ART_FACI, ART_SUB_FACI)) %>%
   mutate(
      keep = case_when(
         site_gf_2024 == 1 ~ 1,
         CENTRAL_ID %in% testing$CENTRAL_ID ~ 1,
         TRUE ~ 1
      )
   ) %>%
   filter(keep == 1) %>%
   select(-keep, -site_gf_2024) %>%
   left_join(
      y  = dx %>%
         select(idnum, reg_sex = sex, transmit, sexhow, confirm_date),
      by = join_by(idnum)
   ) %>%
   left_join(
      y  = tx_reg %>%
         select(art_id, birthdate, uic, baseline_cd4, baseline_cd4_date, baseline_cd4_result),
      by = join_by(art_id)
   ) %>%
   mutate(
      sex            = str_to_title(coalesce(reg_sex, sex)),
      vl_tested_p12m = case_when(
         !is.na(baseline_vl) & !is.na(vlp12m) ~ "Baseline VL",
         !is.na(baseline_vl) &
            !is.na(vlp12m) &
            vlp12m == 1 ~ "VL Undetectable (<50 copies/mL)",
         !is.na(baseline_vl) &
            !is.na(vlp12m) &
            vl_result < 1000 ~ "VL Suppressed (<1000 copies/mL)",
         !is.na(baseline_vl) &
            !is.na(vlp12m) &
            vlp12m == 0 ~ "VL Unsuppressed",
      ),
      curr_age       = calc_age(birthdate, latest_ffupdate)
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~na_if(., "")
   ) %>%
   mutate_if(
      .predicate = is.Date,
      ~if_else(. < -25567, NA_Date_, ., .)
   ) %>%
   select(
      REC_ID,
      CENTRAL_ID,
      art_id,
      idnum,
      prep_id,
      mort_id,
      uic,
      birthdate,
      sex,
      curr_age,
      artstart_date,
      baseline_cd4_date,
      baseline_cd4_result,
      baseline_cd4_cat = baseline_cd4,
      tx_hub,
      tx_reg,
      tx_prov,
      tx_munc,
      outcome,
      latest_ffupdate,
      latest_nextpickup,
      latest_regimen,
      vl_tested_p12m,
      vl_date,
      vl_result,
      who_staging,
      tb_status,
      confirm_date,
      transmit,
      sexhow
   )

con         <- ohasis$conn("lw")
rec_ids     <- tx_gf$REC_ID
rec_ids     <- rec_ids[!is.na(rec_ids)]
is_pregnant <- QB$new(con)$
   from("ohasis_warehouse.form_art_bc")$
   whereIn("REC_ID", rec_ids)$
   whereNotNull("IS_PREGNANT")$
   select(REC_ID, IS_PREGNANT)$
   get()
client_type <- QB$new(con)$
   from("ohasis_warehouse.form_art_bc")$
   whereIn("REC_ID", rec_ids)$
   whereNotNull("CLIENT_TYPE")$
   select(REC_ID, CLIENT_TYPE)$
   get()
dbDisconnect(con)

tx_gf %<>%
   left_join(distinct(is_pregnant)) %>%
   left_join(distinct(client_type)) %>%
   rename(
      dispense_modality = CLIENT_TYPE,
      is_pregnant       = IS_PREGNANT,
   ) %>%
   mutate(
      dispense_modality = remove_code(dispense_modality),
      dispense_modality = case_when(
         dispense_modality == "8" ~ "Courier",
         is.na(dispense_modality) ~ "Walk-in / Outpatient",
         TRUE ~ dispense_modality
      ),
      is_pregnant       = remove_code(is_pregnant),
      is_pregnant       = case_when(
         sex == "Male" ~ NA_character_,
         TRUE ~ is_pregnant
      )
   ) %>%
   select(
      -REC_ID,
   )

write_clip(names(tx_gf))

variables <- read_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "art")
dict      <- data_dictionary(tx_gf, variables)

write_sheet(dict, "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "try")
