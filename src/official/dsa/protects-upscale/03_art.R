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
         select(idnum, reg_sex = sex, transmit, sexhow, confirm_date, confirm_lab),
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
      confirm_date,
      confirm_lab,
      transmit,
      sexhow
   )

con         <- ohasis$conn("lw")
rec_ids     <- tx_gf$REC_ID
rec_ids     <- rec_ids[!is.na(rec_ids)]
form_art_bc <- QB$new(con)$
   from("ohasis_warehouse.form_art_bc")$
   whereIn("REC_ID", rec_ids)$
   get()
dbDisconnect(con)

tx_gf %<>%
   left_join(
      y  = form_art_bc %>%
         select(
            REC_ID,
            CREATED            = CREATED_BY,
            CREATED_AT,
            UPDATED            = UPDATED_BY,
            UPDATED_AT,
            tb_status          = TB_STATUS,
            tb_ipt_start_date  = TB_IPT_START_DATE,
            tb_ipt_status      = TB_IPT_STATUS,
            tb_ipt_outcome     = TB_IPT_OUTCOME,
            tb_site_p          = TB_SITE_P,
            tb_site_ep         = TB_SITE_EP,
            tb_drug_resistance = TB_DRUG_RESISTANCE,
            tb_tx_status       = TB_TX_STATUS,
            tb_tx_outcome      = TB_TX_OUTCOME,
            dispense_modality  = CLIENT_TYPE,
            is_pregnant        = IS_PREGNANT,
            oi_syph            = OI_SYPH_PRESENT,
            oi_hepb            = OI_HEPB_PRESENT,
            oi_hepc            = OI_HEPC_PRESENT,
            oi_pcp             = OI_PCP_PRESENT,
            oi_cmv             = OI_CMV_PRESENT,
            oi_orocandidiasis  = OI_OROCAND_PRESENT,
            oi_herpes_zoster   = OI_HERPES_PRESENT,
            oi_other           = OI_OTHER_PRESENT,
         ) %>%
         ohasis$get_staff(c(CREATED_BY = "CREATED")) %>%
         ohasis$get_staff(c(UPDATED_BY = "UPDATED")) %>%
         mutate_at(
            .vars = vars(
               tb_status,
               tb_ipt_status,
               tb_ipt_outcome,
               tb_site_p,
               tb_site_ep,
               tb_drug_resistance,
               tb_tx_status,
               tb_tx_outcome,
               dispense_modality,
               is_pregnant,
               starts_with("oi_")
            ),
            ~remove_code(.)
         ) %>%
         distinct(REC_ID, .keep_all = TRUE),
      by = join_by(REC_ID)
   ) %>%
   left_join(
      y  = dead %>%
         select(
            CENTRAL_ID,
            date_of_death,
         ) %>%
         mutate(
            reported_dead = 1
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      dispense_modality = case_when(
         dispense_modality == "8" ~ "Courier",
         is.na(dispense_modality) ~ "Walk-in / Outpatient",
         TRUE ~ dispense_modality
      ),
      is_pregnant       = case_when(
         sex == "Male" ~ NA_character_,
         TRUE ~ is_pregnant
      )
   ) %>%
   relocate(CREATED_BY, CREATED_AT, UPDATED_BY, UPDATED_AT, .after = REC_ID) %>%
   relocate(CENTRAL_ID, .before = 1)

write_clip(names(tx_gf))

variables <- read_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "art")
dict      <- data_dictionary(tx_gf, variables)

write_sheet(dict, "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "final-art")
