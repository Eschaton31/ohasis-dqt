source("src/official/dsa/protects-upscale/01_load_reqs.R")

tx_gf <- tx_out %>%
   left_join(sites %>% select(art_faci = faci_id, art_sub_faci = faci_code, site_gf_2024), join_by(art_faci, art_sub_faci)) %>%
   mutate(
      keep = case_when(
         site_gf_2024 == 1 ~ 1,
         central_id %in% testing$central_id ~ 1,
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
      rec_id,
      central_id,
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

con         <- connect('mariadb-lw')
rec_ids     <- tx_gf$rec_id
rec_ids     <- rec_ids[!is.na(rec_ids)]
form_art_bc <- QB$new(con)$
   from("ohasis_warehouse.form_art_bc")$
   whereIn("rec_id", rec_ids)$
   get()
dbDisconnect(con)

tx_gf %<>%
   left_join(
      y  = form_art_bc %>%
         select(
            rec_id,
            created            = created_by,
            created_at,
            updated            = updated_by,
            updated_at,
            tb_status          = tb_status,
            tb_ipt_start_date  = tb_ipt_start_date,
            tb_ipt_status      = tb_ipt_status,
            tb_ipt_outcome     = tb_ipt_outcome,
            tb_site_p          = tb_site_p,
            tb_site_ep         = tb_site_ep,
            tb_drug_resistance = tb_drug_resistance,
            tb_tx_status       = tb_tx_status,
            tb_tx_outcome      = tb_tx_outcome,
            dispense_modality  = client_type,
            is_pregnant        = is_pregnant,
            oi_syph            = oi_syph_present,
            oi_hepb            = oi_hepb_present,
            oi_hepc            = oi_hepc_present,
            oi_pcp             = oi_pcp_present,
            oi_cmv             = oi_cmv_present,
            oi_orocandidiasis  = oi_orocand_present,
            oi_herpes_zoster   = oi_herpes_present,
            oi_other           = oi_other_present,
         ) %>%
         ohasis$get_staff(c(created_by = "created")) %>%
         ohasis$get_staff(c(updated_by = "updated")) %>%
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
         distinct(rec_id, .keep_all = TRUE),
      by = join_by(rec_id)
   ) %>%
   left_join(
      y  = dead %>%
         select(
            central_id,
            date_of_death,
         ) %>%
         mutate(
            reported_dead = 1
         ),
      by = join_by(central_id)
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
   relocate(created_by, created_at, updated_by, updated_at, .after = rec_id) %>%
   relocate(central_id, .before = 1)

write_clip(names(tx_gf))

variables <- read_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "art")
dict      <- data_dictionary(tx_gf, variables)

write_sheet(dict, "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "final-art")
