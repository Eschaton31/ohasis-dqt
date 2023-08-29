##  Cleaning -------------------------------------------------------------------

prep      <- read_dta(hs_data("prep", "reg", 2023, 6)) %>%
   left_join(
      y  = hs_data("prep", "outcome", 2023, 6) %>%
         read_dta(col_select = c(prep_id, prep_reg, prepstart_date, latest_ffupdate, latest_regimen, kp_msm, kp_pwid, kp_sw, faci, branch)),
      by = join_by(prep_id)
   ) %>%
   left_join(
      y  = read_dta("H:/20230710_onprep_2023-06_prepstart_faci+type.dta"),
      by = join_by(prep_id)
   ) %>%
   rename(prepstart_date = prepstart_date.x)
prep_risk <- prep %>%
   select(
      prep_id,
      contains("risk", ignore.case = FALSE)
   ) %>%
   select(-risk_screen) %>%
   pivot_longer(
      cols = contains("risk", ignore.case = FALSE)
   ) %>%
   group_by(prep_id) %>%
   summarise(
      risks = stri_c(collapse = ", ", unique(sort(value)))
   )
prep %<>%
   left_join(prep_risk, join_by(prep_id)) %>%
   mutate(
      prep_enroll_age      = floor(calc_age(birthdate, prepstart_date)),
      prep_enroll_age_band = case_when(
         prep_enroll_age %in% seq(0, 14) ~ "1_<15",
         prep_enroll_age %in% seq(15, 17) ~ "2_15-17",
         prep_enroll_age %in% seq(18, 24) ~ "3_18-24",
         prep_enroll_age %in% seq(25, 35) ~ "4_25-35",
         prep_enroll_age %in% seq(36, 1000) ~ "5_>35",
      ),
      prep_offer           = 1,
      prep_new             = if_else(!is.na(prepstart_date), 1, 0, 0),
      prep_curr            = if_else(prep_new == 1 & latest_ffupdate %within% interval("2022-07-01", "2023-06-30"), 1, 0, 0),

      msm                  = case_when(
         stri_detect_fixed(prep_risk_sexwithm, "yes") ~ 1,
         stri_detect_fixed(hts_risk_sexwithm, "yes") ~ 1,
         # kp_msm == 1 ~ 1,
         TRUE ~ 0
      ),
      sw                   = case_when(
         stri_detect_fixed(hts_risk_paymentforsex, "yes") ~ 1,
         # kp_sw == 1 ~ 1,
         TRUE ~ 0
      ),
      tgw                  = if_else(
         condition = sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero               = if_else(
         condition = sex == self_identity & msm == 0,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      pwid                 = case_when(
         str_detect(prep_risk_injectdrug, "yes") ~ 1,
         str_detect(hts_risk_injectdrug, "yes") ~ 1,
         # kp_pwid == 1 ~ 1,
         TRUE ~ 0
      ),
      unknown              = if_else(
         condition = risks == "(no data)",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      kap_type             = case_when(
         sex == "FEMALE" & sw == 1 ~ "fsw",
         msm == 1 & tgw == 0 ~ "msm",
         msm == 1 & tgw == 1 ~ "tgw",
         hetero == 1 & sex == "FEMALE" ~ "heterof",
         hetero == 1 & sex == "MALE" ~ "heterom",
         pwid == 1 ~ "pwid",
         unknown == 1 ~ "unknown",
         TRUE ~ "others"
      ),
      enroll_reg_in        = if_else(prep_new == 1 & perm_curr_reg == prepstart_reg, 1, 0, 0),
      enroll_reg_out       = if_else(prep_new == 1 & perm_curr_reg != prepstart_reg, 1, 0, 0),

      region               = perm_curr_reg,
      province             = perm_curr_prov,
      muncity              = perm_curr_munc,

      final_msm            = if_else(msm == 1 & tgw == 0, 1, 0, 0),
      final_tgw            = if_else(msm == 1 & tgw == 1, 1, 0, 0),
      final_fsw            = if_else(sex == "FEMALE" & sw == 1, 1, 0, 0),
      final_pwid           = if_else(pwid == 1, 1, 0, 0),

      prepstart_branch     = prepstart_faci,
      prepstart_faci       = case_when(
         stri_detect_regex(prepstart_branch, "^HASH") ~ "HASH",
         stri_detect_regex(prepstart_branch, "^SAIL") ~ "SAIL",
         stri_detect_regex(prepstart_branch, "^TLY") ~ "TLY",
         TRUE ~ prepstart_faci
      ),
      prepstart_branch     = if_else(
         condition = nchar(prepstart_branch) == 3,
         true      = NA_character_,
         false     = prepstart_branch
      ),
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(PREP_FACI = "prepstart_faci", PREP_SUB_FACI = "prepstart_branch")
   ) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   ohasis$get_faci(
      list(PREPFIRST_FACI = c("PREP_FACI", "PREP_SUB_FACI")),
      "name",
   )

##  By Residence ---------------------------------------------------------------

# 1) sites
prep_briefer       <- list()
prep_briefer$sites <- faci$faci_prep %>%
   select(FACI_ID, SUB_FACI_ID) %>%
   left_join(ohasis$ref_faci, join_by(FACI_ID, SUB_FACI_ID)) %>%
   rename(
      region   = FACI_NHSSS_REG,
      province = FACI_NHSSS_PROV,
      muncity  = FACI_NHSSS_MUNC,
   ) %>%
   group_by(region) %>%
   summarise(
      prep_sites = n()
   )

# 2) offer>enroll>curr
prep_briefer$offer_enroll_curr <- prep %>%
   group_by(region) %>%
   summarise_at(
      .vars = vars(prep_offer, prep_new, prep_curr),
      ~sum(.)
   )

# 3) tabstat
prep_briefer$tabstat <- prep %>%
   filter(prep_new == 1) %>%
   group_by(region) %>%
   summarise(
      prep_new_median = median(prep_enroll_age),
      prep_new_min    = min(prep_enroll_age),
      prep_new_max    = max(prep_enroll_age),
   )

# 4) per kp
prep_briefer$per_kp <- prep %>%
   filter(prep_new == 1) %>%
   arrange(region, prep_enroll_age_band) %>%
   mutate(
      prep_enroll_age_band = remove_code(prep_enroll_age_band)
   ) %>%
   group_by(region, prep_enroll_age_band) %>%
   summarise_at(
      .vars = vars(final_msm, final_tgw, final_fsw, final_pwid),
      ~sum(.)
   ) %>%
   rename_all(
      ~str_replace(., "^final_", "")
   ) %>%
   pivot_wider(
      id_cols     = region,
      names_from  = prep_enroll_age_band,
      values_from = c(msm, tgw, fsw, pwid)
   ) %>%
   ungroup() %>%
   mutate(
      `tgw_<15`    = NA_integer_,
      `fsw_<15`    = NA_integer_,
      `pwid_<15`   = NA_integer_,
      `pwid_15-17` = NA_integer_,
      `pwid_18-24` = NA_integer_,
      `pwid_>35`   = NA_integer_,
   ) %>%
   select(
      region,
      `msm_<15`,
      `msm_15-17`,
      `msm_18-24`,
      `msm_25-35`,
      `msm_>35`,
      `tgw_<15`,
      `tgw_15-17`,
      `tgw_18-24`,
      `tgw_25-35`,
      `tgw_>35`,
      `fsw_<15`,
      `fsw_15-17`,
      `fsw_18-24`,
      `fsw_25-35`,
      `fsw_>35`,
      `pwid_<15`,
      `pwid_15-17`,
      `pwid_18-24`,
      `pwid_25-35`,
      `pwid_>35`,
   )

##  By Enrollment --------------------------------------------------------------

# 1) sites
prep_briefer       <- list()
prep_briefer$sites <- faci$faci_prep %>%
   select(FACI_ID, SUB_FACI_ID) %>%
   left_join(ohasis$ref_faci, join_by(FACI_ID, SUB_FACI_ID)) %>%
   rename(
      region   = FACI_NHSSS_REG,
      province = FACI_NHSSS_PROV,
      muncity  = FACI_NHSSS_MUNC,
   ) %>%
   group_by(region) %>%
   summarise(
      prep_sites = n()
   )

# 2) offer>enroll>curr
prep_briefer$offer_enroll_curr <- prep %>%
   select(-perm_curr_reg, -region) %>%
   rename(region = prepstart_reg) %>%
   mutate(
      region = if_else(region != "" & region != "UNKNOWN", region, prep_first_reg),
      region = if_else(region != ""& region != "UNKNOWN", region, prep_reg)
   ) %>%
   filter(region != "") %>%
   group_by(region) %>%
   summarise_at(
      .vars = vars(prep_offer, prep_new, prep_curr),
      ~sum(.)
   )

# 3) tabstat
prep_briefer$tabstat <- prep %>%
   select(-perm_curr_reg, -region) %>%
   rename(region = prepstart_reg) %>%
   filter(region != "") %>%
   filter(prep_new == 1) %>%
   group_by(region) %>%
   summarise(
      prep_new_median = median(prep_enroll_age),
      prep_new_min    = min(prep_enroll_age),
      prep_new_max    = max(prep_enroll_age),
   )

# 4) per kp
prep_briefer$per_kp <- prep %>%
   select(-perm_curr_reg, -region) %>%
   rename(region = prepstart_reg) %>%
   filter(region != "") %>%
   filter(prep_new == 1) %>%
   arrange(region, prep_enroll_age_band) %>%
   mutate(
      prep_enroll_age_band = remove_code(prep_enroll_age_band)
   ) %>%
   group_by(region, prep_enroll_age_band) %>%
   summarise_at(
      .vars = vars(final_msm, final_tgw, final_fsw, final_pwid),
      ~sum(.)
   ) %>%
   rename_all(
      ~str_replace(., "^final_", "")
   ) %>%
   pivot_wider(
      id_cols     = region,
      names_from  = prep_enroll_age_band,
      values_from = c(msm, tgw, fsw, pwid)
   ) %>%
   ungroup() %>%
   mutate(
      `tgw_<15`    = NA_integer_,
      `fsw_<15`    = NA_integer_,
      `pwid_<15`   = NA_integer_,
      `pwid_15-17` = NA_integer_,
      `pwid_18-24` = NA_integer_,
      `pwid_>35`   = NA_integer_,
   ) %>%
   select(
      region,
      `msm_<15`,
      `msm_15-17`,
      `msm_18-24`,
      `msm_25-35`,
      `msm_>35`,
      `tgw_<15`,
      `tgw_15-17`,
      `tgw_18-24`,
      `tgw_25-35`,
      `tgw_>35`,
      `fsw_<15`,
      `fsw_15-17`,
      `fsw_18-24`,
      `fsw_25-35`,
      `fsw_>35`,
      `pwid_<15`,
      `pwid_15-17`,
      `pwid_18-24`,
      `pwid_25-35`,
      `pwid_>35`,
   )

prep %>%
   filter(prep_new == 1) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   mutate(
      perm_curr_prov = if_else(perm_curr_prov == "NCR", perm_curr_munc, perm_curr_prov, perm_curr_prov)
   ) %>%
   group_by(perm_curr_reg, perm_curr_prov) %>%
   summarise_at(
      .vars = vars(enroll_reg_in, enroll_reg_out),
      ~sum(.)
   ) %>%
   mutate(
      reg_order = perm_curr_reg,
      reg_order = case_when(
         reg_order == "1" ~ 1,
         reg_order == "2" ~ 2,
         reg_order == "3" ~ 3,
         reg_order == "4A" ~ 4,
         reg_order == "4B" ~ 5,
         reg_order == "5" ~ 6,
         reg_order == "6" ~ 7,
         reg_order == "7" ~ 8,
         reg_order == "8" ~ 9,
         reg_order == "9" ~ 10,
         reg_order == "10" ~ 11,
         reg_order == "11" ~ 12,
         reg_order == "12" ~ 13,
         reg_order == "ARMM" ~ 14,
         reg_order == "BARMM" ~ 14,
         reg_order == "CAR" ~ 15,
         reg_order == "CARAGA" ~ 16,
         reg_order == "NCR" ~ 17,
         TRUE ~ 9999
      ),
   ) %>%
   arrange(reg_order, perm_curr_prov) %>%
   select(-reg_order) %>%
   write_clip()

prep %>%
   filter(prep_new == 1) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   group_by(prepstart_reg, prepstart_prov, PREPFIRST_FACI) %>%
   summarise_at(
      .vars = vars(enroll_reg_in, enroll_reg_out),
      ~sum(.)
   ) %>%
   mutate(
      reg_order = prepstart_reg,
      reg_order = case_when(
         reg_order == "1" ~ 1,
         reg_order == "2" ~ 2,
         reg_order == "3" ~ 3,
         reg_order == "4A" ~ 4,
         reg_order == "4B" ~ 5,
         reg_order == "5" ~ 6,
         reg_order == "6" ~ 7,
         reg_order == "7" ~ 8,
         reg_order == "8" ~ 9,
         reg_order == "9" ~ 10,
         reg_order == "10" ~ 11,
         reg_order == "11" ~ 12,
         reg_order == "12" ~ 13,
         reg_order == "ARMM" ~ 14,
         reg_order == "BARMM" ~ 14,
         reg_order == "CAR" ~ 15,
         reg_order == "CARAGA" ~ 16,
         reg_order == "NCR" ~ 17,
         TRUE ~ 9999
      ),
   ) %>%
   arrange(reg_order, prepstart_prov) %>%
   select(-reg_order) %>%
   write_clip()


prep %>%
   filter(prep_new == 1) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   mutate(
      prepstart_ym = substr(prepstart_date, 1, 7)
   ) %>%
   filter(year(prepstart_date) >= 2021) %>%
   group_by(prepstart_reg, prepstart_ym, prepstart_faci_type) %>%
   summarise_at(
      .vars = vars(prep_new),
      ~sum(.)
   ) %>%
   pivot_wider(
      id_cols     = c(prepstart_reg, prepstart_ym),
      names_from  = prepstart_faci_type,
      values_from = prep_new
   ) %>%
   select(
      prepstart_reg,
      prepstart_ym,
      SHC,
      `Private clinic`,
      `Private hospital`,
      `Public hospital`,
      CBO,
      LY
   ) %>%
   mutate(
      reg_order = prepstart_reg,
      reg_order = case_when(
         reg_order == "1" ~ 1,
         reg_order == "2" ~ 2,
         reg_order == "3" ~ 3,
         reg_order == "4A" ~ 4,
         reg_order == "4B" ~ 5,
         reg_order == "5" ~ 6,
         reg_order == "6" ~ 7,
         reg_order == "7" ~ 8,
         reg_order == "8" ~ 9,
         reg_order == "9" ~ 10,
         reg_order == "10" ~ 11,
         reg_order == "11" ~ 12,
         reg_order == "12" ~ 13,
         reg_order == "ARMM" ~ 14,
         reg_order == "BARMM" ~ 14,
         reg_order == "CAR" ~ 15,
         reg_order == "CARAGA" ~ 16,
         reg_order == "NCR" ~ 17,
         TRUE ~ 9999
      ),
   ) %>%
   arrange(reg_order) %>%
   select(-reg_order) %>%
   write_clip()


prep %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   filter(prep_new == 1) %>%
   arrange(perm_curr_reg, prep_enroll_age_band, kap_type) %>%
   group_by(perm_curr_reg, prep_enroll_age_band, kap_type) %>%
   summarise(enrolled = n()) %>%
   pivot_wider(
      id_cols     = c(perm_curr_reg, prep_enroll_age_band),
      names_from  = kap_type,
      values_from = enrolled
   ) %>%
   select(
      perm_curr_reg,
      prep_enroll_age_band,
      starts_with("1_"),
      starts_with("2_"),
      starts_with("3_"),
      starts_with("4_"),
      starts_with("5_"),
      starts_with("6_"),
      starts_with("7_"),
   ) %>%
   View()

prep %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   arrange(perm_curr_reg, prep_enroll_age_band, kap_type) %>%
   mutate(
      prep_enroll_age_band = remove_code(prep_enroll_age_band)
   ) %>%
   group_by(perm_curr_reg, prep_enroll_age_band, kap_type) %>%
   summarise_at(
      .vars = vars(prep_offer, prep_new, prep_curr),
      ~sum(.)
   ) %>%
   pivot_wider(
      id_cols     = perm_curr_reg,
      names_from  = c(kap_type, prep_enroll_age_band),
      values_from = c(prep_offer, prep_new, prep_curr)
   ) %>%
   mutate(
      reg_order = perm_curr_reg,
      reg_order = case_when(
         reg_order == "1" ~ 1,
         reg_order == "2" ~ 2,
         reg_order == "3" ~ 3,
         reg_order == "4A" ~ 4,
         reg_order == "4B" ~ 5,
         reg_order == "5" ~ 6,
         reg_order == "6" ~ 7,
         reg_order == "7" ~ 8,
         reg_order == "8" ~ 9,
         reg_order == "9" ~ 10,
         reg_order == "10" ~ 11,
         reg_order == "11" ~ 12,
         reg_order == "12" ~ 13,
         reg_order == "ARMM" ~ 14,
         reg_order == "BARMM" ~ 14,
         reg_order == "CAR" ~ 15,
         reg_order == "CARAGA" ~ 16,
         reg_order == "NCR" ~ 17,
         TRUE ~ 9999
      ),
   ) %>%
   arrange(reg_order) %>%
   select(-reg_order)

combined_reg <- prep_briefer$sites %>%
   full_join(prep_briefer$offer_enroll_curr) %>%
   full_join(prep_briefer$tabstat) %>%
   full_join(prep_briefer$per_kp) %>%
   add_row(region = "CARAGA") %>%
   add_row(region = "9") %>%
   mutate(
      reg_order = region,
      reg_order = case_when(
         reg_order == "1" ~ 1,
         reg_order == "2" ~ 2,
         reg_order == "3" ~ 3,
         reg_order == "4A" ~ 4,
         reg_order == "4B" ~ 5,
         reg_order == "5" ~ 6,
         reg_order == "6" ~ 7,
         reg_order == "7" ~ 8,
         reg_order == "8" ~ 9,
         reg_order == "9" ~ 10,
         reg_order == "10" ~ 11,
         reg_order == "11" ~ 12,
         reg_order == "12" ~ 13,
         reg_order == "ARMM" ~ 14,
         reg_order == "BARMM" ~ 14,
         reg_order == "CAR" ~ 15,
         reg_order == "CARAGA" ~ 16,
         reg_order == "NCR" ~ 17,
         TRUE ~ 9999
      ),
   ) %>%
   arrange(reg_order) %>%
   select(-reg_order) %>%
   slice(1:17)