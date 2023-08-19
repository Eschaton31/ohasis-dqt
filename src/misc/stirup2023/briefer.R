prep      <- read_dta(hs_data("prep", "reg", 2023, 6)) %>%
   left_join(
      y  = hs_data("prep", "outcome", 2023, 6) %>%
         read_dta(col_select = c(prep_id, prepstart_date, latest_ffupdate, latest_regimen, kp_msm, kp_pwid, kp_sw)),
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
         condition = sex == self_identity & msm != 0,
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
         sex == "FEMALE" & sw == 1 ~ "1_FSW",
         msm == 1 & tgw == 0 ~ "2_MSM",
         msm == 1 & tgw == 1 ~ "3_TGW",
         hetero == 1 & sex == "FEMALE" ~ "3_Hetero Female",
         hetero == 1 & sex == "MALE" ~ "4_Hetero Male",
         pwid == 1 ~ "5_PWID",
         unknown == 1 ~ "6_(no data)",
         TRUE ~ "7_Others"
      ),
      enroll_reg_in        = if_else(prep_new == 1 & perm_curr_reg == prepstart_reg, 1, 0, 0),
      enroll_reg_out       = if_else(prep_new == 1 & perm_curr_reg != prepstart_reg, 1, 0, 0),
   )

prep %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   group_by(perm_curr_reg) %>%
   summarise_at(
      .vars = vars(prep_offer, prep_new, prep_curr),
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
   arrange(reg_order) %>%
   select(-reg_order) %>%
   write_clip()

prep %>%
   filter(prep_new == 1) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   group_by(perm_curr_reg) %>%
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
   View()