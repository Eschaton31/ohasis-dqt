source("src/official/dsa/protects-upscale/01_load_reqs.R")

aiha_staff <- c(
   '9900050014', '9900050068', '9900050006', '9900050069', '9900050077', '9900050010', '9900050067', '9900050078',
   '9900050027', '9900050074', '9900050054', '9900050048', '9900050064', '9900050076', '9900050076', '9900050060',
   '9900050065', '9900050053', '9900050040', '9900050047', '9900050052', '9900050036', '9900050011', '9900050031',
   '9900050004', '9900050070', '9900050003', '9900050080', '9900050029', '9900050081', '9900050071', '9900050005',
   '9900050072', '9900050058', '9900050073'
)

# con   <- ohasis$conn("lw")
# forms <- qb$new(con)
# forms$where(function(query = qb$new(con)) {
#    query$whereBetween('record_date', c(min, max), "or")
#    query$whereBetween('date_confirm', c(min, max), "or")
#    query$whereBetween('t0_date', c(min, max), "or")
#    query$whereBetween('t1_date', c(min, max), "or")
#    query$whereBetween('t2_date', c(min, max), "or")
#    query$whereBetween('t3_date', c(min, max), "or")
#    query$whereNested
# })
# forms$where(function(query = qb$new(con)) {
#    query$whereIn('faci_id', sites$faci_id, boolean = "or")
#    query$whereIn('service_faci', sites$faci_id, boolean = "or")
#    query$whereIn('created_by', aiha_staff, boolean = "or")
#    query$whereIn('provider_id', aiha_staff, boolean = "or")
#    query$whereNested
# })
#
# forms$from("ohasis_warehouse.form_hts")
# hts <- forms$get()
#
# forms$from("ohasis_warehouse.form_a")
# a <- forms$get()
#
# cfbs <- qb$new(con)$
#    from("ohasis_warehouse.form_cfbs")$
#    limit(0)$
#    get()

hts <- get_hts(min, max)

not_imported <- read_rds("C:/Users/Bene-g16/Downloads/hts-imports_20250805.rds")
to_append    <- not_imported$data$convert %>%
   rename_all(tolower) %>%
   filter(!is.na(record_date)) %>%
   filter(created_at != "Auto-fill") %>%
   select(
      -ends_with("name_reg"),
      -ends_with("name_prov"),
      -ends_with("name_munc"),
   ) %>%
   mutate(
      curr_psgc        = coalesce(curr_munc, curr_prov, curr_reg),
      birth_psgc       = coalesce(birth_munc, birth_prov, birth_reg),
      hiv_service_psgc = coalesce(hiv_service_munc, hiv_service_prov, hiv_service_reg),
   ) %>%
   left_join(
      y          = ohasis$ref_addr %>%
         select(
            curr_psgc      = psgc_old,
            curr_psgc_reg  = reg,
            curr_psgc_prov = prov,
            curr_psgc_munc = munc
         ),
      by         = join_by(curr_psgc),
      na_matches = "never"
   ) %>%
   left_join(
      y          = ohasis$ref_addr %>%
         select(
            birth_psgc      = psgc_old,
            birth_psgc_reg  = reg,
            birth_psgc_prov = prov,
            birth_psgc_munc = munc
         ),
      by         = join_by(birth_psgc),
      na_matches = "never"
   ) %>%
   left_join(
      y          = ohasis$ref_addr %>%
         select(
            hiv_service_psgc      = psgc_old,
            hiv_service_psgc_reg  = reg,
            hiv_service_psgc_prov = prov,
            hiv_service_psgc_munc = munc
         ),
      by         = join_by(hiv_service_psgc),
      na_matches = "never"
   ) %>%
   rename(
      modality               = service_type,
      test_refuse_other_text = test_refuse_reason_other_text,
      reach_index_testing    = reach_index,
   ) %>%
   mutate(
      form_version    = "HTS Form (v2021)",
      self_ident      = case_when(
         self_ident == "OTHERS" ~ "3_Other",
         TRUE ~ self_ident
      ),
      disease         = "HIV",

      service_condoms = parse_number(service_condoms),
      service_lubes   = parse_number(service_lubes),
   )

form_hts <- hts$hts %>%
   bind_rows(
      to_append %>%
         mutate(
            created_at = parse_date_time(created_at, "YmdHMS"),
            updated_at = parse_date_time(updated_at, "YmdHMS"),
         ) %>%
         mutate(
            rec_id      = stri_c('temp-oh2-', stri_pad_left(row_number(), 16, '0')),
            patient_id  = stri_c('temp-oh2-', stri_pad_left(row_number(), 9, '0')),
            record_date = as.Date(record_date),
            birthdate   = as.Date(birthdate),
            age         = as.integer(age),
            age_mo      = as.integer(age_mo),
            children    = as.integer(children),
            ofw_yr_ret  = as.integer(ofw_yr_ret),
            retest_mos  = as.integer(retest_mos),
            retest_wks  = as.integer(retest_wks),
         ) %>%
         mutate_at(
            .vars = vars(ends_with("date")),
            ~as.Date(.)
         ) %>%
         filter(!is.na(faci_id)) %>%
         select(any_of(names(hts$hts)))
   )

compare_vars <- function(data1, data2, var) {
   print(data1 %>% tab({{var}}))
   data2 %>% tab({{var}})
}

# compare_vars(to_append, hts$hts, disease)

id_reg <- update_idreg()
dbDisconnect(con)

dx         <- read_dta(hs_data("harp_dx", "reg", yr, mo)) %>%
   get_cid(id_reg, patient_id) %>%
   mutate(
      confirm_branch = NA_character_
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(confirm_faci = "confirmlab", confirm_sub_faci = "confirm_branch")
   ) %>%
   ohasis$get_faci(
      list(confirm_lab = c("confirm_faci", "confirm_sub_faci")),
      "name"
   )
dead       <- read_dta(hs_data("harp_dead", "reg", yr, mo)) %>%
   get_cid(id_reg, patient_id)
tx_reg     <- read_dta(hs_data("harp_tx", "reg", yr, mo)) %>%
   get_cid(id_reg, patient_id) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(faci_id = "artstart_hub", sub_faci_id = "artstart_branch")
   ) %>%
   mutate(
      art_faci     = faci_id,
      art_sub_faci = sub_faci_id
   ) %>%
   ohasis$get_faci(
      list(tx_hub = c("faci_id", "sub_faci_id")),
      "name",
      c("tx_reg", "tx_prov", "tx_munc")
   )
tx_out     <- read_dta(hs_data("harp_tx", "outcome", yr, mo)) %>%
   select(-any_of("central_id")) %>%
   left_join(y = tx_reg %>% select(art_id, central_id), by = join_by(art_id)) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(faci_id = "realhub", sub_faci_id = "realhub_branch")
   ) %>%
   mutate(
      art_faci     = faci_id,
      art_sub_faci = sub_faci_id
   ) %>%
   select(-tx_reg, -tx_prov, -tx_munc) %>%
   ohasis$get_faci(
      list(tx_hub = c("faci_id", "sub_faci_id")),
      "name",
      c("tx_reg", "tx_prov", "tx_munc")
   )
prep_curr  <- read_dta(hs_data("prep", "outcome", yr, mo)) %>%
   get_cid(id_reg, patient_id) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(faci_id = "faci", sub_faci_id = "branch")
   ) %>%
   mutate(
      prep_faci     = faci_id,
      prep_sub_faci = sub_faci_id
   ) %>%
   ohasis$get_faci(
      list(site_name = c("faci_id", "sub_faci_id")),
      "name",
   )
prep_start <- read_dta("H:/_R/library/prep/20251007_prepstart_2025-08.dta") %>%
   get_cid(id_reg, patient_id) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(faci_id = "prepstart_faci", sub_faci_id = "prepstart_branch")
   ) %>%
   mutate(
      prep_faci     = faci_id,
      prep_sub_faci = sub_faci_id
   ) %>%
   ohasis$get_faci(
      list(site_name = c("faci_id", "sub_faci_id")),
      "name",
   )

testing   <- process_hts(form_hts, hts$a, hts$cfbs) %>%
   # slice(1:100) %>%
   mutate(
      keep = case_when(
         faci_id %in% sites$faci_id ~ 1,
         service_faci %in% sites$faci_id ~ 1,
         created_by %in% aiha_staff ~ 1,
         provider_id %in% aiha_staff ~ 1,
         TRUE ~ 0
      )
   ) %>%
   filter(keep == 1) %>%
   get_cid(id_reg, patient_id) %>%
   get_latest_pii(
      "central_id",
      c(
         "birthdate",
         "sex",
         "self_ident",
         "self_ident_other",
         "civil_status",
         "nationality",
         "educ_level",
         "curr_reg",
         "curr_prov",
         "curr_munc",
         "perm_reg",
         "perm_prov",
         "perm_munc",
         "birth_reg",
         "birth_prov",
         "birth_munc"
      )
   ) %>%
   mutate(
      use_record_faci = if_else(is.na(service_faci), 1, 0, 0),
      service_faci    = if_else(use_record_faci == 1, faci_id, service_faci),
      site_gf         = service_faci %in% supported$faci_id
   ) %>%
   convert_hts("name") %>%
   generate_gender_identity(sex, self_ident, self_ident_other, gender_identity) %>%
   select(
      rec_id,
      central_id,
      patient_id,
      form_version,
      created_by,
      created_at,
      updated_by,
      updated_at,
      report_faci,
      record_date,
      uic,
      sex,
      birthdate,
      age,
      age_mo,
      self_ident,
      self_ident_other,
      nationality,
      educ_level,
      civil_status,
      is_student,
      living_with_partner,
      children,
      curr_reg,
      curr_prov,
      curr_munc,
      perm_reg,
      perm_prov,
      perm_munc,
      birth_reg,
      birth_prov,
      birth_munc,
      is_pregnant,
      verbal_consent,
      signature_esig,
      signature_name,
      work        = work_text,
      is_employed,
      is_ofw,
      ofw_yr_ret,
      ofw_station,
      ofw_country,
      expose_hiv_mother,
      expose_sex_m,
      expose_sex_m_av_date,
      expose_sex_m_av_nocondom_date,
      expose_sex_f,
      expose_sex_f_av_date,
      expose_sex_f_av_nocondom_date,
      expose_sex_paying,
      expose_sex_paying_date,
      expose_sex_payment,
      expose_sex_payment_date,
      expose_sex_drugs,
      expose_sex_drugs_date,
      expose_drug_inject,
      expose_drug_inject_date,
      expose_blood_transfuse,
      expose_blood_transfuse_date,
      expose_occupation,
      expose_occupation_date,
      test_reason_hiv_expose,
      test_reason_physician,
      test_reason_peer_ed,
      test_reason_employ_ofw,
      test_reason_employ_local,
      test_reason_text_email,
      test_reason_insurance,
      test_reason_other_text,
      prev_tested,
      prev_test_date,
      prev_test_faci,
      prev_test_result,
      med_tb_px,
      med_sti,
      med_hep_b,
      med_hep_c,
      med_prep_px,
      med_pep_px,
      clinical_pic,
      symptoms,
      who_class,
      reach_clinical,
      reach_online,
      reach_index_testing,
      reach_ssnt,
      reach_venue,
      test_refuse_other_text,
      refer_art,
      refer_confirm,
      refer_retest,
      retest_mos,
      retest_wks,
      retest_date,
      service_hiv_101,
      service_iec_mats,
      service_risk_counsel,
      service_prep_refer,
      service_ssnt_offer,
      service_ssnt_accept,
      service_condoms,
      service_lubes,
      cbs_reg,
      cbs_prov,
      cbs_munc,
      cbs_venue,
      hts_reg,
      hts_prov,
      hts_munc,
      hts_faci,
      hts_provider,
      hts_provider_type,
      hts_provider_type_other,
      expose_sex_m_nocondom,
      expose_sex_f_nocondom,
      expose_sex_hiv,
      expose_tattoo,
      expose_sti,
      age_first_sex,
      age_first_inject,
      num_m_partner,
      yr_last_m,
      num_f_partner,
      yr_last_f,
      med_is_pregnant,
      med_cbs_reactive,
      test_reason_retest,
      test_reason_no_reason,
      expose_sex_ever,
      expose_m_sex_oral_anal,
      expose_condomless_anal,
      expose_condomless_anal_date,
      expose_condomless_vaginal,
      expose_condomless_vaginal_date,
      expose_needle_share,
      expose_needle_share_date,
      expose_illicit_drugs,
      expose_illicit_drugs_date,
      expose_sex_hiv_date,
      test_refuse_no_time,
      test_refuse_other,
      test_refuse_no_cure,
      test_refuse_fear_result,
      test_refuse_fear_disclose,
      test_refuse_fear_msm,
      gender_identity,
      hts_date,
      hts_result,
      hts_modality,
      test_agreed,
      hts_client_type,
      risk_motherhashiv,
      risk_sexwithf,
      risk_sexwithf_nocdm,
      risk_sexwithm,
      risk_sexwithm_nocdm,
      risk_payingforsex,
      risk_paymentforsex,
      risk_sexwithhiv,
      risk_injectdrug,
      risk_needlestick,
      risk_bloodtransfuse,
      risk_illicitdrug,
      risk_chemsex,
      risk_tattoo,
      risk_sti,
      online_app  = online_app,
      sexual_risk = sexual_risk,
      kap_unknown,
      kap_msm,
      kap_heterom,
      kap_heterof,
      kap_pwid,
      kap_pip,
      kap_pdl,
      site_gf
   ) %>%
   mutate_at(
      .vars = vars(
         sex,
         self_ident,
         educ_level,
         civil_status,
         living_with_partner,
         is_pregnant,
         verbal_consent,
         signature_esig,
         signature_name,
         is_student,
         is_employed,
         is_ofw,
         ofw_station,
         test_reason_hiv_expose,
         test_reason_physician,
         test_reason_peer_ed,
         test_reason_employ_ofw,
         test_reason_employ_local,
         test_reason_text_email,
         test_reason_insurance,
         prev_tested,
         prev_test_result,
         med_tb_px,
         med_sti,
         med_hep_b,
         med_hep_c,
         med_prep_px,
         med_pep_px,
         clinical_pic,
         who_class,
         reach_clinical,
         reach_online,
         reach_index_testing,
         reach_ssnt,
         reach_venue,
         refer_art,
         refer_confirm,
         refer_retest,
         service_hiv_101,
         service_iec_mats,
         service_risk_counsel,
         service_prep_refer,
         service_ssnt_offer,
         service_ssnt_accept,
         hts_provider_type,
         med_is_pregnant,
         med_cbs_reactive,
         test_reason_retest,
         test_reason_no_reason,
         expose_hiv_mother,
         expose_sex_m,
         expose_sex_f,
         expose_sex_paying,
         expose_sex_payment,
         expose_sex_drugs,
         expose_drug_inject,
         expose_blood_transfuse,
         expose_occupation,
         expose_sex_m_nocondom,
         expose_sex_f_nocondom,
         expose_sex_hiv,
         expose_tattoo,
         expose_sti
      ),
      ~str_to_title(remove_code(.)) %>%
         str_replace_all("\\bHiv\\b", "HIV") %>%
         str_replace_all("\\Cbs\\b", "cbs")
   ) %>%
   left_join(
      y  = dx %>%
         mutate(
            reactive_date = coalesce(blood_extract_date, specimen_receipt_date, test_date, t0_date, visit_date,
                                     confirm_date) %>% as.Date()
         ) %>%
         select(
            central_id,
            reactive_date,
            confirm_hiv_class = class2022,
            confirm_date,
            confirm_lab,
            confirm_code      = labcode2,
            class2022,
            ahd,
         ),
      by = join_by(central_id)
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
   left_join(
      y  = tx_reg %>%
         select(
            central_id,
            artstart_hub = tx_hub
         ),
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = tx_out %>%
         select(
            central_id,
            artstart_date,
            art_latest_hub        = tx_hub,
            art_latest_ffupdate   = latest_ffupdate,
            art_latest_nextpickup = latest_nextpickup,
            art_latest_regimen    = latest_regimen,
            art_outcome           = outcome,
         ),
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = prep_start %>%
         filter(!is.na(prepstart_date)) %>%
         select(
            central_id,
            prepstart_date,
            prepstart_hub = site_name,
         ),
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = prep_curr %>%
         filter(!is.na(prepstart_date)) %>%
         mutate(
            preplast_given = if_else(!is.na(latest_regimen), 1, 0, 0)
         ) %>%
         select(
            central_id,
            preplast_hub   = site_name,
            preplast_visit = latest_ffupdate,
            preplast_given,
         ),
      by = join_by(central_id)
   ) %>%
   mutate_if(
      .predicate = is.Date,
      ~if_else(. < -25567, NA_Date_, ., .)
   ) %>%
   mutate(
      who_class        = toupper(who_class),
      prev_test_result = case_when(
         prev_test_result == "Positive" ~ "Reactive",
         prev_test_result == "Negative" ~ "Non-Reactive",
         TRUE ~ prev_test_result
      )
   ) %>%
   mutate(
      hts_faci = if_else(!site_gf, "(non-gf site)", hts_faci, hts_faci)
   )
variables <- read_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "hts")
dict      <- data_dictionary(testing, variables)

write_sheet(dict, "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "final-hts")
