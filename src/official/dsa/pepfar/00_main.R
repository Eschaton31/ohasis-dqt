flow_register()
pepfar$steps$`01_load_reqs`$.init(yr = 2024, mo = 3, report = "QR", envir = pepfar)
pepfar$steps$`02_prepare_tx`$.init(pepfar)
pepfar$steps$`03_prepare_prep`$.init(pepfar)
pepfar$steps$`04_prepare_reach`$.init(pepfar)
pepfar$steps$`05_aggregate_data`$.init(pepfar)
pepfar$steps$`06_conso_flat`$.init(pepfar)
source("src/official/dsa/pepfar/07_export_mail.R")

##  MER ------------------------------------------------------------------------
oh_dir       <- file.path("O:/My Drive/Data Sharing/EpiC")
file_initial <- file.path(oh_dir, "RecencyTesting-PreProcess.xlsx")
file_final   <- file.path(oh_dir, "RecencyTesting-PostProcess.xlsx")
file_faci    <- file.path(oh_dir, "OHASIS-FacilityIDs.xlsx")
file_json    <- file.path(oh_dir, "DataStatus.json")

oh_ts <- format(
   as.POSIXct(
      paste0(
         strsplit(ohasis$timestamp, "\\.")[[1]][1], "-",
         strsplit(ohasis$timestamp, "\\.")[[1]][2], "-",
         strsplit(ohasis$timestamp, "\\.")[[1]][3], " ",
         StrLeft(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":",
         substr(strsplit(ohasis$timestamp, "\\.")[[1]][4], 3, 4), ":",
         StrRight(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2)
      )
   ),
   "%Y-%m-%d %H:%M:%S"
)

epic                               <- new.env()
epic$data$json                     <- jsonlite::read_json(file_json)
epic$data$json$`Linelist-TX`       <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = oh_ts,
   coverage     = pepfar$coverage[1:5]
)
epic$data$json$`Linelist-PrEP`     <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = oh_ts,
   coverage     = pepfar$coverage[1:5]
)
epic$data$json$`Linelist-HTS-PREV` <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = oh_ts,
   coverage     = pepfar$coverage[1:5]
)

pepfar$ip$EpiC$linelist$tx %>%
   remove_pii() %>%
   write_xlsx(file.path(oh_dir, glue("Linelist-TX.xlsx")))

pepfar$ip$EpiC$linelist$prep %>%
   remove_pii() %>%
   write_xlsx(file.path(oh_dir, glue("Linelist-PrEP.xlsx")))

pepfar$ip$EpiC$linelist$reach %>%
   remove_pii() %>%
   write_xlsx(file.path(oh_dir, glue("Linelist-HTS-PREV.xlsx")))

jsonlite::write_json(epic$data$json, file_json, pretty = TRUE, auto_unbox = TRUE)


tx <- hs_data("harp_tx", "reg", 2023, 4) %>%
   read_dta(col_select = c(art_id, PATIENT_ID)) %>%
   left_join(
      y  = hs_data("harp_tx", "outcome", 2023, 4) %>%
         read_dta(),
      by = join_by(art_id)
   ) %>%
   get_cid(pepfar$forms$id_reg, PATIENT_ID) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(TX_FACI = "hub", TX_SUB_FACI = "branch")
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(REAL_FACI = "realhub", REAL_SUB_FACI = "realhub_branch")
   ) %>%
   mutate_at(
      .vars = vars(TX_FACI, REAL_FACI),
      ~if_else(. == "130000", NA_character_, ., .),
   ) %>%
   distinct(art_id, .keep_all = TRUE) %>%
   inner_join(
      y  = pepfar$coverage$sites %>%
         filter(site_epic_2022 == 1) %>%
         select(REAL_FACI = FACI_ID),
      by = join_by(REAL_FACI)
   ) %>%
   remove_pii() %>%
   select(-starts_with("tx")) %>%
   ohasis$get_faci(
      list(tx_hub = c("REAL_FACI", "REAL_SUB_FACI")),
      "name",
      c("tx_reg", "tx_prov", "tx_munc")
   ) %>%
   select(
      REC_ID,
      CENTRAL_ID,
      art_id,
      idnum,
      prep_id,
      mort_id,
      sex,
      vl_date,
      vl_result,
      baseline_vl,
      vl_suppressed,
      vlp12m,
      curr_age,
      artstart_date,
      class,
      tx_hub,
      tx_reg,
      tx_prov,
      tx_munc,
      outcome,
      onart,
      latest_ffupdate,
      latest_nextpickup,
      latest_regimen,
      previous_ffupdate,
      previous_nextpickup,
      previous_regimen,
      who_staging,
      tb_status,
      oi_syph,
      oi_hepb,
      oi_hepc,
      oi_pcp,
      oi_cmv,
      oi_orocand,
      oi_herpes,
      oi_other,
   )

prep <- hs_data("prep", "reg", 2023, 4) %>%
   read_dta(col_select = c(prep_id, PATIENT_ID)) %>%
   left_join(
      y  = hs_data("prep", "outcome", 2023, 4) %>%
         read_dta() %>%
         select(-PATIENT_ID),
      by = join_by(prep_id)
   ) %>%
   get_cid(pepfar$forms$id_reg, PATIENT_ID) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(PREP_FACI = "faci", PREP_SUB_FACI = "branch")
   ) %>%
   mutate_at(
      .vars = vars(PREP_FACI),
      ~if_else(. == "130000", NA_character_, ., .),
   ) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   inner_join(
      y  = pepfar$coverage$sites %>%
         filter(site_epic_2022 == 1) %>%
         select(PREP_FACI = FACI_ID),
      by = join_by(PREP_FACI)
   ) %>%
   remove_pii() %>%
   select(-prep_reg, -prep_prov, -prep_munc) %>%
   ohasis$get_faci(
      list(prep_hub = c("PREP_FACI", "PREP_SUB_FACI")),
      "name",
      c("prep_reg", "prep_prov", "prep_munc")
   ) %>%
   select(
      REC_ID,
      CENTRAL_ID,
      prep_id,
      idnum,
      art_id,
      mort_id,
      sex,
      self_identity,
      self_identity_other,
      gender_identity,
      with_hts,
      HTS_REC,
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
      kp_tg,
      kp_pwid,
      kp_msm,
      kp_sw,
      kp_other,
      prepstart_date,
      prep_reinit_date,
      prep_first_time,
      prep_plan,
      prep_type,
      prep_status,
      curr_age,
      prep_hub,
      prep_reg,
      prep_prov,
      prep_munc,
      outcome,
      onprep,
      latest_ffupdate,
      latest_nextpickup,
      latest_regimen,
   )

dir <- "O:/My Drive/Data Sharing/EpiC/For Kuya Chard - 20230614"
tx %>%
   format_stata() %>%
   write_dta(file.path(dir, "20230614_onart-vl_EpiC_2023-04.dta"))

prep %>%
   format_stata() %>%
   write_dta(file.path(dir, "20230614_onprep_EpiC_2023-04.dta"))
