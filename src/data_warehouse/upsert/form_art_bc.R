##  Form BC --------------------------------------------------------------------

continue <- 0
id_col   <- c("REC_ID", "REC_ID_GRP")
# px identifiers (demographics, address, etc.)
object   <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      substr(MODULE, 1, 1) == "3",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      is.na(DELETED_BY)
   )

for_delete <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      substr(MODULE, 1, 1) == "3",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new
   ) %>%
   select(REC_ID) %>%
   collect()

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      select(
         -starts_with("PERM_"),
         -starts_with("BIRTH_"),
         -starts_with("SERVICE_"),
         -starts_with("DEATH_")
      ) %>%
      # facility data in form
      left_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_faci_info")) %>%
            filter(
               substr(MODALITY, 1, 6) == "101201"
            ) %>%
            select(
               REC_ID,
               MODALITY,
               SERVICE_FACI,
               SERVICE_SUB_FACI,
               REFER_FACI,
               SERVICE_BY,
               TX_STATUS,
               VISIT_TYPE,
               CLIENT_TYPE,
               CLINIC_NOTES,
               COUNSEL_NOTES
            ),
         by = "REC_ID"
      ) %>%
      collect() %>%
      # key population
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_key_pop")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_KP = case_when(
                  IS_KP == "0" ~ "0_No",
                  IS_KP == "1" ~ "1_Yes"
               )
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = KP,
               values_from = c("IS_KP", "KP_OTHER"),
            ) %>%
            rename_all(
               ~case_when(
                  . == "IS_KP_1" ~ "KP_PDL",
                  . == "IS_KP_2" ~ "KP_TG",
                  . == "IS_KP_3" ~ "KP_PWID",
                  . == "IS_KP_5" ~ "KP_MSM",
                  . == "IS_KP_6" ~ "KP_SW",
                  . == "IS_KP_7" ~ "KP_OFW",
                  . == "IS_KP_8" ~ "KP_PARTNER",
                  . == "KP_OTHER_8888" ~ "KP_OTHER",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               any_of(
                  c("KP_PDL",
                    "KP_TG",
                    "KP_PWID",
                    "KP_MSM",
                    "KP_SW",
                    "KP_OFW",
                    "KP_PARTNER",
                    "KP_OTHER")
               )
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # staging section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_staging")),
               by = "REC_ID"
            ) %>%
            mutate(
               WHO_CLASS = case_when(
                  WHO_CLASS == "1" ~ "1_I",
                  WHO_CLASS == "2" ~ "2_II",
                  WHO_CLASS == "3" ~ "3_III",
                  WHO_CLASS == "4" ~ "4_IV",
                  TRUE ~ as.character(WHO_CLASS)
               )
            ) %>%
            select(
               REC_ID,
               WHO_CLASS
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # labs
      left_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
            filter(
               SNAPSHOT >= snapshot_old & SNAPSHOT <= snapshot_new,
               is.na(DELETED_AT)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "lab_wide")),
               by = "REC_ID"
            ) %>%
            select(
               REC_ID,
               LAB_HEMOG_DATE,
               LAB_HEMOG_RESULT,
               LAB_VIRAL_DATE,
               LAB_VIRAL_RESULT,
               LAB_CD4_DATE,
               LAB_CD4_RESULT,
               LAB_CREA_DATE,
               LAB_CREA_RESULT,
               LAB_CREA_CLEARANCE,
               LAB_HBSAG_DATE,
               LAB_HBSAG_RESULT,
               LAB_XRAY_DATE,
               LAB_XRAY_RESULT,
               LAB_XPERT_DATE,
               LAB_XPERT_RESULT,
               LAB_DSSM_DATE,
               LAB_DSSM_RESULT,
               LAB_HIVDR_DATE,
               LAB_HIVDR_RESULT
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # vaccine
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_vaccine")),
               by = "REC_ID"
            ) %>%
            mutate(
               DISEASE_VAX = case_when(
                  DISEASE_VAX == "102000" ~ "HEPB",
                  DISEASE_VAX == "201000" ~ "COVID19",
                  TRUE ~ DISEASE_VAX
               ),
               VAX_NUM     = case_when(
                  VAX_NUM == 1 ~ "1ST",
                  VAX_NUM == 2 ~ "2ND",
                  VAX_NUM == 3 ~ "3RD",
                  VAX_NUM == 10 ~ "BOOST",
                  TRUE ~ NA_character_
               ),
               IS_VAX      = case_when(
                  IS_VAX == 0 ~ "0_No",
                  IS_VAX == 1 ~ "1_Yes",
                  IS_VAX == 2 ~ "2_Refused",
                  TRUE ~ NA_character_
               ),
            ) %>%
            rename(
               DONE     = IS_VAX,
               LOCATION = VAX_REMARKS,
               DATE     = VAX_DATE,
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = c("DISEASE_VAX", "VAX_NUM"),
               values_from = c("VAX_USED", "DONE", "DATE", "LOCATION"),
               names_glue  = "VAX_{DISEASE_VAX}_{VAX_NUM}_{.value}"
            ) %>%
            select(
               REC_ID,
               any_of(
                  c('VAX_COVID19_1ST_DONE',
                    'VAX_COVID19_1ST_VAX_USED',
                    'VAX_COVID19_1ST_DATE',
                    'VAX_COVID19_1ST_LOCATION',
                    'VAX_COVID19_2ND_DONE',
                    'VAX_COVID19_2ND_VAX_USED',
                    'VAX_COVID19_2ND_DATE',
                    'VAX_COVID19_2ND_LOCATION',
                    'VAX_COVID19_BOOST_DONE',
                    'VAX_COVID19_BOOST_VAX_USED',
                    'VAX_COVID19_BOOST_DATE',
                    'VAX_COVID19_BOOST_LOCATION',
                    'VAX_HEPB_1ST_DONE',
                    'VAX_HEPB_1ST_VAX_USED',
                    'VAX_HEPB_1ST_DATE',
                    'VAX_HEPB_1ST_LOCATION',
                    'VAX_HEPB_2ND_DONE',
                    'VAX_HEPB_2ND_VAX_USED',
                    'VAX_HEPB_2ND_DATE',
                    'VAX_HEPB_2ND_LOCATION',
                    'VAX_HEPB_3RD_DONE',
                    'VAX_HEPB_3RD_VAX_USED',
                    'VAX_HEPB_3RD_DATE',
                    'VAX_HEPB_3RD_LOCATION')
               )
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # tb screening
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_tb")),
               by = "REC_ID"
            ) %>%
            mutate(
               TB_SCREEN         = case_when(
                  TB_SCREEN == "0" ~ "0_No",
                  TB_SCREEN == "1" ~ "1_Yes",
                  TRUE ~ NA_character_
               ),
               TB_ACTIVE_ALREADY = case_when(
                  TB_ACTIVE_ALREADY == "0" ~ "0_No",
                  TB_ACTIVE_ALREADY == "1" ~ "1_Yes",
                  TRUE ~ NA_character_
               ),
               TB_TX_ALREADY     = case_when(
                  TB_TX_ALREADY == "0" ~ "0_No",
                  TB_TX_ALREADY == "1" ~ "1_Yes",
                  TRUE ~ NA_character_
               ),
               TB_STATUS         = case_when(
                  TB_STATUS == "0" ~ "0_No active TB",
                  TB_STATUS == "1" ~ "1_With active TB",
                  TRUE ~ NA_character_
               ),
            ) %>%
            select(
               REC_ID,
               TB_SCREEN,
               TB_STATUS,
               TB_ACTIVE_ALREADY,
               TB_TX_ALREADY
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # tb active
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_tb_active")),
               by = "REC_ID"
            ) %>%
            mutate(
               TB_SITE_P          = case_when(
                  TB_SITE_P == "0" ~ "0_No",
                  TB_SITE_P == "1" ~ "1_Yes",
                  TRUE ~ NA_character_
               ),
               TB_SITE_EP         = case_when(
                  TB_SITE_EP == "0" ~ "0_No",
                  TB_SITE_EP == "1" ~ "1_Yes",
                  TRUE ~ NA_character_
               ),
               TB_DRUG_RESISTANCE = case_when(
                  TB_DRUG_RESISTANCE == "1" ~ "1_Susceptible",
                  TB_DRUG_RESISTANCE == "2" ~ "2_MDR",
                  TB_DRUG_RESISTANCE == "3" ~ "3_XDR",
                  TB_DRUG_RESISTANCE == "4" ~ "4_RR only",
                  TB_DRUG_RESISTANCE == "8888" ~ "8888_Other",
                  TRUE ~ NA_character_
               ),
               TB_TX_STATUS       = case_when(
                  TB_TX_STATUS == "0" ~ "0_Not on Tx",
                  TB_TX_STATUS == "11" ~ "11_Ongoing Tx",
                  TB_TX_STATUS == "12" ~ "12_Started Tx",
                  TB_TX_STATUS == "13" ~ "13_Ended Tx",
                  TRUE ~ NA_character_
               ),
               TB_REGIMEN         = case_when(
                  TB_REGIMEN == "10" ~ "10_Cat I",
                  TB_REGIMEN == "11" ~ "11_Cat Ia",
                  TB_REGIMEN == "20" ~ "20_Cat II",
                  TB_REGIMEN == "21" ~ "21_Cat IIa",
                  TB_REGIMEN == "30" ~ "30_SRDR",
                  TB_REGIMEN == "40" ~ "40_XDR-TB",
                  TRUE ~ NA_character_
               ),
               TB_TX_OUTCOME      = case_when(
                  TB_TX_OUTCOME == "10" ~ "10_Not yet evaluated",
                  TB_TX_OUTCOME == "11" ~ "11_Cured",
                  TB_TX_OUTCOME == "20" ~ "20_Failed",
                  TB_TX_OUTCOME == "8888" ~ "8888_Other",
                  TRUE ~ NA_character_
               ),
            ) %>%
            select(
               REC_ID,
               TB_SITE_P,
               TB_SITE_EP,
               TB_DRUG_RESISTANCE,
               TB_DRUG_RESISTANCE_OTHER,
               TB_TX_STATUS,
               TB_TX_STATUS_OTHER,
               TB_REGIMEN,
               TB_TX_START_DATE,
               TB_TX_END_DATE,
               TB_TX_OUTCOME,
               TB_TX_OUTCOME_OTHER
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # no tb
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_tb_ipt")),
               by = "REC_ID"
            ) %>%
            mutate(
               TB_IPT_STATUS  = case_when(
                  TB_IPT_STATUS == "0" ~ "0_Not on IPT",
                  TB_IPT_STATUS == "11" ~ "11_Ongoing IPT",
                  TB_IPT_STATUS == "12" ~ "12_Started IPT",
                  TB_IPT_STATUS == "13" ~ "13_Ended IPT",
                  TRUE ~ NA_character_
               ),
               TB_IPT_OUTCOME = case_when(
                  TB_IPT_OUTCOME == "1" ~ "1_Completed",
                  TB_IPT_OUTCOME == "2" ~ "2_Stopped before target end",
                  TB_IPT_OUTCOME == "8888" ~ "8888_Other",
                  TRUE ~ NA_character_
               ),
            ) %>%
            select(
               REC_ID,
               TB_IPT_STATUS,
               TB_IPT_START_DATE,
               TB_IPT_END_DATE,
               TB_IPT_OUTCOME,
               TB_IPT_OUTCOME_OTHER
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # oi section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_oi")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_OI = case_when(
                  IS_OI == "0" ~ "0_No",
                  IS_OI == "1" ~ "1_Yes",
                  TRUE ~ NA_character_
               ),
               OI    = case_when(
                  OI == "101000" ~ "HIV",
                  OI == "102000" ~ "HEPB",
                  OI == "103000" ~ "HEPC",
                  OI == "104000" ~ "SYPH",
                  OI == "111000" ~ "PCP",
                  OI == "112000" ~ "CMV",
                  OI == "113000" ~ "OROCAND",
                  OI == "117000" ~ "HERPES",
                  OI == "202000" ~ "TB",
                  OI == "8888" ~ "OTHER",
                  TRUE ~ OI
               )
            ) %>%
            rename(
               PRESENT = IS_OI,
               TEXT    = OI_OTHER
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = OI,
               values_from = c(PRESENT, TEXT),
               names_glue  = "OI_{OI}_{.value}"
            ) %>%
            select(
               REC_ID,
               any_of(
                  c('OI_SYPH_PRESENT',
                    'OI_HEPB_PRESENT',
                    'OI_HEPC_PRESENT',
                    'OI_PCP_PRESENT',
                    'OI_CMV_PRESENT',
                    'OI_OROCAND_PRESENT',
                    'OI_HERPES_PRESENT',
                    'OI_OTHER_PRESENT',
                    'OI_OTHER_TEXT')
               )
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # prophylaxis
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_prophylaxis")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_PROPH    = case_when(
                  IS_PROPH == "0" ~ "0_No",
                  IS_PROPH == "1" ~ "1_Yes",
                  TRUE ~ NA_character_
               ),
               PROPHYLAXIS = case_when(
                  PROPHYLAXIS == "1" ~ "BCG",
                  PROPHYLAXIS == "2" ~ "COTRI",
                  PROPHYLAXIS == "3" ~ "AZITHRO",
                  PROPHYLAXIS == "4" ~ "FLUCANO",
                  TRUE ~ PROPHYLAXIS
               )
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = PROPHYLAXIS,
               values_from = IS_PROPH
            ) %>%
            rename_all(
               ~case_when(
                  . == "COTRI_IS_PROPH" ~ "PROPH_COTRI",
                  . == "AZITHRO_IS_PROPH" ~ "PROPH_AZITHRO",
                  . == "FLUCANO_IS_PROPH" ~ "PROPH_FLUCANO",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               any_of(
                  c("PROPH_COTRI",
                    "PROPH_AZITHRO",
                    "PROPH_FLUCANO")
               )
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # ob section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_ob")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_PREGNANT = case_when(
                  IS_PREGNANT == "0" ~ "0_No",
                  IS_PREGNANT == "1" ~ "1_Yes",
                  TRUE ~ as.character(IS_PREGNANT)
               ),
               FEED_TYPE   = case_when(
                  FEED_TYPE == "1" ~ "1_Breasfeeding",
                  FEED_TYPE == "2" ~ "2_Formula feeding",
                  FEED_TYPE == "3" ~ "3_Mixed feeding",
                  TRUE ~ NA_character_
               )
            ) %>%
            select(
               REC_ID,
               IS_PREGNANT,
               LMP,
               AOG = PREG_WKS,
               EDD,
               DELIVER_DATE,
               DELIVER_FACI,
               FEED_TYPE
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # arv disp data
      left_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
            filter(
               SNAPSHOT >= snapshot_old & SNAPSHOT <= snapshot_new,
               is.na(DELETED_AT)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "disp_meds")),
               by = "REC_ID"
            ) %>%
            collect() %>%
            arrange(REC_ID, DISP_NUM) %>%
            group_by(REC_ID, REC_ID_GRP) %>%
            left_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "inventory_product")) %>%
                  select(
                     MEDICINE = ITEM,
                     SHORT
                  ) %>%
                  collect(),
               by = "MEDICINE"
            ) %>%
            summarise(
               FACI_DISP        = first(FACI_ID, na.rm = TRUE),
               MEDICINE_SUMMARY = paste0(unique(SHORT), collapse = "+"),
               DISP_DATE        = max(DISP_DATE, na.rm = TRUE),
               LATEST_NEXT_DATE = max(NEXT_DATE, na.rm = TRUE),
            ),
         by = "REC_ID"
      ) %>%
      # arv disp data
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_medicine_disc")),
               by = "REC_ID"
            ) %>%
            collect() %>%
            distinct(REC_ID, MEDICINE) %>%
            group_by(REC_ID) %>%
            summarise(
               NUM_OF_DISC = n()
            ),
         by = "REC_ID"
      ) %>%
      mutate(
         VISIT_DATE = case_when(
            RECORD_DATE == as.Date(DISP_DATE) ~ RECORD_DATE,
            RECORD_DATE < as.Date(DISP_DATE) & DISP_DATE >= -25567 ~ as.Date(DISP_DATE),
            RECORD_DATE > as.Date(DISP_DATE) & DISP_DATE >= -25567 ~ as.Date(DISP_DATE),
            is.na(RECORD_DATE) ~ as.Date(DISP_DATE),
            is.na(DISP_DATE) ~ RECORD_DATE,
            TRUE ~ RECORD_DATE
         ),
         .before    = RECORD_DATE
      ) %>%
      # tag ART records
      mutate(
         NUM_OF_DRUGS = stri_count_fixed(MEDICINE_SUMMARY, '+') + 1,
         NUM_OF_DRUGS = if_else(is.na(NUM_OF_DRUGS), as.integer(0), as.integer(NUM_OF_DRUGS)),
         NUM_OF_DISC  = if_else(is.na(NUM_OF_DISC), as.integer(0), as.integer(NUM_OF_DISC)),
         ART_RECORD   = case_when(
            NUM_OF_DISC > 0 & NUM_OF_DRUGS == 0 ~ 'Care',
            is.na(TX_STATUS) & !is.na(MEDICINE_SUMMARY) ~ 'ART',
            !is.na(TX_STATUS) & substr(TX_STATUS, 1, 1) == 1 ~ 'ART',
            !is.na(TX_STATUS) & substr(TX_STATUS, 1, 1) == 2 ~ 'ART',
            substr(TX_STATUS, 1, 1) == 0 ~ 'Care',
            !is.na(TX_STATUS) ~ 'Care',
            TRUE ~ 'Care'
         )
      )
}