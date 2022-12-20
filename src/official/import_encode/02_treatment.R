##  Download encoding documentation --------------------------------------------

import             <- new.env()
import$encoding_ss <- as_id("18hh6GZzjnNBidMg9sxOworj2IhbwTUak")
import$STAFF       <- read_sheet(as_id("1BRohoSaBE73zwRMXQNcWeRf5rC2OcePS64A67ODfxXI"))


local(envir = import, {

   download_ei <- function() {
      encode_mo     <- as_id(drive_ls(import$encoding_ss, pattern = ohasis$ym)$id)
      encode_surv   <- as_id(drive_ls(encode_mo, pattern = "TREATMENT")$id)
      encode_sheets <- drive_ls(encode_surv, pattern = ohasis$ym)
      data          <- list()
      for (sheet in c("FORMS", "DISPENSE", "DISCONTINUE", "ref_faci", "ref_addr", "ref_meds"))
         data[[sheet]] <- bind_rows(lapply(seq_len(nrow(encode_sheets)), function(i) {
            ss   <- encode_sheets[i,]$id
            name <- encode_sheets[i,]$name
            data <- range_speedread(ss, sheet, show_col_types = FALSE, col_types = cols(.default = "c")) %>%
               mutate(
                  ss        = ss,
                  encoder   = str_squish(substr(name, 9, stri_locate_first_fixed(name, ".com") + 4)),
                  sheet_row = row_number()
               )
            return(as.data.frame(data))
         }))

      return(data)
   }

   raw_checks <- function(encoded) {
      checks           <- list()
      checks$enrollees <- encoded$FORMS %>%
         distinct_all() %>%
         filter(
            !is.na(CREATED_TIME),
            nchar(PATIENT_ID) != 18 | is.na(PATIENT_ID)
         ) %>%
         mutate_at(
            .vars = vars(starts_with("TX_FACI")),
            ~if_else(. == "FALSE", NA_character_, ., .)
         ) %>%
         unite(
            starts_with("TX_FACI"),
            sep   = "",
            col   = "TX_FACI",
            na.rm = TRUE
         ) %>%
         left_join(
            y  = encoded$ref_faci %>%
               select(-encoder, -ss, -sheet_row) %>%
               distinct_all(),
            by = c("TX_FACI" = "FACI_CODE")
         ) %>%
         select(
            REC_ID,
            PATIENT_ID,
            TX_FACI,
            FACI_ID,
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX,
            CONFIRMATORY_CODE,
            UIC,
            PHILHEALTH,
            SEX,
            BIRTHDATE,
            PATIENT_CODE,
            ss,
            encoder,
            sheet_row
         ) %>%
         mutate(
            pid_sheetrow = paste0("B", sheet_row + 1)
         )

      checks[['nchar_pid']] <- encoded$FORMS %>%
         filter(nchar(PATIENT_ID) != 18) %>%
         select(
            REC_ID,
            PATIENT_ID,
            TX_FACI,
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX,
            CONFIRMATORY_CODE,
            UIC,
            PHILHEALTH,
            SEX,
            BIRTHDATE,
            PATIENT_CODE,
            ss,
            encoder,
            sheet_row
         ) %>%
         mutate(
            pid_sheetrow = paste0("B", sheet_row + 1)
         )

      return(checks)
   }

   generate_pid <- function(enrollees) {
      enrollees$PATIENT_ID <- NA_character_
      db_conn              <- ohasis$conn("db")
      for (i in seq_len(nrow(enrollees)))
         enrollees[i, "PATIENT_ID"] <- oh_px_id(db_conn, as.character(enrollees[i, "FACI_ID"]))

      dbDisconnect(db_conn)
      return(enrollees)
   }

   write_pid <- function(enrollees) {
      enrollees <- as_tibble(enrollees)
      for (i in seq_len(nrow(enrollees)))
         range_write(
            as_id(enrollees[i,]$ss),
            enrollees[i, "PATIENT_ID"],
            "FORMS",
            enrollees[i,]$pid_sheetrow,
            col_names = FALSE
         )
   }

   standardize_records <- function(encoded) {
      records <- encoded$FORMS %>%
         distinct_all() %>%
         mutate_at(
            .vars = vars(starts_with("TX_FACI")),
            ~if_else(. == "FALSE", NA_character_, ., .)
         ) %>%
         unite(
            starts_with("TX_FACI"),
            sep   = "",
            col   = "TX_FACI",
            na.rm = TRUE
         ) %>%
         mutate(
            TX_FACI = if_else(TX_FACI == "", StrLeft(PAGE_ID, 3), TX_FACI, TX_FACI)
         ) %>%
         filter(
            !is.na(CREATED_TIME),
            CREATED_TIME != "DUPLICATE",
            nchar(PATIENT_ID) == 18,
         ) %>%
         # mutate(
         #    encoder = substr(encoder, 9, 2000),
         # ) %>%
         left_join(
            y  = .GlobalEnv$ohasis$ref_staff %>%
               mutate(
                  EMAIL = str_squish(EMAIL),
                  EMAIL = case_when(
                     EMAIL == "rnrufon.pbsp@gmal.com" ~ "rnrufon.pbsp@gmail.com",
                     TRUE ~ EMAIL
                  )
               ) %>%
               select(
                  encoder    = EMAIL,
                  CREATED_BY = STAFF_ID
               ),
            by = "encoder"
         ) %>%
         left_join(
            y  = encoded$ref_faci %>%
               select(
                  TX_FACI = FACI_CODE,
                  FACI_ID
               ) %>%
               distinct_all(),
            by = "TX_FACI"
         ) %>%
         left_join(
            y  = encoded$ref_faci %>%
               select(
                  DISPENSING_FACI = FACI_CODE,
                  DISP_FACI       = FACI_ID,
                  DISP_SUB_FACI   = SUB_FACI_ID
               ) %>%
               distinct_all(),
            by = "DISPENSING_FACI"
         ) %>%
         left_join(
            y  = encoded$ref_faci %>%
               select(
                  REFER_FACI  = FACI_CODE,
                  REFER_BY_ID = FACI_ID
               ) %>%
               distinct_all(),
            by = "REFER_FACI"
         ) %>%
         left_join(
            y  = .GlobalEnv$import$STAFF %>%
               select(
                  PROVIDER_ID = USER_ID,
                  FACI_ID,
                  PROVIDER_NAME
               ) %>%
               distinct_all(),
            by = c("FACI_ID", "PROVIDER_NAME")
         ) %>%
         mutate(
            .after       = REC_ID,
            CREATED_DATE = case_when(
               stri_detect_fixed(CREATED_DATE, "-") & stri_detect_regex(CREATED_DATE, "^[0-9][0-9]-") ~ as.Date(CREATED_DATE, format = "%m-%d-%Y"),
               stri_detect_fixed(CREATED_DATE, "-") & stri_detect_regex(CREATED_DATE, "^[0-9][0-9][0-9][0-9]") ~ as.Date(CREATED_DATE, format = "%Y-%m-%d"),
               stri_detect_fixed(CREATED_DATE, "/") ~ as.Date(CREATED_DATE, format = "%m/%d/%Y"),
            ),
            CREATED_TIME = format(strptime(CREATED_TIME, "%I:%M:%S %p"), "%H:%M:%S"),
            CREATED_AT   = paste(
               sep = " ",
               CREATED_DATE,
               CREATED_TIME
            ),
         ) %>%
         mutate(
            UPDATED_BY         = "1300000000",
            UPDATED_AT         = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
            REC_ID             = paste(
               sep = "_",
               gsub("[^[:digit:]]", "", CREATED_AT),
               CREATED_BY
            ),
            MODULE             = "3",
            DISEASE            = "101000",
            SERVICE_TYPE       = "101201",
            TB_SITE_P          = if_else(
               condition = StrLeft(TB_SITE, 1) == "1",
               true      = 1,
               false     = 0,
               missing   = 0
            ),
            TB_SITE_EP         = if_else(
               condition = StrLeft(TB_SITE, 1) == "2",
               true      = 1,
               false     = 0,
               missing   = 0
            ),
            TB_TX_OUTCOME      = case_when(
               TB_TX_OUTCOME == "Not yet evaluated" ~ "10",
               TB_TX_OUTCOME == "Cured" ~ "11",
               TB_TX_OUTCOME == "Failed" ~ "20",
               TB_TX_OUTCOME == "Other" ~ "8888",
               TRUE ~ TB_TX_OUTCOME
            ),
            TB_DRUG_RESISTANCE = case_when(
               TB_DRUG_RESISTANCE == "Susceptible" ~ "1",
               TB_DRUG_RESISTANCE == "MDR" ~ "2",
               TB_DRUG_RESISTANCE == "XDR" ~ "3",
               TB_DRUG_RESISTANCE == "RR only" ~ "4",
               TB_DRUG_RESISTANCE == "Other" ~ "8888",
               TRUE ~ TB_DRUG_RESISTANCE
            ),
            TB_TX_STATUS       = case_when(
               TB_TX_STATUS == "Not on Tx" ~ "0",
               TB_TX_STATUS == "Ongoing Tx" ~ "11",
               TB_TX_STATUS == "Started Tx" ~ "12",
               TB_TX_STATUS == "Ended Tx" ~ "13",
               TRUE ~ TB_TX_STATUS
            ),
            TB_REGIMEN         = case_when(
               TB_REGIMEN == "Cat I" ~ "10",
               TB_REGIMEN == "Cat Ia" ~ "11",
               TB_REGIMEN == "Cat II" ~ "20",
               TB_REGIMEN == "Cat IIa" ~ "21",
               TB_REGIMEN == "SRDR" ~ "30",
               TB_REGIMEN == "XDR-TB" ~ "40",
               TRUE ~ TB_REGIMEN
            ),
            DISP_FACI          = if_else(
               condition = is.na(DISP_FACI),
               true      = FACI_ID,
               false     = DISP_FACI,
               missing   = DISP_FACI
            ),
            VERSION            = case_when(
               FORM == "Form ART" ~ "2021",
               FORM == "Form BC" ~ "2017",
               TRUE ~ NA_character_
            ),
            FORM               = case_when(
               FORM == "Form ART" ~ "ART Form",
               FORM == "Form BC" ~ "Form BC",
               TRUE ~ FORM
            )
         ) %>%
         mutate_if(
            .predicate = is.character,
            ~str_squish(.)
         ) %>%
         mutate_if(
            .predicate = is.character,
            ~case_when(
               . == "" ~ NA_character_,
               . %in% c("NULL", "TO FOLLOW", "PENDING") ~ NA_character_,
               . == "TRUE" ~ "1",
               . == "FALSE" ~ "0",
               TRUE ~ .
            )
         ) %>%
         rename(
            RECORD_DATE          = VISIT_DATE,
            TX_STATUS            = ART_STATUS,
            SELF_IDENT           = SELF_IDENTITY,
            SELF_IDENT_OTHER     = SELF_IDENTITY_OTHER,
            WHO_CLASS            = WHO_STAGING,
            TB_ACTIVE_ALREADY    = CURR_ACTIVE_TB,
            TB_TX_ALREADY        = CURR_ON_TBTX,
            TB_SCREEN            = PRESENCE_OF_TB_SYMPTOMS,
            TB_IPT_STATUS        = IPT_STATUS,
            TB_IPT_OUTCOME       = IPT_OUTCOME,
            TB_IPT_OUTCOME_OTHER = IPT_OUTCOME_OTHER,
            TX_NOT_REASONS       = REASON_FOR_NONTX,
            CLIENT_TYPE          = DISPENSE_TYPE
         ) %>%
         mutate_at(
            .vars = vars(
               SEX,
               SELF_IDENT,
               TX_STATUS,
               VISIT_TYPE,
               TB_STATUS,
               TB_SCREEN,
               TB_IPT_STATUS,
               TB_IPT_OUTCOME,
               TB_ACTIVE_ALREADY,
               TB_TX_ALREADY,
               CLIENT_TYPE,
               LAB_HBSAG_RESULT
            ),
            ~keep_code(.)
         )

      return(records)
   }

   get_uploaded <- function(standard) {
      data <- standard %>%
         mutate(RECORD_DATE = as.Date(RECORD_DATE))

      min <- as.character(min(data$CREATED_DATE, na.rm = TRUE))
      max <- as.character(max(data$CREATED_DATE, na.rm = TRUE))

      db                   <- "ohasis_interim"
      db_conn              <- ohasis$conn("db")
      uploaded             <- list()
      uploaded$px_record   <- dbTable(
         db_conn,
         db,
         "px_record",
         raw_where = TRUE,
         where     = glue(r"(
         (DATE(CREATED_AT) BETWEEN '{min}' AND '{max}') AND LEFT(CREATED_AT, 6) = '130000'
         )")
      )
      uploaded$px_medicine <- dbTable(
         db_conn,
         db,
         "px_medicine",
         raw_where = TRUE,
         where     = glue(r"(
         (DATE(CREATED_AT) BETWEEN '{min}' AND '{max}') AND LEFT(CREATED_AT, 6) = '130000'
         )")
      )

      dbDisconnect(db_conn)

      return(uploaded)
   }

   standard_checks <- function(standard) {
      checks            <- list()
      checks$error_date <- standard %>%
         filter(is.na(as.Date(RECORD_DATE, format = "%Y-%m-%d")))

      checks$error_disp <- encoded$DISPENSE %>%
         filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
         filter(is.na(as.Date(DISP_DATE, format = "%Y-%m-%d")))

      checks$error_perday <- encoded$DISPENSE %>%
         filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
         filter(is.na(DOSE_PER_DAY) | DOSE_PER_DAY == 0)

      checks$error_disptotal <- encoded$DISPENSE %>%
         filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
         filter(is.na(TOTAL_DISPENSED_PILLS) | TOTAL_DISPENSED_PILLS == 0)

      checks$error_recid <- standard %>%
         filter(stri_detect_fixed(REC_ID, "NA") | is.na(CREATED_BY))

      checks$dup_recid <- standard %>%
         get_dupes(REC_ID)

      checks$no_faci <- standard %>%
         filter(is.na(FACI_ID))

      checks$no_disp <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            PAGE_ID,
            ss,
            encoder,
            DISP_DATE   = RECORD_DATE,
            FACI_ID     = DISP_FACI,
            SUB_FACI_ID = DISP_SUB_FACI
         ) %>%
         full_join(
            y  = encoded$DISPENSE %>%
               filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
               distinct_all() %>%
               group_by(
                  encoder,
                  PAGE_ID,
                  DISP_DATE
               ) %>%
               mutate(
                  ARV_NUM = row_number()
               ) %>%
               ungroup(),
            by = c("encoder", "PAGE_ID", "DISP_DATE")
         ) %>%
         filter(is.na(DRUG))

      checks$no_form <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            PAGE_ID,
            ss,
            encoder,
            DISP_DATE   = RECORD_DATE,
            FACI_ID     = DISP_FACI,
            SUB_FACI_ID = DISP_SUB_FACI
         ) %>%
         full_join(
            y  = encoded$DISPENSE %>%
               filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
               distinct_all() %>%
               group_by(
                  encoder,
                  PAGE_ID,
                  DISP_DATE
               ) %>%
               mutate(
                  ARV_NUM = row_number()
               ) %>%
               ungroup(),
            by = c("encoder", "PAGE_ID", "DISP_DATE")
         ) %>%
         filter(is.na(REC_ID))

      checks$arv_missing <- encoded$DISPENSE %>%
         filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
         left_join(
            y  = encoded$ref_meds %>%
               select(
                  MEDICINE = 1,
                  DRUG     = 4,
               ) %>%
               distinct_all(),
            by = "DRUG"
         ) %>%
         filter(is.na(MEDICINE))

      return(checks)
   }

   tables_wide <- function(standard) {
      wide <- list(
         "px_record"    = c("REC_ID", "PATIENT_ID"),
         "px_info"      = c("REC_ID", "PATIENT_ID"),
         "px_name"      = c("REC_ID", "PATIENT_ID"),
         "px_faci"      = c("REC_ID", "SERVICE_TYPE"),
         "px_profile"   = "REC_ID",
         "px_staging"   = "REC_ID",
         "px_tb"        = "REC_ID",
         "px_tb_ipt"    = "REC_ID",
         "px_tb_active" = "REC_ID",
         "px_ob"        = "REC_ID",
         "px_form"      = c("REC_ID", "FORM")
      )

      tables <- list()
      for (tbl in names(wide)) {
         db_conn <- ohasis$conn("db")
         cols    <- colnames(tbl(db_conn, dbplyr::in_schema("ohasis_interim", tbl)))

         col_select <- intersect(cols, names(standard))

         tables[[tbl]] <- standard %>%
            select(
               col_select
            )

         dbDisconnect(db_conn)
      }

      sql <- list(
         sql  = wide,
         data = tables
      )

      return(sql)
   }

   tables_long <- function(standard, encoded) {

      long <- list(
         "px_addr"          = c("REC_ID", "ADDR_TYPE"),
         "px_key_pop"       = c("REC_ID", "KP"),
         "px_oi"            = c("REC_ID", "OI"),
         "px_labs"          = c("REC_ID", "LAB_TEST"),
         # "px_other_service" = c("REC_ID", "SERVICE"),
         "px_vaccine"       = c("REC_ID", "DISEASE_VAX", "VAX_NUM"),
         "px_remarks"       = c("REC_ID", "REMARK_TYPE"),
         "px_medicine_disc" = c("REC_ID", "MEDICINE"),
         "px_medicine"      = c("REC_ID", "MEDICINE", "DISP_NUM"),
         "px_prophylaxis"   = c("REC_ID", "PROPHYLAXIS")
      )

      # addr
      tables         <- list()
      tables$px_addr <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            ends_with("_REG"),
            ends_with("_PROV"),
            ends_with("_MUNC"),
            ends_with("_ADDR")
         ) %>%
         pivot_longer(
            cols      = c(
               ends_with("_REG"),
               ends_with("_PROV"),
               ends_with("_MUNC"),
               ends_with("_ADDR")
            ),
            names_to  = "ADDR_DATA",
            values_to = "ADDR_VALUE"
         ) %>%
         separate(
            col  = "ADDR_DATA",
            into = c("ADDR_TYPE", "PIECE")
         ) %>%
         mutate(
            ADDR_TYPE = case_when(
               ADDR_TYPE == "CURR" ~ "1",
               ADDR_TYPE == "PERM" ~ "2",
               ADDR_TYPE == "BIRTH" ~ "3",
               ADDR_TYPE == "DEATH" ~ "4",
               ADDR_TYPE == "SERVICE" ~ "5",
               TRUE ~ ADDR_TYPE
            ),
            PIECE     = case_when(
               PIECE == "ADDR" ~ "TEXT",
               TRUE ~ PIECE
            )
         ) %>%
         pivot_wider(
            id_cols      = c(REC_ID, CREATED_AT, CREATED_BY, ADDR_TYPE),
            names_from   = PIECE,
            values_from  = ADDR_VALUE,
            names_prefix = "NAME_"
         ) %>%
         filter(!is.na(NAME_REG)) %>%
         left_join(
            y  = encoded$ref_addr %>%
               select(
                  NAME_REG,
                  NAME_PROV,
                  NAME_MUNC,
                  ADDR_REG  = PSGC_REG,
                  ADDR_PROV = PSGC_PROV,
                  ADDR_MUNC = PSGC_MUNC,
               ) %>%
               distinct_all(),
            by = c("NAME_REG", "NAME_PROV", "NAME_MUNC")
         ) %>%
         select(
            REC_ID,
            starts_with("ADDR_"),
            ADDR_TEXT = NAME_TEXT,
            CREATED_BY,
            CREATED_AT,
         )

      # labs
      tables$px_labs <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            starts_with("LAB"),
         ) %>%
         pivot_longer(
            cols      = starts_with("LAB"),
            names_to  = "LAB_DATA",
            values_to = "LAB_VALUE"
         ) %>%
         mutate(
            LAB_TEST = substr(LAB_DATA, 5, stri_locate_last_fixed(LAB_DATA, "_") - 1),
            PIECE    = substr(LAB_DATA, stri_locate_last_fixed(LAB_DATA, "_") + 1, 1000),
         ) %>%
         mutate(
            LAB_TEST = case_when(
               LAB_TEST == "HBSAG" ~ "1",
               LAB_TEST == "CREA" ~ "2",
               LAB_TEST == "SYPH" ~ "3",
               LAB_TEST == "VL" ~ "4",
               LAB_TEST == "CD4" ~ "5",
               LAB_TEST == "XRAY" ~ "6",
               LAB_TEST == "XPERT" ~ "7",
               LAB_TEST == "DSSM" ~ "8",
               LAB_TEST == "HIVDR" ~ "9",
               LAB_TEST == "HEMO" ~ "10",
               LAB_TEST == "HEMOG" ~ "10",
               TRUE ~ LAB_TEST
            )
         ) %>%
         pivot_wider(
            id_cols      = c(REC_ID, CREATED_AT, CREATED_BY, LAB_TEST),
            names_from   = PIECE,
            values_from  = LAB_VALUE,
            names_prefix = "LAB_"
         ) %>%
         filter(!is.na(LAB_DATE) | !is.na(LAB_RESULT)) %>%
         arrange(REC_ID, LAB_TEST) %>%
         mutate(
            LAB_DATE = case_when(
               stri_detect_fixed(LAB_DATE, "-") & nchar(LAB_DATE) < 10 ~ as.Date(LAB_DATE, format = "%m-%d-%y"),
               stri_detect_fixed(LAB_DATE, "-") & nchar(LAB_DATE) == 10 ~ as.Date(LAB_DATE, format = "%Y-%m-%d"),
               stri_detect_fixed(LAB_DATE, "/") ~ as.Date(LAB_DATE, format = "%m/%d/%Y"),
            ),
         )

      tables$px_key_pop <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            contains("KP"),
         ) %>%
         rename_all(
            ~case_when(
               . == "KP_PDL" ~ "IS_KP_1",
               . == "KP_TG" ~ "IS_KP_2",
               . == "KP_PWID" ~ "IS_KP_3",
               . == "KP_MSM" ~ "IS_KP_5",
               . == "KP_SW" ~ "IS_KP_6",
               . == "KP_OFW" ~ "IS_KP_7",
               . == "KP_PARTNER" ~ "IS_KP_8",
               . == "OTHER_KP" ~ "IS_KP_8888",
               TRUE ~ .
            )
         ) %>%
         pivot_longer(
            cols      = contains("KP"),
            names_to  = "KP",
            values_to = "IS_KP"
         ) %>%
         mutate(
            KP       = stri_replace_all_fixed(KP, "IS_KP_", ""),
            KP_OTHER = if_else(
               condition = KP == "8888" & !is.na(IS_KP),
               true      = IS_KP,
               false     = NA_character_,
               missing   = NA_character_
            ),
            IS_KP    = if_else(
               condition = KP == "8888" & !is.na(KP_OTHER),
               true      = 1,
               false     = 0,
               missing   = 0
            ),
         ) %>%
         filter(IS_KP == 1)

      tables$px_oi <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            contains("OI"),
         ) %>%
         rename_all(
            ~case_when(
               stri_replace_first_fixed(., "OI_", "") == "HIV" ~ "OI_101000",
               stri_replace_first_fixed(., "OI_", "") == "HEPB" ~ "OI_102000",
               stri_replace_first_fixed(., "OI_", "") == "HEPC" ~ "OI_103000",
               stri_replace_first_fixed(., "OI_", "") == "SYPH" ~ "OI_104000",
               stri_replace_first_fixed(., "OI_", "") == "PCP" ~ "OI_111000",
               stri_replace_first_fixed(., "OI_", "") == "CMV" ~ "OI_112000",
               stri_replace_first_fixed(., "OI_", "") == "OROCAND" ~ "OI_113000",
               stri_replace_first_fixed(., "OI_", "") == "HERPES" ~ "OI_117000",
               stri_replace_first_fixed(., "OI_", "") == "TB" ~ "OI_202000",
               stri_replace_first_fixed(., "OI_", "") == "PCP" ~ "OI_111000",
               stri_replace_first_fixed(., "OI_", "") == "MENINGITIS" ~ "OI_115000",
               stri_replace_first_fixed(., "OI_", "") == "OROPHARYNGEAL" ~ "OI_113000",
               stri_replace_first_fixed(., "OI_", "") == "TOXOPLASMOSIS" ~ "OI_116000",
               stri_replace_first_fixed(., "OI_", "") == "COVID19" ~ "OI_201000",
               stri_replace_first_fixed(., "OI_", "") == "OTHER" ~ "OI_8888",
               TRUE ~ .
            )
         ) %>%
         select(
            -OI_MED_COTRI,
            -OI_MED_AZITHRO,
            -OI_MED_FLUCA,
         ) %>%
         pivot_longer(
            cols      = contains("OI"),
            names_to  = "OI",
            values_to = "IS_OI"
         ) %>%
         mutate(
            OI       = stri_replace_all_fixed(OI, "OI_", ""),
            OI_OTHER = if_else(
               condition = OI == "8888" & !is.na(IS_OI),
               true      = IS_OI,
               false     = NA_character_,
               missing   = NA_character_
            ),
            IS_OI    = if_else(
               condition = OI == "8888" & !is.na(OI_OTHER),
               true      = 1,
               false     = 0,
               missing   = 0
            ),
         ) %>%
         filter(IS_OI == 1)


      # other_service
      # tables$px_other_service <- standard %>%
      #    select(
      #       REC_ID,
      #       CREATED_AT,
      #       CREATED_BY,
      #       starts_with("SERVICE_")
      #    ) %>%
      #    select(-SERVICE_TYPE, -SERVICE_CONDOM, -SERVICE_LUBE) %>%
      #    pivot_longer(
      #       cols      = starts_with("SERVICE_"),
      #       names_to  = "SERVICE",
      #       values_to = "GIVEN"
      #    ) %>%
      #    mutate(
      #       SERVICE = stri_replace_all_regex(SERVICE, "^SERVICE_", ""),
      #       SERVICE = case_when(
      #          SERVICE == "HIV_101" ~ "1013",
      #          SERVICE == "IEC_MATS" ~ "1004",
      #          SERVICE == "RISK_COUNSEL" ~ "1002",
      #          SERVICE == "PREP_REFER" ~ "5001",
      #          SERVICE == "SSNT_OFFER" ~ "5002",
      #          SERVICE == "SSNT_ACCEPT" ~ "5003",
      #          SERVICE == "GIVEN_CONDOMS" ~ "2001",
      #          SERVICE == "GIVEN_LUBES" ~ "2002",
      #          TRUE ~ SERVICE
      #       )
      #    ) %>%
      #    filter(GIVEN == 1) %>%
      #    bind_rows(
      #       standard %>%
      #          select(
      #             REC_ID,
      #             CREATED_AT,
      #             CREATED_BY,
      #             OTHER_SERVICE = CONDOMS
      #          ) %>%
      #          filter(!is.na(OTHER_SERVICE)) %>%
      #          mutate(
      #             SERVICE = "2001",
      #             GIVEN   = "1"
      #          ),
      #       standard %>%
      #          select(
      #             REC_ID,
      #             CREATED_AT,
      #             CREATED_BY,
      #             OTHER_SERVICE = LUBES
      #          ) %>%
      #          filter(!is.na(OTHER_SERVICE)) %>%
      #          mutate(
      #             SERVICE = "2002",
      #             GIVEN   = "1"
      #          )
      #    )

      tables$px_vaccine <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            starts_with("HEPB"),
         ) %>%
         rename(HEPB_DATE_FIRST = HEPB_DATE) %>%
         mutate(
            HEPB_DATE_SECOND = HEPB_DATE_FIRST,
            HEPB_DATE_THIRD  = HEPB_DATE_FIRST,
         ) %>%
         pivot_longer(
            cols      = starts_with("HEPB"),
            names_to  = "VAX_DATA",
            values_to = "VAX_VALUE"
         ) %>%
         mutate(
            VAX_DATA    = stri_replace_all_fixed(VAX_DATA, "HEPB_", ""),
            PIECE       = substr(VAX_DATA, 1, stri_locate_last_fixed(VAX_DATA, "_") - 1),
            PIECE       = case_when(
               PIECE == "DATE" ~ "DATE",
               PIECE == "DOSE" ~ "RESULT",
               TRUE ~ PIECE
            ),
            VAX_NUM     = substr(VAX_DATA, stri_locate_last_fixed(VAX_DATA, "_") + 1, 1000),
            VAX_NUM     = case_when(
               VAX_NUM == "FIRST" ~ "1",
               VAX_NUM == "SECOND" ~ "2",
               VAX_NUM == "THIRD" ~ "3",
               TRUE ~ VAX_NUM
            ),
            DISEASE_VAX = "102000"
         ) %>%
         pivot_wider(
            id_cols      = c(REC_ID, CREATED_AT, CREATED_BY, DISEASE_VAX, VAX_NUM),
            names_from   = PIECE,
            values_from  = VAX_VALUE,
            names_prefix = "VAX_"
         ) %>%
         filter(VAX_RESULT == 1) %>%
         arrange(REC_ID, DISEASE_VAX, VAX_NUM) %>%
         mutate(
            VAX_DATE = case_when(
               stri_detect_fixed(VAX_DATE, "-") & nchar(VAX_DATE) < 10 ~ as.Date(VAX_DATE, format = "%m-%d-%y"),
               stri_detect_fixed(VAX_DATE, "-") & nchar(VAX_DATE) == 10 ~ as.Date(VAX_DATE, format = "%Y-%m-%d"),
               stri_detect_fixed(VAX_DATE, "/") ~ as.Date(VAX_DATE, format = "%m/%d/%Y"),
            ),
         )

      tables$px_prophylaxis <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            starts_with("OI_MED"),
         ) %>%
         pivot_longer(
            cols      = starts_with("OI_MED"),
            names_to  = "PROPHYLAXIS",
            values_to = "IS_PROPH"
         ) %>%
         mutate(
            PROPHYLAXIS = case_when(
               PROPHYLAXIS == "OI_MED_COTRI" ~ "2",
               PROPHYLAXIS == "OI_MED_AZITHRO" ~ "3",
               PROPHYLAXIS == "OI_MED_FLUCA" ~ "4",
               TRUE ~ PROPHYLAXIS
            )
         )

      tables$px_remarks <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            ends_with("_NOTES"),
         ) %>%
         pivot_longer(
            cols      = ends_with("_NOTES"),
            names_to  = "REMARK_TYPE",
            values_to = "REMARKS"
         ) %>%
         mutate(
            REMARK_TYPE = case_when(
               REMARK_TYPE == "CLINIC_NOTES" ~ "1",
               REMARK_TYPE == "COUNSELING_NOTES" ~ "2",
               REMARK_TYPE == "COUNSELNOTES" ~ "2",
               TRUE ~ REMARK_TYPE
            )
         ) %>%
         filter(!is.na(REMARKS))

      tables$px_medicine <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            PAGE_ID,
            encoder,
            DISP_DATE   = RECORD_DATE,
            FACI_ID     = DISP_FACI,
            SUB_FACI_ID = DISP_SUB_FACI
         ) %>%
         full_join(
            y  = encoded$DISPENSE %>%
               filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
               distinct_all() %>%
               group_by(
                  encoder,
                  PAGE_ID,
                  DISP_DATE
               ) %>%
               mutate(
                  ARV_NUM = row_number()
               ) %>%
               ungroup(),
            by = c("encoder", "PAGE_ID", "DISP_DATE")
         ) %>%
         filter(!is.na(REC_ID)) %>%
         left_join(
            y  = encoded$ref_meds %>%
               select(
                  MEDICINE = 1,
                  DRUG     = 4,
               ) %>%
               distinct_all(),
            by = "DRUG"
         ) %>%
         mutate_at(
            .vars = vars(DISP_DATE, NEXT_PICKUP),
            ~case_when(
               stri_detect_fixed(., "-") & nchar(.) < 10 ~ as.Date(., format = "%m-%d-%y"),
               stri_detect_fixed(., "-") & nchar(.) == 10 ~ as.Date(., format = "%Y-%m-%d"),
               stri_detect_fixed(., "/") ~ as.Date(., format = "%m/%d/%Y"),
            )
         ) %>%
         mutate_at(
            .vars = vars(DOSE_PER_DAY, TOTAL_DISPENSED_PILLS, PILLS_LEFT),
            ~if_else(is.na(.), "0", .) %>% as.numeric()
         ) %>%
         mutate(
            UNIT_BASIS  = "2",
            # DISP_DATE   = if_else(
            #    condition = is.na(DISP_DATE),
            #    true      = RECORD_DATE,
            #    false     = DISP_DATE,
            #    missing   = DISP_DATE
            # ),
            days        = floor((TOTAL_DISPENSED_PILLS + PILLS_LEFT) / DOSE_PER_DAY),
            NEXT_PICKUP = if_else(
               condition = is.na(NEXT_PICKUP),
               true      = DISP_DATE %m+% days(days),
               false     = NEXT_PICKUP,
               missing   = NEXT_PICKUP
            ),
         ) %>%
         select(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            MEDICINE,
            DISP_NUM        = ARV_NUM,
            BATCH_NUM,
            UNIT_BASIS,
            PER_DAY         = DOSE_PER_DAY,
            DISP_TOTAL      = TOTAL_DISPENSED_PILLS,
            MEDICINE_LEFT   = PILLS_LEFT,
            MEDICINE_MISSED = DOSES_MISSED,
            DISP_DATE,
            NEXT_DATE       = NEXT_PICKUP,
         )

      tables$px_medicine_disc <- standard %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            PAGE_ID,
            encoder,
            RECORD_DATE,
            FACI_ID     = DISP_FACI,
            SUB_FACI_ID = DISP_SUB_FACI
         ) %>%
         inner_join(
            y  = encoded$DISCONTINUE %>%
               mutate(
                  encoder = stri_replace_all_fixed(encoder, glue("{ohasis$ym}_"), ""),
               ),
            by = c("encoder", "PAGE_ID")
         ) %>%
         left_join(
            y  = encoded$ref_meds %>%
               select(
                  MEDICINE = 1,
                  DRUG     = 4,
               ) %>%
               distinct_all(),
            by = "DRUG"
         ) %>%
         mutate_at(
            .vars = vars(RECORD_DATE, DISC_DATE),
            ~case_when(
               stri_detect_fixed(., "-") & nchar(.) < 10 ~ as.Date(., format = "%m-%d-%y"),
               stri_detect_fixed(., "-") & nchar(.) == 10 ~ as.Date(., format = "%Y-%m-%d"),
               stri_detect_fixed(., "/") ~ as.Date(., format = "%m/%d/%Y"),
            )
         ) %>%
         mutate(
            DISC_DATE                = if_else(
               condition = is.na(DISC_DATE),
               true      = RECORD_DATE,
               false     = DISC_DATE,
               missing   = DISC_DATE
            ),
            REASON_FOR_DISCONTINUING = keep_code(REASON_FOR_DISCONTINUING)
         ) %>%
         select(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            MEDICINE,
            DISC_DATE,
            DISC_REASON       = REASON_FOR_DISCONTINUING,
            DISC_REASON_OTHER = OTHER_REASONS,
         )

      sql <- list(
         sql  = long,
         data = tables
      )
      return(sql)
   }

   .init <- function() {
      p <- parent.env(environment())
      local(envir = p, {
         check          <- list()
         encoded        <- download_ei()
         check$raw      <- raw_checks(encoded)
         records        <- standardize_records(encoded)
         done           <- get_uploaded(records)
         check$standard <- standard_checks(records)

         tbls      <- list()
         tbls$wide <- tables_wide(
            records %>%
               anti_join(
                  y  = done$px_record %>%
                     select(REC_ID) %>%
                     inner_join(
                        y  = done$px_medicine %>%
                           select(REC_ID),
                        by = "REC_ID"
                     ),
                  by = "REC_ID"
               )
         )
         tbls$long <- tables_long(
            records %>%
               anti_join(
                  y  = done$px_record %>%
                     select(REC_ID) %>%
                     inner_join(
                        y  = done$px_medicine %>%
                           select(REC_ID),
                        by = "REC_ID"
                     ),
                  by = "REC_ID"
               ),
            encoded
         )
         # results    <- get_pdf_data()
         # match      <- match_encode_pdf(encoded, results)
         #
         # for_import <- prepare_import(.GlobalEnv$nhsss$harp_dx$pdf_saccl$data, match)
         # tables     <- generate_tables(for_import)
      })
   }

})

##  Upsert data ----------------------------------------------------------------

invisible(lapply(names(import$tbls$wide$data), function(table) {
   id_cols <- import$tbls$wide$sql[[table]]
   data    <- import$tbls$wide$data[[table]]

   db_conn     <- ohasis$conn("db")
   table_space <- Id(schema = "ohasis_interim", table = table)
   dbxUpsert(
      db_conn,
      table_space,
      data,
      id_cols
   )
   dbDisconnect(db_conn)
}))

invisible(lapply(names(import$tbls$long$data), function(table) {
   id_cols <- import$tbls$long$sql[[table]]
   data    <- import$tbls$long$data[[table]]

   db_conn     <- ohasis$conn("db")
   table_space <- Id(schema = "ohasis_interim", table = table)
   dbxUpsert(
      db_conn,
      table_space,
      data,
      id_cols
   )
   dbDisconnect(db_conn)
}))
