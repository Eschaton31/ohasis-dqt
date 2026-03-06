TxCentral <- R6Class(
   'TxCentral',
   public  = list(
      yr               = NA_character_,
      mo               = NA_character_,
      ym               = NA_character_,
      sheets           = tibble(),
      raw              = list(
         forms       = tibble(),
         dispense    = tibble(),
         discontinue = tibble()
      ),
      refs             = list(
         addr     = tibble(),
         ref_addr = tibble(),
         meds     = tibble()
      ),
      data             = list(
         forms       = tibble(),
         dispense    = tibble(),
         discontinue = tibble(),
         forImport   = tibble(),
         uploaded    = list()
      ),
      issues           = list(),

      initialize       = function(yr, mo) {
         self$yr <- stri_pad_left(yr, 4, '0')
         self$mo <- stri_pad_left(mo, 2, '0')

         self$ym <- stri_c(self$yr, ".", self$mo)
      },

      getRefs          = function() {
         local_gs4_quiet()

         self$refs$addr     <- range_speedread("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "addr", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet")
         self$refs$ref_addr <- range_speedread("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "ref_addr", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet")

         conn           <- connect('ohasis-live')
         self$refs$meds <- QB$new(conn)$
            select("product_id as MEDICINE", "short as SHORT", "name as NAME")$
            from('ohasis.products')$
            where('category', '2000')$
            get() %>%
            mutate(
               DRUG = stri_c(coalesce(SHORT, ""), " | ", NAME)
            )
         dbDisconnect(conn)

         invisible(self)
      },

      getSheets        = function() {
         dir_all   <- as_id(drive_ls(as_id("18hh6GZzjnNBidMg9sxOworj2IhbwTUak"), pattern = substr(self$ym, 1, 4))$id)
         dir_month <- as_id(drive_ls(dir_all, pattern = self$ym)$id)
         dir_tx    <- as_id(drive_ls(dir_month, pattern = "Treatment")$id)

         self$sheets <- drive_ls(dir_tx, pattern = self$ym)

         invisible(self)
      },

      readSheets       = function() {
         forms       <- apply(self$sheets, 1, private$readWithSettings, "FORMS")
         dispense    <- apply(self$sheets, 1, private$readWithSettings, "DISPENSE")
         discontinue <- apply(self$sheets, 1, private$readWithSettings, "DISCONTINUE")

         names(forms)       <- tools::file_path_sans_ext(self$sheets$name)
         names(dispense)    <- tools::file_path_sans_ext(self$sheets$name)
         names(discontinue) <- tools::file_path_sans_ext(self$sheets$name)

         self$raw$forms       <- bind_rows(forms, .id = 'name')
         self$raw$dispense    <- bind_rows(dispense, .id = 'name')
         self$raw$discontinue <- bind_rows(discontinue, .id = 'name')

         invisible(self)
      },

      consolidate      = function() {
         self$data$forms <- self$raw$forms %>%
            distinct() %>%
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
               TX_FACI = if_else(TX_FACI == "", str_left(PAGE_ID, 3), TX_FACI, TX_FACI)
            ) %>%
            filter(
               !is.na(CREATED_TIME),
               CREATED_TIME != "DUPLICATE",
               toupper(VISIT_DATE) != "BLANK",
               nchar(PATIENT_ID) == 18,
            ) %>%
            mutate(
               CREATED_BY = case_when(
                  encoder == "jrrebanal.pbsp@gmail.com" ~ "1300000059",
                  encoder == "aespiritu.pbsp@gmail.com" ~ "1300000054",
                  encoder == "chenee.doh@gmail.com" ~ "1300000025",
                  encoder == "rnrufon.pbsp@gmail.com" ~ "1300000012",
                  encoder == "tayagallen14.doh@gmail.com" ~ "1300000029",
                  TRUE ~ encoder
               )
            ) %>%
            left_join(
               y          = ohasis$ref_faci_code %>%
                  distinct(
                     TX_FACI     = faci_code,
                     FACI_ID     = faci_id,
                     SUB_FACI_ID = sub_faci_id,
                  ),
               by         = "TX_FACI",
               na_matches = "never"
            ) %>%
            left_join(
               y          = ohasis$ref_faci_code %>%
                  distinct(
                     DISPENSING_FACI = faci_code,
                     DISP_FACI       = faci_id,
                     DISP_SUB_FACI   = sub_faci_id,
                  ),
               by         = "DISPENSING_FACI",
               na_matches = "never"
            ) %>%
            left_join(
               y          = ohasis$ref_faci_code %>%
                  distinct(
                     REFER_FACI  = faci_code,
                     REFER_BY_ID = faci_id,
                  ),
               by         = "REFER_FACI",
               na_matches = "never"
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
               UPDATED_BY         = "1300000048",
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
                  condition = str_left(TB_SITE, 1) == "1",
                  true      = 1,
                  false     = 0,
                  missing   = 0
               ),
               TB_SITE_EP         = if_else(
                  condition = str_left(TB_SITE, 1) == "2",
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
            ) %>%
            mutate_at(
               .vars = vars(RECORD_DATE),
               ~as.Date(parse_date_time(., c("Ymd", "mdY", "mdy")))
            )

         self$data$forms %<>%
            select(-starts_with("CORR_NAME_")) %>%
            mutate(
               CURR_REG  = toupper(CURR_REG),
               CURR_PROV = toupper(CURR_PROV),
               CURR_MUNC = toupper(CURR_MUNC)
            ) %>%
            left_join(
               y  = self$refs$addr %>%
                  select(
                     CURR_REG  = NAME_REG,
                     CURR_PROV = NAME_PROV,
                     CURR_MUNC = NAME_MUNC,
                     CORR_REG  = CORR_NAME_REG,
                     CORR_PROV = CORR_NAME_PROV,
                     CORR_MUNC = CORR_NAME_MUNC
                  ),
               by = join_by(CURR_REG, CURR_PROV, CURR_MUNC)
            ) %>%
            mutate(
               CURR_REG  = coalesce(CORR_REG, CURR_REG),
               CURR_PROV = coalesce(CORR_PROV, CURR_PROV),
               CURR_MUNC = coalesce(CORR_MUNC, CURR_MUNC),
            ) %>%
            left_join(
               y  = self$refs$ref_addr %>%
                  mutate_at(
                     .vars = vars(NAME_REG, NAME_PROV, NAME_MUNC),
                     ~str_squish(toupper(.))
                  ) %>%
                  select(
                     CURR_REG       = NAME_REG,
                     CURR_PROV      = NAME_PROV,
                     CURR_MUNC      = NAME_MUNC,
                     CURR_PSGC_REG  = PSGC_REG,
                     CURR_PSGC_PROV = PSGC_PROV,
                     CURR_PSGC_MUNC = PSGC_MUNC
                  ),
               by = join_by(CURR_REG, CURR_PROV, CURR_MUNC)
            ) %>%
            mutate(
               CURR_PSGC = coalesce(CURR_PSGC_MUNC, CURR_PSGC_PROV, CURR_PSGC_REG),
            ) %>%
            left_join(
               y          = ohasis$ref_addr %>%
                  select(
                     CURR_PSGC = psgc_old,
                     curr_reg  = reg,
                     curr_prov = prov,
                     curr_munc = munc
                  ),
               by         = join_by(CURR_PSGC),
               na_matches = "never"
            )

         self$data$dispense <- self$raw$dispense %>%
            filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
            mutate(
               DRUG = case_when(
                  DRUG == "TDF/3TC/DTG | Tenofovir Disoproxil Fumarate/Lamivudine/Dolutegravir 300 mg/300 mg/50 mg" ~ "TDF/3TC/DTG | Tenofovir Disoproxil Fumarate/Lamivudine/Dolutegravir 300 mg/300 mg/50 mg (TLD)",
                  DRUG == "TDF/3TC/EFV | Tenofovir Disoproxil Fumarate/Lamivudine/Efavirenz 300 mg/300 mg/600 mg" ~ "TDF/3TC/EFV | Tenofovir Disoproxil Fumarate/Lamivudine/Efavirenz 300 mg/300 mg/600 mg (LTE)",
                  TRUE ~ DRUG
               )
            ) %>%
            left_join(
               y  = self$refs$meds,
               by = "DRUG"
            ) %>%
            mutate_at(
               .vars = vars(DISP_DATE, NEXT_PICKUP),
               ~as.Date(parse_date_time(., c("Ymd", "mdY", "mdy")))
            ) %>%
            group_by(
               encoder,
               PAGE_ID,
               DISP_DATE
            ) %>%
            mutate(
               ARV_NUM = row_number()
            ) %>%
            ungroup()

         self$data$discontinue <- self$raw$discontinue %>%
            filter(!is.na(PAGE_ID), !stri_detect_fixed(DISC_DATE, "DUPLICATE")) %>%
            mutate(
               DRUG = case_when(
                  DRUG == "TDF/3TC/DTG | Tenofovir Disoproxil Fumarate/Lamivudine/Dolutegravir 300 mg/300 mg/50 mg" ~ "TDF/3TC/DTG | Tenofovir Disoproxil Fumarate/Lamivudine/Dolutegravir 300 mg/300 mg/50 mg (TLD)",
                  DRUG == "TDF/3TC/EFV | Tenofovir Disoproxil Fumarate/Lamivudine/Efavirenz 300 mg/300 mg/600 mg" ~ "TDF/3TC/EFV | Tenofovir Disoproxil Fumarate/Lamivudine/Efavirenz 300 mg/300 mg/600 mg (LTE)",
                  TRUE ~ DRUG
               )
            ) %>%
            left_join(
               y  = self$refs$meds,
               by = "DRUG"
            ) %>%
            mutate_at(
               .vars = vars(DISC_DATE),
               ~as.Date(parse_date_time(., c("Ymd", "mdY", "mdy")))
            )

         invisible(self)
      },

      filterImportable = function() {
         self$data$forImport <- self$data$forms %>%
            select(
               -starts_with("CORR_"),
               -starts_with("CURR_PSGC"),
               -contains("."),
               -any_of(c(
                  "CURR_REG",
                  "CURR_PROV",
                  "CURR_MUNC"
               ))
            ) %>%
            mutate(
               client_mobile = NA_character_,
               client_email  = NA_character_,
            ) %>%
            anti_join(
               self$data$uploaded$px_medicine,
               join_by(REC_ID == rec_id)
            )

         invisible(self)
      },

      getExisting      = function() {
         min      <- as.character(min(self$data$forms$CREATED_DATE, na.rm = TRUE))
         max      <- as.character(max(self$data$forms$CREATED_DATE, na.rm = TRUE))
         encoders <- unique(self$data$forms$CREATED_BY)

         db_conn                        <- ohasis$conn("db")
         self$data$uploaded$px_record   <- QB$new(db_conn)$from('ohasis.px_record')$whereIn('created_by', encoders)$whereBetween("created_at", c(min, max))$get()
         self$data$uploaded$px_medicine <- QB$new(db_conn)$from('ohasis.px_medicine')$whereIn('rec_id', self$data$uploaded$px_record$rec_id)$get()
         dbDisconnect(db_conn)

         return()
      },

      checkIssues      = function() {
         self$issues$enrollees <- self$raw$forms %>%
            distinct() %>%
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
               y  = ohasis$ref_faci_code %>%
                  distinct(
                     TX_FACI = faci_code,
                     FACI_ID = faci_id,
                  ),
               by = join_by(TX_FACI)
            ) %>%
            select(
               ss,
               encoder,
               pid_sheetrow,
               PAGE_ID,
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
            )

         self$issues$categorical <- categorical_values(
            self$data$forms,
            c(
               "encoder",
               "CREATED_BY",
               "SEX",
               "SELF_IDENT",
               "VISIT_TYPE",
               "TX_STATUS",
               "CLIENT_TYPE"
            )
         )

         self$issues$error_date <- self$data$forms %>%
            filter(is.na(RECORD_DATE))

         self$issues$error_disp <- self$raw$dispense %>%
            filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
            filter(is.na(as.Date(DISP_DATE, format = "%Y-%m-%d")))

         self$issues$error_perday <- self$raw$dispense %>%
            filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
            filter(is.na(DOSE_PER_DAY) | DOSE_PER_DAY == 0)

         self$issues$error_disptotal <- self$raw$dispense %>%
            filter(!is.na(PAGE_ID), !stri_detect_fixed(DISP_DATE, "DUPLICATE")) %>%
            filter(is.na(TOTAL_DISPENSED_PILLS) | TOTAL_DISPENSED_PILLS == 0)

         self$issues$error_recid <- self$data$forms %>%
            filter(stri_detect_fixed(REC_ID, "NA") | is.na(CREATED_BY))

         self$issues$dup_recid <- self$data$forms %>%
            get_dupes(REC_ID)

         self$issues$no_faci <- self$data$forms %>%
            filter(is.na(FACI_ID))

         self$issues$no_disp <- self$data$forms %>%
            select(
               ss,
               encoder,
               REC_ID,
               CREATED_AT,
               CREATED_BY,
               PAGE_ID,
               DISP_DATE   = RECORD_DATE,
               FACI_ID     = DISP_FACI,
               SUB_FACI_ID = DISP_SUB_FACI
            ) %>%
            full_join(
               y  = self$data$dispense,
               by = join_by(encoder, PAGE_ID, DISP_DATE),
            ) %>%
            filter(is.na(DRUG))

         self$issues$no_form <- self$data$forms %>%
            select(
               ss,
               encoder,
               REC_ID,
               CREATED_AT,
               CREATED_BY,
               PAGE_ID,
               DISP_DATE   = RECORD_DATE,
               FACI_ID     = DISP_FACI,
               SUB_FACI_ID = DISP_SUB_FACI
            ) %>%
            full_join(
               y  = self$data$dispense,
               by = join_by(encoder, PAGE_ID, DISP_DATE),
            ) %>%
            filter(is.na(REC_ID))

         self$issues$arv_missing <- self$data$dispense %>%
            filter(is.na(MEDICINE))
      }

   ),
   private = list(
      readGsheet       = function(ss, sheet) {
         local_gs4_quiet()
         data <- range_speedread(ss, sheet = sheet, col_types = cols(.default = "c"), name_repair = "unique_quiet")
         data %<>%
            mutate(
               pid_sheetrow = paste0("B", row_number() + 1),
               .before      = 1
            )

         return(data)
      },

      readWithSettings = function(params, sheet) {
         ss    <- as_id(params$id)
         name  <- params$name
         email <- str_squish(substr(name, 9, stri_locate_first_fixed(name, ".com") + 4))
         log_info(green(email))

         data <- private$readGsheet(ss, sheet) %>%
            mutate(
               encoder = email,
               ss      = ss,
               .before = 1
            )

         return(data)
      }
   )
)

import <- TxCentral$new(2025, 12)
import$getRefs()
import$getSheets()
import$readSheets()
import$consolidate()
import$getExisting()
import$checkIssues()
import$filterImportable()

tables <- import$data$forImport %>%
   deconstruct_art(import$data$dispense, import$data$discontinue)

idreg <- update_idreg()
tables$patients$data %<>%
   get_cid(idreg, patient_id) %>%
   mutate(birthdate = as.Date(birthdate)) %>%
   get_latest_pii(
      "central_id",
      c(
         'confirmatory_code',
         'patient_code',
         'uic',
         'philhealth_no',
         'philsys_id',
         'first',
         'middle',
         'last',
         'suffix',
         'birthdate',
         'sex',
         'self_ident',
         'self_ident_other',
         'client_email',
         'client_mobile',
         'nationality',
         'civil_status',
         'educ_level',
         'curr_reg',
         'curr_prov',
         'curr_munc',
         'curr_brgy',
         'perm_reg',
         'perm_prov',
         'perm_munc',
         'perm_brgy',
         'birth_reg',
         'birth_prov',
         'birth_munc',
         'birth_brgy'
      )
   ) %>%
   select(-central_id) %>%
   mutate_at(
      .vars = vars(civil_status, educ_level, sex, self_ident),
      keep_code
   )

long   <- c("px_labs", "px_med_profile", "px_vaccine", "px_key_pop", "px_oi", "px_prophylaxis", "px_remarks", "px_medicine", "px_medicine_disc", "px_other_service")
delete <- tables$px_record$data %>% select(rec_id)

db_conn <- connect('ohasis-live')
pblapply(long, function(table) dbxDelete(db_conn, Id(schema = "ohasis", table = table), delete))
pblapply(tables, function(ref, db_conn) {
   table_space <- Id(schema = "ohasis", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
}, db_conn)
dbDisconnect(db_conn)

