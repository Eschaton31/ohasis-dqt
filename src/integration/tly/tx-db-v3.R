LyArt <- R6Class(
   "LyArt",
   public = list(
      root              = "",
      data              = list(
         artDb     = tibble(),
         arv       = tibble(),
         ids       = tibble(),
         converted = tibble(),
         existing  = tibble(),
         forUpload = tibble()
      ),
      issues            = list(),
      tables            = list(),

      initialize        = function() {
         self$root <- file.path(getwd(), "data", "ly-imports", format(Sys.time(), "%Y%m%d"))

         invisible(self)
      },
      downloadArtDb     = function() {
         local_drive_quiet()
         local_gs4_quiet()

         ss     <- "1smORFFrPwFFrbXQuUUqxNnxyD9VInEPFL7XgL-dmvUM"
         sheets <- range_speedread(ss, "art", col_types = cols(.default = "c"))

         # ! ART
         dir <- file.path(self$root, "art")
         check_dir(dir)

         for (i in seq_len(nrow(sheets))) {
            branch <- sheets[i,]$branch
            link   <- sheets[i,]$link
            file   <- file.path(dir, stri_c(branch, ".ods"))
            log_info("Downloading ART = {green(branch)}.")
            drive_download(link, file, overwrite = TRUE)
         }

         invisible(self)
      },
      downloadArv       = function() {
         local_drive_quiet()
         local_gs4_quiet()

         # ! ARV
         dir <- file.path(self$root, "arv")
         check_dir(dir)

         ss     <- "1DX8S-5ykevEhX5tgvfOetKBOpfFGd8YlAfTdjCxDmU0"
         sheets <- sheet_names(ss)

         for (branch in sheets) {
            if (!(branch %in% c("TEMPLATE", "DataImport"))) {
               link <- as_id(ss)
               file <- file.path(dir, stri_c(branch, ".ods"))
               log_info("Downloading ARV = {green(branch)}.")
               write_ods(read_sheet(link, branch), file)
            }
         }

         invisible(self)
      },
      readIds           = function() {

         con           <- connect("ohasis-lw")
         self$data$ids <- QB$new(con)$from("ohasis_lake.ly_clients")$get()
         dbDisconnect(con)

         invisible(self)
      },
      readArtDb         = function() {
         files       <- list.files(file.path(self$root, "art"), full.names = TRUE)
         data        <- pblapply(files, read_ods, sheet = "Client Information", col_types = cols(.default = "c"), .name_repair = "unique_quiet")
         data        <- lapply(data, mutate_all, toupper)
         data        <- lapply(data, mutate_all, ~na_if(., "0"))
         data        <- lapply(data, mutate_all, ~na_if(., "-"))
         data        <- lapply(data, mutate_all, ~na_if(., "N/A"))
         data        <- lapply(data, mutate_all, ~na_if(., "Err:522"))
         data        <- lapply(data, mutate_all, ~na_if(., "NULL"))
         data        <- lapply(data, mutate_all, ~na_if(., "#REF!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#NAME!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#VALUE!"))
         data        <- lapply(data, mutate, Row = stri_c("A", row_number() + 1), .before = 1)
         data        <- lapply(data, rename_all, ~toupper(stri_replace_all_regex(., "\\s", "")))
         names(data) <- tools::file_path_sans_ext(basename(files))

         self$data$artDb <- bind_rows(data, .id = "SRC") %>%
            mutate(row_id = row_number()) %>%
            mutate(Branch = if_else(SRC %in% c("ANGLO-1", "ANGLO-2"), "ANGLO", SRC, SRC)) %>%
            select(
               row_id,
               Branch,
               Row              = `ROW`,
               FILE             = `SRC`,
               STATUS           = `STATUS`,
               LY_STARTED_TX    = `TLYSTARTEDTREATMENT`,
               PATIENT_CODE     = `CLIENTCODE`,
               UIC              = `UIC`,
               ACCESSION_CODE   = `ACCESSIONCODE`,
               LAST             = `LEGALSURNAME`,
               FIRST            = `LEGALFIRSTNAME`,
               MIDDLE           = `LEGALMIDDLENAME`,
               SUFFIX           = `SUFFIX`,
               NICKNAME         = `PREFERREDNAME`,
               BIRTHDATE_AUTO   = `DATEOFBIRTHMM/DD/YYYY(AUTO)`,
               SEX              = `SEXATBIRTH`,
               SELF_IDENT       = `GENDERIDENTITY`,
               CURR_ADDR        = `HOMEADDRESS`,
               WORK_ADDR        = `WORKADDRESS`,
               IS_PREGNANT      = `PREGONSTART`,
               CLIENT_MOBILE    = `CONTACT#`,
               CLIENT_EMAIL     = `EMAILADDRESS`,
               COUNSELOR        = `LIFECOACH/COUNSELOR`,
               PHILHEALTH_NO    = `PHILHEALTH#`,
               BIRTHDATE_MANUAL = `DATEOFBIRTHMM/DD/YYYY`,
            ) %>%
            mutate(
               BIRTHDATE_AUTO = if_else(is.na(BIRTHDATE_AUTO) & !is.na(UIC), stri_c(sep = "-", substr(UIC, 7, 8), substr(UIC, 9, 10), str_right(UIC, 4)), BIRTHDATE_AUTO, BIRTHDATE_AUTO),
               BIRTHDATE      = as.Date(parse_date_time(coalesce(BIRTHDATE_MANUAL, BIRTHDATE_AUTO), "mdY")),
               .after         = UIC
            ) %>%
            remove_empty("rows", 0.154)

         invisible(self)
      },
      readArv           = function() {
         files       <- list.files(file.path(self$root, "arv"), full.names = TRUE)
         data        <- pblapply(files, read_ods, col_types = cols(.default = "c"), .name_repair = "unique_quiet")
         data        <- lapply(data, mutate_all, toupper)
         data        <- lapply(data, mutate_all, ~na_if(., ""))
         data        <- lapply(data, mutate_all, ~na_if(., "0"))
         data        <- lapply(data, mutate_all, ~na_if(., "-"))
         data        <- lapply(data, mutate_all, ~na_if(., "---"))
         data        <- lapply(data, mutate_all, ~na_if(., "N/A"))
         data        <- lapply(data, mutate_all, ~na_if(., "#N/A"))
         data        <- lapply(data, mutate_all, ~na_if(., "Err:522"))
         data        <- lapply(data, mutate_all, ~na_if(., "NULL"))
         data        <- lapply(data, mutate_all, ~na_if(., "#REF!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#NAME!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#VALUE!"))
         data        <- lapply(data, mutate, Row = stri_c("A", row_number() + 1), .before = 1)
         data        <- lapply(data, rename_all, ~toupper(stri_replace_all_regex(., "\\s", "")))
         names(data) <- tools::file_path_sans_ext(basename(files))

         self$data$arv <- bind_rows(data, .id = "Branch") %>%
            mutate(row_id = row_number()) %>%
            select(
               row_id,
               Branch,
               Row             = `ROW`,
               DISP_DATE       = `DATEDISPENSED`,
               PATIENT_CODE    = `CLIENTCODE`,
               STATUS          = `CLIENTSTATUS`,
               UIC             = `UNIQUEIDENTIFIERCODE(UIC)`,
               PHILHEALTH_NO   = `PHILHEALTHNUMBER`,
               VISIT_TYPE      = `VISITTYPE`,
               TB_SCREEN       = `TBSYMPTOMS`,
               TB_IPT_STATUS   = `TPTSTATUS`,
               TX_STATUS       = `ARTSTATUS`,
               ARV_REGIMEN     = `REGIMENONFILE`,
               OTHER_REGIMEN   = `OTHERMEDICATIONSONFILE`,
               ARV_DISP        = `MEDSGIVEN(PLEASEINPUT)`,
               CLIENT_TYPE     = `DISPENSINGMODALITY`,
               DISP_TOTAL      = `PILLDISPENSED`,
               PER_DAY         = `PILLSPERDAY`,
               MEDICINE_MISSED = `MISSEDPILLS`,
               MEDICINE_LEFT   = `PILLSLEFT`,
               NEXT_DATE       = `NEXTREFILL`,
               REMARKS         = `REMARKS`,
               NAME            = `NAME`,
               HUB_ORIGIN      = `HUBOFORIGIN`,
               REGIMEN         = `REGIMEN`,
            ) %>%
            filter(!if_all(c(DISP_DATE, PATIENT_CODE), ~is.na(.))) %>%
            mutate(PATIENT_CODE = coalesce(PATIENT_CODE, NAME)) %>%
            mutate_at(
               .vars = vars(DISP_DATE, NEXT_DATE),
               ~as.Date(parse_date_time(., "Ymd"))
            )

         invisible(self)
      },
      convert           = function() {
         self$data$converted <- self$data$arv %>%
            mutate(
               # ARV_REGIMEN   = coalesce(ARV_DISP, ARV_REGIMEN, REGIMEN),
               FINAL_ARV     = coalesce(ARV_REGIMEN, REGIMEN),
               FINAL_ARV     = case_when(
                  FINAL_ARV == 'TENOFOVIR+EMTRICITABINE+EFAVIRENZ' ~ 'TDF+FTC+EFV',
                  FINAL_ARV == 'LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG + LOPINAVIR 200MG + RITONAVIR 50MG (3TC/TDF + LPV/R) (LAMI/TENO + LOPI/RITO)' ~ 'TDF/3TC+LPV/r',
                  FINAL_ARV == 'ZIDOVUDINE-LAMIVUDINE-RILPIVIRINE + EFAVIRENZ' ~ 'AZT/3TC+RIL+EFV',
                  FINAL_ARV == 'LAMIVUDINE 150MG / ZIDOVUDINE 300MG + LOPINAVIR 200MG + RITONAVIR 50MG (3TC/AZT + LPV/R) (LAMI/ZIDO + LOPI/RITO)' ~ 'AZT/3TC+LPV/r',
                  FINAL_ARV == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) (TLD) + DTG' ~ 'TDF/3TC/DTG+DTG',
                  FINAL_ARV == 'ABACAVIR 300MG + LAMIVUDINE 150MG + EFAVIRENZ 600MG (ABC + 3TC + EFV)' ~ 'ABC+3TC+EFV',
                  FINAL_ARV == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) (TLD)' ~ 'TDF/3TC/DTG',
                  FINAL_ARV == 'LAMIVUDINE 300MG +LOPINAVIR /RITONAVIR(RITOCOM) 200MG/50MG+DOLUTEGRAVIR 50MG' ~ '3TC+LPV/r+DTG',
                  FINAL_ARV == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) (TLD' ~ 'TDF/3TC/DTG',
                  FINAL_ARV == 'EFAVIRENZ 600MG + LAMIVUDINE 300MG + TENOFOVIR DISOPROXIL FUMARATE 300MG (EFV/3TC/TDF) (LTE)' ~ 'TDF/3TC/EFV',
                  FINAL_ARV == 'DOLUTEGRAVIR 50 MG+EMTRICITABINE 200MG+ TENOFOVIR ALAFENAMIDE 25 MG' ~ 'TDF+FTC+DTG',
                  FINAL_ARV == 'ABACAVIR-LAMIVUDINE-NEVIRAPINE' ~ 'ABC+3TC+NVP',
                  FINAL_ARV == 'ARV UNKNOWN' ~ '',
                  FINAL_ARV == 'LAMIVUDINE 150MG / ZIDOVUDINE 300MG + EFAVIRENZ 600MG (3TC/AZT + EFV) (LAMI/ZIDO + EFV)' ~ 'AZT/3TC+EFV',
                  FINAL_ARV == 'LAMIVUDINE 300MG +LOPINAVIR/RITONAVIR(RITOCOM)+DOLUTEGRAVIR' ~ '3TC+LPV/r+DTG',
                  FINAL_ARV == 'LOPINAVIR+RITONAVIR(LPV/R)' ~ 'LPV/r',
                  FINAL_ARV == 'DOLUTEGRAVIR 50MG / EMTRICITABINE 200MG / TENOFOVIR ALAFENAMIDE 25MG (PEP)' ~ 'TAF/3TC/DTG',
                  TRUE ~ FINAL_ARV
               ),


               CLIENT_TYPE   = case_when(
                  CLIENT_TYPE == "COURIER" ~ "8",
                  CLIENT_TYPE == "PICK-UP" ~ "2",
                  TRUE ~ CLIENT_TYPE
               ),
               TB_SCREEN     = if_else(!is.na(TB_SCREEN), "1", NA_character_),
               VISIT_TYPE    = case_when(
                  str_detect(VISIT_TYPE, "CONTINUING") ~ "2",
                  str_detect(VISIT_TYPE, "FIRST CONSULT") ~ "1",
                  str_detect(VISIT_TYPE, "FOLLOW-UP") ~ "2",
                  str_detect(VISIT_TYPE, "SHIFTING") ~ "2",
                  str_detect(VISIT_TYPE, "TRANSFER OUT") ~ "2",
                  TRUE ~ VISIT_TYPE
               ),
               TX_STATUS     = case_when(
                  str_detect(TX_STATUS, "CONTINUING") ~ "2",
                  str_detect(TX_STATUS, "ENROLLING") ~ "1",
                  TRUE ~ TX_STATUS
               ),
               TB_STATUS     = if_else(!is.na(TB_IPT_STATUS), "0", NA_character_),
               TB_IPT_STATUS = case_when(
                  TB_IPT_STATUS == "NOT ON TPT" ~ "0",
                  TB_IPT_STATUS == "STARTED" ~ "12",
                  TB_IPT_STATUS == "ONGOING" ~ "11",
                  TB_IPT_STATUS == "ENDED" ~ "13",
                  TRUE ~ TB_IPT_STATUS
               ),
            ) %>%
            left_join(self$data$art %>% select(-Branch, -Row, -row_id), join_by(PATIENT_CODE)) %>%
            mutate(
               UIC           = coalesce(UIC.x, UIC.y),
               PHILHEALTH_NO = coalesce(PHILHEALTH_NO.x, PHILHEALTH_NO.y),
               BIRTHDATE     = if_else(is.na(BIRTHDATE) & !is.na(UIC), as.Date(stri_c(sep = "-", str_right(UIC, 4), substr(UIC, 7, 8), substr(UIC, 9, 10))), BIRTHDATE, BIRTHDATE),
            ) %>%
            select(-ends_with(".x"), -ends_with(".y")) %>%
            left_join(
               y  = self$data$ids %>%
                  filter(!is.na(CENTRAL_ID)) %>%
                  select(-row_id) %>%
                  rename(PATIENT_CODE = CLIENT_CODE),
               by = join_by(
                  Branch == BRANCH,
                  PATIENT_CODE,
                  BIRTHDATE,
                  LAST,
                  FIRST,
                  MIDDLE,
                  SUFFIX,
                  SEX,
                  CLIENT_MOBILE,
                  CLIENT_EMAIL,
                  UIC,
                  PHILHEALTH_NO
               )
            ) %>%
            arrange(Branch, DISP_DATE) %>%
            distinct(row_id, .keep_all = TRUE)

         invisible(self)
      },
      checkIssues       = function() {
         self$issues <- list(
            `categorical`    = self$data$converted %>%
               cateogrical_values(c(
                  "SEX",
                  "SELF_IDENT",
                  "VISIT_TYPE",
                  "TB_SCREEN",
                  "TB_IPT_STATUS",
                  "TX_STATUS",
                  "CLIENT_TYPE",
                  "FINAL_ARV",
                  "TB_STATUS"
               )),
            `tx-not2025`     = self$data$converted %>% filter(DISP_DATE < "2025-01-01" | DISP_DATE > now()),
            `tx-no_name`     = self$data$converted %>% filter(is.na(FIRST), is.na(LAST)),
            `tx-no_arv`      = self$data$converted %>% filter(is.na(FINAL_ARV)),
            `tx-no_dispense` = self$data$converted %>% filter(coalesce(parse_number(DISP_TOTAL), 0) <= 0),
            `tx-not_in_txdb` = self$data$converted %>% filter(is.na(FILE)),
            `tx-no_bdate`    = self$data$converted %>% filter(is.na(BIRTHDATE)),
            `tx-more6mos`    = self$data$converted %>% filter(parse_number(DISP_TOTAL) > 180),
            `tx-no_cid`      = self$data$converted %>%
               filter(is.na(CENTRAL_ID)) %>%
               distinct(
                  CENTRAL_ID,
                  CONFIRMATORY_CODE,
                  PATIENT_CODE,
                  UIC,
                  BIRTHDATE,
                  SEX,
                  FIRST,
                  MIDDLE,
                  LAST,
                  SUFFIX,
                  PHILHEALTH_NO,
                  CLIENT_MOBILE,
                  CLIENT_EMAIL,
                  CURR_ADDR
               )
         )

         invisible(self)
      },
      addNewPatients    = function() {
         max_id <- max(self$data$ids$row_id)
         new    <- self$data$converted %>%
            filter(is.na(CENTRAL_ID)) %>%
            mutate(
               row_id = max_id + row_number()
            ) %>%
            distinct(
               row_id,
               BRANCH = Branch,
               PATIENT_CODE,
               CONFIRMATORY_CODE,
               LAST,
               FIRST,
               MIDDLE,
               SUFFIX,
               UIC,
               BIRTHDATE,
               PHILHEALTH_NO,
               SEX,
               CLIENT_MOBILE,
               CLIENT_EMAIL,
               CURR_ADDR
            ) %>%
            mutate(
               FACI_ID = '130001',
               drop    = is.na(UIC) & is.na(BIRTHDATE) & is.na(FIRST)
            ) %>%
            filter(!drop) %>%
            select(-drop)

         created <- oh_batch_newpx(new, "row_id")

         con <- ohasis$conn("lw")
         dbxUpsert(
            con,
            Id(schema = "ohasis_lake", table = "ly_clients"),
            created %>%
               rename(CLIENT_CODE = PATIENT_CODE) %>%
               rename(CENTRAL_ID = PATIENT_ID) %>%
               select(-CURR_ADDR, -FACI_ID, -SUB_FACI_ID, -PHILSYS_ID, -REC_ID, -CREATED_AT, -CREATED_BY),
            "row_id"
         )
         dbDisconnect(con)

         invisible(self)
      },
      getExisting       = function() {
         lw_conn            <- ohasis$conn("lw")
         self$data$existing <- QB$new(lw_conn)$
            from('ohasis_warehouse.form_art_bc AS art')$
            leftJoin("ohasis_warehouse.id_registry AS id", "art.PATIENT_ID", "=", "id.PATIENT_ID")$
            select("art.REC_ID", "art.VISIT_DATE", "art.MEDICINE_SUMMARY", "art.CREATED_BY", "art.CREATED_AT", "art.PATIENT_ID")$
            selectRaw("COALESCE(id.CENTRAL_ID, art.PATIENT_ID) AS CENTRAL_ID")$
            whereBetween("art.VISIT_DATE", c("2025-01-01", format(Sys.time(), "%Y-%m-%d")))$
            get()

         self$data$existing %<>%
            mutate_if(
               .predicate = is.POSIXct,
               ~as.Date(.)
            )

         dbDisconnect(lw_conn)

         invisible(self)
      },
      prepareUpload     = function() {
         local_gs4_quiet()

         TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
         faci_id   <- read_sheet("1c334aEKFTOl3Cg9Uji7tq1rZjlPM8RKpdXwQgOGyAIg", "facility_id", col_types = "c")

         for_import <- self$data$converted %>%
            filter(DISP_DATE >= as.Date("2025-01-01")) %>%
            filter(DISP_DATE <= now()) %>%
            left_join(faci_id %>% rename(Branch = SITE)) %>%
            mutate(
               DISP_DATE = as.Date(DISP_DATE),
            ) %>%
            rename(
               VISIT_DATE       = DISP_DATE,
               SERVICE_FACI     = FACI_ID,
               SERVICE_SUB_FACI = SUB_FACI_ID,
               CLINIC_NOTES     = REMARKS,
               LATEST_NEXT_DATE = NEXT_DATE,
               MEDICINE_SUMMARY = FINAL_ARV,
               PATIENT_ID       = CENTRAL_ID
            ) %>%
            mutate(
               RECORD_DATE  = VISIT_DATE,
               FORM_VERSION = "ART Form (v2021)",
            ) %>%
            # get records id if existing
            left_join(
               y  = self$data$existing %>%
                  select(
                     PATIENT_ID = CENTRAL_ID,
                     REC_ID,
                     CREATED_BY,
                     CREATED_AT,
                     CENTRAL_ID,
                     VISIT_DATE
                  ),
               by = join_by(PATIENT_ID, VISIT_DATE)
            ) %>%
            mutate(BIRTHDATE = as.Date(BIRTHDATE)) %>%
            # retain only not uploaded and those with changes
            filter(!is.na(PATIENT_ID), !is.na(MEDICINE_SUMMARY)) %>%
            anti_join(
               y  = self$data$existing,
               by = join_by(REC_ID, VISIT_DATE, MEDICINE_SUMMARY),
            ) %>%
            mutate(
               old_rec    = if_else(!is.na(REC_ID), 1, 0, 0),
               CREATED_BY = coalesce(CREATED_BY, "1300000048"),
               CREATED_AT = coalesce(as.character(CREATED_AT), TIMESTAMP),
               UPDATED_BY = if_else(old_rec == 1, "1300000048", NA_character_),
               UPDATED_AT = if_else(old_rec == 1, TIMESTAMP, NA_character_)
            ) %>%
            relocate(any_of(names(self$data$existing)), .before = 1) %>%
            select(-old_rec) %>%
            distinct(row_id, .keep_all = TRUE)

         final_import <- for_import %>%
            filter(!is.na(REC_ID)) %>%
            bind_rows(
               batch_rec_ids(for_import %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
            )

         final_import %<>%
            mutate(
               UPDATED_BY = "1300000048",
               UPDATED_AT = TIMESTAMP
            ) %>%
            left_join(
               y  = self$data$existing %>%
                  select(REC_ID, CORR_PID = PATIENT_ID),
               by = join_by(REC_ID)
            ) %>%
            mutate(
               PATIENT_ID = coalesce(CORR_PID, PATIENT_ID)
            )

         self$data$forUpload <- final_import

         invisible(self)
      },
      deconstructTables = function() {
         tables           <- list()
         tables$px_record <- list(
            name = "px_record",
            pk   = c("REC_ID", "PATIENT_ID"),
            data = self$data$forUpload %>%
               mutate(
                  FACI_ID     = "130001",
                  SUB_FACI_ID = NA_character_,
                  DISEASE     = "101000",
                  MODULE      = 3
               ) %>%
               select(
                  REC_ID,
                  PATIENT_ID,
                  FACI_ID,
                  SUB_FACI_ID,
                  RECORD_DATE,
                  DISEASE,
                  MODULE,
                  CREATED_BY,
                  CREATED_AT,
                  UPDATED_BY,
                  UPDATED_AT,
               )
         )

         tables$px_info <- list(
            name = "px_info",
            pk   = c("REC_ID", "PATIENT_ID"),
            data = self$data$forUpload %>%
               select(
                  REC_ID,
                  PATIENT_ID,
                  CONFIRMATORY_CODE,
                  UIC,
                  PATIENT_CODE,
                  SEX,
                  BIRTHDATE,
                  CREATED_BY,
                  CREATED_AT,
                  UPDATED_BY,
                  UPDATED_AT,
               ) %>%
               mutate(
                  SEX = case_when(
                     SEX == "MALE" ~ "1",
                     SEX == "FEMALE" ~ "2",
                  )
               )
         )

         tables$px_name <- list(
            name = "px_name",
            pk   = c("REC_ID", "PATIENT_ID"),
            data = self$data$forUpload %>%
               select(
                  REC_ID,
                  PATIENT_ID,
                  FIRST,
                  MIDDLE,
                  LAST,
                  SUFFIX,
                  CREATED_BY,
                  CREATED_AT,
                  UPDATED_BY,
                  UPDATED_AT,
               )
         )

         tables$px_contact <- list(
            name = "px_contact",
            pk   = c("REC_ID", "CONTACT_TYPE"),
            data = self$data$forUpload %>%
               select(
                  REC_ID,
                  CREATED_BY,
                  CREATED_AT,
                  UPDATED_BY,
                  UPDATED_AT,
                  CLIENT_MOBILE,
                  CLIENT_EMAIL
               ) %>%
               pivot_longer(
                  cols      = c(CLIENT_MOBILE, CLIENT_EMAIL),
                  names_to  = "CONTACT_TYPE",
                  values_to = "CONTACT"
               ) %>%
               mutate(
                  CONTACT_TYPE = case_when(
                     CONTACT_TYPE == "CLIENT_MOBILE" ~ "1",
                     CONTACT_TYPE == "CLIENT_EMAIL" ~ "2",
                     TRUE ~ CONTACT_TYPE
                  )
               )
         )

         tables$px_faci <- list(
            name = "px_faci",
            pk   = c("REC_ID", "SERVICE_TYPE"),
            data = self$data$forUpload %>%
               mutate(
                  SERVICE_TYPE = "101201",
                  SERVICE_FACI = as.character(SERVICE_FACI),
               ) %>%
               select(
                  REC_ID,
                  FACI_ID     = SERVICE_FACI,
                  SUB_FACI_ID = SERVICE_SUB_FACI,
                  SERVICE_TYPE,
                  VISIT_TYPE,
                  CLIENT_TYPE,
                  TX_STATUS,
                  CREATED_BY,
                  CREATED_AT,
                  UPDATED_BY,
                  UPDATED_AT,
               ) %>%
               mutate_at(
                  .vars = vars(VISIT_TYPE, CLIENT_TYPE, TX_STATUS),
                  ~keep_code(.)
               )
         )

         tables$px_form <- list(
            name = "px_form",
            pk   = c("REC_ID", "FORM"),
            data = self$data$forUpload %>%
               mutate(
                  FORM    = "ART Form",
                  VERSION = "2021"
               ) %>%
               select(
                  REC_ID,
                  FORM,
                  VERSION,
                  CREATED_BY,
                  CREATED_AT,
                  UPDATED_BY,
                  UPDATED_AT,
               )
         )

         tables$px_profile <- list(
            name = "px_profile",
            pk   = "REC_ID",
            data = self$data$forUpload %>%
               mutate(
                  AGE              = calc_age(BIRTHDATE, VISIT_DATE),
                  SELF_IDENT_OTHER = if_else(!(SELF_IDENT %in% c("MAN", "WOMAN", "MALE", "FEMALE")), SELF_IDENT, NA_character_),
                  SELF_IDENT       = case_when(
                     SELF_IDENT == "MALE" ~ "1",
                     SELF_IDENT == "MAN" ~ "1",
                     SELF_IDENT == "FEMALE" ~ "2",
                     SELF_IDENT == "WOMAN" ~ "2",
                     SELF_IDENT == "Q/NB/NC" ~ "3",
                     SELF_IDENT == "TRANSWOMAN" ~ "3",
                     SELF_IDENT == "TRANSMAN" ~ "3",
                     !is.na(SELF_IDENT_OTHER) ~ "3",
                     TRUE ~ SELF_IDENT
                  ),
               ) %>%
               select(
                  REC_ID,
                  AGE,
                  SELF_IDENT,
                  SELF_IDENT_OTHER,
                  CREATED_BY,
                  CREATED_AT,
                  UPDATED_BY,
                  UPDATED_AT,
               ) %>%
               mutate_at(
                  .vars = vars(SELF_IDENT),
                  ~keep_code(.)
               )
         )

         tables$px_tb <- list(
            name = "px_tb",
            pk   = "REC_ID",
            data = self$data$forUpload %>%
               mutate(
                  TB_STATUS = case_when(
                     !is.na(TB_IPT_STATUS) ~ "0_No active TB",
                     TRUE ~ TB_STATUS
                  )
               ) %>%
               select(
                  REC_ID,
                  TB_SCREEN,
                  TB_STATUS,
                  CREATED_BY,
                  CREATED_AT,
                  UPDATED_BY,
                  UPDATED_AT,
               ) %>%
               mutate_at(
                  .vars = vars(TB_SCREEN, TB_STATUS),
                  ~keep_code(.)
               )
         )

         tables$px_tb_ipt <- list(
            name = "px_tb_ipt",
            pk   = "REC_ID",
            data = self$data$forUpload %>%
               mutate(
                  TB_STATUS = case_when(
                     !is.na(TB_IPT_STATUS) ~ "0_No active TB",
                     TRUE ~ TB_STATUS
                  )
               ) %>%
               select(
                  REC_ID,
                  TB_IPT_STATUS,
                  CREATED_BY,
                  CREATED_AT,
                  UPDATED_BY,
                  UPDATED_AT,
               ) %>%
               mutate_at(
                  .vars = vars(TB_IPT_STATUS),
                  ~keep_code(.)
               )
         )

         tables$px_medicine <- list(
            name = "px_medicine",
            pk   = c("REC_ID", "MEDICINE", "DISP_NUM"),
            data = self$data$forUpload %>%
               separate_longer_delim(
                  cols  = MEDICINE_SUMMARY,
                  delim = "+"
               ) %>%
               mutate(
                  SERVICE_FACI = as.character(SERVICE_FACI),
                  UNIT_BASIS   = "2",
                  MEDICINE     = case_when(
                     MEDICINE_SUMMARY == "TDF/3TC/DTG" ~ "2029",
                     MEDICINE_SUMMARY == "TDF/3TC/EFV" ~ "2015",
                     MEDICINE_SUMMARY == "AZT/3TC" ~ "2018",
                     MEDICINE_SUMMARY == "LPV/r" ~ "2009",
                     MEDICINE_SUMMARY == "DTG" ~ "2035",
                     MEDICINE_SUMMARY == "EFV" ~ "2004",
                     MEDICINE_SUMMARY == "3TC" ~ "2023",
                     MEDICINE_SUMMARY == "ABC" ~ "2002",
                     MEDICINE_SUMMARY == "FTC" ~ "2031",
                     MEDICINE_SUMMARY == "NVP" ~ "2011",
                     MEDICINE_SUMMARY == "RIL" ~ "2014",
                     MEDICINE_SUMMARY == "TDF" ~ "2017",
                     MEDICINE_SUMMARY == "TDF/3TC" ~ "2016",
                  ),
               ) %>%
               filter(!is.na(MEDICINE)) %>%
               group_by(row_id) %>%
               mutate(
                  DISP_NUM = row_number(),
                  # DISP_TOTAL = TYPICAL_PER_DAY * DISP_TOTAL
               ) %>%
               ungroup() %>%
               select(
                  REC_ID,
                  FACI_ID     = SERVICE_FACI,
                  SUB_FACI_ID = SERVICE_SUB_FACI,
                  MEDICINE,
                  DISP_NUM,
                  UNIT_BASIS,
                  PER_DAY,
                  DISP_TOTAL,
                  MEDICINE_LEFT,
                  MEDICINE_MISSED,
                  DISP_DATE   = VISIT_DATE,
                  NEXT_DATE   = LATEST_NEXT_DATE,
               )
         )

         self$tables <- tables

         invisible(self)
      },
      upload            = function() {
         db_conn <- ohasis$conn("db")
         dbxDelete(
            db_conn,
            Id(schema = "ohasis_interim", table = "px_medicine"),
            self$data$forUpload %>% select(REC_ID),
            batch_size = 1000
         )
         lapply(self$tables, function(ref, db_conn) {
            log_info("Uploading {green(ref$name)}.")
            table_space <- Id(schema = "ohasis_interim", table = ref$name)
            dbxUpsert(db_conn, table_space, ref$data, ref$pk)
         }, db_conn)
         dbDisconnect(db_conn)

         invisible(self)
      }
   )
)
