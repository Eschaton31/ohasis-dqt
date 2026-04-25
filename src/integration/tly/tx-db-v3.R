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
         google_account("eb@loveyourself.ph")

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
         google_account("eb@loveyourself.ph")

         # ! ARV
         dir <- file.path(self$root, "arv")
         check_dir(dir)

         ss     <- "1Nx8nvtH_TgrmAxFVX8TCVCXtNX0BGEJDn3gGNT1LUW8"
         sheets <- sheet_names(ss)

         for (branch in sheets) {
            if (!(branch %in% c("template", "DataImport", 'ARV REGIMEN', 'TEMPLATE', 'COMBINER'))) {
               link <- as_id(ss)
               file <- file.path(dir, stri_c(branch, ".ods"))
               log_info("Downloading ARV = {green(branch)}.")
               write_ods(read_sheet(link, branch, col_types = "c"), file)
            }
         }

         ss     <- "1AcDP1xgl4asJ3LrXW_H8JFUAtUSyf6TcAVpUHFExqI0"
         sheets <- sheet_names(ss)

         for (branch in sheets) {
            if (!(branch %in% c("template", "DataImport", 'ARV REGIMEN', 'TEMPLATE', 'COMBINER'))) {
               link <- as_id(ss)
               file <- file.path(dir, stri_c(branch, ".ods"))
               log_info("Downloading ARV = {green(branch)}.")
               write_ods(read_sheet(link, branch, col_types = "c"), file)
            }
         }

         invisible(self)
      },
      readIds           = function() {

         con           <- connect("old-lw")
         self$data$ids <- QB$new(con)$from("ohasis_lake.ly_clients")$get()
         dbDisconnect(con)

         invisible(self)
      },
      readArtDb         = function() {
         google_account("nhsss@doh.gov.ph")
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

         self$data$artDb <- bind_rows(data, .id = "src") %>%
            mutate(row_id = row_number()) %>%
            mutate(Branch = if_else(src %in% c("ANGLO-1", "ANGLO-2"), "ANGLO", src, src)) %>%
            rename_all(tolower) %>%
            select(
               row_id,
               Branch           = branch,
               Row              = `row`,
               file             = `src`,
               status           = `status`,
               ly_started_tx    = `tlystartedtreatment`,
               patient_code     = `clientcode`,
               uic              = `uic`,
               accession_code   = `accessioncode`,
               last             = `legalsurname`,
               first            = `legalfirstname`,
               middle           = `legalmiddlename`,
               suffix           = `suffix`,
               nickname         = `preferredname`,
               birthdate_auto   = `dateofbirthmm/dd/yyyy(auto)`,
               sex              = `sexatbirth`,
               self_ident       = `genderidentity`,
               curr_addr        = `homeaddress`,
               work_addr        = `workaddress`,
               is_pregnant      = `pregonstart`,
               client_mobile    = `contact#`,
               client_email     = `emailaddress`,
               counselor        = `lifecoach/counselor`,
               philhealth_no    = `philhealth#`,
               birthdate_manual = `dateofbirthmm/dd/yyyy`,
            ) %>%
            mutate(
               birthdate_auto = if_else(is.na(birthdate_auto) & nchar(uic) == 14, stri_c(sep = "-", substr(uic, 7, 8), substr(uic, 9, 10), str_right(uic, 4)), birthdate_auto, birthdate_auto),
               birthdate      = as.Date(parse_date_time(coalesce(birthdate_manual, birthdate_auto), "mdY")),
               .after         = uic
            ) %>%
            remove_empty("rows", 0.154)

         addr     <- range_speedread("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "addr", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet")
         ref_addr <- range_speedread("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "ref_addr", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet")
         self$data$artDb %<>%
            select(-starts_with("CORR_NAME_")) %>%
            mutate(
               CURR_NAME_REG  = "UNKNOWN",
               CURR_NAME_PROV = "UNKNOWN",
               CURR_NAME_MUNC = curr_addr
            ) %>%
            left_join(
               y  = addr %>%
                  select(
                     CURR_NAME_REG  = NAME_REG,
                     CURR_NAME_PROV = NAME_PROV,
                     CURR_NAME_MUNC = NAME_MUNC,
                     CORR_NAME_REG,
                     CORR_NAME_PROV,
                     CORR_NAME_MUNC
                  ),
               by = join_by(CURR_NAME_REG, CURR_NAME_PROV, CURR_NAME_MUNC)
            ) %>%
            mutate(
               CURR_NAME_REG  = coalesce(CORR_NAME_REG, CURR_NAME_REG),
               CURR_NAME_PROV = coalesce(CORR_NAME_PROV, CURR_NAME_PROV),
               CURR_NAME_MUNC = coalesce(CORR_NAME_MUNC, CURR_NAME_MUNC),
            ) %>%
            left_join(
               y  = ref_addr %>%
                  mutate_at(
                     .vars = vars(NAME_REG, NAME_PROV, NAME_MUNC),
                     ~str_squish(toupper(.))
                  ) %>%
                  select(
                     CURR_NAME_REG  = NAME_REG,
                     CURR_NAME_PROV = NAME_PROV,
                     CURR_NAME_MUNC = NAME_MUNC,
                     CURR_PSGC_REG  = PSGC_REG,
                     CURR_PSGC_PROV = PSGC_PROV,
                     CURR_PSGC_MUNC = PSGC_MUNC
                  ),
               by = join_by(CURR_NAME_REG, CURR_NAME_PROV, CURR_NAME_MUNC)
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

         invisible(self)
      },
      readArv           = function() {
         google_account("nhsss@doh.gov.ph")
         files       <- list.files(file.path(self$root, "arv"), full.names = TRUE)
         data        <- pblapply(files, function(file) {
            # log_info(file)
            return(read_ods(file, col_types = cols(.default = "c"), .name_repair = "unique_quiet"))
         })
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
            rename_all(tolower) %>%
            select(
               row_id,
               Branch          = branch,
               Row             = `row`,
               disp_date       = `datedispensed`,
               patient_code    = `clientcode`,
               status          = `clientstatus`,
               uic             = `uniqueidentifiercode(uic)`,
               philhealth_no   = `philhealthnumber`,
               visit_type      = `visittype`,
               tb_screen       = `tbsymptoms`,
               tb_ipt_status   = `tptstatus`,
               tx_status       = `artstatus`,
               arv_regimen     = `regimenonfile`,
               other_regimen   = `othermedicationsonfile`,
               arv_disp        = `medsgiven(pleaseinput)`,
               client_type     = `dispensingmodality`,
               disp_total      = `pilldispensed`,
               per_day         = `pillsperday`,
               medicine_missed = `missedpills`,
               medicine_left   = `pillsleft`,
               next_date       = `nextrefill`,
               remarks         = `remarks`,
               name            = `name`,
               hub_origin      = `huboforigin`,
               regimen         = `regimen`,
            ) %>%
            filter(!if_all(c(disp_date, patient_code), ~is.na(.))) %>%
            mutate(patient_code = coalesce(patient_code, name)) %>%
            mutate_at(
               .vars = vars(disp_date, next_date),
               ~as.Date(parse_date_time(., c("Ymd", "mdY")))
            )

         invisible(self)
      },
      convert           = function() {
         self$data$converted <- self$data$arv %>%
            mutate(
               # arv_regimen   = coalesce(arv_disp, arv_regimen, regimen),
               arv_regimen   = toupper(arv_regimen),
               arv_regimen   = na_if(arv_regimen, "NO ARV ON FILE"),
               regimen       = toupper(regimen),
               arv_disp      = toupper(arv_disp),
               final_arv     = str_squish(toupper(coalesce(arv_regimen, arv_disp, regimen))),
               final_arv     = case_when(
                  final_arv == 'TENOFOVIR+EMTRICITABINE+EFAVIRENZ' ~ 'TDF+FTC+EFV',
                  final_arv == 'LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG + LOPINAVIR 200MG + RITONAVIR 50MG (3TC/TDF + LPV/R) (LAMI/TENO + LOPI/RITO)' ~ 'TDF/3TC+LPV/R',
                  final_arv == 'ZIDOVUDINE-LAMIVUDINE-RILPIVIRINE + EFAVIRENZ' ~ 'AZT/3TC+RIL+EFV',
                  final_arv == 'LAMIVUDINE 150MG / ZIDOVUDINE 300MG + LOPINAVIR 200MG + RITONAVIR 50MG (3TC/AZT + LPV/R) (LAMI/ZIDO + LOPI/RITO)' ~ 'AZT/3TC+LPV/R',
                  final_arv == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) (TLD) + DTG' ~ 'TDF/3TC/DTG+DTG',
                  final_arv == 'ABACAVIR 300MG + LAMIVUDINE 150MG + EFAVIRENZ 600MG (ABC + 3TC + EFV)' ~ 'ABC+3TC+EFV',
                  final_arv == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) (TLD)' ~ 'TDF/3TC/DTG',
                  final_arv == 'DOLUTEGRAVIR 50MG/ LAMIVUDINE 300MG/ TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) TLD' ~ 'TDF/3TC/DTG',
                  final_arv == 'LAMIVUDINE 300MG +LOPINAVIR /RITONAVIR(RITOCOM) 200MG/50MG+DOLUTEGRAVIR 50MG' ~ '3TC+LPV/R+DTG',
                  final_arv == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) (TLD' ~ 'TDF/3TC/DTG',
                  final_arv == 'EFAVIRENZ 600MG + LAMIVUDINE 300MG + TENOFOVIR DISOPROXIL FUMARATE 300MG (EFV/3TC/TDF) (LTE)' ~ 'TDF/3TC/EFV',
                  final_arv == 'DOLUTEGRAVIR 50 MG+EMTRICITABINE 200MG+ TENOFOVIR ALAFENAMIDE 25 MG' ~ 'DTG/FTC/TAF',
                  final_arv == 'ABACAVIR-LAMIVUDINE-NEVIRAPINE' ~ 'ABC+3TC+NVP',
                  final_arv == 'ARV UNKNOWN' ~ '',
                  final_arv == 'LAMIVUDINE 150MG / ZIDOVUDINE 300MG + EFAVIRENZ 600MG (3TC/AZT + EFV) (LAMI/ZIDO + EFV)' ~ 'AZT/3TC+EFV',
                  final_arv == 'LAMIVUDINE 300MG +LOPINAVIR/RITONAVIR(RITOCOM)+DOLUTEGRAVIR' ~ '3TC+LPV/R+DTG',
                  final_arv == 'LOPINAVIR+RITONAVIR(LPV/R)' ~ 'LPV/R',
                  final_arv == 'DOLUTEGRAVIR 50MG / EMTRICITABINE 200MG / TENOFOVIR ALAFENAMIDE 25MG (PEP)' ~ 'DTG/FTC/TAF',
                  final_arv == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG + DOLUTEGRAVIR 50MG - (DTG/3TC/TDF + DTG)' ~ 'TDF/3TC/DTG+DTG',
                  final_arv == 'ABACAVIR 300MG / LAMIVUDINE 150MG / DOLUTEGRAVIR 50MG' ~ 'ABC+3TC+DTG',
                  final_arv == 'LAMIVUDINE 150MG/ZIDOVUDINE 300MG/ DOLUTEGRAVIR 50MG' ~ 'AZT/3TC+DTG',
                  final_arv == '3TC/AZT + DTG' ~ 'AZT/3TC+DTG',
                  final_arv == 'LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG + RILPIVIRINE 25MG (3TC/TDF + RPV)' ~ 'TDF/3TC+RIL',
                  final_arv == 'DOLUTEGRAVIR 50MG + EMTRICITABINE 200MG + TENOFOVIR ALAFENAMIDE 25MG (DTG+FTC+TAF)' ~ 'DTG/FTC/TAF',
                  final_arv == 'LAMIVUDINE 150MG + LOPINAVIR 200MG/RITONAVIR 50MG + DOLUTEGRAVIR 50MG' ~ '3TC+LPV/R+DTG',
                  final_arv == 'LAMIVUDINE + DOLUTEGRAVIR' ~ '3TC+DTG',
                  final_arv == 'DTG 50MG / 3TC 300MG / TDF 300MG (TLD) (1 BOTTLE)' ~ 'TDF/3TC/DTG',
                  final_arv == 'DTG , PEN-G' ~ 'DTG+PEN-G',
                  final_arv == 'EMTRI + DTG' ~ 'FTC/TAF',
                  final_arv == 'TLD' ~ 'TDF/3TC/DTG',
                  final_arv == 'SOLO DOLU ONLY' ~ 'DTG',
                  final_arv == 'LAMIZIDO / ALUVIA' ~ 'AZT/3TC+LPV/R',
                  final_arv == 'PREP + DTG' ~ 'TDF/FTC+DTG',
                  final_arv == 'TLD , COTRI-30 ,3HP-36' ~ 'TDF/3TC/DTG+CPT-30+3HP-36',
                  final_arv == 'TLD / COTRI -60PCS' ~ 'TDF/3TC/DTG+CPT-60',
                  final_arv == 'TLD , ISO-90' ~ 'TDF/3TC/DTG+INH-90',
                  final_arv == 'LAMI/ZIDO + LPV/R' ~ 'AZT/3TC+LPV/R',
                  final_arv == 'AZT/ DTG' ~ 'AZT+DTG',
                  final_arv == 'ABC+3TC+DOLUTEGRAVIR' ~ 'ABC/DTG/3TC',
                  final_arv == 'TLD/IPT#30/CPT#30' ~ 'TDF/3TC/DTG+INH-30+CPT-30',
                  final_arv == 'TLD/IPT#30' ~ 'TDF/3TC/DTG+INH-30',
                  final_arv == 'IPT GIVEN 30 TABS, CPT GIVEN 30 TABS' ~ 'INH-30+CPT-30',
                  final_arv == 'TLD/IPT' ~ 'TDF/3TC/DTG+INH-30',
                  final_arv == 'TLD/IPT30/CPT30' ~ 'TDF/3TC/DTG+INH-30+CPT-30',
                  final_arv == 'TLD/IPT30//CPT30' ~ 'TDF/3TC/DTG+INH-30+CPT-30',
                  final_arv == 'LAMIVUDINE60/ABACAVIR60/DOLUTEGRAVIR30' ~ 'ABC/DTG/3TC',
                  final_arv == 'TLD/IPT30' ~ 'TDF/3TC/DTG+INH-30',
                  final_arv == 'DTG 50MG / 3TC 300MG / TDF 300MG (TLD) (1 BOTTLE)' ~ 'TDF/3TC/DTG',
                  final_arv == 'COTRINIDAZOLE / ISONIAZID' ~ 'CPT-30+INH-30',
                  final_arv == 'ISONIAZID / COTRINIDAZOLE' ~ 'CPT-30+INH-30',
                  final_arv == 'ISONIAZID' ~ 'INH-30',
                  TRUE ~ final_arv
               ),


               client_type   = case_when(
                  client_type == "COURIER" ~ "8",
                  client_type == "PICK-UP" ~ "2",
                  TRUE ~ client_type
               ),
               tb_screen     = if_else(!is.na(tb_screen), "1", NA_character_),
               visit_type    = case_when(
                  str_detect(visit_type, "CONTINUING") ~ "2",
                  str_detect(visit_type, "FIRST CONSULT") ~ "1",
                  str_detect(visit_type, "FOLLOW-UP") ~ "2",
                  str_detect(visit_type, "SHIFTING") ~ "2",
                  str_detect(visit_type, "TRANSFER OUT") ~ "2",
                  TRUE ~ visit_type
               ),
               tx_status     = case_when(
                  str_detect(tx_status, "CONTINUING") ~ "2",
                  str_detect(tx_status, "ENROLLING") ~ "1",
                  str_detect(tx_status, "FOLLOW-UP") ~ "1",
                  TRUE ~ tx_status
               ),
               tb_status     = if_else(!is.na(tb_ipt_status), "0", NA_character_),
               tb_ipt_status = case_when(
                  tb_ipt_status == "NOT ON TPT" ~ "0",
                  tb_ipt_status == "STARTED" ~ "12",
                  tb_ipt_status == "ONGOING" ~ "11",
                  tb_ipt_status == "ENDED" ~ "13",
                  TRUE ~ tb_ipt_status
               ),
            ) %>%
            left_join(self$data$art %>% select(-Branch, -Row, -row_id), join_by(patient_code)) %>%
            mutate(
               uic           = coalesce(uic.x, uic.y),
               philhealth_no = coalesce(philhealth_no.x, philhealth_no.y),
               birthdate     = if_else(is.na(birthdate) & nchar(uic) == 14, as.Date(stri_c(sep = "-", str_right(uic, 4), substr(uic, 7, 8), substr(uic, 9, 10))), birthdate, birthdate),
            ) %>%
            select(-ends_with(".x"), -ends_with(".y")) %>%
            left_join(
               y  = self$data$ids %>%
                  rename_all(tolower) %>%
                  filter(!is.na(central_id)) %>%
                  select(-row_id) %>%
                  rename(patient_code = client_code) %>%
                  mutate(
                     sex = case_when(
                        sex == '1' ~ 'MALE',
                        sex == '2' ~ 'FEMALE',
                        TRUE ~ sex
                     )
                  ) %>%
                  distinct(
                     patient_code,
                     birthdate,
                     last,
                     first,
                     middle,
                     suffix,
                     sex,
                     client_mobile,
                     client_email,
                     uic,
                     philhealth_no,
                     .keep_all = TRUE
                  ),
               by = join_by(
                  patient_code,
                  birthdate,
                  last,
                  first,
                  middle,
                  suffix,
                  sex,
                  client_mobile,
                  client_email,
                  uic,
                  philhealth_no
               )
            ) %>%
            arrange(Branch, disp_date) %>%
            distinct(row_id, .keep_all = TRUE) %>%
            left_join(
               read_sheet("1c334aEKFTOl3Cg9Uji7tq1rZjlPM8RKpdXwQgOGyAIg", "facility_id", col_types = "c")
                  %>% rename(
                  Branch      = SITE,
                  faci_id     = FACI_ID,
                  sub_faci_id = SUB_FACI_ID
               ),
               join_by(Branch)
            ) %>%
            mutate(
               sex              = case_when(
                  sex == "MALE" ~ "1",
                  sex == "FEMALE" ~ "2",
               ),
               age              = calc_age(birthdate, disp_date),
               self_ident_other = if_else(!(self_ident %in% c("MAN", "WOMAN", "MALE", "FEMALE")), self_ident, NA_character_),
               self_ident       = case_when(
                  self_ident == "MALE" ~ "1",
                  self_ident == "MAN" ~ "1",
                  self_ident == "FEMALE" ~ "2",
                  self_ident == "WOMAN" ~ "2",
                  self_ident == "Q/NB/NC" ~ "3",
                  self_ident == "TRANSWOMAN" ~ "3",
                  self_ident == "TRANSMAN" ~ "3",
                  self_ident == "OTHER" ~ "3",
                  !is.na(self_ident_other) ~ "3",
                  TRUE ~ self_ident
               ),
               central_id       = na_if(central_id, '20250523130000167P')
            )

         invisible(self)
      },
      checkIssues       = function() {
         self$issues <- list(
            `categorical`    = self$data$converted %>%
               categorical_values(c(
                  "sex",
                  "self_ident",
                  "visit_type",
                  "tb_screen",
                  "tb_ipt_status",
                  "tx_status",
                  "client_type",
                  "final_arv",
                  "tb_status"
               )),
            `tx-not2025`     = self$data$converted %>% filter(disp_date < "2025-01-01" | disp_date > now()),
            `tx-no_name`     = self$data$converted %>% filter(is.na(first), is.na(last)),
            `tx-no_arv`      = self$data$converted %>% filter(is.na(final_arv)),
            `tx-no_dispense` = self$data$converted %>% filter(coalesce(parse_number(disp_total), 0) <= 0),
            `tx-not_in_txdb` = self$data$converted %>% filter(is.na(file)),
            `tx-no_bdate`    = self$data$converted %>% filter(is.na(birthdate)),
            `tx-more6mos`    = self$data$converted %>% filter(parse_number(disp_total) > 180),
            `tx-no_cid`      = self$data$converted %>%
               filter(is.na(central_id)) %>%
               filter(if_any(c(patient_code, confirmatory_code, last, first, middle, suffix, uic, birthdate, philhealth_no, sex, client_mobile, client_email), ~!is.na(.))) %>%
               distinct(
                  central_id,
                  confirmatory_code,
                  patient_code,
                  uic,
                  birthdate,
                  sex,
                  first,
                  middle,
                  last,
                  suffix,
                  philhealth_no,
                  client_mobile,
                  client_email,
                  curr_addr
               ),
            `tx-no_addr`     = self$data$artDb %>%
               filter(is.na(curr_reg)) %>%
               distinct(NAME_REG = CURR_NAME_REG, NAME_PROV = CURR_NAME_PROV, NAME_MUNC = CURR_NAME_MUNC)
         )

         invisible(self)
      },
      addNewPatients    = function() {
         max_id <- max(self$data$ids$row_id)
         new    <- self$data$converted %>%
            filter(is.na(central_id)) %>%
            filter(if_any(c(patient_code, confirmatory_code, last, first, middle, suffix, uic, birthdate, philhealth_no, sex, client_mobile, client_email), ~!is.na(.))) %>%
            # select(
            #    -curr_reg,
            #    -curr_prov,
            #    -curr_munc
            # ) %>%
            left_join(read_sheet("1c334aEKFTOl3Cg9Uji7tq1rZjlPM8RKpdXwQgOGyAIg", "facility_id", col_types = "c") %>% rename(Branch = SITE), join_by(Branch)) %>%
            # left_join(read_sheet("1c334aEKFTOl3Cg9Uji7tq1rZjlPM8RKpdXwQgOGyAIg", "addr", col_types = "c") %>% rename(curr_addr = addr), join_by(curr_addr)) %>%
            # left_join(
            #    y  = ohasis$ref_addr %>%
            #       select(
            #          psgc,
            #          curr_reg  = reg,
            #          curr_prov = prov,
            #          curr_munc = munc,
            #          curr_brgy = brgy,
            #       ),
            #    by = join_by(psgc)
            # ) %>%
            distinct(
               faci_id     = FACI_ID,
               sub_faci_id = SUB_FACI_ID,
               patient_code,
               confirmatory_code,
               last,
               first,
               middle,
               suffix,
               uic,
               birthdate,
               philhealth_no,
               sex,
               client_mobile,
               client_email,
               curr_reg,
               curr_prov,
               curr_munc,
               # curr_brgy,
               curr_addr,
               self_ident
            ) %>%
            mutate(
               row_id = max_id + row_number(),
               drop   = is.na(uic) & is.na(birthdate) & is.na(first)
            ) %>%
            filter(!drop) %>%
            select(-drop)

         created <- oh_batch_newpx(new, "row_id")

         con <- connect('old-lw')
         dbxUpsert(
            con,
            Id(schema = "ohasis_lake", table = "ly_clients"),
            created %>%
               rename(client_code = patient_code) %>%
               rename(central_id = patient_id) %>%
               select(-curr_addr, -curr_reg, -curr_prov, -curr_munc, -curr_brgy, -self_ident, -faci_id, -sub_faci_id, -philsys_id, -created_at, -created_by) %>%
               rename_all(toupper) %>%
               rename(row_id = ROW_ID) %>%
               select(any_of(names(self$data$ids))),
            "row_id"
         )
         dbDisconnect(con)

         invisible(self)
      },
      getExisting       = function() {
         self$data$idreg <- update_idreg()

         lw_conn            <- connect('mariadb-lw')
         ids                <- (ohasis$ref_faci %>% filter(str_detect(faci_code, 'TLY')))$faci_id
         self$data$existing <- QB$new(lw_conn)$
            from('ohasis_warehouse.form_art_bc as art')$
            select("art.rec_id", "art.record_date as visit_date", "art.medicine_summary", "art.created_by", "art.created_at", "art.patient_id")$
            whereIn("art.faci_id", ids, boolean = 'or')$
            whereIn("art.service_faci", ids, boolean = 'or')$
            # whereBetween("art.record_date", c("2025-01-01", format(Sys.time(), "%Y-%m-%d")))$
            get()

         self$data$existing %<>%
            mutate_if(
               .predicate = is.POSIXct,
               ~as.Date(.)
            ) %>%
            get_cid(self$data$idreg, patient_id)

         dbDisconnect(lw_conn)

         invisible(self)
      },
      prepareUpload     = function() {
         local_gs4_quiet()

         timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

         for_import <- self$data$converted %>%
            filter(disp_date >= as.Date("2025-01-01")) %>%
            filter(disp_date <= now()) %>%
            mutate(
               disp_date    = as.Date(disp_date),
               service_type = '101201',
               disease      = '101000',
               module       = '3',
            ) %>%
            rename(
               visit_date       = disp_date,
               clinic_notes     = remarks,
               latest_next_date = next_date,
               medicine_summary = final_arv,
               patient_id       = central_id
            ) %>%
            get_cid(self$data$idreg, patient_id) %>%
            mutate(
               record_date      = visit_date,
               form_version     = "ART Form (v2021)",
               service_faci     = faci_id,
               service_sub_faci = sub_faci_id,
            ) %>%
            # get records id if existing
            left_join(
               y  = self$data$existing %>%
                  select(
                     rec_id,
                     created_by,
                     created_at,
                     central_id,
                     visit_date
                  ),
               by = join_by(central_id, visit_date)
            ) %>%
            mutate(birthdate = as.Date(birthdate)) %>%
            # retain only not uploaded and those with changes
            filter(!is.na(patient_id), !is.na(medicine_summary)) %>%
            anti_join(
               y  = self$data$existing,
               by = join_by(central_id, visit_date, medicine_summary),
               # by = join_by(rec_id, visit_date, medicine_summary),
            ) %>%
            mutate(
               old_rec    = if_else(!is.na(rec_id), 1, 0, 0),
               created_by = coalesce(created_by, "1300000048"),
               created_at = coalesce(as.character(created_at), timestamp),
               updated_by = if_else(old_rec == 1, "1300000048", NA_character_),
               updated_at = if_else(old_rec == 1, timestamp, NA_character_)
            ) %>%
            relocate(any_of(names(self$data$existing)), .before = 1) %>%
            select(-old_rec) %>%
            distinct(row_id, .keep_all = TRUE)

         final_import <- for_import %>%
            filter(!is.na(rec_id)) %>%
            bind_rows(
               batch_rec_ids(for_import %>% filter(is.na(rec_id)), rec_id, created_by, "row_id")
            )

         final_import %<>%
            mutate(
               updated_by = "1300000048",
               updated_at = timestamp
            ) %>%
            left_join(
               y  = self$data$existing %>%
                  select(rec_id, corr_pid = patient_id),
               by = join_by(rec_id)
            ) %>%
            mutate(
               patient_id = coalesce(corr_pid, patient_id)
            )

         self$data$forUpload <- final_import

         invisible(self)
      },
      deconstructTables = function() {
         # tables           <- list()
         # tables$px_record <- list(
         #    name = "px_record",
         #    pk   = c("rec_id", "patient_id"),
         #    data = self$data$forUpload %>%
         #       mutate(
         #          faci_id     = "130001",
         #          sub_faci_id = NA_character_,
         #          disease     = "101000",
         #          module      = 3
         #       ) %>%
         #       select(
         #          rec_id,
         #          patient_id,
         #          faci_id,
         #          sub_faci_id,
         #          record_date,
         #          disease,
         #          module,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       )
         # )
         #
         # tables$px_info <- list(
         #    name = "px_info",
         #    pk   = c("rec_id", "patient_id"),
         #    data = self$data$forUpload %>%
         #       select(
         #          rec_id,
         #          patient_id,
         #          confirmatory_code,
         #          uic,
         #          patient_code,
         #          sex,
         #          birthdate,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       )
         # )
         #
         # tables$px_name <- list(
         #    name = "px_name",
         #    pk   = c("rec_id", "patient_id"),
         #    data = self$data$forUpload %>%
         #       select(
         #          rec_id,
         #          patient_id,
         #          first,
         #          middle,
         #          last,
         #          suffix,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       )
         # )
         #
         # tables$px_contact <- list(
         #    name = "px_contact",
         #    pk   = c("rec_id", "contact_type"),
         #    data = self$data$forUpload %>%
         #       select(
         #          rec_id,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #          client_mobile,
         #          client_email
         #       ) %>%
         #       pivot_longer(
         #          cols      = c(client_mobile, client_email),
         #          names_to  = "contact_type",
         #          values_to = "contact"
         #       ) %>%
         #       mutate(
         #          contact_type = case_when(
         #             contact_type == "client_mobile" ~ "1",
         #             contact_type == "client_email" ~ "2",
         #             TRUE ~ contact_type
         #          )
         #       )
         # )
         #
         # tables$px_faci <- list(
         #    name = "px_faci",
         #    pk   = c("rec_id", "service_type"),
         #    data = self$data$forUpload %>%
         #       mutate(
         #          service_type = "101201",
         #          service_faci = as.character(service_faci),
         #       ) %>%
         #       select(
         #          rec_id,
         #          faci_id     = service_faci,
         #          sub_faci_id = service_sub_faci,
         #          service_type,
         #          visit_type,
         #          client_type,
         #          tx_status,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       ) %>%
         #       mutate_at(
         #          .vars = vars(visit_type, client_type, tx_status),
         #          ~keep_code(.)
         #       )
         # )
         #
         # tables$px_form <- list(
         #    name = "px_form",
         #    pk   = c("rec_id", "form"),
         #    data = self$data$forUpload %>%
         #       mutate(
         #          form    = "ART Form",
         #          version = "2021"
         #       ) %>%
         #       select(
         #          rec_id,
         #          form,
         #          version,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       )
         # )
         #
         # tables$px_profile <- list(
         #    name = "px_profile",
         #    pk   = "rec_id",
         #    data = self$data$forUpload %>%
         #       select(
         #          rec_id,
         #          age,
         #          self_ident,
         #          self_ident_other,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       ) %>%
         #       mutate_at(
         #          .vars = vars(self_ident),
         #          ~keep_code(.)
         #       )
         # )
         #
         # tables$px_tb <- list(
         #    name = "px_tb",
         #    pk   = "rec_id",
         #    data = self$data$forUpload %>%
         #       mutate(
         #          tb_status = case_when(
         #             !is.na(tb_ipt_status) ~ "0_No active TB",
         #             TRUE ~ tb_status
         #          )
         #       ) %>%
         #       select(
         #          rec_id,
         #          tb_screen,
         #          tb_status,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       ) %>%
         #       mutate_at(
         #          .vars = vars(tb_screen, tb_status),
         #          ~keep_code(.)
         #       )
         # )
         #
         # tables$px_tb_ipt <- list(
         #    name = "px_tb_ipt",
         #    pk   = "rec_id",
         #    data = self$data$forUpload %>%
         #       mutate(
         #          tb_status = case_when(
         #             !is.na(tb_ipt_status) ~ "0_No active TB",
         #             TRUE ~ tb_status
         #          )
         #       ) %>%
         #       select(
         #          rec_id,
         #          tb_ipt_status,
         #          created_by,
         #          created_at,
         #          updated_by,
         #          updated_at,
         #       ) %>%
         #       mutate_at(
         #          .vars = vars(tb_ipt_status),
         #          ~keep_code(.)
         #       )
         # )
         #
         # tables$px_medicine <- list(
         #    name = "px_medicine",
         #    pk   = c("rec_id", "medicine", "disp_num"),
         #    data = self$data$forUpload %>%
         #       separate_longer_delim(
         #          cols  = medicine_summary,
         #          delim = "+"
         #       ) %>%
         #       mutate(
         #          service_faci = as.character(service_faci),
         #          unit_basis   = "2",
         #          medicine     = case_when(
         #             medicine_summary == "tdf/3tc/dtg" ~ "2029",
         #             medicine_summary == "tdf/3tc/efv" ~ "2015",
         #             medicine_summary == "azt/3tc" ~ "2018",
         #             medicine_summary == "lpv/r" ~ "2009",
         #             medicine_summary == "dtg" ~ "2035",
         #             medicine_summary == "efv" ~ "2004",
         #             medicine_summary == "3tc" ~ "2023",
         #             medicine_summary == "abc" ~ "2002",
         #             medicine_summary == "ftc" ~ "2031",
         #             medicine_summary == "nvp" ~ "2011",
         #             medicine_summary == "ril" ~ "2014",
         #             medicine_summary == "tdf" ~ "2017",
         #             medicine_summary == "tdf/3tc" ~ "2016",
         #          ),
         #       ) %>%
         #       filter(!is.na(medicine)) %>%
         #       group_by(row_id) %>%
         #       mutate(
         #          disp_num = row_number(),
         #          # disp_total = typical_per_day * disp_total
         #       ) %>%
         #       ungroup() %>%
         #       select(
         #          rec_id,
         #          faci_id     = service_faci,
         #          sub_faci_id = service_sub_faci,
         #          medicine,
         #          disp_num,
         #          unit_basis,
         #          per_day,
         #          disp_total,
         #          medicine_left,
         #          medicine_missed,
         #          disp_date   = visit_date,
         #          next_date   = latest_next_date,
         #       )
         # )

         self$tables <- deconstruct_art(self$data$forUpload)

         idreg <- update_idreg()
         # self$tables$patients$data %<>%
         #    get_cid(idreg, patient_id) %>%
         #    mutate(birthdate = as.Date(birthdate)) %>%
         #    get_latest_pii(
         #       "central_id",
         #       c(
         #          'confirmatory_code',
         #          'patient_code',
         #          'uic',
         #          'philhealth_no',
         #          'philsys_id',
         #          'first',
         #          'middle',
         #          'last',
         #          'suffix',
         #          'birthdate',
         #          'sex',
         #          'self_ident',
         #          'self_ident_other',
         #          'client_email',
         #          'client_mobile',
         #          'nationality',
         #          'civil_status',
         #          'educ_level',
         #          'curr_reg',
         #          'curr_prov',
         #          'curr_munc',
         #          'curr_brgy',
         #          'perm_reg',
         #          'perm_prov',
         #          'perm_munc',
         #          'perm_brgy',
         #          'birth_reg',
         #          'birth_prov',
         #          'birth_munc',
         #          'birth_brgy'
         #       )
         #    ) %>%
         #    select(-central_id) %>%
         #    mutate_at(
         #       .vars = vars(civil_status, educ_level, sex, self_ident),
         #       keep_code
         #    )

         for (table in names(self$tables)) {
            value_cols <- names(self$tables[[table]]$data)
            value_cols <- setdiff(value_cols, self$tables[[table]]$pk)
            value_cols <- setdiff(value_cols, c('created_at', 'created_by', 'updated_at', 'updated_by', 'deleted_at', 'deleted_by', 'faci_id', 'sub_faci_id'))

            self$tables[[table]]$data %<>%
               filter(if_any(all_of(value_cols), ~!is.na(.)))
         }

         invisible(self)
      },
      upload            = function() {
         db_conn <- ohasis$conn("db")
         dbxDelete(
            db_conn,
            Id(schema = "ohasis", table = "px_medicine"),
            self$data$forUpload %>% select(rec_id),
            batch_size = 1000
         )
         lapply(self$tables, function(ref, db_conn) {
            log_info("Uploading {green(ref$name)}.")
            table_space <- Id(schema = "ohasis", table = ref$name)
            dbxUpsert(db_conn, table_space, ref$data, ref$pk)
         }, db_conn)
         dbDisconnect(db_conn)

         invisible(self)
      }
   )
)
