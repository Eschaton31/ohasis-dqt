LyPrep <- R6Class(
   "LyPrep",
   public = list(
      root              = "",
      data              = list(
         prep      = tibble(),
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
      downloadPrep      = function() {
         local_drive_quiet()
         local_gs4_quiet()

         ss     <- "1smORFFrPwFFrbXQuUUqxNnxyD9VInEPFL7XgL-dmvUM"
         sheets <- range_speedread(ss, "prep", col_types = cols(.default = "c"))

         # ! PrEP
         dir <- file.path(self$root, "prep")
         check_dir(dir)

         for (i in seq_len(nrow(sheets))) {
            branch <- sheets[i,]$branch
            link   <- sheets[i,]$link
            file   <- file.path(dir, stri_c(branch, ".ods"))
            log_info("Downloading PrEP = {green(branch)}.")
            drive_download(link, file, overwrite = TRUE)
         }

         invisible(self)
      },
      readIds           = function() {

         con           <- connect("old-lw")
         self$data$ids <- QB$new(con)$from("ohasis_lake.ly_clients")$get()
         dbDisconnect(con)

         invisible(self)
      },
      readPrep          = function() {
         log_info("Reading files.")
         files       <- list.files(file.path(self$root, "prep"), full.names = TRUE)
         data        <- pblapply(files, self$readSheets)
         names(data) <- tools::file_path_sans_ext(basename(files))

         log_info("Combining {red('Initial')}.")
         initial        <- lapply(data, purrr::keep, ~ncol(.) >= 40 & ncol(.) <= 50)
         names(initial) <- tools::file_path_sans_ext(basename(files))

         initial <- lapply(initial, lapply, self$renameInitial)
         initial <- lapply(initial, bind_rows, .id = "Sheet")
         initial <- bind_rows(initial, .id = "src") %>% filter(!if_all(c(record_date, patient_code, uic), is.na))

         log_info("Combining {red('Refills')}.")
         refills        <- lapply(data, purrr::keep, ~ncol(.) >= 35 & ncol(.) <= 39)
         names(refills) <- tools::file_path_sans_ext(basename(files))

         refills <- lapply(refills, lapply, self$renameRefill)
         refills <- lapply(refills, bind_rows, .id = "Sheet")
         refills <- bind_rows(refills, .id = "src") %>% filter(!if_all(c(record_date, patient_code), is.na))

         # cols <- lapply(initial, lapply, names)
         # cols <- lapply(cols, lapply, as_tibble)
         # cols <- lapply(cols, lapply, mutate, col = row_number())
         # cols <- lapply(cols, bind_rows, .id = "Sheet")
         # cols <- bind_rows(cols, .id = "src")

         # cols %>%
         #    pivot_wider(
         #       id_cols     = c(src, Sheet),
         #       names_from  = col,
         #       values_from = value
         #    )


         log_info("Consolidating forms.")
         self$data$prep <- initial %>%
            mutate(
               form_id = 'prepScreen2020',
               .before = 1
            ) %>%
            bind_rows(
               refills %>%
                  mutate(
                     form_id = 'prepFollowup2020',
                     .before = 1
                  )
            ) %>%
            mutate(row_id = row_number()) %>%
            mutate(branch = if_else(src %in% c("ANGLO-1", "ANGLO-2"), "ANGLO", src, src)) %>%
            mutate_at(
               .vars = vars(contains("date")),
               ~as.Date(parse_date_time(., c("mdY", "Ymd")))
            ) %>%
            mutate(
               birthdate = if_else(nchar(uic) == 14, stri_c(sep = "/", substr(uic, 7, 8), substr(uic, 9, 10), str_right(uic, 4)), NA_character_, NA_character_),
               birthdate = as.Date(parse_date_time(birthdate, "mdY")),
               .after    = uic
            )

         invisible(self)
      },
      convert           = function() {
         self$data$converted <- self$data$prep %>%
            left_join(
               y  = self$data$ids %>%
                  rename_all(tolower) %>%
                  filter(!is.na(central_id)) %>%
                  select(-row_id) %>%
                  rename(patient_code = client_code) %>%
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
                     .keep_all = TRUE
                  ) %>%
                  select(
                     central_id,
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
               )
            ) %>%
            left_join(
               y  = self$data$ids %>%
                  rename_all(tolower) %>%
                  filter(!is.na(central_id)) %>%
                  select(-row_id) %>%
                  rename(patient_code = client_code) %>%
                  distinct(
                     patient_code,
                     client_mobile,
                     client_email,
                     .keep_all = TRUE
                  ) %>%
                  select(
                     central_id,
                     patient_code,
                     client_mobile,
                     client_email,
                  ),
               by = join_by(
                  patient_code,
                  client_mobile,
                  client_email,
               )
            ) %>%
            arrange(branch, record_date) %>%
            distinct(row_id, .keep_all = TRUE) %>%
            mutate(
               central_id = coalesce(central_id.x, central_id.y)
            ) %>%
            select(-central_id.x, -central_id.y)

         with_ids <- self$data$converted %>%
            filter(!is.na(central_id)) %>%
            filter(!is.na(patient_code)) %>%
            select(src, sheet_cid = central_id, patient_code) %>%
            distinct(src, patient_code, .keep_all = TRUE)

         self$data$converted %<>%
            left_join(
               y  = with_ids,
               by = join_by(src, patient_code)
            ) %>%
            mutate(
               central_id = coalesce(central_id, sheet_cid)
            )

         self$data$converted %<>%
            left_join(
               read_sheet("1c334aEKFTOl3Cg9Uji7tq1rZjlPM8RKpdXwQgOGyAIg", "facility_id", col_types = "c")
                  %>% rename(
                  branch      = SITE,
                  faci_id     = FACI_ID,
                  sub_faci_id = SUB_FACI_ID
               ),
               join_by(branch)
            ) %>%
            mutate(
               sex                       = case_when(
                  sex == 'MALE' ~ '1',
                  sex == 'MAN' ~ '1',
                  sex == 'FEMALE' ~ '2',
                  TRUE ~ sex
               ),
               self_ident_other          = case_when(
                  self_ident == 'OTHERS' ~ self_ident,
                  self_ident == 'MAN, OTHERS' ~ self_ident,
                  self_ident == 'WOMAN, OTHERS' ~ self_ident,
                  self_ident == 'OTHERS (NON-BINARY, QUEER, NON-CONFORMING, ETC.)' ~ self_ident,
                  self_ident == 'MAN, WOMAN' ~ self_ident,
                  self_ident == 'TGW' ~ self_ident,
                  TRUE ~ NA_character_
               ),
               self_ident                = case_when(
                  self_ident == 'MAN' ~ '1',
                  self_ident == 'WOMAN' ~ '2',
                  self_ident == 'OTHERS' ~ '3',
                  !is.na(self_ident_other) ~ '3',
                  TRUE ~ self_ident
               ),

               lab_crea_result           = str_squish(stri_replace_all_fixed(lab_crea_result, "ML/MIN", "")),
               lab_crea_result           = str_squish(stri_replace_all_fixed(lab_crea_result, "MLMIN", "")),
               lab_crea_result           = str_squish(stri_replace_all_fixed(lab_crea_result, "UMOL/L", "")),
               lab_crea_result           = str_squish(stri_replace_all_fixed(lab_crea_result, "UMOL/", "")),
               lab_crea_result           = str_squish(stri_replace_all_fixed(lab_crea_result, "MG/DL", "")),
               lab_crea_result           = str_squish(stri_replace_all_fixed(lab_crea_result, "MGDL", "")),
               lab_crea_result           = str_squish(stri_replace_all_fixed(lab_crea_result, "---", "")),

               # arv_regimen   = coalesce(arv_disp, arv_regimen, regimen),
               weight                    = str_squish(stri_replace_all_fixed(weight, "KG", "")),
               weight                    = str_squish(stri_replace_all_fixed(weight, "KILOS", "")),
               age                       = calc_age(birthdate, disp_date),

               week_avg_sex              = case_when(
                  week_avg_sex == 'ONE OR LESS A WEEK' ~ '1',
                  week_avg_sex == 'ONE OR LESS SEX A WEEK' ~ '1',
                  week_avg_sex == 'ONE OR LESS SEX ACTS A WEEK' ~ '1',
                  week_avg_sex == 'TWO OR MORE A WEEK' ~ '2',
                  week_avg_sex == 'TWO OR MORE SEX ACTS A WEEK' ~ '2',
                  week_avg_sex == 'YES' ~ '1',
                  week_avg_sex == 'NO' ~ NA_character_,
                  TRUE ~ week_avg_sex
               ),
               prep_plan                 = case_when(
                  prep_plan == 'CLINIC SUPPORTED' ~ '1',
                  prep_plan == 'OFFER CLIENT-SUPPORTED PREP' ~ '2',
                  prep_plan == 'OFFER CLINIC SUPPORT PREP' ~ '1',
                  prep_plan == 'OFFER CLINIC-SUPPORTED PREP' ~ '1',
                  prep_plan == 'OFFER COST SHARED PREP' ~ '3',
                  TRUE ~ prep_plan
               ),
               disp_total                = str_replace_all(disp_total, "[^[:alnum:]]", ""),
               medicine_left             = case_when(
                  medicine_left == '`9' ~ '9',
                  medicine_left == 'O' ~ '0',
                  medicine_left == 'LESS THAN 5 PILLS' ~ '4',
                  medicine_left == 'LESS THAN 5' ~ '4',
                  medicine_left == '>30' ~ '31',
                  medicine_left == '6-10 PILLS' ~ '9',
                  medicine_left == '15+ PILLS' ~ '16',
                  medicine_left == '10-15 PILLS' ~ '14',
                  TRUE ~ medicine_left
               ),

               disp_total                = parse_number(disp_total),
               medicine_left             = parse_number(medicine_left),
               medicine_summary          = if_else(!is.na(disp_total), 'TDF/FTC', NA_character_),

               kp_pdl                    = case_when(
                  str_detect(key_population, "PDL") ~ "1",
                  str_detect(key_population, "PERSON DEPRIVED OF LIBERTY") ~ "1",
                  str_detect(key_population, "PERSONS DEPRIVED OF LIBERTY") ~ "1",
                  str_detect(key_population_other, "PDL") ~ "1",
                  str_detect(key_population_other, "PERSON DEPRIVED OF LIBERTY") ~ "1",
                  str_detect(key_population_other, "PERSONS DEPRIVED OF LIBERTY") ~ "1",
                  TRUE ~ "0"
               ),
               kp_sw                     = case_when(
                  str_detect(key_population, "SW") ~ "1",
                  str_detect(key_population, "SEX WORKER") ~ "1",
                  str_detect(key_population, "SEXWORKER") ~ "1",
                  str_detect(key_population_other, "SW") ~ "1",
                  str_detect(key_population_other, "SEX WORKER") ~ "1",
                  str_detect(key_population_other, "SEXWORKER") ~ "1",
                  TRUE ~ "0"
               ),
               kp_tg                     = case_when(
                  str_detect(key_population, "TG") ~ "1",
                  str_detect(key_population, "TP") ~ "1",
                  str_detect(key_population, "TRANSGE") ~ "1",
                  str_detect(key_population_other, "TG") ~ "1",
                  str_detect(key_population_other, "TP") ~ "1",
                  str_detect(key_population_other, "TRANSGE") ~ "1",
                  TRUE ~ "0"
               ),
               kp_pwid                   = case_when(
                  str_detect(key_population, "PWID") ~ "1",
                  str_detect(key_population, "PEOPLE WHO INJECT DRUG") ~ "1",
                  str_detect(key_population, "PEOPLE WHO INJECT DRUGS") ~ "1",
                  str_detect(key_population_other, "PWID") ~ "1",
                  str_detect(key_population_other, "PEOPLE WHO INJECT DRUG") ~ "1",
                  str_detect(key_population, "PEOPLE WHO INJECT DRUGS") ~ "1",
                  TRUE ~ "0"
               ),
               kp_msm                    = case_when(
                  str_detect(key_population, "MSM") ~ "1",
                  str_detect(key_population, "MEN HAVING SEX WITH MEN") ~ "1",
                  str_detect(key_population_other, "MSM") ~ "1",
                  str_detect(key_population, "MEN HAVING SEX WITH MEN") ~ "1",
                  TRUE ~ "0"
               ),
               kp_ofw                    = case_when(
                  str_detect(key_population, "OFW") ~ "1",
                  str_detect(key_population_other, "OFW") ~ "1",
                  TRUE ~ "0"
               ),
               kp_partner                = case_when(
                  str_detect(key_population, "PARTNER") ~ "1",
                  str_detect(key_population_other, "PARTNER") ~ "1",
                  TRUE ~ "0"
               ),
               kp_other                  = coalesce(key_population, key_population_other),

               ars_sx_fever              = if_else(str_detect(ars_sx, "FEVER"), '1', '0', '0'),
               ars_sx_sore_throat        = if_else(str_detect(ars_sx, "SORE THROAT"), '1', '0', '0'),
               ars_sx_diarrhea           = if_else(str_detect(ars_sx, "DIARRHEA"), '1', '0', '0'),
               ars_sx_swollen_lymph      = if_else(str_detect(ars_sx, "SWOLLEN LYMPH GLANDS"), '1', '0', '0'),
               ars_sx_swollen_tonsils    = if_else(str_detect(ars_sx, "SWOLLEN TONSILS"), '1', '0', '0'),
               ars_sx_rash               = if_else(str_detect(ars_sx, "RASH"), '1', '0', '0'),
               ars_sx_muscle_pains       = if_else(str_detect(ars_sx, "JOINT AND MUSCLE PAIN"), '1', '0', '0'),
               ars_sx_other              = case_when(
                  !is.na(ars_sx) ~ "1",
                  TRUE ~ "0"
               ),
               ars_sx_other_text         = ars_sx,
               ars_sx_none               = NA_character_,

               sti_sx_pain_urine         = if_else(str_detect(sti_sx, "PAINFUL URINATION"), '1', '0', '0'),
               sti_sx_discharge_urethral = if_else(str_detect(sti_sx, "URETHRAL DISCHARGE"), '1', '0', '0'),
               sti_sx_warts_genital      = if_else(str_detect(sti_sx, "GENETAL WARTS"), '1', '0', '0'),
               sti_sx_ulcer_genital      = if_else(str_detect(sti_sx, "GENETAL ULCER"), '1', '0', '0'),
               sti_sx_ulcer_oral         = if_else(str_detect(sti_sx, "ORAL ULCER"), '1', '0', '0'),
               sti_sx_pain_abdomen       = if_else(str_detect(sti_sx, "LOWER ABDOMINAL PAIN"), '1', '0', '0'),
               sti_sx_discharge_anal     = if_else(str_detect(sti_sx, "ANAL DISCHARGE"), '1', '0', '0'),
               sti_sx_discharge_vaginal  = if_else(str_detect(sti_sx, "VAGINAL DISCHARGE"), '1', '0', '0'),
               sti_sx_swollen_scrotum    = if_else(str_detect(sti_sx, "SCROTAL SWELLING"), '1', '0', '0'),
               sti_sx_other              = case_when(
                  !is.na(sti_sx) ~ "1",
                  TRUE ~ "0"
               ),
               sti_sx_other_text         = sti_sx,
               sti_sx_none               = NA_character_,

               prep_side_effects         = if_else(!is.na(prep_side_effects_specify), '1', '0', '0')
            ) %>%
            mutate_at(
               .vars = vars(starts_with("risk_")),
               ~case_when(
                  . == "PAST 30 DAYS" ~ "4_Yes, within the past 30 days",
                  . == "YES, PAST 30 DAYS" ~ "4_Yes, within the past 30 days",
                  . == "YES, (WITHIN THE LAST 30 DAYS)" ~ "4_Yes, within the past 30 days",
                  . == "YES, PAST 60 DAYS" ~ "3_Yes, within the past 6 months",
                  . == "PAST 60 DAYS" ~ "3_Yes, within the past 6 months",
                  . == "YES, PAST 6 MONTHS" ~ "3_Yes, within the past 6 months",
                  . == "PAST 6 MONTHS" ~ "3_Yes, within the past 6 months",
                  . == "YES, NO" ~ "2_Yes",
                  . == "YES" ~ "2_Yes",
                  . == "NO" ~ "0_No",
                  TRUE ~ .
               )
            ) %>%
            mutate_at(
               .vars = vars(starts_with("pre_init"), eligible_behavior, prep_requested, first_time, prep_shift, prep_missed),
               ~case_when(
                  . == "YES" ~ "1",
                  . == "NO" ~ "0",
                  . == "EVENT DRIVEN" ~ "1",
                  TRUE ~ .
               )
            ) %>%
            mutate_at(
               .vars = vars(prep_type, prep_type_last_visit),
               ~case_when(
                  . == "DAILY" ~ "1",
                  . == "EVENT DRIVEN" ~ "2",
                  . == "EVENT-DRIVEN" ~ "2",
                  TRUE ~ .
               )
            )

         invisible(self)
      },
      checkIssues       = function() {
         self$issues <- list(
            `categorical`         = self$data$converted %>%
               categorical_values(c(
                  'sex',
                  'self_ident',
                  'key_population',
                  'key_population_other',
                  'risk_condomless_anal',
                  'risk_condomless_vaginal',
                  'risk_drug_inject',
                  'risk_drug_sex',
                  'risk_transact_sex',
                  'risk_hiv_vl_unknown',
                  'risk_hiv_unknown',
                  'risk_sti',
                  'risk_pep',
                  'lab_hbsag_result',
                  'lab_syph_result',
                  'ars_sx',
                  'sti_sx',
                  'pre_init_hiv_nr',
                  'pre_init_weight',
                  'pre_init_no_arv_allergy',
                  'pre_init_no_ars',
                  'pre_init_crea_clear',
                  'eligible_behavior',
                  'prep_requested',
                  'prep_plan',
                  'first_time',
                  'prep_type',
                  'disp_total',
                  'week_avg_sex',
                  'prep_type_last_visit',
                  'prep_shift',
                  'prep_missed',
                  'prep_side_effects_specify',
                  'medicine_left'
               )),
            `prep-not2025`        = self$data$converted %>%
               filter(disp_date < "2023-01-01" |
                         disp_date > now() |
                         record_date < "2023-01-01" |
                         record_date > now()) %>%
               mutate(
                  disp_cell = str_mid(sheet_row, 2, 1000),
                  disp_cell = case_when(
                     str_detect(toupper(Sheet), "INITIAL") ~ stri_c("AP", disp_cell),
                     str_detect(toupper(Sheet), "REFILL") ~ stri_c("AF", disp_cell),
                  ),
                  .before   = 1
               ) %>%
               arrange(src, Sheet, disp_cell),
            `prep-mimsatch_dates` = self$data$converted %>%
               filter(abs(interval(record_date, disp_date) / days(1)) > 7) %>%
               mutate(
                  disp_cell = str_mid(sheet_row, 2, 1000),
                  disp_cell = case_when(
                     str_detect(toupper(Sheet), "INITIAL") ~ stri_c("AP", disp_cell),
                     str_detect(toupper(Sheet), "REFILL") ~ stri_c("AF", disp_cell),
                  ),
                  .before   = 1
               ) %>%
               arrange(src, Sheet, disp_cell),
            `prep-more6mos`       = self$data$converted %>% filter(disp_total > 180),
            `prep-no_cid`         = self$data$converted %>%
               filter(is.na(central_id)) %>%
               distinct(
                  central_id,
                  patient_code,
                  uic,
                  birthdate,
                  sex,
                  first,
                  middle,
                  last,
                  suffix,
                  client_mobile,
                  client_email
               )
         )

         invisible(self)
      },
      addNewPatients    = function() {
         max_id <- max(self$data$ids$row_id)
         new    <- self$data$converted %>%
            filter(is.na(central_id)) %>%
            mutate(
               confirmatory_code = NA_character_,
               philhealth_no     = NA_character_,
            ) %>%
            distinct(
               faci_id,
               sub_faci_id,
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
               self_ident,
               self_ident_other
            ) %>%
            # mutate(
            #    drop = is.na(uic) & is.na(birthdate) & is.na(first)
            # ) %>%
            # filter(!drop) %>%
            # select(-drop) %>%
            mutate(
               row_id  = max_id + row_number(),
               .before = 1
            )

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
               select(any_of(names(self$data$ids))) %>%
               mutate(
                  SEX = case_when(
                     SEX == "1" ~ "MALE",
                     SEX == "2" ~ "FEMALE",
                  )
               ),
            "row_id"
         )
         dbDisconnect(con)

         invisible(self)
      },
      getExisting       = function() {
         self$data$idreg <- update_idreg()

         lw_conn            <- connect('mariadb-lw')
         self$data$existing <- QB$new(lw_conn)$
            from('ohasis_warehouse.form_prep as prep')$
            select("prep.rec_id", "prep.record_date", "prep.medicine_summary", "prep.created_by", "prep.created_at", "prep.patient_id")$
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
            filter(record_date <= now()) %>%
            mutate(
               service_type  = '101301',
               disease       = '101000',
               module        = '6',
               prep_visit    = case_when(
                  form_id == "prepScreen2020" ~ "1",
                  form_id == "prepFollowup2020" ~ "2",
               ),
               prep_accepted = if_else(!is.na(disp_total), '1', '0', '0')
            ) %>%
            rename(
               patient_id   = central_id,
               side_effects = prep_side_effects_specify
            ) %>%
            get_cid(self$data$idreg, patient_id) %>%
            mutate(
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
                     record_date
                  ),
               by = join_by(central_id, record_date)
            ) %>%
            mutate(birthdate = as.Date(birthdate)) %>%
            # retain only not uploaded and those with changes
            filter(!is.na(patient_id)) %>%
            anti_join(
               y  = self$data$existing %>% filter(created_by != '1300000048'),
               by = join_by(rec_id, record_date, medicine_summary),
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

         self$data$forUpload <- final_import %>%
            distinct(rec_id, .keep_all = TRUE)

         invisible(self)
      },
      deconstructTables = function() {
         self$tables <- deconstruct_prep(self$data$forUpload)

         # idreg <- update_idreg()
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
      },
      readSheets        = function(file) {
         sheets      <- ods_sheets(file)
         data        <- lapply(sheets, read_ods, path = file, col_types = cols(.default = "c"), .name_repair = "unique_quiet", skip = 1)
         data        <- lapply(data, mutate_all, toupper)
         data        <- lapply(data, mutate_all, ~na_if(., "0"))
         data        <- lapply(data, mutate_all, ~na_if(., "-"))
         data        <- lapply(data, mutate_all, ~na_if(., "NA"))
         data        <- lapply(data, mutate_all, ~na_if(., "N/A"))
         data        <- lapply(data, mutate_all, ~na_if(., "Err:522"))
         data        <- lapply(data, mutate_all, ~na_if(., "NULL"))
         data        <- lapply(data, mutate_all, ~na_if(., "NONE"))
         data        <- lapply(data, mutate_all, ~na_if(., "#REF!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#NAME!"))
         data        <- lapply(data, mutate_all, ~na_if(., "#VALUE!"))
         data        <- lapply(data, mutate_all, ~na_if(., "NOT DONE"))
         data        <- lapply(data, mutate, Row = stri_c("A", row_number() + 2), .before = 1)
         data        <- lapply(data, rename_all, ~toupper(stri_replace_all_regex(., "\\s", "")))
         names(data) <- sheets

         return(data)
      },

      renameInitial     = function(data) {
         cols   <- names(data)
         marker <- cols[14]

         final <- data
         if (marker == 'KEYPOPULATION') {
            final %<>%
               select(
                  sheet_row               = 1,
                  record_date             = 2,
                  patient_code            = 3,
                  branch                  = 4,
                  last                    = 5,
                  first                   = 6,
                  middle                  = 7,
                  suffix                  = 8,
                  uic                     = 9,
                  sex                     = 10,
                  self_ident              = 11,
                  client_mobile           = 12,
                  client_email            = 13,
                  key_population          = 14,
                  risk_condomless_anal    = 15,
                  risk_condomless_vaginal = 16,
                  risk_drug_inject        = 17,
                  risk_drug_sex           = 18,
                  risk_transact_sex       = 19,
                  risk_hiv_vl_unknown     = 20,
                  risk_hiv_unknown        = 21,
                  risk_sti                = 22,
                  risk_pep                = 23,
                  lab_hbsag_date          = 24,
                  lab_hbsag_result        = 25,
                  lab_syph_date           = 26,
                  lab_syph_result         = 27,
                  lab_crea_date           = 28,
                  lab_crea_result         = 29,
                  weight                  = 30,
                  ars_sx                  = 31,
                  sti_sx                  = 32,
                  prep_hiv_date           = 33,
                  pre_init_hiv_nr         = 34,
                  pre_init_weight         = 35,
                  pre_init_no_arv_allergy = 36,
                  pre_init_no_ars         = 37,
                  pre_init_crea_clear     = 38,
                  eligible_behavior       = 39,
                  prep_requested          = 40,
                  prep_plan               = 41,
                  first_time              = 42,
                  prep_type               = 43,
                  disp_date               = 44,
                  disp_total              = 45,
                  disp_by                 = 46,
                  curr_addr               = 47,
               )
         }
         if (marker == 'UNPROTECTEDANALSEXWITHMORETHANONEPARTNER') {
            final %<>%
               select(
                  sheet_row               = 1,
                  record_date             = 2,
                  patient_code            = 3,
                  branch                  = 4,
                  last                    = 5,
                  first                   = 6,
                  middle                  = 7,
                  uic                     = 8,
                  sex                     = 9,
                  self_ident              = 10,
                  client_mobile           = 11,
                  client_email            = 12,
                  key_population          = 13,
                  risk_condomless_anal    = 14,
                  risk_condomless_vaginal = 15,
                  risk_drug_inject        = 16,
                  risk_drug_sex           = 17,
                  risk_transact_sex       = 18,
                  risk_hiv_vl_unknown     = 19,
                  risk_hiv_unknown        = 20,
                  risk_sti                = 21,
                  risk_pep                = 22,
                  lab_hbsag_date          = 23,
                  lab_hbsag_result        = 24,
                  lab_syph_date           = 25,
                  lab_syph_result         = 26,
                  lab_crea_date           = 27,
                  lab_crea_result         = 28,
                  weight                  = 29,
                  ars_sx                  = 30,
                  sti_sx                  = 31,
                  prep_hiv_date           = 32,
                  pre_init_hiv_nr         = 33,
                  pre_init_weight         = 34,
                  pre_init_no_arv_allergy = 35,
                  pre_init_no_ars         = 36,
                  pre_init_crea_clear     = 37,
                  eligible_behavior       = 38,
                  prep_requested          = 39,
                  prep_plan               = 40,
                  first_time              = 41,
                  prep_type               = 42,
                  disp_date               = 43,
                  disp_total              = 44,
                  disp_by                 = 45,
               )
         }
         if (marker == "OTHERKP'S") {
            final %<>%
               select(
                  sheet_row               = 1,
                  record_date             = 2,
                  patient_code            = 3,
                  branch                  = 4,
                  last                    = 5,
                  first                   = 6,
                  middle                  = 7,
                  uic                     = 8,
                  sex                     = 9,
                  self_ident              = 10,
                  client_mobile           = 11,
                  client_email            = 12,
                  key_population          = 13,
                  key_population_other    = 14,
                  risk_condomless_anal    = 15,
                  risk_condomless_vaginal = 16,
                  risk_drug_inject        = 17,
                  risk_drug_sex           = 18,
                  risk_transact_sex       = 19,
                  risk_hiv_vl_unknown     = 20,
                  risk_hiv_unknown        = 21,
                  risk_sti                = 22,
                  risk_pep                = 23,
                  lab_hbsag_date          = 24,
                  lab_hbsag_result        = 25,
                  lab_syph_date           = 26,
                  lab_syph_result         = 27,
                  lab_crea_date           = 28,
                  lab_crea_result         = 29,
                  weight                  = 30,
                  ars_sx                  = 31,
                  sti_sx                  = 32,
                  prep_hiv_date           = 33,
                  pre_init_hiv_nr         = 34,
                  pre_init_weight         = 35,
                  pre_init_no_arv_allergy = 36,
                  pre_init_no_ars         = 37,
                  pre_init_crea_clear     = 38,
                  eligible_behavior       = 39,
                  prep_requested          = 40,
                  prep_plan               = 41,
                  first_time              = 42,
                  prep_type               = 43,
                  disp_date               = 44,
                  disp_total              = 45,
                  disp_by                 = 46,
               )
         }

         return(final)
      },
      renameRefill      = function(data) {
         cols   <- names(data)
         marker <- cols[14]

         final <- data %>%
            select(
               sheet_row                 = 1,
               record_date               = 2,
               patient_code              = 3,
               age                       = 4,
               branch                    = 5,
               self_ident                = 6,
               client_mobile             = 7,
               client_email              = 8,
               risk_condomless_anal      = 9,
               risk_condomless_vaginal   = 10,
               risk_transact_sex         = 11,
               risk_drug_sex             = 12,
               risk_drug_inject          = 13,
               risk_hiv_vl_unknown       = 14,
               risk_sti                  = 15,
               week_avg_sex              = 16,
               key_population            = 17,
               lab_hbsag_date            = 18,
               lab_hbsag_result          = 19,
               lab_syph_date             = 20,
               lab_syph_result           = 21,
               lab_crea_date             = 22,
               lab_crea_result           = 23,
               weight                    = 24,
               ars_sx                    = 25,
               sti_sx                    = 26,
               prep_type_last_visit      = 27,
               prep_shift                = 28,
               prep_missed               = 29,
               prep_side_effects_specify = 30,
               prep_type                 = 31,
               prep_plan                 = 32,
               disp_date                 = 33,
               disp_total                = 34,
               medicine_left             = 35,
               disp_by                   = 36,
            )

         return(final)
      }
   )
)
