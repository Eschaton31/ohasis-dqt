QcArt <- R6Class(
   "QcArt",
   public = list(
      root              = "",
      data              = list(
         new        = tibble(),
         update     = tibble(),
         masterlist = tibble(),
         pii        = tibble(),
         ids        = tibble(),
         converted  = tibble(),
         existing   = tibble(),
         forUpload  = tibble(),
         breakdown  = list()
      ),
      refs              = list(),
      issues            = list(),
      tables            = list(),

      initialize        = function() {
         self$root <- file.path(
            getwd(),
            "data",
            "qc-imports",
            format(Sys.time(), "%Y%m%d")
         )

         invisible(self)
      },
      getExisting       = function() {
         self$data$idreg <- update_idreg()

         lw_conn            <- connect('mariadb-lw')
         self$data$existing <- QB$new(lw_conn)$from(
            'ohasis_warehouse.form_art_bc as art'
         )$select(
            "art.rec_id",
            "art.record_date as visit_date",
            "art.medicine_summary",
            "art.created_by",
            "art.created_at",
            "art.patient_id"
         )$whereBetween(
            "art.record_date",
            c("2025-01-01", format(Sys.time(), "%Y-%m-%d"))
         )$whereIn(
            "art.faci_id",
            c(
               '130032',
               '130666',
               '130031',
               '130008',
               '130033',
               '130009',
               '130018',
               '130004',
               '130994'
            )
         )$get()

         self$data$existing %<>%
            mutate_if(
               .predicate = is.POSIXct,
               ~as.Date(.)
            ) %>%
            get_cid(self$data$idreg, patient_id)

         dbDisconnect(lw_conn)

         invisible(self)
      },
      download          = function() {
         local_drive_quiet()
         local_gs4_quiet()

         # ! ART
         dir <- file.path(self$root, "art")
         check_dir(dir)

         ss   <- "1S5We4abrrTYE-VFFa7fr4uvOWtyRuZXHplacUtlSAi4"
         link <- as_id(ss)
         file <- file.path(dir, "arv.ods")
         drive_download(link, file, overwrite = TRUE)

         invisible(self)
      },
      getRefs           = function() {
         local_gs4_quiet()

         self$refs$corr_addr <- range_speedread(
            "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc",
            "addr",
            range     = "A:F",
            col_types = cols(
               .default = "c"
            )
         )

         invisible(self)
      },
      readNew           = function() {
         self$data$new <- read_ods(
            file.path(self$root, "art", "arv.ods"),
            col_types    = cols(.default = "c"),
            .name_repair = "unique_quiet",
            col_names    = FALSE
         ) %>%
            select(
               1:60,
               98
            ) %>%
            row_to_names(1) %>%
            mutate(
               row_id        = row_number(),
               pxcode_cell   = stri_c("D", row_id + 1),
               artstart_cell = stri_c("AH", row_id + 1),
               ffup_cell     = stri_c("AX", row_id + 1),
            ) %>%
            filter(row_id > 15) %>%
            select(
               row_id,
               pxcode_cell,
               artstart_cell,
               ffup_cell,
               created_at           = `Timestamp`,
               report_type          = `Type of Report`,
               ml_num               = `Masterlist count`,
               patient_code         = `PT CODE`,
               last                 = `Last Name`,
               first                = `First Name`,
               middle               = `Middle Name`,
               uic_mom              = `Mother's First Name (First 2letters)`,
               uic_dad              = `Father's First Name  (First 2letters)`,
               uic_order            = `Birth Order`,
               birthdate            = `Date of Birth`,
               sex                  = `Sex`,
               curr_brgy            = `Barangay`,
               curr_munc            = `City`,
               philhealth_no        = `PhilHealth Number`,
               first_visit_date     = `Date first seen in the facility`,
               confirmatory_code    = `SACCL Code (Confirmatory Code)`,
               date_confirm         = `Date of Diagnosis`,
               mot                  = `Mode of Transmission`,
               who_class            = `WHO Clinical Staging`,
               baseline_cd4_result  = `Baseline CD4 Count`,
               baseline_cd4_date    = `Date of Baseline CD4`,
               latest_cd4_result    = `Latest CD4 Count`,
               latest_cd4_date      = `Latest CD4 Count Date`,
               tb_ipt_start_date    = `TPT Start Date`,
               tb_ipt_outcome       = `TPT Outcome`,
               tb_cpt_start_date    = `CPT Start Date`,
               tb_cpt_outcome       = `CPT Outcome`,
               baseline_hepb_status = `Baseline Hepatitis B Status`,
               pregnant_dx          = `Pregnant at the time of diagnosis?`,
               is_transin           = `Is the client TRANS-IN?`,
               transin_hub          = `Recent Treatment Hub`,
               arv_regimen          = `ARV REGIMEN`,
               art_start_date       = `Date of Enrollment`,
               art_stop_date        = `ARV STOP Date`,
               art_stop_reason      = `REASON FOR DISCONTINUING ARV`,
               baseline_tb_status   = `BASELINE TB Status`,
               tb_status            = `TB-Current Status/Outcome`,
               coinfection_curr     = `Current Co-Infection`,
               coinfection_status   = `Co-Infection Current Status/Outcome`,
               tbdots_faci          = `TB-DOTS Facility`,
               oi_drug              = `OI DRUG`,
               oi_start_date        = `OI Start Date`,
               is_dead              = `Died?`,
               date_of_death        = `Date of Death`,
               cause_of_death       = `Cause of Death`,
               is_transout          = `Trans-out?`,
               transout_date        = `Date Trans-out`,
               transout_hub         = `Hub of Trans-out`,
               latest_ffupdate      = `Date of Last follow up`,
               disp_total           = `Number of tablets given`,
               dx_lab               = `SHC/Lab of Diagnosis`,
               enroll_hub           = `SHC/Lab enrolled`,
               cm_date_enroll       = `Date enrolled on CM`,
               cm_name              = `Name of Case Manager`,
               vaccinations         = `Vaccinations Given:`,
               baseline_vl_date     = `Date of Baseline Viral Load`,
               baseline_vl_result   = `Baseline Viral Load`,
               last_vl_date         = `Date of Latest Viral Load`,
               last_vl_result       = `Latest Viral Load`,
               baseline_hepc_status = `Baseline Hepatitis C Status`,
            ) %>%
            filter(report_type == "New Case Enrollment")

         invisible(self)
      },
      readUpdate        = function() {
         self$data$update <- read_ods(
            file.path(self$root, "art", "arv.ods"),
            col_types    = cols(.default = "c"),
            .name_repair = "unique_quiet",
            col_names    = FALSE
         ) %>%
            select(1:2, 61:68, 70:97, 99) %>%
            row_to_names(1) %>%
            mutate(
               row_id        = row_number(),
               pxcode_cell   = stri_c("BK", row_id + 1),
               artstart_cell = stri_c("BT", row_id + 1),
               ffup_cell     = stri_c("CJ", row_id + 1),
            ) %>%
            filter(row_id > 15) %>%
            select(
               row_id,
               pxcode_cell,
               artstart_cell,
               ffup_cell,
               created_at           = `Timestamp`,
               report_type          = `Type of Report`,
               report_type          = `Type of Report`,
               data_main_id         = `Data_Main_ID`,
               ml_num               = `Masterlist count`,
               patient_code         = `PT CODE`,
               philhealth_no        = `PhilHealth Number`,
               latest_cd4_date      = `Latest CD4 Count Date`,
               tb_ipt_start_date    = `TPT Start Date`,
               tb_ipt_outcome       = `TPT Outcome`,
               tb_cpt_start_date    = `CPT Start Date`,
               baseline_hepb_status = `Baseline Hepatitis B Status`,
               arv_regimen          = `ARV REGIMEN`,
               art_start_date       = `Date of Enrollment`,
               art_stop_date        = `ARV STOP DATE`,
               disc_reason_other    = `Reason for Discontinue ARV`,
               baseline_tb_status   = `Baseline TB status`,
               tb_status            = `TB-Current Status/Outcome`,
               coinfection_curr     = `Current Co-infection`,
               coinfection_status   = `Co-infection Current Status/Outcome`,
               tbdots_faci          = `TB-DOTS Facility`,
               oi_drug              = `OI DRUG`,
               oi_start_date        = `OI start Date`,
               is_dead              = `Died?`,
               date_of_death        = `Date of Death`,
               cause_of_death       = `Cause of Death`,
               is_transout          = `Trans-out?`,
               transout_date        = `Date Trans-out`,
               transout_hub         = `Hub of Trans-out`,
               latest_ffupdate      = `Date of last follow up`,
               disp_total           = `No. of tablets given`,
               vaccinations         = `Vaccinations Given`,
               last_vl_date         = `Date of Last Viral Load`,
               last_vl_result       = `Latest Viral Load`,
               OTHERS               = `Others:`,
               latest_cd4_result    = `Latest CD4 Count`,
               confirm_date_1       = `Date of Confirmator`,
               confirm_date_2       = `Date of Confirmatory`,
               baseline_hepc_status = `Baseline Hepatitis C Status`,
               ltfu_reason          = `If LTFU patient, reason:`,
            ) %>%
            filter(report_type == "Update Case Details")

         invisible(self)
      },
      readMasterlist    = function(path) {
         self$data$masterlist <- read_excel(path, col_types = "text") %>%
            bind_rows(read_sheet(
               "1qyaXK3u0UTlSlbYjILHhfGGGilB1S8qFN2m1-eTpzzE",
               "xl",
               col_types = "c"
            )) %>%
            select(
               data_main_id         = `Data_Main_ID`,
               ml_num               = `Masterlist count`,
               patient_code         = `PT CODE`,
               name                 = `Name`,
               uic                  = `UIC`,
               birthdate            = `DATE OF BIRTH (dd/mm/yyyy)`,
               sex                  = `SEX`,
               curr_brgy            = `BRGY`,
               curr_munc            = `CITY`,
               philhealth_no        = `PhilHEalth Number`,
               first_visit_date     = `Date first seen in the facility`,
               confirmatory_code    = `SACCL Code (Confirmatory Code)`,
               date_confirm         = `Date of Diagnosis (dd/mm/yyyy)`,
               age_dx               = `Age of diagnosis`,
               mot                  = `Mode of Transmission`,
               who_class            = `WHO Clinical Staging`,
               baseline_cd4_result  = `Baseline CD4 count`,
               baseline_cd4_date    = `Date of Baseline CD4`,
               latest_cd4_result    = `Latest CD4 Count`,
               latest_cd4_date      = `Latest CD4 Count Date`,
               tb_ipt_start_date    = `TPT Start Date`,
               tb_ipt_outcome       = `TPT Outcome`,
               tb_cpt_start_date    = `CPT Start Date`,
               tb_cpt_outcome       = `CPT Outcome`,
               baseline_hepb_status = `Baseline Hepatitis B Status`,
               pregnant_dx          = `Pregnant at the time of diagnosis? (Y/N) for female`,
               is_transin           = `Is the client TRANS-IN? (Y/N)`,
               transin_hub          = `Recent Treatment Hub`,
               arv_regimen          = `ARV REGIMEN`,
               art_start_date       = `DATE OF ENROLLMENT`,
               art_stop__date       = `ARV STOP DATE`,
               art_stop_reason      = `REASON FOR DISCONTINUING ARV`,
               baseline_tb_status   = `BASELINE TB status`,
               tb_status            = `TB- Current Status/ Outcome`,
               coinfection_curr     = `Current Co-infection`,
               coinfection_status   = `Co-infection Current Status/Outcome`,
               tbdots_faci          = `TB-DOTS Facility`,
               oi_drug              = `OI DRUG`,
               oi_start_date        = `OI START DATE`,
               is_dead              = `Died? (yes or No)`,
               date_of_death        = `Date of Death`,
               cause_of_death       = `Cause of death?`,
               is_transout          = `Trans-out? (yes or No)2`,
               transout_date        = `Date Trans-out`,
               transout_hub         = `Hub of Trans-out`,
               latest_ffupdate      = `Date of last ff-up (dd/mm/yyyy)`,
               disp_total           = `No of tablets given`,
               latest_nextpickup    = `Expected  refill date`,
               dx_lab               = `SHC/Lab of Diagnosis`,
               enroll_hub           = `SHC/Lab Enrolled`,
               cm_date_enroll       = `Date Enrolled on CM`,
               cm_name              = `Name of Case Manager`,
               vaccinations         = `Vaccinations Given:`,
               baseline_vl_date     = `Date of Baseline Viral Load`,
               baseline_vl_result   = `Baseline Viral Load`,
               latest_vl_date       = `Date of Latest Viral Load`,
               latest_vl_result     = `Latest Viral Load`,
               is_vl_tested         = `Viral Load Tested?`,
               is_vl_suppressed     = `Virally Suppressed`,
            ) %>%
            split_names(name, first, middle, last) %>%
            mutate_at(
               .vars = vars(contains("date")),
               ~excel_numeric_to_date(parse_number(.))
            )

         invisible(self)
      },
      createPii         = function() {
         self$data$pii <- self$data$new %>%
            select(
               ml_num,
               patient_code,
               last,
               first,
               middle,
               uic_mom,
               uic_dad,
               uic_order,
               birthdate,
               sex,
               curr_brgy,
               curr_munc,
               philhealth_no,
               confirmatory_code,
            ) %>%
            mutate_if(is.character, toupper) %>%
            mutate(
               birthdate = as.Date(parse_date_time(birthdate, "mdY")),
               uic       = stri_c(
                  stri_pad_right(str_left(uic_mom, 2), 2, "X"),
                  stri_pad_right(str_left(uic_dad, 2), 2, "X"),
                  stri_pad_left(str_left(uic_order, 2), 2, "0"),
                  format(birthdate, "%m%d%Y")
               ),
               uic_match = stri_c(
                  stri_pad_right(str_left(uic_mom, 2), 2, "X"),
                  stri_pad_right(str_left(uic_dad, 2), 2, "X"),
                  stri_pad_left(str_left(uic_order, 2), 2, "0"),
                  format(birthdate, "%Y%m%d")
               ),
               .before   = birthdate
            ) %>%
            select(-uic_mom, -uic_dad, -uic_order) %>%
            mutate(
               curr_prov = "UNKNOWN",
               curr_reg  = "UNKNOWN",
               .after    = curr_munc
            ) %>%
            left_join(
               y  = self$refs$corr_addr %>% rename_all(tolower),
               by = join_by(
                  curr_reg == name_reg,
                  curr_prov == name_prov,
                  curr_munc == name_munc
               )
            ) %>%
            bind_rows(
               self$data$masterlist %>%
                  select(
                     data_main_id,
                     ml_num,
                     patient_code,
                     last,
                     first,
                     middle,
                     uic,
                     birthdate,
                     sex,
                     curr_brgy,
                     curr_munc,
                     philhealth_no,
                     confirmatory_code,
                  ) %>%
                  mutate(
                     curr_prov = "UNKNOWN",
                     curr_reg  = "UNKNOWN",
                     .after    = curr_munc
                  ) %>%
                  left_join(
                     y  = self$refs$corr_addr %>% rename_all(tolower),
                     by = join_by(
                        curr_reg == name_reg,
                        curr_prov == name_prov,
                        curr_munc == name_munc
                     )
                  )
            ) %>%
            distinct(
               data_main_id,
               ml_num,
               patient_code,
               last,
               first,
               middle,
               uic,
               birthdate,
               sex,
               corr_name_reg,
               corr_name_prov,
               corr_name_munc,
               philhealth_no,
               confirmatory_code,
               .keep_all = TRUE
            ) %>%
            mutate(
               sex = case_when(
                  sex == "M" ~ "MALE",
                  sex == "F" ~ "FEMALE",
                  TRUE ~ sex
               )
            ) %>%
            left_join(
               y  = self$data$ids %>%
                  filter(!is.na(central_id)) %>%
                  distinct(
                     central_id,
                     data_main_id,
                     ml_num,
                     patient_code,
                     last,
                     first,
                     middle,
                     uic,
                     birthdate,
                     sex,
                     corr_name_reg,
                     corr_name_prov,
                     corr_name_munc,
                     philhealth_no,
                     confirmatory_code,
                  ),
               by = join_by(
                  ml_num,
                  data_main_id,
                  patient_code,
                  last,
                  first,
                  middle,
                  uic_match == uic,
                  birthdate,
                  sex,
                  corr_name_reg,
                  corr_name_prov,
                  corr_name_munc,
                  philhealth_no,
                  confirmatory_code,
               )
            ) %>%
            left_join(
               y  = self$data$ids %>%
                  filter(!is.na(central_id)) %>%
                  distinct(
                     central_id,
                     data_main_id,
                     ml_num,
                     patient_code,
                     last,
                     first,
                     middle,
                     uic,
                     birthdate,
                     sex,
                     corr_name_reg,
                     corr_name_prov,
                     corr_name_munc,
                     philhealth_no,
                     confirmatory_code,
                  ),
               by = join_by(
                  ml_num,
                  data_main_id,
                  patient_code,
                  last,
                  first,
                  middle,
                  uic,
                  birthdate,
                  sex,
                  corr_name_reg,
                  corr_name_prov,
                  corr_name_munc,
                  philhealth_no,
                  confirmatory_code,
               )
            ) %>%
            mutate(
               central_id = coalesce(central_id.x, central_id.y)
            ) %>%
            relocate(central_id, .before = 1) %>%
            mutate(pii_id = row_number())

         invisible(self)
      },
      convert           = function() {
         self$data$converted <- self$data$new %>%
            bind_rows(self$data$update) %>%
            relocate(latest_ffupdate, .after = patient_code) %>%
            mutate(
               created_at = parse_date_time(created_at, "mdYHMS")
            ) %>%
            mutate_at(
               .vars = vars(contains("date")),
               ~str_replace_all(., "0024", "2024") %>%
                  str_replace_all("0025", "2025")
            ) %>%
            mutate_at(
               .vars = vars(contains("date")),
               ~as.Date(parse_date_time(., "mdY"))
            )

         invisible(self)
      },
      prepareUpload     = function() {
         art_qc <- self$data$converted %>%
            mutate(
               patient_code = self$cleanPatientCode(patient_code, data_main_id)
            ) %>%
            left_join(
               y  = self$data$pii %>%
                  mutate(
                     patient_code = self$cleanPatientCode(
                        patient_code,
                        data_main_id
                     )
                  ) %>%
                  select(
                     pii_1 = pii_id,
                     cid_1 = central_id,
                     data_main_id,
                     patient_code
                  ) %>%
                  distinct(data_main_id, patient_code, .keep_all = TRUE),
               by = join_by(data_main_id, patient_code)
            ) %>%
            left_join(
               y  = self$data$pii %>%
                  mutate(
                     patient_code = self$cleanPatientCode(
                        patient_code,
                        data_main_id
                     )
                  ) %>%
                  select(
                     pii_2 = pii_id,
                     cid_2 = central_id,
                     data_main_id,
                     ml_num,
                     patient_code
                  ) %>%
                  distinct(
                     data_main_id,
                     ml_num,
                     patient_code,
                     .keep_all = TRUE
                  ),
               by = join_by(data_main_id, ml_num, patient_code)
            ) %>%
            mutate_if(is.character, ~na_if(., "N/A")) %>%
            mutate(
               pii_id     = coalesce(pii_1, pii_2),
               patient_id = coalesce(cid_1, cid_2),
               .before    = 1
            ) %>%
            get_cid(self$data$idreg, patient_id) %>%
            select(-curr_brgy, -curr_munc, -data_main_id) %>%
            left_join(
               y  = self$data$pii %>%
                  select(pii_id, data_main_id),
               by = join_by(pii_id)
            ) %>%
            left_join(
               y  = self$data$masterlist %>%
                  mutate_if(is.character, ~na_if(., "N/A")) %>%
                  select(
                     -ml_num,
                     -patient_code,
                  ),
               by = join_by(data_main_id)
            )

         names_overlap <- get_names(art_qc, "\\.x")
         for (var in names_overlap) {
            col1  <- as.name(var)
            col2  <- as.name(str_replace(var, "\\.x$", ".y"))
            final <- as.name(str_replace(var, "\\.x$", ""))

            art_qc %<>%
               mutate(
                  {{final}} := coalesce({{col1}}, {{col2}}),
                  .after    = {{col1}}
               ) %>%
               select(-{{col1}}, -{{col2}})
         }

         art_breakdown <- list(
            vl_baseline  = art_qc %>%
               filter(
                  if_all(c(baseline_vl_date, baseline_vl_result), ~!is.na(.))
               ) %>%
               distinct(central_id, baseline_vl_date, baseline_vl_result),
            vl_latest    = art_qc %>%
               filter(
                  if_all(c(last_vl_date, last_vl_result), ~!is.na(.))
               ) %>%
               distinct(central_id, last_vl_date, last_vl_result),
            cd4_baseline = art_qc %>%
               filter(
                  if_all(c(baseline_cd4_date, baseline_cd4_result), ~!is.na(.))
               ) %>%
               distinct(central_id, baseline_cd4_date, baseline_cd4_result),
            cd4_latest   = art_qc %>%
               filter(
                  if_all(c(latest_cd4_date, latest_cd4_result), ~!is.na(.))
               ) %>%
               distinct(central_id, latest_cd4_date, latest_cd4_result),
            artstart     = art_qc %>%
               filter(
                  if_any(c(art_start_date, arv_regimen), ~!is.na(.))
               ) %>%
               distinct(central_id, art_start_date, arv_regimen),
            artstop      = art_qc %>%
               filter(
                  if_any(c(art_stop_date, art_stop_reason), ~!is.na(.))
               ) %>%
               distinct(central_id, art_stop_date, art_stop_reason),
            dead         = art_qc %>%
               filter(
                  if_any(c(date_of_death, cause_of_death), ~!is.na(.))
               ) %>%
               distinct(central_id, date_of_death, cause_of_death),
            visits       = art_qc %>%
               filter(
                  if_any(
                     c(latest_ffupdate, first_visit_date, disp_total),
                     ~!is.na(.)
                  )
               ) %>%
               select(
                  pii_id,
                  row_id,
                  pxcode_cell,
                  artstart_cell,
                  ffup_cell,
                  central_id,
                  patient_id,
                  created_at,
                  latest_ffupdate,
                  first_visit_date,
                  who_class,
                  tb_ipt_start_date,
                  tb_ipt_outcome,
                  tb_cpt_start_date,
                  tb_cpt_outcome,
                  arv_regimen,
                  disp_total,
                  enroll_hub
               ) %>%
               left_join(
                  y  = self$data$pii %>%
                     mutate(
                        curr_addr = str_squish(stri_c(coalesce(na_if(curr_brgy, "N/A"), ""), " ", coalesce(curr_munc, "")))
                     ) %>%
                     select(
                        pii_id,
                        confirmatory_code,
                        patient_code,
                        uic,
                        first,
                        middle,
                        last,
                        birthdate,
                        sex,
                        philhealth_no,
                        curr_reg  = corr_name_reg,
                        curr_prov = corr_name_prov,
                        curr_munc = corr_name_munc,
                        curr_addr
                     ),
                  by = join_by(pii_id)
               ) %>%
               mutate(
                  visit_date = coalesce(
                     latest_ffupdate,
                     first_visit_date,
                     tb_ipt_start_date,
                     tb_cpt_start_date
                  )
               ) %>%
               distinct(
                  pii_id,
                  central_id,
                  patient_id,
                  latest_ffupdate,
                  first_visit_date,
                  who_class,
                  tb_ipt_start_date,
                  tb_ipt_outcome,
                  tb_cpt_start_date,
                  tb_cpt_outcome,
                  arv_regimen,
                  disp_total,
                  enroll_hub,
                  .keep_all = TRUE
               )
         )

         timestamp  <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
         for_import <- art_breakdown$visits %>%
            left_join(
               y  = art_breakdown$vl_latest,
               by = join_by(central_id, closest(visit_date >= last_vl_date))
            ) %>%
            arrange(row_id, desc(last_vl_date)) %>%
            distinct(row_id, .keep_all = TRUE) %>%
            left_join(
               y  = art_breakdown$vl_baseline,
               by = join_by(central_id, closest(visit_date >= baseline_vl_date))
            ) %>%
            arrange(row_id, desc(baseline_vl_date)) %>%
            distinct(row_id, .keep_all = TRUE) %>%
            left_join(
               y  = art_breakdown$cd4_latest,
               by = join_by(central_id, closest(visit_date >= latest_cd4_date))
            ) %>%
            arrange(row_id, desc(latest_cd4_date)) %>%
            distinct(row_id, .keep_all = TRUE) %>%
            left_join(
               y  = art_breakdown$cd4_baseline,
               by = join_by(
                  central_id,
                  closest(visit_date >= baseline_cd4_date)
               )
            ) %>%
            arrange(row_id, desc(baseline_cd4_date)) %>%
            distinct(row_id, .keep_all = TRUE) %>%
            mutate(
               lab_viral_date   = if_else(
                  !is.na(last_vl_date),
                  last_vl_date,
                  baseline_vl_date
               ),
               lab_viral_result = if_else(
                  !is.na(last_vl_date),
                  last_vl_result,
                  baseline_vl_result
               ),
               lab_cd4_date     = if_else(
                  !is.na(latest_cd4_date),
                  latest_cd4_date,
                  baseline_cd4_date
               ),
               lab_cd4_result   = if_else(
                  !is.na(latest_cd4_date),
                  latest_cd4_result,
                  baseline_cd4_result
               ),

               medicine_summary = str_replace_all(
                  toupper(arv_regimen),
                  "\\s",
                  ""
               ),
               medicine_summary = str_replace_all(
                  medicine_summary,
                  "LTE",
                  "TDF/3TC/EFV"
               ),
               medicine_summary = str_replace_all(
                  medicine_summary,
                  "3TC/TDF/EFV",
                  "TDF/3TC/EFV"
               ),
               medicine_summary = str_replace_all(
                  medicine_summary,
                  "TLD",
                  "TDF/3TC/DTG"
               ),
               medicine_summary = str_replace_all(
                  medicine_summary,
                  "LPV/R",
                  "LPV/r"
               ),
               medicine_summary = str_replace_all(
                  medicine_summary,
                  "3TC/AZT",
                  "AZT/3TC"
               ),
               medicine_summary = str_replace_all(
                  medicine_summary,
                  "3TC/TDF",
                  "TDF/3TC"
               ),

               faci_id          = case_when(
                  enroll_hub == 'AJM SHC' ~ '130666',
                  enroll_hub == 'BATASAN SHC' ~ '130008',
                  enroll_hub == 'BERNARDO SHC' ~ '130009',
                  enroll_hub == 'KLINIKA BATASAN' ~ '130031',
                  enroll_hub == 'KLINIKA BERNARDO' ~ '130004',
                  enroll_hub == 'KLINIKA EASTWOOD' ~ '130994',
                  enroll_hub == 'KLINIKA NOVALICHES' ~ '130032',
                  enroll_hub == 'KLINIKA PROJECT 7' ~ '130033',
                  enroll_hub == 'PROJECT 7 SHC' ~ '130018',
               )
            ) %>%
            get_cid(self$data$idreg, patient_id) %>%
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
            # retain only not uploaded and those with changes
            filter(
               !is.na(medicine_summary),
               !is.na(faci_id)
            ) %>%
            anti_join(
               y  = self$data$existing %>% filter(!is.na(medicine_summary)),
               by = join_by(rec_id, visit_date),
            ) %>%
            mutate(
               old_rec    = if_else(!is.na(rec_id), 1, 0, 0),
               created_by = coalesce(created_by, "1300000048"),
               created_at = coalesce(created_at.x, created_at.y),
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
               batch_rec_ids(
                  for_import %>% filter(is.na(rec_id)),
                  rec_id,
                  created_by,
                  "row_id"
               )
            )

         final_import <- final_import %>%
            filter(!is.na(patient_id)) %>%
            bind_rows(
               batch_px_ids(
                  final_import %>% filter(is.na(patient_id)),
                  patient_id,
                  faci_id,
                  "row_id"
               )
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

         addr     <- range_speedread(
            "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc",
            "addr",
            show_col_types = FALSE,
            col_types      = cols(.default = "c"),
            name_repair    = "unique_quiet"
         )
         ref_addr <- range_speedread(
            "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc",
            "ref_addr",
            show_col_types = FALSE,
            col_types      = cols(.default = "c"),
            name_repair    = "unique_quiet"
         )
         final_import %<>%
            select(-starts_with("CORR_NAME_")) %>%
            mutate(
               CURR_NAME_REG  = "UNKNOWN",
               CURR_NAME_PROV = "UNKNOWN",
               CURR_NAME_MUNC = curr_munc,
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
               CURR_PSGC = coalesce(
                  CURR_PSGC_MUNC,
                  CURR_PSGC_PROV,
                  CURR_PSGC_REG
               ),
            ) %>%
            select(-curr_reg, -curr_prov, -curr_munc) %>%
            left_join(
               y  = ohasis$ref_addr %>%
                  select(
                     CURR_PSGC = psgc_old,
                     curr_reg  = reg,
                     curr_prov = prov,
                     curr_munc = munc
                  ),
               by = join_by(CURR_PSGC)
            )

         self$data$forUpload <- final_import
         self$data$breakdown <- art_breakdown

         invisible(self)
      },
      readIds           = function() {
         con           <- connect("old-lw")
         self$data$ids <- QB$new(con)$from("ohasis_lake.qc_clients")$get() %>%
            rename_all(tolower)
         dbDisconnect(con)

         invisible(self)
      },
      checkIssues       = function() {
         self$issues <- list(
            dupe_px_code = self$data$pii %>% get_dupes(patient_code, ml_num),
            `not2025`    = self$data$converted %>%
               filter(
                  (latest_ffupdate < "2024-11-01" &
                     year(latest_ffupdate) != year(created_at)) |
                     latest_ffupdate > now()
               ),
            `clean_addr` = self$data$pii %>%
               filter(is.na(corr_name_reg)) %>%
               distinct(curr_reg, curr_prov, curr_munc),
            `no_cid`     = self$data$pii %>%
               filter(is.na(central_id)),
            `not_in_ml`  = self$data$converted %>%
               mutate(
                  patient_code = self$cleanPatientCode(
                     patient_code,
                     data_main_id
                  )
               ) %>%
               anti_join(
                  self$data$pii %>%
                     mutate(
                        patient_code = self$cleanPatientCode(
                           patient_code,
                           data_main_id
                        )
                     ) %>%
                     distinct(data_main_id, patient_code)
               ) %>%
               anti_join(
                  self$data$pii %>%
                     mutate(
                        patient_code = self$cleanPatientCode(
                           patient_code,
                           data_main_id
                        )
                     ) %>%
                     distinct(data_main_id, ml_num, patient_code)
               )
         )
      },
      addNewPatients    = function() {
         max_id <- max(self$data$ids$row_id)
         new    <- self$data$pii %>%
            filter(is.na(central_id)) %>%
            mutate(
               row_id = max_id + row_number()
            ) %>%
            distinct(
               row_id,
               data_main_id,
               ml_num,
               patient_code,
               last,
               first,
               middle,
               uic,
               birthdate,
               sex,
               corr_name_reg,
               corr_name_prov,
               corr_name_munc,
               philhealth_no,
               confirmatory_code,
            ) %>%
            mutate(
               faci_id = '130000',
               drop    = is.na(uic) & is.na(birthdate) & is.na(first)
            ) %>%
            filter(!drop) %>%
            select(-drop)

         created <- oh_batch_newpx(new, "row_id")

         con <- ohasis$conn("lw")
         dbxUpsert(
            con,
            Id(schema = "ohasis_lake", table = "qc_clients"),
            created %>%
               select(
                  row_id,
                  central_id = patient_id,
                  data_main_id,
                  ml_num,
                  patient_code,
                  last,
                  first,
                  middle,
                  uic,
                  birthdate,
                  sex,
                  corr_name_reg,
                  corr_name_prov,
                  corr_name_munc,
                  philhealth_no,
                  confirmatory_code,
               ),
            "row_id"
         )
         dbDisconnect(con)

         invisible(self)
      },
      cleanPatientCode  = function(column, dataMain) {
         clean <- toupper({{column}})
         clean <- case_when(
            {{dataMain}} == 3515 ~ "BSCH-23-RBR 2",
            {{dataMain}} == 179 ~ "AJMSHC23-WBH",
            {{dataMain}} == 2883 ~ "BAT279-19",
            {{dataMain}} == 3245 ~ "BAT646-23",
            {{dataMain}} == 3426 ~ "BSHC-13 FAC",
            {{dataMain}} == 3430 ~ "BSHC-14 DGD",
            {{dataMain}} == 3576 ~ "BSHC-17 JAL",
            {{dataMain}} == 364 ~ "KBAT 96-19",
            {{dataMain}} == 3789 ~ "BSHC-19 HSA",
            {{dataMain}} == 3878 ~ "BSHC-20 EDB",
            {{dataMain}} == 3952 ~ "BSHC-21 LAL",
            {{dataMain}} == 3984 ~ "BSHC-21 DGD",
            {{dataMain}} == 4169 ~ "BSHC-23 RBT",
            {{dataMain}} == 4249 ~ "BSHC-23 RMM",
            {{dataMain}} == 4252 ~ "BSHC-24 SMT",
            {{dataMain}} == 4269 ~ "BSHC-24 DTT",
            {{dataMain}} == 5304 ~ "P721-VAG",
            {{dataMain}} == 5337 ~ "P721-JEM",
            {{dataMain}} == 5581 ~ "P723-RDD",
            {{dataMain}} == 704 ~ "KBAT 436-23",
            {{dataMain}} == 7186 ~ "BSHC-24 MCR",
            {{dataMain}} == 7620 ~ "AJMSHC24-YFN",
            {{dataMain}} == 7681 ~ "AJMSHC24-EDA",
            {{dataMain}} == 7824 ~ "BSHC-24 WSM",
            {{dataMain}} == 8016 ~ "BSHC-24 JPA",
            {{dataMain}} == 8259 ~ "P725-LDD",
            {{dataMain}} == 8277 ~ "BSHC-25 MDR",
            {{dataMain}} == 8416 ~ "KN25-REF",
            {{dataMain}} == 4232 ~ "BSHC-24 DEO",
            {{dataMain}} == 5280 ~ "P720-NLA",
            {{dataMain}} == 5343 ~ "P721-SJM",
            {{dataMain}} == 7482 ~ "KE 24-JPT",
            {{dataMain}} == 7728 ~ "KE 24-JVS",
            {{dataMain}} == 7866 ~ "KE 24-MCC 2",
            {{dataMain}} == 711 ~ "KBAT 433-23",
            {{dataMain}} == 8052 ~ "KE 24-RDC",
            {{dataMain}} == 7809 ~ "BAT709-24",
            {{dataMain}} == 80 ~ "AJMSHC20-ADS",
            {{dataMain}} == 7021 ~ "BSHC-24 LET",
            {{dataMain}} == 4168 ~ "BSHC-23 MCA",
            TRUE ~ clean
         )
         clean <- str_replace_all(clean, "^AJMSHC\\s", "AJMSHC")
         clean <- str_replace_all(clean, "^BAT\\s", "BAT")
         clean <- str_replace_all(clean, "^BSHC\\s(?!-)", "BSHC-")
         clean <- str_replace_all(clean, "^KBAT(?!\\s)", "KBAT ")
         clean <- str_replace_all(clean, "^KE(?!\\s)", "KE ")
         clean <- str_replace_all(clean, "^(KE [0-9][0-9])(?!-)", "\\1-")
         clean <- str_replace_all(clean, "^(BSHC-[0-9][0-9])-", "\\1 ")
         clean <- str_replace_all(clean, "\\s-", "-")
         clean <- str_replace_all(clean, "-\\s", "-")
         clean <- str_squish(clean)

         return(clean)
      },

      deconstructTables = function() {
         tables           <- list()
         tables$px_record <- list(
            name = "px_record",
            pk   = c("rec_id", "patient_id"),
            data = self$data$forUpload %>%
               mutate(
                  sub_faci_id = NA_character_,
                  disease     = "101000",
                  module      = 3
               ) %>%
               select(
                  rec_id,
                  patient_id,
                  faci_id,
                  sub_faci_id,
                  record_date = visit_date,
                  disease,
                  module,
                  created_by,
                  created_at,
                  updated_by,
                  updated_at,
               )
         )

         tables$px_info <- list(
            name = "px_info",
            pk   = c("rec_id", "patient_id"),
            data = self$data$forUpload %>%
               select(
                  rec_id,
                  patient_id,
                  confirmatory_code,
                  uic,
                  patient_code,
                  sex,
                  birthdate,
                  created_by,
                  created_at,
                  updated_by,
                  updated_at,
               ) %>%
               mutate(
                  sex = case_when(
                     sex == "MALE" ~ "1",
                     sex == "FEMALE" ~ "2",
                  )
               )
         )

         tables$px_name <- list(
            name = "px_name",
            pk   = c("rec_id", "patient_id"),
            data = self$data$forUpload %>%
               select(
                  rec_id,
                  patient_id,
                  first,
                  middle,
                  last,
                  created_by,
                  created_at,
                  updated_by,
                  updated_at,
               )
         )

         tables$px_faci <- list(
            name = "px_faci",
            pk   = c("rec_id", "service_type"),
            data = self$data$forUpload %>%
               mutate(
                  sub_faci_id  = NA_character_,
                  service_type = "101201",
               ) %>%
               select(
                  rec_id,
                  faci_id,
                  sub_faci_id,
                  service_type,
                  created_by,
                  created_at,
                  updated_by,
                  updated_at,
               )
         )

         tables$px_form <- list(
            name = "px_form",
            pk   = c("rec_id", "form"),
            data = self$data$forUpload %>%
               mutate(
                  form    = "ART Form",
                  version = "2021"
               ) %>%
               select(
                  rec_id,
                  form,
                  version,
                  created_by,
                  created_at,
                  updated_by,
                  updated_at,
               )
         )

         tables$px_medicine <- list(
            name = "px_medicine",
            pk   = c("rec_id", "medicine", "disp_num"),
            data = self$data$forUpload %>%
               separate_longer_delim(
                  cols  = medicine_summary,
                  delim = "+"
               ) %>%
               mutate(
                  sub_faci_id = NA_character_,
                  unit_basis  = "2",
                  medicine    = case_when(
                     medicine_summary == "TDF/3TC/DTG" ~ "2029",
                     medicine_summary == "TDF/3TC/EFV" ~ "2015",
                     medicine_summary == "3TC/TDF/EFV" ~ "2015",
                     medicine_summary == "AZT/3TC" ~ "2018",
                     medicine_summary == "3TC/AZT" ~ "2018",
                     medicine_summary == "LPV/r" ~ "2009",
                     medicine_summary == "DTG" ~ "2035",
                     medicine_summary == "EFV" ~ "2004",
                     medicine_summary == "3TC" ~ "2023",
                     medicine_summary == "ABC" ~ "2002",
                     medicine_summary == "FTC" ~ "2031",
                     medicine_summary == "NVP" ~ "2011",
                     medicine_summary == "RIL" ~ "2014",
                     medicine_summary == "RPV" ~ "2014",
                     medicine_summary == "TDF" ~ "2017",
                     medicine_summary == "TDF/3TC" ~ "2016",
                     medicine_summary == "3TC/TDF" ~ "2016",
                  ),
               ) %>%
               filter(!is.na(medicine)) %>%
               group_by(row_id) %>%
               mutate(
                  disp_num = row_number(),
               ) %>%
               ungroup() %>%
               select(
                  rec_id,
                  faci_id,
                  sub_faci_id,
                  medicine,
                  disp_num,
                  unit_basis,
                  disp_total,
                  disp_date = visit_date,
               )
         )

         tables$px_addr <- list(
            name = "px_addr",
            pk   = c("rec_id", "addr_type"),
            data = self$data$forUpload %>%
               select(
                  rec_id,
                  created_at,
                  created_by,
                  curr_reg,
                  curr_prov,
                  curr_munc,
               ) %>%
               left_join(
                  y  = ohasis$ref_addr %>%
                     select(
                        curr_reg  = name_reg,
                        curr_prov = name_prov,
                        curr_munc = name_munc,
                        psgc_reg,
                        psgc_prov,
                        psgc_munc
                     ) %>%
                     mutate_at(vars(starts_with("curr_")), toupper),
                  by = join_by(curr_reg, curr_prov, curr_munc)
               ) %>%
               mutate(
                  addr_type = "2"
               ) %>%
               distinct(rec_id, .keep_all = TRUE) %>%
               select(
                  rec_id,
                  addr_type,
                  addr_reg  = psgc_reg,
                  addr_prov = psgc_prov,
                  addr_munc = psgc_munc,
                  created_by,
                  created_at,
               )
         )

         # labs
         tables$px_labs <- list(
            name = "px_labs",
            pk   = c("rec_id", "lab_test"),
            data = self$data$forUpload %>%
               select(
                  rec_id,
                  created_at,
                  created_by,
                  starts_with("lab"),
               ) %>%
               mutate_all(as.character) %>%
               pivot_longer(
                  cols      = starts_with("lab"),
                  names_to  = "lab_data",
                  values_to = "lab_value"
               ) %>%
               mutate(
                  lab_test = substr(
                     lab_data,
                     5,
                     stri_locate_last_fixed(lab_data, "_") - 1
                  ),
                  piece    = substr(
                     lab_data,
                     stri_locate_last_fixed(lab_data, "_") + 1,
                     1000
                  ),
               ) %>%
               mutate(
                  lab_test = case_when(
                     lab_test == "hbsag" ~ "1",
                     lab_test == "crea" ~ "2",
                     lab_test == "syph" ~ "3",
                     lab_test == "vl" ~ "4",
                     lab_test == "viral" ~ "4",
                     lab_test == "CD4" ~ "5",
                     lab_test == "xray" ~ "6",
                     lab_test == "xpert" ~ "7",
                     lab_test == "dssm" ~ "8",
                     lab_test == "hivdr" ~ "9",
                     lab_test == "hemo" ~ "10",
                     lab_test == "hemog" ~ "10",
                     TRUE ~ lab_test
                  )
               ) %>%
               distinct(
                  rec_id,
                  created_at,
                  created_by,
                  lab_test,
                  piece,
                  .keep_all = TRUE
               ) %>%
               pivot_wider(
                  id_cols      = c(rec_id, created_at, created_by, lab_test),
                  names_from   = piece,
                  values_from  = lab_value,
                  names_prefix = "lab_"
               ) %>%
               filter(!is.na(lab_date) | !is.na(lab_result)) %>%
               arrange(rec_id, lab_test) %>%
               mutate(
                  lab_date = case_when(
                     stri_detect_fixed(lab_date, "-") & nchar(lab_date) < 10 ~
                        as.Date(lab_date, format = "%m-%d-%y"),
                     stri_detect_fixed(lab_date, "-") & nchar(lab_date) == 10 ~
                        as.Date(lab_date, format = "%Y-%m-%d"),
                     stri_detect_fixed(lab_date, "/") ~
                        as.Date(lab_date, format = "%m/%d/%Y"),
                  ),
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
            self$data$forUpload %>% select(rec_id),
            batch_size = 1000
         )
         lapply(
            self$tables,
            function(ref, db_conn) {
               log_info("Uploading {green(ref$name)}.")
               table_space <- Id(schema = "ohasis_interim", table = ref$name)
               dbxUpsert(db_conn, table_space, ref$data, ref$pk)
            },
            db_conn
         )
         dbDisconnect(db_conn)

         invisible(self)
      }
   )
)

art <- QcArt$new()
# art$download()
art$getRefs()
art$getExisting()
art$readIds()
art$readNew()
art$readUpdate()
art$readMasterlist("C:/Users/Bene-G16/Downloads/ART Masterlist-QC (1).xlsx")
art$createPii()
art$convert()
art$checkIssues()
art$prepareUpload()
art$deconstructTables()

meds   <- art$data$forUpload %>%
   separate_longer_delim(
      cols  = medicine_summary,
      delim = "+"
   ) %>%
   mutate(
      sub_faci_id = NA_character_,
      unit_basis  = "2",
      medicine    = case_when(
         medicine_summary == "TDF/3TC/DTG" ~ "2029",
         medicine_summary == "TDF/3TC/EFV" ~ "2015",
         medicine_summary == "3TC/TDF/EFV" ~ "2015",
         medicine_summary == "AZT/3TC" ~ "2018",
         medicine_summary == "3TC/AZT" ~ "2018",
         medicine_summary == "LPV/r" ~ "2009",
         medicine_summary == "DTG" ~ "2035",
         medicine_summary == "EFV" ~ "2004",
         medicine_summary == "3TC" ~ "2023",
         medicine_summary == "ABC" ~ "2002",
         medicine_summary == "FTC" ~ "2031",
         medicine_summary == "NVP" ~ "2011",
         medicine_summary == "RIL" ~ "2014",
         medicine_summary == "RPV" ~ "2014",
         medicine_summary == "TDF" ~ "2017",
         medicine_summary == "TDF/3TC" ~ "2016",
         medicine_summary == "3TC/TDF" ~ "2016",
      ),
      per_day     = case_when(
         medicine == '2029' ~ 1,
         medicine == '2023' ~ 2,
         medicine == '2011' ~ 2,
         medicine == '2004' ~ 1,
         medicine == '2009' ~ 4,
         medicine == '2035' ~ 1,
         medicine == '2015' ~ 1,
         medicine == '2018' ~ 2,
         medicine == '2016' ~ 1,
         medicine == '2014' ~ 2,
         medicine == '2002' ~ 2,
         TRUE ~ 1
      ),

      next_date   = visit_date %m+% days(floor(as.numeric(disp_total) / per_day)),
   ) %>%
   filter(!is.na(medicine)) %>%
   group_by(row_id) %>%
   mutate(
      disp_num = row_number(),
   ) %>%
   ungroup() %>%
   select(
      rec_id,
      faci_id,
      sub_faci_id,
      medicine,
      disp_num,
      unit_basis,
      disp_total,
      disp_date = visit_date,
      next_date,
      per_day,
   )
tables <- art$data$forUpload %>%
   mutate(
      sub_faci_id      = NA_character_,
      service_faci     = faci_id,
      service_sub_faci = sub_faci_id,
      client_mobile    = NA_character_,
      client_email     = NA_character_,
      medicine_left    = NA_character_,
      medicine_missed  = NA_character_,
   ) %>%
   left_join(
      y  = meds %>%
         select(rec_id, per_day, latest_next_date = next_date) %>%
         distinct(rec_id, .keep_all = TRUE),
      by = join_by(rec_id)
   ) %>%
   deconstruct_art()

tables$px_record$data %<>%
   mutate(
      disease = '101000',
      module  = 3
   )
tables$px_service$data %<>%
   mutate(
      service_type = '101201',
   )

db_conn <- ohasis$conn("db")
dbxDelete(
   db_conn,
   Id(schema = "ohasis", table = "px_medicine"),
   art$data$forUpload %>%
      filter(!is.na(medicine_summary)) %>%
      select(rec_id),
   batch_size = 1000
)
lapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
}, db_conn)
dbDisconnect(db_conn)


art$upload()
# art$addNewPatients()

db_conn     <- ohasis$conn("db")
table_space <- Id(schema = "ohasis_interim", table = 'px_medicine')
dbxUpsert(
   db_conn,
   table_space,
   art$tables$px_medicine$data %>%
      mutate(
         per_day   = case_when(
            medicine == '2029' ~ 1,
            medicine == '2023' ~ 2,
            medicine == '2011' ~ 2,
            medicine == '2004' ~ 1,
            medicine == '2009' ~ 4,
            medicine == '2035' ~ 1,
            medicine == '2015' ~ 1,
            medicine == '2018' ~ 2,
            medicine == '2016' ~ 1,
            medicine == '2014' ~ 2,
            medicine == '2002' ~ 2,
            TRUE ~ 1
         ),

         next_date = disp_date %m+% days(as.numeric(disp_total) / per_day),
      ),
   art$tables$px_medicine$pk
)
table_space <- Id(schema = "ohasis_interim", table = 'px_addr')
dbxUpsert(db_conn, table_space, art$tables$px_addr$data, art$tables$px_addr$pk)
table_space <- Id(schema = "ohasis_interim", table = 'px_labs')
dbxUpsert(db_conn, table_space, art$tables$px_labs$data, art$tables$px_labs$pk)
dbDisconnect(db_conn)

timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
try       <- art$data$forUpload %>%
   select(-rec_id) %>%
   # get records id if existing
   left_join(
      y  = art$data$existing %>%
         select(
            rec_id,
            created_by,
            created_at,
            central_id,
            visit_date
         ),
      by = join_by(central_id, visit_date)
   ) %>%
   # retain only not uploaded and those with changes
   filter(!is.na(patient_id), !is.na(medicine_summary)) %>%
   anti_join(
      y  = art$data$existing %>% filter(!is.na(medicine_summary)),
      by = join_by(rec_id, visit_date),
   ) %>%
   mutate(
      old_rec    = if_else(!is.na(rec_id), 1, 0, 0),
      created_by = coalesce(created_by, "1300000048"),
      created_at = coalesce(created_at.x, created_at.y),
      created_at = coalesce(as.character(created_at), timestamp),
      updated_by = if_else(old_rec == 1, "1300000048", NA_character_),
      updated_at = if_else(old_rec == 1, timestamp, NA_character_)
   ) %>%
   relocate(any_of(names(art$data$existing)), .before = 1) %>%
   select(-old_rec) %>%
   distinct(row_id, .keep_all = TRUE)

write_rds(art, "H:/20250602_qc-arv.rds")

self$data$masterlist %>%
   mutate(ml_num = parse_integer(ml_num)) %>%
   arrange(enroll_hub, ml_num, data_main_id) %>%
   relocate(data_main_id, .before = ml_num) %>%
   View('ml')
self$issues$not_in_ml %>%
   filter(year(latest_ffupdate) == 2025, latest_ffupdate <= '2025-04-30') %>%
   relocate(data_main_id, .before = ml_num) %>%
   View('2025')

# mlnum + pxcode
data_main <- self$data$pii %>%
   filter(!is.na(data_main_id)) %>%
   distinct(
      ml_num,
      patient_code,
      .keep_all = TRUE
   ) %>%
   select(
      data_main_id,
      ml_num,
      patient_code,
   )

need_corr <- self$issues$not_in_ml %>%
   left_join(
      y = data_main %>%
         select(
            corr_data_main = data_main_id,
            ml_num,
            patient_code,
         )
   ) %>%
   filter(data_main_id != corr_data_main) %>%
   relocate(data_main_id, .before = ml_num) %>%
   relocate(corr_data_main, .before = data_main_id)

# mlnum + datamain
data_main <- self$data$pii %>%
   filter(!is.na(data_main_id)) %>%
   distinct(
      ml_num,
      data_main_id,
      .keep_all = TRUE
   ) %>%
   select(
      data_main_id,
      ml_num,
      patient_code,
   )

need_corr <- self$issues$not_in_ml %>%
   left_join(
      y = data_main %>%
         select(
            corr_pxcode = patient_code,
            ml_num,
            data_main_id,
         )
   ) %>%
   filter(patient_code != corr_pxcode) %>%
   relocate(data_main_id, .before = ml_num) %>%
   relocate(corr_pxcode, .before = patient_code)


apply(need_corr, 1, function(row) {
   cell <- row[["pxcode_cell"]]
   cell <- str_replace(cell, "^bk", "bi")

   data <- data.frame(data_main_id = as.integer(row[['corr_data_main']]))

   range_write(
      "1S5We4abrrTYE-VFFa7fr4uvOWtyRuZXHplacUtlSAi4",
      data,
      "Form Responses 1",
      cell,
      col_names = FALSE
   )
})

need_corr %>%
   select(
      pxcode_cell,
      ml_pxcode          = corr_pxcode,
      shc_klinika_pxcode = patient_code,
      data_main_id,
      ml_num,
      first,
      middle,
      last,
      birthdate,
      sex,
      confirmatory_code,
      enroll_hub
   ) %>%
   write_sheet(
      "1ekHFrKJDcrtrtSdmZcEmzLUdWcDA3OSOCUXPRb5XgZ8",
      "qc-pxcode_mismatch"
   )

self$data$converted %>% tab(arv_regimen)
self$data$masterlist %>% get_dupes(patient_code)

pii <- new %>%
   select(
      row_id,
      ml_num,
      patient_code,
      last,
      first,
      middle,
      uic_mom,
      uic_dad,
      uic_order,
      birthdate,
      sex,
      curr_brgy,
      curr_munc,
      philhealth_no,
      confirmatory_code,
   ) %>%
   mutate_if(is.character, toupper) %>%
   mutate(
      curr_reg  = "unknown",
      curr_prov = "unknown",
   ) %>%
   left_join(
      y  = addr,
      by = join_by(
         curr_reg == name_reg,
         curr_prov == name_prov,
         curr_munc == name_munc
      )
   ) %>%
   distinct(
      ml_num,
      patient_code,
      last,
      first,
      middle,
      uic_mom,
      uic_dad,
      uic_order,
      birthdate,
      sex,
      corr_name_reg,
      corr_name_prov,
      corr_name_munc,
      philhealth_no,
      confirmatory_code,
      .keep_all = TRUE
   ) %>%
   get_dupes(patient_code, ml_num)


ohasis$upsert(conn, "lake", "qc_clients", try, "row_id")
conn <- connect("ohasis-lw")
ohasis$upsert(
   conn,
   "lake",
   "qc_clients",
   self$data$pii %>%
      mutate(central_id = NA_character_, .before = 1) %>%
      select(
         row_id,
         central_id,
         ml_num,
         patient_code,
         last,
         first,
         middle,
         uic_mom,
         uic_dad,
         uic_order,
         birthdate,
         sex,
         corr_name_reg,
         corr_name_prov,
         corr_name_munc,
         philhealth_no,
         confirmatory_code
      ),
   "row_id"
)
dbDisconnect(conn)

final_import %>%
   tab(medicine_summary)
