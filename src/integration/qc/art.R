QcArt <- R6Class(
   "QcArt",
   public  = list(
      root           = "",
      data           = list(
         new        = tibble(),
         update     = tibble(),
         masterlist = tibble(),
         pii        = tibble(),
         ids        = tibble(),
         converted  = tibble(),
         existing   = tibble(),
         forUpload  = tibble()
      ),
      refs           = list(),
      issues         = list(),
      tables         = list(),

      initialize     = function() {
         self$root <- file.path(getwd(), "data", "qc-imports", format(Sys.time(), "%Y%m%d"))

         invisible(self)
      },
      download       = function() {
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
      getRefs        = function() {
         local_gs4_quiet()

         self$refs$corr_addr <- range_speedread("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "addr", range =
            "A:F", col_types                                                                                  = cols(
            .default = "c"))

         invisible(self)
      },
      readNew        = function() {
         self$data$new <- read_ods(file.path(self$root, "art", "arv.ods"), col_types = cols(.default = "c"),
                                   .name_repair                                      = "unique_quiet", col_names =
                                      FALSE) %>%
            select(
               1:60, 98
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
               CREATED_AT           = `Timestamp`,
               REPORT_TYPE          = `Type of Report`,
               ML_NUM               = `Masterlist count`,
               PATIENT_CODE         = `PT CODE`,
               LAST                 = `Last Name`,
               FIRST                = `First Name`,
               MIDDLE               = `Middle Name`,
               UIC_MOM              = `Mother's First Name (First 2letters)`,
               UIC_DAD              = `Father's First Name  (First 2letters)`,
               UIC_ORDER            = `Birth Order`,
               BIRTHDATE            = `Date of Birth`,
               SEX                  = `Sex`,
               CURR_BRGY            = `Barangay`,
               CURR_MUNC            = `City`,
               PHILHEALTH_NO        = `PhilHealth Number`,
               FIRST_VISIT_DATE     = `Date first seen in the facility`,
               CONFIRMATORY_CODE    = `SACCL Code (Confirmatory Code)`,
               DATE_CONFIRM         = `Date of Diagnosis`,
               MOT                  = `Mode of Transmission`,
               WHO_CLASS            = `WHO Clinical Staging`,
               BASELINE_CD4_RESULT  = `Baseline CD4 Count`,
               BASELINE_CD4_DATE    = `Date of Baseline CD4`,
               LATEST_CD4_RESULT    = `Latest CD4 Count`,
               LATEST_CD4_DATE      = `Latest CD4 Count Date`,
               TB_IPT_START_DATE    = `TPT Start Date`,
               TB_IPT_OUTCOME       = `TPT Outcome`,
               TB_CPT_START_DATE    = `CPT Start Date`,
               TB_CPT_OUTCOME       = `CPT Outcome`,
               BASELINE_HEPB_STATUS = `Baseline Hepatitis B Status`,
               PREGNANT_DX          = `Pregnant at the time of diagnosis?`,
               IS_TRANSIN           = `Is the client TRANS-IN?`,
               TRANSIN_HUB          = `Recent Treatment Hub`,
               ARV_REGIMEN          = `ARV REGIMEN`,
               ART_START_DATE       = `Date of Enrollment`,
               ART_STOP_DATE        = `ARV STOP Date`,
               ART_STOP_REASON      = `REASON FOR DISCONTINUING ARV`,
               BASELINE_TB_STATUS   = `BASELINE TB Status`,
               TB_STATUS            = `TB-Current Status/Outcome`,
               COINFECTION_CURR     = `Current Co-Infection`,
               COINFECTION_STATUS   = `Co-Infection Current Status/Outcome`,
               TBDOTS_FACI          = `TB-DOTS Facility`,
               OI_DRUG              = `OI DRUG`,
               OI_START_DATE        = `OI Start Date`,
               IS_DEAD              = `Died?`,
               DATE_OF_DEATH        = `Date of Death`,
               CAUSE_OF_DEATH       = `Cause of Death`,
               IS_TRANSOUT          = `Trans-out?`,
               TRANSOUT_DATE        = `Date Trans-out`,
               TRANSOUT_HUB         = `Hub of Trans-out`,
               LATEST_FFUPDATE      = `Date of Last follow up`,
               DISP_TOTAL           = `Number of tablets given`,
               DX_LAB               = `SHC/Lab of Diagnosis`,
               ENROLL_HUB           = `SHC/Lab enrolled`,
               CM_DATE_ENROLL       = `Date enrolled on CM`,
               CM_NAME              = `Name of Case Manager`,
               VACCINATIONS         = `Vaccinations Given:`,
               BASELINE_VL_DATE     = `Date of Baseline Viral Load`,
               BASELINE_VL_RESULT   = `Baseline Viral Load`,
               LAST_VL_DATE         = `Date of Latest Viral Load`,
               LAST_VL_RESULT       = `Latest Viral Load`,
               BASELINE_HEPC_STATUS = `Baseline Hepatitis C Status`,
            ) %>%
            filter(REPORT_TYPE == "New Case Enrollment")

         invisible(self)
      },
      readUpdate     = function() {
         self$data$update <- read_ods(file.path(self$root, "art", "arv.ods"), col_types = cols(.default = "c"),
                                      .name_repair                                      = "unique_quiet", col_names =
                                         FALSE) %>%
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
               CREATED_AT           = `Timestamp`,
               REPORT_TYPE          = `Type of Report`,
               REPORT_TYPE          = `Type of Report`,
               DATA_MAIN_ID         = `Data_Main_ID`,
               ML_NUM               = `Masterlist count`,
               PATIENT_CODE         = `PT CODE`,
               PHILHEALTH_NO        = `PhilHealth Number`,
               LATEST_CD4_DATE      = `Latest CD4 Count Date`,
               TB_IPT_START_DATE    = `TPT Start Date`,
               TB_IPT_OUTCOME       = `TPT Outcome`,
               TB_CPT_START_DATE    = `CPT Start Date`,
               BASELINE_HEPB_STATUS = `Baseline Hepatitis B Status`,
               ARV_REGIMEN          = `ARV REGIMEN`,
               ART_START_DATE       = `Date of Enrollment`,
               ART_STOP_DATE        = `ARV STOP DATE`,
               DISC_REASON_OTHER    = `Reason for Discontinue ARV`,
               BASELINE_TB_STATUS   = `Baseline TB status`,
               TB_STATUS            = `TB-Current Status/Outcome`,
               COINFECTION_CURR     = `Current Co-infection`,
               COINFECTION_STATUS   = `Co-infection Current Status/Outcome`,
               TBDOTS_FACI          = `TB-DOTS Facility`,
               OI_DRUG              = `OI DRUG`,
               OI_START_DATE        = `OI start Date`,
               IS_DEAD              = `Died?`,
               DATE_OF_DEATH        = `Date of Death`,
               CAUSE_OF_DEATH       = `Cause of Death`,
               IS_TRANSOUT          = `Trans-out?`,
               TRANSOUT_DATE        = `Date Trans-out`,
               TRANSOUT_HUB         = `Hub of Trans-out`,
               LATEST_FFUPDATE      = `Date of last follow up`,
               DISP_TOTAL           = `No. of tablets given`,
               VACCINATIONS         = `Vaccinations Given`,
               LAST_VL_DATE         = `Date of Last Viral Load`,
               LAST_VL_RESULT       = `Latest Viral Load`,
               OTHERS               = `Others:`,
               LATEST_CD4_RESULT    = `Latest CD4 Count`,
               CONFIRM_DATE_1       = `Date of Confirmator`,
               CONFIRM_DATE_2       = `Date of Confirmatory`,
               BASELINE_HEPC_STATUS = `Baseline Hepatitis C Status`,
               LTFU_REASON          = `If LTFU patient, reason:`,
            ) %>%
            filter(REPORT_TYPE == "Update Case Details")

         invisible(self)
      },
      readMasterlist = function(path) {
         self$data$masterlist <- read_excel(path, col_types = "text") %>%
            bind_rows(read_sheet("1qyaXK3u0UTlSlbYjILHhfGGGilB1S8qFN2m1-eTpzzE", "xl", col_types = "c")) %>%
            select(
               DATA_MAIN_ID         = `Data_Main_ID`,
               ML_NUM               = `Masterlist count`,
               PATIENT_CODE         = `PT CODE`,
               NAME                 = `Name`,
               UIC                  = `UIC`,
               BIRTHDATE            = `DATE OF BIRTH (dd/mm/yyyy)`,
               SEX                  = `SEX`,
               CURR_BRGY            = `BRGY`,
               CURR_MUNC            = `CITY`,
               PHILHEALTH_NO        = `PhilHEalth Number`,
               FIRST_VISIT_DATE     = `Date first seen in the facility`,
               CONFIRMATORY_CODE    = `SACCL Code (Confirmatory Code)`,
               DATE_CONFIRM         = `Date of Diagnosis (dd/mm/yyyy)`,
               AGE_DX               = `Age of diagnosis`,
               MOT                  = `Mode of Transmission`,
               WHO_CLASS            = `WHO Clinical Staging`,
               BASELINE_CD4_RESULT  = `Baseline CD4 count`,
               BASELINE_CD4_DATE    = `Date of Baseline CD4`,
               LATEST_CD4_RESULT    = `Latest CD4 Count`,
               LATEST_CD4_DATE      = `Latest CD4 Count Date`,
               TB_IPT_START_DATE    = `TPT Start Date`,
               TB_IPT_OUTCOME       = `TPT Outcome`,
               TB_CPT_START_DATE    = `CPT Start Date`,
               TB_CPT_OUTCOME       = `CPT Outcome`,
               BASELINE_HEPB_STATUS = `Baseline Hepatitis B Status`,
               PREGNANT_DX          = `Pregnant at the time of diagnosis? (Y/N) for female`,
               IS_TRANSIN           = `Is the client TRANS-IN? (Y/N)`,
               TRANSIN_HUB          = `Recent Treatment Hub`,
               ARV_REGIMEN          = `ARV REGIMEN`,
               ART_START_DATE       = `DATE OF ENROLLMENT`,
               ART_STOP_DATE        = `ARV STOP DATE`,
               ART_STOP_REASON      = `REASON FOR DISCONTINUING ARV`,
               BASELINE_TB_STATUS   = `BASELINE TB status`,
               TB_STATUS            = `TB- Current Status/ Outcome`,
               COINFECTION_CURR     = `Current Co-infection`,
               COINFECTION_STATUS   = `Co-infection Current Status/Outcome`,
               TBDOTS_FACI          = `TB-DOTS Facility`,
               OI_DRUG              = `OI DRUG`,
               OI_START_DATE        = `OI START DATE`,
               IS_DEAD              = `Died? (yes or No)`,
               DATE_OF_DEATH        = `Date of Death`,
               CAUSE_OF_DEATH       = `Cause of death?`,
               IS_TRANSOUT          = `Trans-out? (yes or No)2`,
               TRANSOUT_DATE        = `Date Trans-out`,
               TRANSOUT_HUB         = `Hub of Trans-out`,
               LATEST_FFUPDATE      = `Date of last ff-up (dd/mm/yyyy)`,
               DISP_TOTAL           = `No of tablets given`,
               LATEST_NEXTPICKUP    = `Expected  refill date`,
               DX_LAB               = `SHC/Lab of Diagnosis`,
               ENROLL_HUB           = `SHC/Lab Enrolled`,
               CM_DATE_ENROLL       = `Date Enrolled on CM`,
               CM_NAME              = `Name of Case Manager`,
               VACCINATIONS         = `Vaccinations Given:`,
               BASELINE_VL_DATE     = `Date of Baseline Viral Load`,
               BASELINE_VL_RESULT   = `Baseline Viral Load`,
               LATEST_VL_DATE       = `Date of Latest Viral Load`,
               LATEST_VL_RESULT     = `Latest Viral Load`,
               IS_VL_TESTED         = `Viral Load Tested?`,
               IS_VL_SUPPRESSED     = `Virally Suppressed`,
            ) %>%
            split_names(NAME, FIRST, MIDDLE, LAST) %>%
            mutate_at(
               .vars = vars(contains("date")),
               ~excel_numeric_to_date(parse_number(.))
            )

         invisible(self)
      },
      createPii      = function() {
         self$data$pii <- self$data$new %>%
            select(
               ML_NUM,
               PATIENT_CODE,
               LAST,
               FIRST,
               MIDDLE,
               UIC_MOM,
               UIC_DAD,
               UIC_ORDER,
               BIRTHDATE,
               SEX,
               CURR_BRGY,
               CURR_MUNC,
               PHILHEALTH_NO,
               CONFIRMATORY_CODE,
            ) %>%
            mutate_if(is.character, toupper) %>%
            mutate(
               BIRTHDATE = as.Date(parse_date_time(BIRTHDATE, "mdY")),
               UIC       = stri_c(
                  stri_pad_right(str_left(UIC_MOM, 2), 2, "X"),
                  stri_pad_right(str_left(UIC_DAD, 2), 2, "X"),
                  stri_pad_left(str_left(UIC_ORDER, 2), 2, "0"),
                  format(BIRTHDATE, "%Y%m%d")
               ),
               .before   = BIRTHDATE
            ) %>%
            select(-UIC_MOM, -UIC_DAD, -UIC_ORDER) %>%
            mutate(
               CURR_PROV = "UNKNOWN",
               CURR_REG  = "UNKNOWN",
               .after    = CURR_MUNC
            ) %>%
            left_join(
               y  = self$refs$corr_addr,
               by = join_by(
                  CURR_REG == NAME_REG,
                  CURR_PROV == NAME_PROV,
                  CURR_MUNC == NAME_MUNC
               )
            ) %>%
            bind_rows(
               self$data$masterlist %>%
                  select(
                     DATA_MAIN_ID,
                     ML_NUM,
                     PATIENT_CODE,
                     LAST,
                     FIRST,
                     MIDDLE,
                     UIC,
                     BIRTHDATE,
                     SEX,
                     CURR_BRGY,
                     CURR_MUNC,
                     PHILHEALTH_NO,
                     CONFIRMATORY_CODE,
                  ) %>%
                  mutate(
                     CURR_PROV = "UNKNOWN",
                     CURR_REG  = "UNKNOWN",
                     .after    = CURR_MUNC
                  ) %>%
                  left_join(
                     y  = self$refs$corr_addr,
                     by = join_by(
                        CURR_REG == NAME_REG,
                        CURR_PROV == NAME_PROV,
                        CURR_MUNC == NAME_MUNC
                     )
                  )
            ) %>%
            distinct(
               DATA_MAIN_ID,
               ML_NUM,
               PATIENT_CODE,
               LAST,
               FIRST,
               MIDDLE,
               UIC,
               BIRTHDATE,
               SEX,
               CORR_NAME_REG,
               CORR_NAME_PROV,
               CORR_NAME_MUNC,
               PHILHEALTH_NO,
               CONFIRMATORY_CODE,
               .keep_all = TRUE
            ) %>%
            mutate(
               SEX = case_when(
                  SEX == "M" ~ "MALE",
                  SEX == "F" ~ "FEMALE",
                  TRUE ~ SEX
               )
            ) %>%
            left_join(
               y  = self$data$ids %>%
                  filter(!is.na(CENTRAL_ID)) %>%
                  distinct(
                     CENTRAL_ID,
                     DATA_MAIN_ID,
                     ML_NUM,
                     PATIENT_CODE,
                     LAST,
                     FIRST,
                     MIDDLE,
                     UIC,
                     BIRTHDATE,
                     SEX,
                     CORR_NAME_REG,
                     CORR_NAME_PROV,
                     CORR_NAME_MUNC,
                     PHILHEALTH_NO,
                     CONFIRMATORY_CODE,
                  ),
               by = join_by(
                  ML_NUM,
                  DATA_MAIN_ID,
                  PATIENT_CODE,
                  LAST,
                  FIRST,
                  MIDDLE,
                  UIC,
                  BIRTHDATE,
                  SEX,
                  CORR_NAME_REG,
                  CORR_NAME_PROV,
                  CORR_NAME_MUNC,
                  PHILHEALTH_NO,
                  CONFIRMATORY_CODE,
               )
            ) %>%
            relocate(CENTRAL_ID, .before = 1)

         invisible(self)
      },
      convert        = function() {
         self$data$converted <- self$data$new %>%
            bind_rows(self$data$update) %>%
            relocate(LATEST_FFUPDATE, .after = PATIENT_CODE) %>%
            mutate(
               CREATED_AT = parse_date_time(CREATED_AT, "mdYHMS")
            ) %>%
            mutate_at(
               .vars = vars(contains("DATE")),
               ~str_replace_all(., "0024", "2024") %>%
                  str_replace_all("0025", "2025")
            ) %>%
            mutate_at(
               .vars = vars(contains("DATE")),
               ~as.Date(parse_date_time(., "mdY"))
            )

         invisible(self)
      },
      readIds        = function() {
         con           <- connect("ohasis-lw")
         self$data$ids <- QB$new(con)$from("ohasis_lake.qc_clients")$get()
         dbDisconnect(con)

         invisible(self)
      },
      checkIssues    = function() {
         self$issues <- list(
            dupe_px_code = self$data$pii %>% get_dupes(PATIENT_CODE, ML_NUM),
            `not2025`    = self$data$converted %>%
               filter(
                  (LATEST_FFUPDATE < "2024-11-01" & year(LATEST_FFUPDATE) != year(CREATED_AT))
                     | LATEST_FFUPDATE > now()
               ),
            `clean_addr` = self$data$pii %>%
               filter(is.na(CORR_NAME_REG)) %>%
               distinct(CURR_REG, CURR_PROV, CURR_MUNC),
            `no_cid`     = self$data$pii %>%
               filter(is.na(CENTRAL_ID)),
            `not_in_ml`  = self$data$converted %>%
               mutate(
                  PATIENT_CODE = private$cleanPatientCode(PATIENT_CODE, DATA_MAIN_ID)
               ) %>%
               anti_join(
                  art$data$pii %>%
                     mutate(
                        PATIENT_CODE = private$cleanPatientCode(PATIENT_CODE, DATA_MAIN_ID)
                     ) %>%
                     distinct(DATA_MAIN_ID, PATIENT_CODE)
               ) %>%
               anti_join(
                  art$data$pii %>%
                     mutate(
                        PATIENT_CODE = private$cleanPatientCode(PATIENT_CODE, DATA_MAIN_ID)
                     ) %>%
                     distinct(DATA_MAIN_ID, ML_NUM, PATIENT_CODE)
               )
         )
      },
      addNewPatients = function() {
         max_id <- max(art$data$ids$row_id)
         new    <- self$data$pii %>%
            filter(is.na(CENTRAL_ID)) %>%
            mutate(
               row_id = max_id + row_number()
            ) %>%
            distinct(
               row_id,
               DATA_MAIN_ID,
               ML_NUM,
               PATIENT_CODE,
               LAST,
               FIRST,
               MIDDLE,
               UIC,
               BIRTHDATE,
               SEX,
               CORR_NAME_REG,
               CORR_NAME_PROV,
               CORR_NAME_MUNC,
               PHILHEALTH_NO,
               CONFIRMATORY_CODE,
            ) %>%
            mutate(
               FACI_ID = '130000',
               drop    = is.na(UIC) & is.na(BIRTHDATE) & is.na(FIRST)
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
                  CENTRAL_ID = PATIENT_ID,
                  DATA_MAIN_ID,
                  ML_NUM,
                  PATIENT_CODE,
                  LAST,
                  FIRST,
                  MIDDLE,
                  UIC,
                  BIRTHDATE,
                  SEX,
                  CORR_NAME_REG,
                  CORR_NAME_PROV,
                  CORR_NAME_MUNC,
                  PHILHEALTH_NO,
                  CONFIRMATORY_CODE,
               ),
            "row_id"
         )
         dbDisconnect(con)

         invisible(self)
      }
   ),
   private = list(
      cleanPatientCode = function(column, dataMain) {
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
      }
   )
)

art <- QcArt$new()
# art$download()
art$getRefs()
art$readIds()
art$readNew()
art$readUpdate()
art$readMasterlist("C:/Users/Bene-G16/Downloads/ART Masterlist-QC.xlsx")
art$createPii()
art$convert()
art$checkIssues()
# art$addNewPatients()

write_rds(art, "H:/20250602_qc-arv.rds")

art$data$masterlist %>%
   mutate(ML_NUM = parse_integer(ML_NUM)) %>%
   arrange(ENROLL_HUB, ML_NUM, DATA_MAIN_ID) %>%
   relocate(DATA_MAIN_ID, .before = ML_NUM) %>%
   View('ml')
art$issues$not_in_ml %>%
   filter(year(LATEST_FFUPDATE) == 2025, LATEST_FFUPDATE <= '2025-04-30') %>%
   relocate(DATA_MAIN_ID, .before = ML_NUM) %>%
   View('2025')

# mlnum + pxcode
data_main <- art$data$pii %>%
   filter(!is.na(DATA_MAIN_ID)) %>%
   distinct(
      ML_NUM,
      PATIENT_CODE,
      .keep_all = TRUE
   ) %>%
   select(
      DATA_MAIN_ID,
      ML_NUM,
      PATIENT_CODE,
   )

need_corr <- art$issues$not_in_ml %>%
   left_join(
      y = data_main %>%
         select(
            CORR_DATA_MAIN = DATA_MAIN_ID,
            ML_NUM,
            PATIENT_CODE,
         )
   ) %>%
   filter(DATA_MAIN_ID != CORR_DATA_MAIN) %>%
   relocate(DATA_MAIN_ID, .before = ML_NUM) %>%
   relocate(CORR_DATA_MAIN, .before = DATA_MAIN_ID)

# mlnum + datamain
data_main <- art$data$pii %>%
   filter(!is.na(DATA_MAIN_ID)) %>%
   distinct(
      ML_NUM,
      DATA_MAIN_ID,
      .keep_all = TRUE
   ) %>%
   select(
      DATA_MAIN_ID,
      ML_NUM,
      PATIENT_CODE,
   )

need_corr <- art$issues$not_in_ml %>%
   left_join(
      y = data_main %>%
         select(
            CORR_PXCODE = PATIENT_CODE,
            ML_NUM,
            DATA_MAIN_ID,
         )
   ) %>%
   filter(PATIENT_CODE != CORR_PXCODE) %>%
   relocate(DATA_MAIN_ID, .before = ML_NUM) %>%
   relocate(CORR_PXCODE, .before = PATIENT_CODE)


apply(need_corr, 1, function(row) {
   cell <- row[["pxcode_cell"]]
   cell <- str_replace(cell, "^BK", "BI")

   data <- data.frame(DATA_MAIN_ID = as.integer(row[['CORR_DATA_MAIN']]))

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
      ML_PXCODE          = CORR_PXCODE,
      SHC_KLINIKA_PXCODE = PATIENT_CODE,
      DATA_MAIN_ID,
      ML_NUM,
      FIRST,
      MIDDLE,
      LAST,
      BIRTHDATE,
      SEX,
      CONFIRMATORY_CODE,
      ENROLL_HUB
   ) %>%
   write_sheet("1ekHFrKJDcrtrtSdmZcEmzLUdWcDA3OSOCUXPRb5XgZ8", "qc-pxcode_mismatch")

art$data$converted %>% tab(ARV_REGIMEN)
art$data$masterlist %>% get_dupes(PATIENT_CODE)

pii <- new %>%
   select(
      row_id,
      ML_NUM,
      PATIENT_CODE,
      LAST,
      FIRST,
      MIDDLE,
      UIC_MOM,
      UIC_DAD,
      UIC_ORDER,
      BIRTHDATE,
      SEX,
      CURR_BRGY,
      CURR_MUNC,
      PHILHEALTH_NO,
      CONFIRMATORY_CODE,
   ) %>%
   mutate_if(is.character, toupper) %>%
   mutate(
      CURR_REG  = "UNKNOWN",
      CURR_PROV = "UNKNOWN",
   ) %>%
   left_join(
      y  = addr,
      by = join_by(
         CURR_REG == NAME_REG,
         CURR_PROV == NAME_PROV,
         CURR_MUNC == NAME_MUNC
      )
   ) %>%
   distinct(
      ML_NUM,
      PATIENT_CODE,
      LAST,
      FIRST,
      MIDDLE,
      UIC_MOM,
      UIC_DAD,
      UIC_ORDER,
      BIRTHDATE,
      SEX,
      CORR_NAME_REG,
      CORR_NAME_PROV,
      CORR_NAME_MUNC,
      PHILHEALTH_NO,
      CONFIRMATORY_CODE,
      .keep_all = TRUE
   ) %>%
   get_dupes(PATIENT_CODE, ML_NUM)


ohasis$upsert(conn, "lake", "qc_clients", try, "row_id")
conn <- connect("ohasis-lw")
ohasis$upsert(conn, "lake", "qc_clients", art$data$pii %>%
   mutate(CENTRAL_ID = NA_character_, .before = 1) %>%
   select(
      row_id,
      CENTRAL_ID,
      ML_NUM,
      PATIENT_CODE,
      LAST,
      FIRST,
      MIDDLE,
      UIC_MOM,
      UIC_DAD,
      UIC_ORDER,
      BIRTHDATE,
      SEX,
      CORR_NAME_REG,
      CORR_NAME_PROV,
      CORR_NAME_MUNC,
      PHILHEALTH_NO,
      CONFIRMATORY_CODE
   ), "row_id")
dbDisconnect(conn)

art$data$update %>%
   distinct(PATIENT_CODE, ML_NUM) %>%
   mutate_if(is.character, toupper) %>%
   anti_join(art$data$pii)

art$data$converted %>%
   filter(LATEST_FFUPDATE < "2024-11-01")

art$issues$not_in_ml %>%
   relocate(DATA_MAIN_ID, .before = ML_NUM) %>%
   write_sheet("1ekHFrKJDcrtrtSdmZcEmzLUdWcDA3OSOCUXPRb5XgZ8", "qc-not_in_ml")
art$data$masterlist %>%
   relocate(DATA_MAIN_ID, .before = ML_NUM) %>%
   write_sheet("1ekHFrKJDcrtrtSdmZcEmzLUdWcDA3OSOCUXPRb5XgZ8", "qc-ml")
art$data$masterlist %>%
   get_dupes(PATIENT_CODE, ML_NUM)

art$data$masterlist %>%
   mutate(
      PATIENT_CODE = str_replace_all(PATIENT_CODE, "^AJMSHC\\s", "AJMSHC"),
      PATIENT_CODE = str_replace_all(PATIENT_CODE, "^BAT\\s", "BAT"),
      PATIENT_CODE = str_replace_all(PATIENT_CODE, "^BSHC\\s(?=-)", "BSHC-"),
      PATIENT_CODE = str_replace_all(PATIENT_CODE, "^KBAT(?= )", "KBAT "),
      PATIENT_CODE = str_replace_all(PATIENT_CODE, "^KE(?= )", "KE "),
   )
