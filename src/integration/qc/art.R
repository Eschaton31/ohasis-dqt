QcArt <- R6Class(
   "QcArt",
   public = list(
      root        = "",
      data        = list(
         new       = tibble(),
         update    = tibble(),
         pii       = tibble(),
         ids       = tibble(),
         converted = tibble(),
         existing  = tibble(),
         forUpload = tibble()
      ),
      refs        = list(),
      issues      = list(),
      tables      = list(),

      initialize  = function() {
         self$root <- file.path(getwd(), "data", "qc-imports", format(Sys.time(), "%Y%m%d"))

         invisible(self)
      },
      download    = function() {
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
      getRefs     = function() {
         local_gs4_quiet()

         self$refs$corr_addr <- range_speedread("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "addr", range = "A:F", col_types = cols(.default = "c"))

         invisible(self)
      },
      readNew     = function() {
         self$data$new <- read_ods(file.path(self$root, "art", "arv.ods"), col_types = cols(.default = "c"), .name_repair = "unique_quiet", col_names = FALSE) %>%
            select(
               1:60, 98
            ) %>%
            row_to_names(1) %>%
            mutate(
               row_id        = row_number(),
               artstart_cell = stri_c("AH", row_id + 1),
               ffup_cell     = stri_c("AX", row_id + 1),
            ) %>%
            filter(row_id > 15) %>%
            select(
               row_id,
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
      readUpdate  = function() {
         self$data$update <- read_ods(file.path(self$root, "art", "arv.ods"), col_types = cols(.default = "c"), .name_repair = "unique_quiet", col_names = FALSE) %>%
            select(1:2, 61:68, 70:97, 99) %>%
            row_to_names(1) %>%
            mutate(
               row_id        = row_number(),
               artstart_cell = stri_c("BT", row_id + 1),
               ffup_cell     = stri_c("CJ", row_id + 1),
            ) %>%
            filter(row_id > 15) %>%
            select(
               row_id,
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
      createPii   = function() {
         self$data$pii <- self$data$new %>%
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
               y  = self$refs$corr_addr,
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
            )

         invisible(self)
      },
      convert     = function() {
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
      readIds     = function() {
         con           <- connect("ohasis-lw")
         self$data$ids <- QB$new(con)$from("ohasis_lake.qc_clients")$get()
         dbDisconnect(con)

         invisible(self)
      },
      checkIssues = function() {
         self$issues <- list(
            dupe_px_code = self$data$pii %>% get_dupes(PATIENT_CODE, ML_NUM),
            `not2025`    = self$data$converted %>%
               filter((LATEST_FFUPDATE < "2024-11-01" & year(LATEST_FFUPDATE) != year(CREATED_AT)) | LATEST_FFUPDATE > now())
         )
      }
   )
)

art <- QcArt$new()
# art$download()
art$getRefs()
art$readNew()
art$readUpdate()
art$createPii()
art$convert()
art$checkIssues()

art$data$converted %>% tab(ARV_REGIMEN)

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


data  <- read_excel("C:/Users/Administrator/Downloads/ART_Masterlist.xlsx")
names <- data %>% select(Data_Main_ID, Name)
names %>%
   mutate(
      name         = toupper(Name),
      name         = str_replace_all(name, "N/A", ""),
      name         = str_replace_all(name, "[^[:alnum:],]", " "),
      name         = str_replace_all(name, ",", ", "),
      name         = str_replace_all(name, "\\sJR\\b", "/JR"),
      name         = str_replace_all(name, "\\sII\\b", "/II"),
      name         = str_replace_all(name, "\\sIII\\b", "/III"),
      name         = str_replace_all(name, "\\sIV\\b", "/IV"),
      name         = str_replace_all(name, "\\bSAN\\s", "SAN_"),
      name         = str_replace_all(name, "\\bDE\\s", "DE_"),
      name         = str_replace_all(name, "\\bDEL\\s", "DEL_"),
      name         = str_replace_all(name, "\\bDELA\\s", "DELA_"),
      name         = str_replace_all(name, "\\bDELOS\\s", "DELOS_"),
      name         = str_squish(name),

      with_comma   = str_detect(name, ","),

      last_suffix  = case_when(
         with_comma ~ str_left(name, stri_locate_first_fixed(name, ",") - 1),
         TRUE ~ word(name, -1)
      ),
      last_suffix  = str_replace_all(last_suffix, ",", ""),
      first_middle = case_when(
         with_comma ~ str_replace(name, stri_c("^", last_suffix, ", "), ""),
         TRUE ~ str_replace(name, stri_c(" ", last_suffix, "$"), "")
      ),

      middle       = word(first_middle, -1),
      middle       = if_else(middle == first_middle, NA_character_, middle, middle),

      first        = coalesce(str_replace(first_middle, stri_c(" ", middle, "$"), ""), first_middle),
      first        = str_replace_all(first, "^/", ""),
   ) %>%
   separate_wider_delim(
      last_suffix,
      delim    = "/",
      names    = c("last", "suffix"),
      too_few  = "align_start",
      too_many = "merge"
   ) %>%
   mutate(
      suffix       = coalesce(suffix, str_extract(first, "/(.+)", 1)),
      first        = coalesce(str_replace_all(first, stri_c("/", suffix), ""), first),
   ) %>%
   relocate(first, middle, last, suffix, .after = name) %>%
   mutate_at(
      .vars = vars(first, middle, last, suffix),
      ~str_replace_all(., "_", " ")
   ) %>%
   View()