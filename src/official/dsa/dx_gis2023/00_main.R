pacman::p_load(
   dplyr,
   sf,
   viridis,
   leaflet,
   leaflegend,
   RColorBrewer,
   mapview,
   webshot2,
   nngeo
)
dsa    <- new.env()
epictr <- new.env()

# set params
dsa$dirs      <- list()
dsa$dirs$refs <- file.path(getwd(), "src", "official", "hivepicenter")
dsa$dirs$wd   <- file.path(getwd(), "src", "official", "harp_maps")

# get references
source(file.path(dsa$dirs$refs, "00_refs.R"))
source(file.path(dsa$dirs$refs, "02_estimates.R"))

# read gis files
dsa$spdf            <- list()
dsa$spdf$raw        <- read_sf(Sys.getenv("GEOJSON_PH"))
dsa$spdf$merged_mla <- dsa$spdf$raw %>%
   # merge manila geometry into one
   mutate(
      ADM2_EN    = case_when(
         ADM3_PCODE == "PH129804000" ~ "Cotabato",
         TRUE ~ ADM2_EN
      ),
      ADM2_PCODE = case_when(
         ADM3_PCODE == "PH129804000" ~ "PH124700000",
         TRUE ~ ADM2_PCODE
      ),
      ADM3_EN    = case_when(
         ADM2_PCODE == "PH133900000" ~ ADM2_EN,
         TRUE ~ ADM3_EN
      ),
      ADM3_PCODE = case_when(
         ADM2_PCODE == "PH133900000" ~ ADM2_PCODE,
         TRUE ~ ADM3_PCODE
      ),
   ) %>%
   group_by(ADM1_PCODE, ADM1_EN, ADM2_PCODE, ADM2_EN, ADM3_PCODE, ADM3_EN) %>%
   summarize(
      geometry = st_union(st_make_valid(geometry))
   ) %>%
   ungroup() %>%
   mutate_at(
      .vars = vars(starts_with("ADM")),
      ~gsub("^PH", "", .)
   ) %>%
   rename(
      NAME_REG  = ADM1_EN,
      PSGC_REG  = ADM1_PCODE,
      NAME_PROV = ADM2_EN,
      PSGC_PROV = ADM2_PCODE,
      NAME_MUNC = ADM3_EN,
      PSGC_MUNC = ADM3_PCODE,
   )

dsa$spdf$aem <- dsa$spdf$merged_mla %>%
   left_join(
      y = epictr$ref_addr %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            PSGC_AEM,
            NAME_AEM
         )
   ) %>%
   group_by(PSGC_REG, NAME_REG, PSGC_PROV, NAME_PROV, PSGC_AEM, NAME_AEM) %>%
   summarize(
      geometry = st_union(st_make_valid(geometry))
   ) %>%
   ungroup()


##  OHASIS Data ----------------------------------------------------------------

lw_conn   <- ohasis$conn("lw")
min       <- "2021-01-01"
max       <- "2023-03-31"
hts_where <- glue(r"(
   (RECORD_DATE BETWEEN '{min}' AND '{max}') OR
      (DATE(DATE_CONFIRM) BETWEEN '{min}' AND '{max}') OR
      (DATE(T3_DATE) BETWEEN '{min}' AND '{max}') OR
      (DATE(T2_DATE) BETWEEN '{min}' AND '{max}') OR
      (DATE(T1_DATE) BETWEEN '{min}' AND '{max}') OR
      (DATE(T0_DATE) BETWEEN '{min}' AND '{max}')
   )")
cbs_where <- glue(r"(
   (RECORD_DATE BETWEEN '{min}' AND '{max}') OR
      (DATE(TEST_DATE) BETWEEN '{min}' AND '{max}')
   )")
dbname    <- "ohasis_warehouse"
dsa$oh    <- list(
   id_reg = dbTable(lw_conn, dbname, "id_registry"),
   a      = dbTable(lw_conn, dbname, "form_a", where = hts_where, raw_where = TRUE),
   hts    = dbTable(lw_conn, dbname, "form_hts", where = hts_where, raw_where = TRUE),
   cfbs   = dbTable(lw_conn, dbname, "form_cfbs", where = cbs_where, raw_where = TRUE)
)
dbDisconnect(lw_conn)
rm(lw_conn, hts_where, cbs_where, min, max, dbname)

##  HARP Data ------------------------------------------------------------------

dsa$harp <- list(
   dx  = read_dta(hs_data("harp_dx", "reg", 2023, 3)),
   est = epictr$estimates$cata_rotp %>%
      select(
         PSGC_REG,
         PSGC_PROV,
         PSGC_AEM,
         report_yr,
         est,
      ) %>%
      pivot_wider(
         id_cols      = c(PSGC_REG, PSGC_PROV, PSGC_AEM),
         names_from   = report_yr,
         names_prefix = "est",
         values_from  = est
      ) %>%
      mutate_at(
         .vars = vars(starts_with("PSGC")),
         ~gsub("^PH", "", .)
      )
)
dsa$harp <- lapply(dsa$harp, function(data) data %<>% get_cid(dsa$oh$id_reg, PATIENT_ID))

##  PEPFAR Sites ---------------------------------------------------------------

dsa$sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw") %>%
   filter(site_epic_2022 == 1) %>%
   distinct(FACI_ID) %>%
   mutate(
      site_epic = 1
   )

##  GIS Data Table 6 -----------------------------------------------------------

dsa$gis$data    <- list()
dsa$gis$data$dx <- dsa$harp$dx %>%
   mutate(
      overseas_addr = case_when(
         muncity == "OUT OF COUNTRY" ~ 1,
         TRUE ~ 0
      ),

      muncity       = case_when(
         muncity == "PINAMUNGAHAN" & province == "CEBU" ~ "PINAMUNGAJAN",
         muncity == "SAN JUAN" & province == "BULACAN" ~ "MALOLOS",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ muncity
      ),
      province      = case_when(
         muncity == "UNKNOWN" & province == "NCR" ~ "UNKNOWN",
         province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ province
      ),
      region        = case_when(
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ region
      ),
   ) %>%
   select(-contains("PSGC")) %>%
   left_join(
      y  = epictr$ref_addr %>%
         select(
            region      = NHSSS_REG,
            province    = NHSSS_PROV,
            muncity     = NHSSS_MUNC,
            PSGC_AEM,
            MUNCITY_AEM = NAME_AEM
         ),
      by = join_by(region, province, muncity)
   ) %>%
   dxlab_to_id(
      c("HARPDX_FACI", "HARPDX_SUB_FACI"),
      c("dx_region", "dx_province", "dx_muncity", "dxlab_standard")
   ) %>%
   mutate(
      FACILITY_CODE     = HARPDX_FACI,
      FACILITY_SUB_CODE = HARPDX_SUB_FACI,
   ) %>%
   ohasis$get_faci(
      list(DXLAB_STANDARD = c("HARPDX_FACI", "HARPDX_SUB_FACI")),
      "name",
      c("DX_REGION", "DX_PROVINCE", "DX_MUNCITY")
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         mutate(
            drop = case_when(
               StrLeft(PSGC_PROV, 4) == "1339" & (PSGC_MUNC != "133900000" | is.na(PSGC_MUNC)) ~ 1,
               StrLeft(PSGC_REG, 4) == "1300" & PSGC_MUNC == "" ~ 1,
               stri_detect_fixed(NAME_PROV, "City") & NHSSS_MUNC == "UNKNOWN" ~ 1,
               TRUE ~ 0
            ),
         ) %>%
         filter(drop == 0) %>%
         select(
            PERM_PSGC_REG  = PSGC_REG,
            PERM_PSGC_PROV = PSGC_PROV,
            PERM_PSGC_MUNC = PSGC_MUNC,
            region         = NHSSS_REG,
            province       = NHSSS_PROV,
            muncity        = NHSSS_MUNC,
         ) %>%
         distinct_all() %>%
         filter(!is.na(muncity)),
      by = join_by(region, province, muncity)
   ) %>%
   mutate(
      # age group
      AGE_GROUP = case_when(
         age < 15 ~ 0,
         age >= 15 & age < 24 ~ 1,
         age >= 25 & age < 34 ~ 2,
         age >= 35 ~ 3
      ),
      AGE_GROUP = factor(
         AGE_GROUP,
         labels = c(
            "0 - <15",
            "1 - 15-24",
            "2 - 25-34",
            "3 - 35+"
         )
      ),

      # kp
      msm       = if_else(transmit == "SEX" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"), 1, 0, 0),
      tgw       = if_else(sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"), 1, 0, 0),
      KP        = case_when(
         msm == 1 & tgw == 0 ~ 1,
         msm == 1 & tgw == 1 ~ 2,
         transmit != "UNKNOWN" ~ 0
      ),
      KP        = factor(
         KP,
         labels = c(
            "0 - other",
            "1 - MSM",
            "2 - TGW"
         )
      ),

      # sex
      SEX       = case_when(
         sex == "MALE" ~ 1,
         sex == "FEMALE" ~ 0,
      ),
      SEX       = factor(
         SEX,
         labels = c(
            "0 - female",
            "1 - male"
         )
      ),

      # educ level
      EDUC      = case_when(
         highest_educ %in% c(1, 2, 7) ~ 0,
         highest_educ == 3 ~ 1,
         highest_educ %in% c(4, 5, 6) ~ 2,
      ),
      EDUC      = factor(
         EDUC,
         c(
            "0 - NO EDUCATION, ELEMENTARY",
            "1 - HIGH SCHOOL",
            "2 - COLLEGE LEVEL +"
         )
      ),

      # year dx
      YEAR_DX   = if_else(year <= 2017, 2017, year, year) %>% as.integer()
   ) %>%
   mutate(
      TEMP_REG  = PERM_PSGC_REG,
      TEMP_PROV = PERM_PSGC_PROV,
      TEMP_MUNC = PERM_PSGC_MUNC,
   ) %>%
   ohasis$get_addr(
      c(
         REGION   = "TEMP_REG",
         PROVINCE = "TEMP_PROV",
         MUNCITY  = "TEMP_MUNC"
      ),
      "name"
   ) %>%
   select(
      CENTRAL_ID,
      PSGC_REG  = PERM_PSGC_REG,
      PSGC_PROV = PERM_PSGC_PROV,
      PSGC_MUNC = PERM_PSGC_MUNC,
      PSGC_AEM,
      REGION,
      PROVINCE,
      MUNCITY,
      MUNCITY_AEM,
      AGE_GROUP,
      KP,
      SEX,
      EDUC,
      FACILITY_CODE,
      FACILITY_SUB_CODE,
      DXLAB_STANDARD,
      DX_REGION,
      DX_PROVINCE,
      DX_MUNCITY,
      YEAR_DX
   ) %>%
   left_join(dsa$sites, join_by(FACILITY_CODE == FACI_ID))

##  GIS Data Table 7 -----------------------------------------------------------

dsa$oh$hts_all <- process_hts(dsa$oh$hts, dsa$oh$a, dsa$oh$cfbs) %>%
   get_cid(dsa$oh$id_reg, PATIENT_ID) %>%
   mutate_if(
      .predicate = is.POSIXct,
      ~null_dates(., "POSIXct")
   ) %>%
   mutate_if(
      .predicate = is.Date,
      ~null_dates(., "Date")
   ) %>%
   mutate(
      hts_priority = case_when(
         CONFIRM_RESULT %in% c(1, 2, 3) ~ 1,
         hts_result != "(no data)" & src %in% c("hts2021", "a2017") ~ 2,
         hts_result != "(no data)" & hts_modality == "FBT" ~ 3,
         hts_result != "(no data)" & hts_modality == "CBS" ~ 4,
         hts_result != "(no data)" & hts_modality == "FBS" ~ 5,
         hts_result != "(no data)" & hts_modality == "ST" ~ 6,
         TRUE ~ 9999
      )
   ) %>%
   left_join(
      y  = dsa$harp$dx %>%
         mutate(
            ref_report = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
         ) %>%
         dxlab_to_id(
            c("HARPDX_FACI", "HARPDX_SUB_FACI"),
            c("dx_region", "dx_province", "dx_muncity", "dxlab_standard")
         ) %>%
         select(
            CENTRAL_ID,
            idnum,
            transmit,
            sexhow,
            confirm_date,
            ref_report,
            HARPDX_BIRTHDATE  = bdate,
            HARPDX_SEX        = sex,
            HARPDX_SELF_IDENT = self_identity,
            HARPDX_FACI,
            HARPDX_SUB_FACI
         ),
      by = join_by(CENTRAL_ID),
   ) %>%
   mutate(
      # old confirmed
      old_dx = case_when(
         confirm_date >= hts_date ~ 0,
         confirm_date < hts_date ~ 1,
         TRUE ~ 0
      ),
   ) %>%
   mutate(
      PERM_PSGC_REG  = case_when(
         is.na(PERM_PSGC_REG) & !is.na(PERM_PSGC_PROV) ~ stri_pad_right(StrLeft(PERM_PSGC_PROV, 2), 9, "0"),
         is.na(PERM_PSGC_REG) & !is.na(PERM_PSGC_MUNC) ~ stri_pad_right(StrLeft(PERM_PSGC_MUNC, 2), 9, "0"),
         TRUE ~ PERM_PSGC_REG
      ),
      PERM_PSGC_PROV = case_when(
         is.na(PERM_PSGC_PROV) & !is.na(PERM_PSGC_MUNC) ~ stri_pad_right(StrLeft(PERM_PSGC_MUNC, 4), 9, "0"),
         TRUE ~ PERM_PSGC_PROV
      ),
      CURR_PSGC_REG  = case_when(
         is.na(CURR_PSGC_REG) & !is.na(CURR_PSGC_PROV) ~ stri_pad_right(StrLeft(CURR_PSGC_PROV, 2), 9, "0"),
         is.na(CURR_PSGC_REG) & !is.na(CURR_PSGC_MUNC) ~ stri_pad_right(StrLeft(CURR_PSGC_MUNC, 2), 9, "0"),
         TRUE ~ CURR_PSGC_REG
      ),
      CURR_PSGC_PROV = case_when(
         is.na(CURR_PSGC_PROV) & !is.na(CURR_PSGC_MUNC) ~ stri_pad_right(StrLeft(CURR_PSGC_MUNC, 4), 9, "0"),
         TRUE ~ CURR_PSGC_PROV
      ),

      use_curr       = if_else(
         condition = (is.na(PERM_PSGC_MUNC) & !is.na(CURR_PSGC_MUNC)) | StrLeft(PERM_PSGC_MUNC, 2) == '99',
         true      = 1,
         false     = 0
      ),
      PERM_PSGC_REG  = if_else(
         condition = use_curr == 1,
         true      = CURR_PSGC_REG,
         false     = PERM_PSGC_REG
      ),
      PERM_PSGC_PROV = if_else(
         condition = use_curr == 1,
         true      = CURR_PSGC_PROV,
         false     = PERM_PSGC_PROV
      ),
      PERM_PSGC_MUNC = if_else(
         condition = use_curr == 1,
         true      = CURR_PSGC_MUNC,
         false     = PERM_PSGC_MUNC
      ),

      PERM_PSGC_REG  = if_else(!is.na(PERM_PSGC_REG), stri_pad_right(StrLeft(PERM_PSGC_REG, 2), 9, "0"), NA_character_, NA_character_),
      PERM_PSGC_PROV = if_else(!is.na(PERM_PSGC_PROV), stri_pad_right(StrLeft(PERM_PSGC_PROV, 4), 9, "0"), NA_character_, NA_character_),
      PERM_PSGC_PROV = if_else(PERM_PSGC_MUNC == "129804000", "124700000", PERM_PSGC_PROV, PERM_PSGC_PROV),
      PERM_PSGC_MUNC = coalesce(PERM_PSGC_MUNC, ""),
      PERM_PSGC_MUNC = if_else(PERM_PSGC_PROV == "133900000", "133900000", PERM_PSGC_MUNC, PERM_PSGC_MUNC)
   ) %>%
   left_join(
      y  = epictr$ref_addr %>%
         select(
            PERM_PSGC_REG  = PSGC_REG,
            PERM_PSGC_PROV = PSGC_PROV,
            PERM_PSGC_MUNC = PSGC_MUNC,
            PSGC_AEM,
            MUNCITY_AEM    = NAME_AEM
         ),
      by = join_by(PERM_PSGC_REG, PERM_PSGC_PROV, PERM_PSGC_MUNC)
   )

confirm_data <- dsa$oh$hts_all %>%
   mutate(
      FINAL_CONFIRM_DATE = case_when(
         old_dx == 0 & hts_priority == 1 ~ as.Date(coalesce(DATE_CONFIRM, T3_DATE, T2_DATE, T1_DATE)),
         old_dx == 1 ~ confirm_date,
         TRUE ~ NA_Date_
      )
   ) %>%
   filter(!is.na(FINAL_CONFIRM_DATE)) %>%
   select(CENTRAL_ID, FINAL_CONFIRM_DATE) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE)

risk <- dsa$oh$hts_all %>%
   select(
      REC_ID,
      contains("risk", ignore.case = FALSE)
   ) %>%
   pivot_longer(
      cols = contains("risk", ignore.case = FALSE)
   ) %>%
   group_by(REC_ID) %>%
   summarise(
      risks = stri_c(collapse = ", ", unique(sort(value)))
   )

dsa$oh$hts_all %<>%
   left_join(confirm_data, join_by(CENTRAL_ID)) %>%
   left_join(risk, join_by(REC_ID)) %>%
   mutate(
      # tag if central to be used
      use_harpdx      = if_else(
         condition = !is.na(idnum),
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      # tag those without form faci
      use_record_faci = if_else(
         condition = is.na(SERVICE_FACI),
         true      = 1,
         false     = 0
      ),

      # tag which test to be used
      FINAL_FACI      = case_when(
         old_dx == 0 & !is.na(HARPDX_FACI) ~ HARPDX_FACI,
         use_record_faci == 1 ~ FACI_ID,
         TRUE ~ SERVICE_FACI
      ),
      FINAL_SUB_FACI  = case_when(
         old_dx == 0 & !is.na(HARPDX_FACI) ~ HARPDX_SUB_FACI,
         use_record_faci == 1 & FACI_ID == "130000" ~ SPECIMEN_SUB_SOURCE,
         !(SERVICE_FACI %in% c("130001", "130605", "040200")) ~ NA_character_,
         nchar(SERVICE_SUB_FACI) == 6 ~ NA_character_,
         TRUE ~ SERVICE_SUB_FACI
      ),
   )

dsa$gis$data$hts <- dsa$oh$hts_all %>%
   mutate(
      TEMP_REG  = PERM_PSGC_REG,
      TEMP_PROV = PERM_PSGC_PROV,
      TEMP_MUNC = PERM_PSGC_MUNC,
   ) %>%
   ohasis$get_addr(
      c(
         REGION   = "TEMP_REG",
         PROVINCE = "TEMP_PROV",
         MUNCITY  = "TEMP_MUNC"
      ),
      "name"
   ) %>%
   mutate(
      # age group
      BIRTHDATE  = coalesce(HARPDX_BIRTHDATE, BIRTHDATE),
      AGE        = if_else(
         condition = is.na(AGE) & !is.na(AGE_MO),
         true      = AGE_MO / 12,
         false     = as.double(AGE)
      ),
      AGE_DTA    = calc_age(BIRTHDATE, hts_date),
      AGE        = coalesce(AGE, AGE_DTA) %>% floor(),
      AGE_GROUP  = case_when(
         AGE < 15 ~ 0,
         AGE >= 15 & AGE < 24 ~ 1,
         AGE >= 25 & AGE < 34 ~ 2,
         AGE >= 35 ~ 3
      ),
      AGE_GROUP  = factor(
         AGE_GROUP,
         labels = c(
            "0 - <15",
            "1 - 15-24",
            "2 - 25-34",
            "3 - 35+"
         )
      ),

      # kp
      Sex        = coalesce(StrLeft(coalesce(HARPDX_SEX, remove_code(SEX)), 1), "(no data)"),
      msm        = case_when(
         use_harpdx == 1 &
            Sex == "M" &
            sexhow %in% c("HOMOSEXUAL", "BISEXUAL") ~ 1,
         use_harpdx == 0 &
            Sex == "M" &
            grepl("yes-", risk_sexwithm) ~ 1,
         TRUE ~ 0
      ),
      tgw        = case_when(
         use_harpdx == 1 &
            msm == 1 &
            HARPDX_SELF_IDENT %in% c("FEMALE", "OTHERS") ~ 1,
         use_harpdx == 0 &
            msm == 1 &
            keep_code(SELF_IDENT) %in% c("2", "3") ~ 1,
         TRUE ~ 0
      ),
      hetero     = case_when(
         use_harpdx == 1 & sexhow == "HETEROSEXUAL" ~ 1,
         use_harpdx == 0 &
            Sex == "M" &
            !grepl("yes-", risk_sexwithm) &
            grepl("yes-", risk_sexwithf) ~ 1,
         use_harpdx == 0 &
            Sex == "F" &
            grepl("yes-", risk_sexwithm) &
            !grepl("yes-", risk_sexwithf) ~ 1,
         TRUE ~ 0
      ),
      pwid       = case_when(
         use_harpdx == 1 & transmit == "IVDU" ~ 1,
         use_harpdx == 0 & grepl("yes-", risk_injectdrug) ~ 1,
         TRUE ~ 0
      ),
      unknown    = case_when(
         transmit == "UNKNOWN" ~ 1,
         risks == "(no data)" ~ 1,
         TRUE ~ 0
      ),
      KP         = case_when(
         msm == 1 & tgw == 0 ~ 1,
         msm == 1 & tgw == 1 ~ 2,
         unknown == 0 ~ 0
      ),
      KP         = factor(
         KP,
         labels = c(
            "0 - other",
            "1 - MSM",
            "2 - TGW"
         )
      ),

      # sex
      SEX        = case_when(
         Sex == "M" ~ 1,
         Sex == "F" ~ 0,
      ),
      SEX        = factor(
         SEX,
         labels = c(
            "0 - female",
            "1 - male"
         )
      ),

      # educ level
      EDUC_LEVEL = keep_code(EDUC_LEVEL),
      EDUC       = case_when(
         EDUC_LEVEL %in% c(1, 2, 7) ~ 0,
         EDUC_LEVEL == 3 ~ 1,
         EDUC_LEVEL %in% c(4, 5, 6) ~ 2,
      ),
      EDUC       = factor(
         EDUC,
         c(
            "0 - NO EDUCATION, ELEMENTARY",
            "1 - HIGH SCHOOL",
            "2 - COLLEGE LEVEL +"
         )
      ),

      # occupation
      OCCUPATION = keep_code(IS_EMPLOYED),
      OCCUPATION = factor(
         OCCUPATION,
         c(
            "0 - unemployed",
            "1 - employed"
         )
      ),

      # year dx
      year       = year(hts_date),
      YEAR_DX    = if_else(year <= 2017, 2017, year, year) %>% as.integer(),

      # testing data
      MODALITY   = case_when(
         hts_modality == "FBT" ~ 0,
         hts_modality %in% c("FBS", "CBS") ~ 1,
         hts_modality == "ST" ~ 2,
      ),
      MODALITY   = factor(
         MODALITY,
         c(
            "0 - facility-based",
            "1 - community-based",
            "2 - self-testing"
         )
      ),
      RESULT     = case_when(
         hts_modality == "NR" ~ 0,
         hts_modality == "R" ~ 1,
         hts_modality == "IND" ~ 2,
      ),
      RESULT     = factor(
         RESULT,
         c(
            "0 - non-reactive",
            "1 - reactive",
            "2 - indeterminate"
         )
      )
   ) %>%
   mutate(
      FACILITY_CODE     = FINAL_FACI,
      FACILITY_SUB_CODE = FINAL_SUB_FACI,
   ) %>%
   ohasis$get_faci(
      list(DXLAB_STANDARD = c("HARPDX_FACI", "HARPDX_SUB_FACI")),
      "name",
      c("DX_REGION", "DX_PROVINCE", "DX_MUNCITY")
   ) %>%
   select(
      CENTRAL_ID,
      PSGC_REG  = PERM_PSGC_REG,
      PSGC_PROV = PERM_PSGC_PROV,
      PSGC_MUNC = PERM_PSGC_MUNC,
      PSGC_AEM,
      REGION,
      PROVINCE,
      MUNCITY,
      MUNCITY_AEM,
      AGE_GROUP,
      KP,
      SEX,
      EDUC,
      OCCUPATION,
      FACILITY_CODE,
      FACILITY_SUB_CODE,
      DXLAB_STANDARD,
      DX_REGION,
      DX_PROVINCE,
      DX_MUNCITY,
      YEAR_DX,
      MODALITY,
      RESULT
   ) %>%
   left_join(dsa$sites, join_by(FACILITY_CODE == FACI_ID))

dsa$gis$random <- lapply(dsa$gis$data, function(df) as.data.frame(lapply(df, sample), stringsAsFactors = F))
dsa$gis$subset <- lapply(dsa$gis$data, function(df) {
   n_rows   <- nrow(df)
   perc     <- floor(n_rows * .25)
   df$order <- runif(n_rows, 1, n_rows)
   df %<>%
      arrange(order) %>%
      slice(1:perc) %>%
      select(-order)
})

dsa$dirs$output <- "O:/My Drive/Data Sharing/GIS Analysis"
write_rds(
   dsa$gis$random,
   file.path(dsa$dirs$output, "20230614_tab6-7_randomized.rds")
)
write_rds(
   dsa$gis$subset,
   file.path(dsa$dirs$output, "20230614_tab6-7_subset25.rds")
)
write_rds(
   dsa$spdf,
   file.path(dsa$dirs$output, "20230614_shape_object-merged_mla-aem.rds")
)
write_rds(
   dsa$harp$est,
   file.path(dsa$dirs$output, "20230614_plhiv_estimates-as_of_Oct2022.rds")
)
