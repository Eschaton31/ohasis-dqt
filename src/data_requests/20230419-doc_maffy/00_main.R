dr <- list()
dr$sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw") %>%
  distinct(FACI_ID, .keep_all = TRUE) %>%
  mutate(site_pepfar = coalesce(site_epic_2022, site_icap_2023)) %>%
  filter(site_pepfar == 1)

dr$harp$dx <- hs_data("harp_dx", "reg", 2022, 12) %>%
  read_dta()

dr$harp$tx <- hs_data("harp_tx", "reg", 2022, 12) %>%
  read_dta() %>%
  select(-REC_ID) %>%
  left_join(
    y = hs_data("harp_tx", "outcome", 2022, 12) %>%
      read_dta(col_select = c(art_id, REC_ID, onart, baseline_vl, vlp12m, latest_ffupdate, latest_nextpickup))
  ) %>%
  get_cid(forms$id_reg, PATIENT_ID)

dr$harp$prep <- hs_data("prep", "reg", 2023, 2) %>%
  read_dta() %>%
  left_join(
    y = hs_data("prep", "outcome", 2023, 2) %>%
      read_dta(col_select = c(prep_id, prepstart_date))
  ) %>%
  get_cid(forms$id_reg, PATIENT_ID)

min <- "2022-01-01"
max <- "2022-12-31"
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
lw_conn <- ohasis$conn("lw")
dbname <- "ohasis_warehouse"
forms <- list(
  hts = dbTable(lw_conn, dbname, "form_hts", raw_where = TRUE, where = hts_where),
  a = dbTable(lw_conn, dbname, "form_a", raw_where = TRUE, where = hts_where),
  cfbs = dbTable(lw_conn, dbname, "form_cfbs", raw_where = TRUE, where = cbs_where),
  id_reg = dbTable(lw_conn, dbname, "id_registry", c("PATIENT_ID", "CENTRAL_ID"))
)
forms$art <- dbTable(lw_conn, dbname, "form_art_bc", raw_where = TRUE, where = r"(
YEAR(VISIT_DATE) = 2022
)")
forms$art2 <- dbGetQuery(lw_conn, "SELECT REC_ID, CLIENT_TYPE FROM ohasis_warehouse.form_art_bc WHERE REC_ID IN (?)", params = list(dr$harp$tx$REC_ID))
dbDisconnect(lw_conn)
hts_data <- process_hts(forms$hts, forms$a, forms$cfbs) %>%
  get_cid(forms$id_reg, PATIENT_ID) %>%
  remove_pii() %>%
  mutate(
    CBS_VENUE = toupper(str_squish(HIV_SERVICE_ADDR)),
    ONLINE_APP = case_when(
      grepl("GRINDR", CBS_VENUE) ~ "GRINDR",
      grepl("GRNDR", CBS_VENUE) ~ "GRINDR",
      grepl("GRINDER", CBS_VENUE) ~ "GRINDR",
      grepl("TWITTER", CBS_VENUE) ~ "TWITTER",
      grepl("FACEBOOK", CBS_VENUE) ~ "FACEBOOK",
      grepl("MESSENGER", CBS_VENUE) ~ "FACEBOOK",
      grepl("\\bFB\\b", CBS_VENUE) ~ "FACEBOOK",
      grepl("\\bGR\\b", CBS_VENUE) ~ "GRINDR",
    ),
    REACH_ONLINE = if_else(!is.na(ONLINE_APP), "1_Yes", REACH_ONLINE, REACH_ONLINE),
    REACH_CLINICAL = if_else(
      condition = if_all(starts_with("REACH_"), ~is.na(.)) & hts_modality == "FBT",
      true = "1_Yes",
      false = REACH_CLINICAL,
      missing = REACH_CLINICAL
    )
  ) %>%
  rename(
    CREATED = CREATED_BY,
    UPDATED = UPDATED_BY,
  ) %>%
  generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
  rename(
    HTS_PROVIDER_TYPE = PROVIDER_TYPE,
    HTS_PROVIDER_TYPE_OTHER = PROVIDER_TYPE_OTHER,
  ) %>%
  left_join(
    y = dr$sites %>%
      filter(site_epic_2022 == 1) %>%
      select(SERVICE_FACI = FACI_ID, site_epic_2022)
  ) %>%
  ohasis$get_faci(
    list(HTS_FACI = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
    "name",
    c("hts_reg", "hts_prov", "hts_munc")
  ) %>%
  ohasis$get_faci(
    list(SPECIMEN_SOURCE_FACI = c("SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE")),
    "name"
  ) %>%
  ohasis$get_faci(
    list(CONFIRM_LAB = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
    "name"
  ) %>%
  ohasis$get_addr(
    c(
      PERM_REG = "PERM_PSGC_REG",
      PERM_PROV = "PERM_PSGC_PROV",
      PERM_MUNC = "PERM_PSGC_MUNC"
    ),
    "name"
  ) %>%
  ohasis$get_addr(
    c(
      CURR_REG = "CURR_PSGC_REG",
      CURR_PROV = "CURR_PSGC_PROV",
      CURR_MUNC = "CURR_PSGC_MUNC"
    ),
    "name"
  ) %>%
  ohasis$get_addr(
    c(
      BIRTH_REG = "BIRTH_PSGC_REG",
      BIRTH_PROV = "BIRTH_PSGC_PROV",
      BIRTH_MUNC = "BIRTH_PSGC_MUNC"
    ),
    "name"
  ) %>%
  ohasis$get_addr(
    c(
      CBS_REG = "HIV_SERVICE_PSGC_REG",
      CBS_PROV = "HIV_SERVICE_PSGC_PROV",
      CBS_MUNC = "HIV_SERVICE_PSGC_MUNC"
    ),
    "name"
  ) %>%
  ohasis$get_staff(c(CREATED_BY = "CREATED")) %>%
  ohasis$get_staff(c(UPDATED_BY = "UPDATED")) %>%
  ohasis$get_staff(c(HTS_PROVIDER = "SERVICE_BY")) %>%
  ohasis$get_staff(c(ANALYZED_BY = "SIGNATORY_1")) %>%
  ohasis$get_staff(c(REVIEWED_BY = "SIGNATORY_2")) %>%
  ohasis$get_staff(c(NOTED_BY = "SIGNATORY_3"))

hts_data %>%
  filter(site_epic_2022 == 1) %>%
  filter(hts_result != "(no data)") %>%
  filter(REACH_SSNT == "1_Yes") %>%
  tab(hts_result)

hts_data %>%
  filter(site_epic_2022 == 1) %>%
  filter(hts_result != "(no data)") %>%
  mutate(
    REACH_ONLINE = if_else(StrLeft(REACH_ONLINE, 1) == "1", "online", NA_character_),
    REACH_CLINICAL = if_else(StrLeft(REACH_CLINICAL, 1) == "1", "clinical", NA_character_),
    REACH_VENUE = if_else(StrLeft(REACH_VENUE, 1) == "1", "venue", NA_character_),
    REACH_SSNT = if_else(StrLeft(REACH_SSNT, 1) == "1", "ssnt", NA_character_),
    REACH_INDEX_TESTING = if_else(StrLeft(REACH_INDEX_TESTING, 1) == "1", "index", NA_character_),
  ) %>%
  unite(
    col = "reach",
    sep = ", ",
    na.rm = TRUE,
    starts_with("REACH_")
  ) %>%
  tab(reach)


hts_data %>%
  filter(site_epic_2022 == 1) %>%
  filter(hts_result == "NR") %>%
  left_join(
    y = dr$harp$prep %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      select(CENTRAL_ID, prepstart_date) %>%
      mutate(
        screened = 1,
        everonprep = if_else(!is.na(prepstart_date), 1, as.numeric(NA))
      )
  ) %>%
  tab(screened, everonprep)


hts_data %>%
  filter(site_epic_2022 == 1) %>%
  filter(hts_result == "R") %>%
  left_join(
    y = dr$harp$tx %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      select(CENTRAL_ID, artstart_date) %>%
      mutate(everonart = 1)
  ) %>%
  tab(hts_modality, everonart)


persons <- list()
persons$online <- hts_data %>%
  filter(grepl("Iloilo", hts_munc)) %>%
  filter(hts_result != "(no data)") %>%
  filter(REACH_ONLINE == "1_Yes") %>%
  distinct(CENTRAL_ID)
persons$clinical <- hts_data %>%
  filter(grepl("Iloilo", hts_munc)) %>%
  filter(hts_result != "(no data)") %>%
  filter(REACH_CLINICAL == "1_Yes") %>%
  distinct(CENTRAL_ID)
persons$venue <- hts_data %>%
  filter(grepl("Iloilo", hts_munc)) %>%
  filter(hts_result != "(no data)") %>%
  filter(REACH_VENUE == "1_Yes") %>%
  distinct(CENTRAL_ID)
persons$ssnt <- hts_data %>%
  filter(grepl("Iloilo", hts_munc)) %>%
  filter(hts_result != "(no data)") %>%
  filter(REACH_SSNT == "1_Yes") %>%
  distinct(CENTRAL_ID)
persons$index <- hts_data %>%
  filter(grepl("Iloilo", hts_munc)) %>%
  filter(hts_result != "(no data)") %>%
  filter(REACH_INDEX_TESTING == "1_Yes") %>%
  distinct(CENTRAL_ID)

unique_reach <- persons$clinical %>%
  mutate(
    reach_clinical = "clinical",
  ) %>%
  full_join(persons$index %>% mutate(reach_index = 'index')) %>%
  full_join(persons$venue %>% mutate(reach_venue = "venue")) %>%
  full_join(persons$online %>% mutate(reach_online = "online")) %>%
  full_join(persons$ssnt %>% mutate(reach_ssnt = "ssnt")) %>%
  unite(
    col = "reach",
    sep = ", ",
    na.rm = TRUE,
    starts_with("REACH_")
  ) %>%
  left_join(
    y = dr$harp$tx %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      select(CENTRAL_ID, artstart_date) %>%
      mutate(everonart = 1)
  )

df <- dr$harp$dx %>%
  filter(year == 2022) %>%
  dxlab_to_id(
    c("HARPDX_FACI", "HARPDX_SUB_FACI"),
    c("dx_region", "dx_province", "dx_muncity", "dxlab_standard")
  ) %>%
  inner_join(
    y = dr$sites %>%
      filter(site_epic_2022 == 1) %>%
      select(HARPDX_FACI = FACI_ID)
  )

art_data <- forms$art %>%
  get_cid(forms$id_reg, PATIENT_ID)

art_data %>% tab(CLIENT_TYPE)

dr$harp$tx %>%
  left_join(
    y = forms$art %>%
      select(REC_ID, CLIENT_TYPE)
  ) %>%
  filter(onart == 1) %>%
  mutate(
    mmd_months = floor(interval(latest_ffupdate, latest_nextpickup) / months(1)),
    mmd = case_when(
      mmd_months <= 1 ~ "(1) 1 mo. of ARVs",
      mmd_months %in% seq(2, 3) ~ "(2) 2-3 mos. of ARVs",
      mmd_months %in% seq(4, 5) ~ "(3) 4-5 mos. of ARVs",
      mmd_months %in% seq(6, 12) ~ "(4) 6-12 mos. of ARVs",
      mmd_months > 12 ~ "(5) 12+ mos. worth of ARVs",
      TRUE ~ "(no data)"
    ),
  ) %>%
  tab(mmd)

hts_data %>%
  filter(hts_result != "(no data)") %>%
  filter(grepl("Iloilo", hts_munc)) %>%
  tab(REACH_SSNT)