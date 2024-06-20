##  List of reporting units ----------------------------------------------------

continue <- 1
id_col   <- c("FACI_ID", "SUB_FACI_ID")
object_1 <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "facility")) %>%
   group_by(FACI_ID) %>%
   summarise(
      EDIT_NUM = max(EDIT_NUM, na.rm = TRUE)
   ) %>%
   inner_join(
      y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "facility")),
      by = c("FACI_ID", "EDIT_NUM")
   ) %>%
   inner_join(
      y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "facility_unit")) %>%
         select(
            FACI_ID,
            SUB_FACI_ID,
            SUB_FACI_NAME,
            SUB_FACI_NAME_CLEAN,
            SUB_FACI_CODE,
            SUB_ALT_FACI_NAME = ALT_FACI_NAME,
            SUB_REG           = REG,
            SUB_PROV          = PROV,
            SUB_MUNC          = MUNC,
            SUB_ADDRESS       = ADDRESS,
            SUB_LAT           = LAT,
            SUB_LONG          = LONG,
            SUB_CREATED_AT    = CREATED_AT,
            SUB_UPDATED_AT    = UPDATED_AT
         ),
      by = 'FACI_ID'
   ) %>%
   mutate(
      SNAPSHOT          = case_when(
         SUB_UPDATED_AT > CREATED_AT ~ SUB_UPDATED_AT,
         SUB_CREATED_AT > CREATED_AT ~ SUB_CREATED_AT,
         TRUE ~ CREATED_AT
      ),
      REG               = if_else(!is.na(SUB_REG), SUB_REG, REG),
      PROV              = if_else(!is.na(SUB_REG), SUB_PROV, PROV),
      MUNC              = if_else(!is.na(SUB_REG), SUB_MUNC, MUNC),
      ADDRESS           = if_else(!is.na(SUB_ADDRESS), SUB_ADDRESS, ADDRESS),
      LAT               = if_else(!is.na(SUB_LAT), SUB_LAT, LAT),
      LONG              = if_else(!is.na(SUB_LONG), SUB_LONG, LONG),
      SUB_ALT_FACI_NAME = if_else(is.na(SUB_ALT_FACI_NAME), SUB_FACI_NAME, SUB_ALT_FACI_NAME),
      FACI_CODE         = if_else(!is.na(SUB_FACI_CODE), SUB_FACI_CODE, FACI_CODE),
      ALT_FACI_NAME     = if_else(!is.na(SUB_ALT_FACI_NAME), SUB_ALT_FACI_NAME, ALT_FACI_NAME),
      FACI_NAME         = if_else(!is.na(ALT_FACI_NAME), ALT_FACI_NAME, FACI_NAME),
      FACI_NAME_CLEAN   = if_else(!is.na(SUB_FACI_NAME_CLEAN), SUB_FACI_NAME_CLEAN, FACI_NAME_CLEAN),
      CREATED_AT        = if_else(!is.na(SUB_CREATED_AT), SUB_CREATED_AT, CREATED_AT),
   ) %>%
   filter(
      SNAPSHOT >= snapshot_old,
      SNAPSHOT < snapshot_new
   ) %>%
   mutate_at(
      .vars = vars(REG, PROV, MUNC),
      ~coalesce(., '')
   ) %>%
   mutate(
      PROV = if_else(MUNC == '129804000', '129800000', PROV, PROV)
   ) %>%
   rename(
      FACI_PSGC_REG  = REG,
      FACI_PSGC_PROV = PROV,
      FACI_PSGC_MUNC = MUNC
   ) %>%
   collect() %>%
   left_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "ref_addr")) %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            NAME_REG,
            NAME_PROV,
            NAME_MUNC,
            NHSSS_REG,
            NHSSS_PROV,
            NHSSS_MUNC
         ) %>%
         rename_all(
            ~paste0("FACI_", .)
         ) %>%
         collect(),
      by = c("FACI_PSGC_REG", "FACI_PSGC_PROV", "FACI_PSGC_MUNC")
   ) %>%
   mutate(
      FACI_LABEL = paste0(FACI_ID, "_", FACI_NAME),
   ) %>%
   select(
      FACI_ID,
      SUB_FACI_ID,
      FACI_LABEL,
      FACI_NAME,
      FACI_NAME_CLEAN,
      FACI_CODE,
      FACI_TYPE,
      PUBPRIV,
      LAT,
      LONG,
      EMAIL,
      MOBILE,
      LANDLINE,
      FACI_PSGC_REG,
      FACI_PSGC_PROV,
      FACI_PSGC_MUNC,
      FACI_NAME_REG,
      FACI_NAME_PROV,
      FACI_NAME_MUNC,
      FACI_NHSSS_REG,
      FACI_NHSSS_PROV,
      FACI_NHSSS_MUNC,
      FACI_ADDR  = ADDRESS,
      CREATED_AT,
      UPDATED_AT = SUB_UPDATED_AT
   )

object_2 <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "facility")) %>%
   group_by(FACI_ID) %>%
   summarise(
      EDIT_NUM = max(EDIT_NUM, na.rm = TRUE)
   ) %>%
   inner_join(
      y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "facility")),
      by = c("FACI_ID", "EDIT_NUM")
   ) %>%
   mutate(
      SNAPSHOT  = CREATED_AT,
      FACI_NAME = if_else(!is.na(ALT_FACI_NAME), ALT_FACI_NAME, FACI_NAME)
   ) %>%
   filter(
      SNAPSHOT >= snapshot_old,
      SNAPSHOT < snapshot_new
   ) %>%
   mutate_at(
      .vars = vars(REG, PROV, MUNC),
      ~coalesce(., '')
   ) %>%
   mutate(
      PROV = if_else(MUNC == '129804000', '129800000', PROV, PROV)
   ) %>%
   rename(
      FACI_PSGC_REG  = REG,
      FACI_PSGC_PROV = PROV,
      FACI_PSGC_MUNC = MUNC
   ) %>%
   collect() %>%
   left_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "ref_addr")) %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            NAME_REG,
            NAME_PROV,
            NAME_MUNC,
            NHSSS_REG,
            NHSSS_PROV,
            NHSSS_MUNC
         ) %>%
         rename_all(
            ~paste0("FACI_", .)
         ) %>%
         collect(),
      by = c("FACI_PSGC_REG", "FACI_PSGC_PROV", "FACI_PSGC_MUNC")
   ) %>%
   mutate(
      FACI_LABEL = paste0(FACI_ID, "_", FACI_NAME),
   ) %>%
   select(
      FACI_ID,
      FACI_LABEL,
      FACI_NAME,
      FACI_NAME_CLEAN,
      FACI_CODE,
      FACI_TYPE,
      PUBPRIV,
      LAT,
      LONG,
      EMAIL,
      MOBILE,
      LANDLINE,
      FACI_PSGC_REG,
      FACI_PSGC_PROV,
      FACI_PSGC_MUNC,
      FACI_NAME_REG,
      FACI_NAME_PROV,
      FACI_NAME_MUNC,
      FACI_NHSSS_REG,
      FACI_NHSSS_PROV,
      FACI_NHSSS_MUNC,
      FACI_ADDR = ADDRESS,
      CREATED_AT,
      UPDATED_AT
   )

object <- bind_rows(object_1, object_2) %>%
   mutate(
      FACI_TYPE = case_when(
         FACI_TYPE == '10000' ~ 'Health Service Facility',
         FACI_TYPE == '20000' ~ 'Private Entity',
         FACI_TYPE == '21001' ~ 'NGO',
         FACI_TYPE == '21002' ~ 'CBO',
         FACI_TYPE == '22001' ~ 'Private Company',
         FACI_TYPE == '30000' ~ 'Public Entity',
         FACI_TYPE == '31000' ~ "Gov't - National",
         FACI_TYPE == '32000' ~ "Gov't - Regional",
         FACI_TYPE == '40000' ~ 'International Entity',
         TRUE ~ FACI_TYPE
      ),
      PUBPRIV   = case_when(
         PUBPRIV == 1 ~ "PUBLIC",
         PUBPRIV == 2 ~ "PRIVATE",
         TRUE ~ NA_character_
      )
   ) %>%
   mutate(
      FACI_NAME_CLEAN = coalesce(FACI_NAME_CLEAN, toupper(FACI_NAME))
   )
