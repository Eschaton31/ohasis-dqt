##------------------------------------------------------------------------------
##  List of reporting units
##------------------------------------------------------------------------------

continue <- 1
id_col   <- c("FACI_ID", "SUB_FACI_ID")
object   <- tbl(db_conn, "facility") %>%
   group_by(FACI_ID) %>%
   summarise(
      EDIT_NUM = max(EDIT_NUM, na.rm = TRUE)
   ) %>%
   inner_join(
      y  = tbl(db_conn, "facility"),
      by = c("FACI_ID", "EDIT_NUM")
   ) %>%
   left_join(
      y  = tbl(db_conn, "facility_unit") %>%
         select(
            FACI_ID,
            SUB_FACI_ID,
            SUB_FACI_NAME,
            SUB_FACI_NAME_CLEAN,
            SUB_ALT_FACI_NAME = ALT_FACI_NAME,
            SUB_REG           = REG,
            SUB_PROV          = PROV,
            SUB_MUNC          = MUNC,
            SUB_CREATED_AT    = CREATED_AT,
            SUB_UPDATED_AT    = UPDATED_AT
         ),
      by = 'FACI_ID'
   ) %>%
   mutate(
      SNAPSHOT        = case_when(
         SUB_UPDATED_AT > CREATED_AT ~ SUB_UPDATED_AT,
         SUB_CREATED_AT > CREATED_AT ~ SUB_CREATED_AT,
         TRUE ~ CREATED_AT
      ),
      REG             = if_else(!is.na(SUB_REG), SUB_REG, REG),
      PROV            = if_else(!is.na(SUB_PROV), SUB_PROV, PROV),
      MUNC            = if_else(!is.na(SUB_MUNC), SUB_MUNC, MUNC),
      ALT_FACI_NAME   = if_else(!is.na(SUB_ALT_FACI_NAME), SUB_ALT_FACI_NAME, ALT_FACI_NAME),
      FACI_NAME       = if_else(!is.na(ALT_FACI_NAME), ALT_FACI_NAME, FACI_NAME),
      FACI_NAME_CLEAN = if_else(!is.na(SUB_FACI_NAME_CLEAN), SUB_FACI_NAME_CLEAN, FACI_NAME_CLEAN),
      CREATED_AT      = if_else(!is.na(SUB_CREATED_AT), SUB_CREATED_AT, CREATED_AT),
   ) %>%
   filter(
      SNAPSHOT >= snapshot_old,
      SNAPSHOT < snapshot_new
   ) %>%
   rename(
      FACI_PSGC_REG  = REG,
      FACI_PSGC_PROV = PROV,
      FACI_PSGC_MUNC = MUNC
   ) %>%
   collect() %>%
   left_join(
      y  = tbl(dl_conn, "ref_addr") %>%
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