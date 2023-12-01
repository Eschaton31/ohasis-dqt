##  Philippine Standard Geographic Codes ---------------------------------------

continue <- 1
id_col   <- c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
object   <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "addr_reg")) %>%
   select(
      PSGC_REG  = REG,
      NAME_REG  = NAME,
      NHSSS_REG = NHSSS
   ) %>%
   left_join(
      y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "addr_prov")) %>%
         select(
            PSGC_REG    = REG,
            PSGC_PROV   = PROV,
            NAME_PROV   = NAME,
            NHSSS_PROV  = NHSSS,
            INCOME_PROV = CLASS_INCOME
         ),
      by = "PSGC_REG"
   ) %>%
   left_join(
      y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "addr_munc")) %>%
         select(
            PSGC_PROV        = PROV,
            PSGC_MUNC        = MUNC,
            NAME_MUNC        = NAME,
            NHSSS_MUNC       = NHSSS,
            INCOME_MUNC      = CLASS_INCOME,
            POPCEN_MUNC_2015 = POPCEN_2015,
            POPCEN_MUNC_2020 = POPCEN_2020,
            CREATED_AT
         ),
      by = "PSGC_PROV"
   ) %>%
   union(
      y = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "addr_reg")) %>%
         select(
            PSGC_REG  = REG,
            NAME_REG  = NAME,
            NHSSS_REG = NHSSS,
            CREATED_AT
         ) %>%
         mutate(
            PSGC_PROV        = NA_character_,
            NAME_PROV        = "Unknown",
            NHSSS_PROV       = "UNKNOWN",
            PSGC_MUNC        = NA_character_,
            NAME_MUNC        = "Unknown",
            NHSSS_MUNC       = "UNKNOWN",
            INCOME_MUNC      = NA_character_,
            POPCEN_MUNC_2015 = NA_integer_,
            POPCEN_MUNC_2020 = NA_integer_,
         )
   ) %>%
   union(
      y = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "addr_reg")) %>%
         select(
            PSGC_REG  = REG,
            NAME_REG  = NAME,
            NHSSS_REG = NHSSS
         ) %>%
         left_join(
            y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "addr_prov")) %>%
               select(
                  PSGC_REG    = REG,
                  PSGC_PROV   = PROV,
                  NAME_PROV   = NAME,
                  NHSSS_PROV  = NHSSS,
                  INCOME_PROV = CLASS_INCOME,
                  CREATED_AT
               ),
            by = "PSGC_REG"
         ) %>%
         mutate(
            PSGC_MUNC        = NA_character_,
            NAME_MUNC        = "Unknown",
            NHSSS_MUNC       = "UNKNOWN",
            INCOME_MUNC      = NA_character_,
            POPCEN_MUNC_2015 = NA_integer_,
            POPCEN_MUNC_2020 = NA_integer_,
         )
   ) %>%
   collect() %>%
   mutate_all(~as.character(.)) %>%
   bind_rows(
      tbl(db_conn, dbplyr::in_schema("ohasis_interim", "addr_reg")) %>%
         head(n = 1) %>%
         mutate(
            PSGC_REG         = NA_character_,
            NAME_REG         = "Unknown",
            NHSSS_REG        = "UNKNOWN",
            PSGC_PROV        = NA_character_,
            NAME_PROV        = "Unknown",
            NHSSS_PROV       = "UNKNOWN",
            INCOME_PROV      = CLASS_INCOME,
            PSGC_MUNC        = NA_character_,
            NAME_MUNC        = "Unknown",
            NHSSS_MUNC       = "UNKNOWN",
            INCOME_MUNC      = NA_character_,
            POPCEN_MUNC_2015 = NA_integer_,
            POPCEN_MUNC_2020 = NA_integer_,
         ) %>%
         select(
            PSGC_REG,
            NAME_REG,
            NHSSS_REG,
            PSGC_PROV,
            NAME_PROV,
            NHSSS_PROV,
            INCOME_PROV,
            PSGC_MUNC,
            NAME_MUNC,
            NHSSS_MUNC,
            INCOME_MUNC,
            POPCEN_MUNC_2015,
            POPCEN_MUNC_2020,
            CREATED_AT
         ) %>%
         collect() %>%
         mutate_all(~as.character(.))
   ) %>%
   mutate(
      LABEL_REG  = if_else(
         condition = !is.na(PSGC_REG),
         true      = paste0(substr(PSGC_REG, 1, 2), "_", NAME_REG),
         false     = NA_character_
      ),
      LABEL_PROV = if_else(
         condition = !is.na(PSGC_PROV),
         true      = paste0(substr(PSGC_PROV, 1, 4), "_", NAME_PROV),
         false     = NA_character_
      ),
      LABEL_MUNC = if_else(
         condition = !is.na(PSGC_MUNC),
         true      = paste0(substr(PSGC_MUNC, 1, 6), "_", NAME_MUNC),
         false     = NA_character_
      ),
      NHSSS_REG  = coalesce(NHSSS_REG, toupper(NAME_REG)),
      NHSSS_PROV = coalesce(NHSSS_PROV, toupper(NAME_PROV)),
      NHSSS_MUNC = coalesce(NHSSS_MUNC, toupper(NAME_MUNC)),
   ) %>%
   filter(
      CREATED_AT >= snapshot_old,
      CREATED_AT < snapshot_new
   ) %>%
   collect()