##------------------------------------------------------------------------------
##  Philippine Standard Geographic Codes
##------------------------------------------------------------------------------

continue <- 1
id_col   <- c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
object   <- tbl(db_conn, "addr_reg") %>%
   select(
      PSGC_REG  = REG,
      NAME_REG  = NAME,
      NHSSS_REG = NHSSS
   ) %>%
   left_join(
      y  = tbl(db_conn, "addr_prov") %>%
         select(
            PSGC_REG   = REG,
            PSGC_PROV  = PROV,
            NAME_PROV  = NAME,
            NHSSS_PROV = NHSSS
         ),
      by = "PSGC_REG"
   ) %>%
   left_join(
      y  = tbl(db_conn, "addr_munc") %>%
         select(
            PSGC_PROV  = PROV,
            PSGC_MUNC  = MUNC,
            NAME_MUNC  = NAME,
            NHSSS_MUNC = NHSSS,
            CREATED_AT
         ),
      by = "PSGC_PROV"
   ) %>%
   union(
      y = tbl(db_conn, "addr_reg") %>%
         select(
            PSGC_REG  = REG,
            NAME_REG  = NAME,
            NHSSS_REG = NHSSS,
            CREATED_AT
         ) %>%
         mutate(
            PSGC_PROV  = NA_character_,
            NAME_PROV  = "Unknown",
            NHSSS_PROV = "UNKNOWN",
            PSGC_MUNC  = NA_character_,
            NAME_MUNC  = "Unknown",
            NHSSS_MUNC = "UNKNOWN"
         )
   ) %>%
   union(
      y = tbl(db_conn, "addr_reg") %>%
         select(
            PSGC_REG  = REG,
            NAME_REG  = NAME,
            NHSSS_REG = NHSSS
         ) %>%
         left_join(
            y  = tbl(db_conn, "addr_prov") %>%
               select(
                  PSGC_REG   = REG,
                  PSGC_PROV  = PROV,
                  NAME_PROV  = NAME,
                  NHSSS_PROV = NHSSS,
                  CREATED_AT
               ),
            by = "PSGC_REG"
         ) %>%
         mutate(
            PSGC_MUNC  = NA_character_,
            NAME_MUNC  = "Unknown",
            NHSSS_MUNC = "UNKNOWN"
         )
   ) %>%
   union(
      y = tbl(db_conn, "addr_reg") %>%
         head(n = 1) %>%
         mutate(
            PSGC_REG   = NA_character_,
            NAME_REG   = "Unknown",
            NHSSS_REG  = "UNKNOWN",
            PSGC_PROV  = NA_character_,
            NAME_PROV  = "Unknown",
            NHSSS_PROV = "UNKNOWN",
            PSGC_MUNC  = NA_character_,
            NAME_MUNC  = "Unknown",
            NHSSS_MUNC = "UNKNOWN"
         ) %>%
         select(
            PSGC_REG,
            NAME_REG,
            NHSSS_REG,
            PSGC_PROV,
            NAME_PROV,
            NHSSS_PROV,
            PSGC_MUNC,
            NAME_MUNC,
            NHSSS_MUNC,
            CREATED_AT
         )
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
   ) %>%
   filter(
      CREATED_AT >= snapshot_old,
      CREATED_AT < snapshot_new
   ) %>%
   collect()