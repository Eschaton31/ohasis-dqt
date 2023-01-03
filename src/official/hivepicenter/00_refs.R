epictr$iso3166_2 <- read_dta("E:/_R/iso3166_2_ph.dta")
epictr$ref_addr  <- ohasis$ref_addr %>%
   mutate(
      drop = case_when(
         StrLeft(PSGC_PROV, 4) == "1339" & (PSGC_MUNC != "133900000" | is.na(PSGC_MUNC)) ~ 1,
         StrLeft(PSGC_REG, 4) == "1300" & PSGC_MUNC == "" ~ 1,
         stri_detect_fixed(NAME_PROV, "City") & NHSSS_MUNC == "UNKNOWN" ~ 1,
         TRUE ~ 0
      ),
   ) %>%
   filter(drop == 0) %>%
   add_row(
      PSGC_REG   = "130000000",
      PSGC_PROV  = "",
      PSGC_MUNC  = "",
      NAME_REG   = "National Capital Region (NCR)",
      NAME_PROV  = "Unknown",
      NAME_MUNC  = "Unknown",
      NHSSS_REG  = "NCR",
      NHSSS_PROV = "UNKNOWN",
      NHSSS_MUNC = "UNKNOWN",
   ) %>%
   left_join(
      y  = epictr$iso3166_2 %>% distinct_all(),
      by = c("NAME_PROV" = "NAME")
   ) %>%
   mutate(
      ISO = case_when(
         NAME_PROV == "City of Cotabato (Not a Province)" ~ "PH-NCO",
         NAME_PROV == "City of Isabela (Not a Province)" ~ "PH-BAS",
         NAME_PROV == "Davao de Oro" ~ "PH-COM",
         NAME_PROV == "Davao Occidental" ~ "PH-DVO",
         NAME_PROV == "Dinagat Islands" ~ "PH-DIN",
         NAME_PROV == "NCR, City of Manila, First District (Not a Province)" ~ "PH-MNL",
         NAME_PROV == "NCR, Fourth District (Not a Province)" ~ "PH-MNL",
         NAME_PROV == "NCR, Second District (Not a Province " ~ "PH-MNL",
         NAME_PROV == "NCR, Third District (Not a Province)" ~ "PH-MNL",
         NAME_PROV == "Occidental Mindoro" ~ "PH-MDC",
         NAME_PROV == "Oriental Mindoro" ~ "PH-MDR",
         TRUE ~ ISO
      )
   ) %>%
   left_join(
      y          = read_sheet(
         as_id("16_ytFIRiAgmo6cqtoy0JkZoI62S5IuWxKyNKHO_R1fs"),
         "v2022"
      ) %>%
         select(
            aem_class,
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC
         ),
      na_matches = "never"
   ) %>%
   mutate(
      PSGC_AEM = if_else(aem_class %in% c("a", "ncr", "cebu city", "cebu province"), PSGC_MUNC, PSGC_PROV, PSGC_PROV),
      NAME_AEM = if_else(aem_class %in% c("a", "ncr", "cebu city", "cebu province"), NAME_MUNC, NAME_PROV, NAME_PROV),
   ) %>%
   mutate(
      NAME_PROV = case_when(
         stri_detect_fixed(NAME_PROV, "NCR") ~ stri_replace_all_fixed(NAME_PROV, " (Not a Province)", ""),
         TRUE ~ NAME_PROV
      ),
      NAME_AEM  = case_when(
         PSGC_MUNC == "031405000" ~ "Bulacan City",
         TRUE ~ NAME_AEM
      ),
      NAME_MUNC = case_when(
         PSGC_MUNC == "031405000" ~ "Bulacan City",
         TRUE ~ NAME_MUNC
      )
   )

lw_conn     <- ohasis$conn("lw")
table_space <- Id(schema = "harp", table = "ref_addr")
if (dbExistsTable(lw_conn, table_space))
   dbRemoveTable(lw_conn, table_space)

dbWriteTable(lw_conn, table_space, epictr$ref_addr %>% filter(!(NAME_AEM %in% c("Unknown", "Overseas"))))
dbDisconnect(lw_conn)

epictr$ref_faci <- ohasis$ref_faci_code %>%
   mutate(
      FACI_CODE     = case_when(
         stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
         stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
         stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
         TRUE ~ FACI_CODE
      ),
      SUB_FACI_CODE = if_else(
         condition = nchar(SUB_FACI_CODE) == 3,
         true      = NA_character_,
         false     = SUB_FACI_CODE
      ),
      SUB_FACI_CODE = case_when(
         FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
         FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
         FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
         TRUE ~ SUB_FACI_CODE
      ),
   ) %>%
   select(
      FACI_ID,
      final_hub    = FACI_CODE,
      final_branch = SUB_FACI_CODE,
      TX_FACI      = FACI_NAME,
      TX_PSGC_REG  = FACI_PSGC_REG,
      TX_PSGC_PROV = FACI_PSGC_PROV,
      TX_PSGC_MUNC = FACI_PSGC_MUNC,
   ) %>%
   distinct_all()

local(envir = epictr, {
   params    <- list()
   params$mo <- input(prompt = "What is the reporting month?", max.char = 2)
   params$yr <- input(prompt = "What is the reporting year?", max.char = 4)
   params$mo <- params$mo %>% stri_pad_left(width = 2, pad = "0")
   params$yr <- params$yr %>% stri_pad_left(width = 4, pad = "0")

   params$min <- paste(sep = "-", params$yr, params$mo, "01")
   params$max <- params$min %>%
      as.Date() %>%
      ceiling_date(unit = "month") %m-%
      days(1) %>%
      as.character()
})