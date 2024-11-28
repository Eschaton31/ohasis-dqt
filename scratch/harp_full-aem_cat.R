tmp <- tempfile(fileext = ".xlsx")
drive_download(as_id("1v65nEUWVoDTQ9VUl_6-h1u36I3pdt3cF"), tmp, overwrite = TRUE)
ref <- read_excel(tmp, "2023 Interim", col_types = "text") %>%
   select(
      region,
      province,
      muncity,
      aemclass_2019,
      aemclass_2023
   ) %>%
   mutate(
      region  = case_when(
         region == "7.0" ~ "7",
         TRUE ~ region
      ),
      muncity = case_when(
         muncity == "DASMARIAS" ~ "DASMARI\u00D1AS",
         muncity == "BIAN" ~ "BI\u00D1AN",
         muncity == "MUOZ" ~ "MU\u00D1OZ",
         muncity == "LOS BAOS" ~ "LOS BA\u00D1OS",
         muncity == "SANTO NIO" ~ "SANTO NI\u00D1O",
         muncity == "PEABLANCA" ~ "PE\u00D1ABLANCA",
         muncity == "DOA REMEDIOS TRINIDAD" ~ "DO\u00D1A REMEDIOS TRINIDAD",
         muncity == "PEARANDA" ~ "PE\u00D1ARANDA",
         muncity == "DUEAS" ~ "DUE\u00D1AS",
         muncity == "PIAN" ~ "PI\u00D1AN",
         muncity == "SERGIO OSMEA SR." ~ "SERGIO OSME\u00D1A SR.",
         muncity == "SOFRONIO ESPAOLA" ~ "SOFRONIO ESPA\u00D1OLA",
         muncity == "PEARRUBIA" ~ "PE\u00D1ARRUBIA",
         region == "5" & muncity == "SAGAY" ~ "SAG\u00D1AY",
         TRUE ~ muncity
      ),
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PSGC_REG  = "region",
         PSGC_PROV = "province",
         PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = FALSE
   )


full <- QB$new(`oh-lw`)$from("harp_full.reg_202406")$get()


categorized <- full %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(REAL_FACI = "realhub", REAL_SUB_FACI = "realhub_branch")
   ) %>%
   ohasis$get_faci(
      list("treat_hub" = c("REAL_FACI", "REAL_SUB_FACI")),
      "code",
      c("treat_reg", "treat_prov", "treat_munc")
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         RES_PSGC_REG  = "region",
         RES_PSGC_PROV = "province",
         RES_PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   left_join(
      y  = ref %>%
         select(
            RES_PSGC_REG  = PSGC_REG,
            RES_PSGC_PROV = PSGC_PROV,
            RES_PSGC_MUNC = PSGC_MUNC,
            aemclass_2023,
         ),
      by = join_by(RES_PSGC_REG, RES_PSGC_PROV, RES_PSGC_MUNC)
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         TX_PSGC_REG  = "treat_reg",
         TX_PSGC_PROV = "treat_prov",
         TX_PSGC_MUNC = "treat_munc"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   left_join(
      y  = ref %>%
         select(
            TX_PSGC_REG      = PSGC_REG,
            TX_PSGC_PROV     = PSGC_PROV,
            TX_PSGC_MUNC     = PSGC_MUNC,
            aemclass_2023_tx = aemclass_2023,
         ),
      by = join_by(TX_PSGC_REG, TX_PSGC_PROV, TX_PSGC_MUNC)
   ) %>%
   mutate(
      # aemclass_2023 = coalesce(aemclass_2023, aemclass_2023_tx),
      # aemclass_2023 = case_when(
      #    aemclass_2023 == "a1" ~ "a",
      #    aemclass_2023 == "a2" ~ "a",
      #    TRUE ~ aemclass_2023
      # )
      aemclass_2023    = case_when(
         is.na(aemclass_2023) & muncity != "UNKNOWN" ~ "c",
         TRUE ~ aemclass_2023
      ),
      aemclass_2023_tx = case_when(
         is.na(aemclass_2023_tx) & treat_munc != "UNKNOWN" ~ "c",
         TRUE ~ aemclass_2023_tx
      )
   ) %>%
   distinct(idnum, .keep_all = TRUE)

flow_dta(categorized, "harp_full", "aem", 2024, 6)
