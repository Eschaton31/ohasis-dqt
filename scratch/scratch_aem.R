tmp <- tempfile(fileext = ".xlsx")
drive_download(as_id("1-OuXcpHOz2ck3s3n7tfV758jY5jxBHbf"), tmp, overwrite = TRUE)
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

unlink(tmp)

dx <- read_dta(hs_data('harp_dx', 'reg', 2023, 12), col_select = c(idnum, region, province, muncity))

periods   <- seq(2015, 2023)
files     <- lapply(periods, hs_data, sys = 'harp_tx', type = 'outcome', mo = 12)
tx        <- lapply(files, read_dta)
tx        <- lapply(tx, select, -any_of(c("region", "province", "muncity")))
names(tx) <- periods

tx$`2015` %<>% left_join(y = tx$`2017` %>% select(sacclcode, idnum), by = join_by(saccl_code == sacclcode))
tx$`2016` %<>% left_join(y = tx$`2017` %>% select(sacclcode, idnum), by = join_by(saccl_code == sacclcode))

for (yr in c('2015', '2016', '2017', '2018', '2019', '2020', '2021')) {
   tx[[yr]] %<>%
      mutate(
         outcome30 = hiv_tx_outcome(outcome, latest_nextpickup, end_ym(yr, 12), 30, "days"),
         outcome30 = coalesce(na_if(outcome30, "(no data)"), outcome)
      )
}
for (yr in c('2022', '2023')) {
   tx[[yr]] %<>%
      mutate(
         outcome30 = outcome,
         outcome = hiv_tx_outcome(outcome, latest_nextpickup, end_ym(yr, 12), 3, "months")
      ) %>%
      select(-any_of("idnum")) %>%
      left_join(
         y  = hs_data("harp_tx", "reg", yr, 12) %>%
            read_dta(col_select = c(art_id, birthdate, idnum)),
         by = join_by(art_id)
      )
}

tx      <- lapply(tx, left_join, y = dx, by = join_by(idnum))
tx[1:7] <- lapply(tx[1:7], mutate, realhub = toupper(hub), realhub_branch = NA_character_)

for (yr in periods) {
   yr_txt <- as.character(yr)
   tx[[yr_txt]] %<>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         c(REAL_FACI = "realhub", REAL_SUB_FACI = "realhub_branch")
      ) %>%
      ohasis$get_faci(
         list("treat_hub" = c("REAL_FACI", "REAL_SUB_FACI")),
         "code",
         c("treat_reg", "treat_prov", "treat_munc")
      ) %>%
      mutate(
         age_end  = calc_age(birthdate, end_ym(yr, 12)),
         age_band = case_when(
            age_end %in% seq(0, 4) ~ '0-4',
            age_end %in% seq(5, 9) ~ '5-9',
            age_end %in% seq(10, 14) ~ '10-14',
            age_end %in% seq(15, 19) ~ '15-19',
            age_end %in% seq(20, 24) ~ '20-24',
            age_end %in% seq(25, 29) ~ '25-29',
            age_end %in% seq(30, 34) ~ '30-34',
            age_end %in% seq(35, 39) ~ '35-39',
            age_end %in% seq(40, 44) ~ '40-44',
            age_end %in% seq(45, 49) ~ '45-49',
            age_end %in% seq(50, 54) ~ '50-54',
            age_end %in% seq(55, 59) ~ '55-59',
            age_end %in% seq(60, 64) ~ '60-64',
            age_end %in% seq(65, 69) ~ '65-69',
            age_end %in% seq(70, 74) ~ '70-74',
            age_end %in% seq(75, 79) ~ '75-79',
            age_end %in% seq(80, 99) ~ '80-99',
         )
      ) %>%
      mutate(
         use_tx   = if_else(is.na(muncity), 1, 0, 0),
         region   = if_else(use_tx == 1, treat_reg, region, region),
         province = if_else(use_tx == 1, treat_prov, province, province),
         muncity  = if_else(use_tx == 1, treat_munc, muncity, muncity),
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
         aemclass_2023 = coalesce(aemclass_2023, aemclass_2023_tx),
         aemclass_2023 = case_when(
            aemclass_2023 == "a1" ~ "a",
            aemclass_2023 == "a2" ~ "a",
            TRUE ~ aemclass_2023
         )
      )
}

disagg_sex_onart <- function(tx) {
   disagg <- tx %>%
      filter(outcome30 == "alive on arv") %>%
      mutate(
         sex           = substr(sex, 1, 1),
         aemclass_2023 = case_when(
            aemclass_2023 == "a1" ~ "a",
            aemclass_2023 == "a2" ~ "a",
            region == "UNKNOWN" ~ "(no data on location)",
            region == "OVERSEAS" ~ "(overseas)",
            is.na(aemclass_2023) ~ "c",
            TRUE ~ aemclass_2023
         )
      ) %>%
      group_by(aemclass_2023, sex, age_band) %>%
      summarise(
         onart = n()
      ) %>%
      ungroup() %>%
      pivot_wider(
         id_cols     = c(aemclass_2023, sex),
         names_from  = age_band,
         values_from = onart
      ) %>%
      adorn_totals()
}

analysis <- lapply(tx, disagg_sex_onart)
analysis <- bind_rows(analysis, .id = "year")

analysis %>%
   slackr_csv("20240523_spectrum-tx.csv", row.names = FALSE, channels = "#dat_coordinators")

###