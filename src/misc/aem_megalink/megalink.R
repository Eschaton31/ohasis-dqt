harp <- list(
   yrs = c(2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023)
)

harp$tx        <- lapply(harp$yrs, function(yr) {
   mo   <- ifelse(yr <= 2022, 12, 8)
   data <- hs_data("harp_tx", "outcome", yr, mo) %>%
      read_dta(
         col_select = any_of(c(
            'art_id',
            'idnum',
            'outcome',
            'confirmatory_code',
            'sacclccode',
            'sacclcode',
            'saccl_code',
            'saccl',
            'artstart_date',
            'latest_regimen',
            'latest_ffupdate',
            'latest_nextpickup',
            'death_transout_date',
            'age',
            'sex',
            'year',
            'curr_age',
            'hub',
            'txreg',
            'txprov',
            'txmunc',
            'real_reg',
            'real_prov',
            'real_munc',
            'realhub',
            'realhub_branch',
            'birthdate'
         ))
      ) %>%
      mutate(
         year                = yr,
         death_transout_date = if (!("death_transout_date" %in% colnames(.))) NA_character_ else as.character(death_transout_date),
         transin_date        = if (!("transin_date" %in% colnames(.))) NA_Date_ else transin_date,
         age                 = if (yr >= 2022) curr_age else age,
         end_date            = end_ym(yr, mo),
         latest_nextpickup   = if_else(is.na(latest_nextpickup) & !is.na(latest_ffupdate), latest_ffupdate %m+% days(30), latest_nextpickup, latest_nextpickup),
         outcome3mos         = hiv_tx_outcome(outcome, latest_nextpickup, as.Date(end_date), 3, "months"),
         outcome30dy         = hiv_tx_outcome(outcome, latest_nextpickup, as.Date(end_date), 30),
         outcome3mos         = if_else(outcome3mos == "(no data)" & outcome != "", outcome, outcome3mos, outcome3mos),
         outcome30dy         = if_else(outcome30dy == "(no data)" & outcome != "", outcome, outcome30dy, outcome30dy),
      )

   if (yr >= 2022) {
      data %<>%
         left_join(
            y  = hs_data("harp_tx", "reg", yr, mo) %>%
               read_dta(col_select = c("confirmatory_code", "art_id", "birthdate")) %>%
               mutate(
                  sacclcode = if_else(!is.na(confirmatory_code), str_replace_all(confirmatory_code, "[^[:alnum:]]", ""), NA_character_)
               ),
            by = join_by(art_id)
         )
   }

   if (yr <= 2016)
      data %<>%
         mutate(
            sacclcode = if (yr <= 2016) saccl_code else sacclcode
         )

   if ("realhub" %in% names(data) && data %>% filter(realhub != "") %>% nrow() > 0) {
      data %<>%
         select(-any_of("hub")) %>%
         rename(
            hub    = realhub,
            branch = realhub_branch,
            txreg  = real_reg,
            txprov = real_prov,
            txmunc = real_munc
         )
   }

   data %<>%
      mutate(
         end_age = coalesce(calc_age(birthdate, end_date), age)
      )

   return(data)
})
names(harp$tx) <- harp$yrs

harp$dead <- read_dta(hs_data("harp_dead", "reg", 2023, 8)) %>%
   mutate(
      proxy_death_date = as.Date(ceiling_date(as.Date(paste(sep = '-', year, month, '01')), unit = 'month')) - 1,
      ref_death_date   = if_else(
         condition = is.na(date_of_death),
         true      = proxy_death_date,
         false     = date_of_death
      )
   )

harp$dx <- read_dta(hs_data("harp_dx", "reg", 2023, 8))

tx_all <- harp$tx %>%
   bind_rows(.id = "yr") %>%
   ungroup() %>%
   mutate(
      hub = toupper(hub)
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "hub", SUB_FACI_ID = "branch")
   ) %>%
   select(-txreg, -txprov, -txmunc) %>%
   mutate(
      FACI = FACI_ID,
      SUB  = SUB_FACI_ID
   ) %>%
   ohasis$get_faci(
      list(realhub = c("FACI", "SUB")),
      "code",
      c("txreg", "txprov", "txmunc")
   ) %>%
   select(-realhub) %>%
   rename(
      ffup_date     = latest_ffupdate,
      lastpill_date = latest_nextpickup,
   ) %>%
   ohasis$get_faci(
      list(hub_name = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   ) %>%
   pivot_wider(
      id_cols     = sacclcode,
      names_from  = yr,
      values_from = c(art_id, idnum, hub_name, txreg, txprov, txmunc, ffup_date, lastpill_date, outcome30dy, outcome3mos, end_age, sex)
   )

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

mega <- tx_all %>%
   rename(
      art_id = art_id_2023,
      idnum  = idnum_2023,
      tx_sex = sex_2023
   ) %>%
   filter(!is.na(art_id)) %>%
   select(
      -starts_with("art_id_"),
      -starts_with("idnum_"),
      -starts_with("sex_"),
   ) %>%
   full_join(
      y          = harp$dx %>%
         select(
            idnum,
            region,
            province,
            muncity,
            bdate,
            transmit,
            sexhow,
            dx_sex = sex,
            age,
            year
         ),
      by         = join_by(idnum),
      na_matches = "never"
   ) %>%
   left_join(
      y          = harp$dead %>%
         select(
            idnum,
            ref_death_date
         ),
      by         = join_by(idnum),
      na_matches = "never"
   ) %>%
   mutate_at(
      .vars = vars(region, province, muncity, starts_with("txreg"), starts_with("txprov"), starts_with("txmunc")),
      ~coalesce(na_if(., ""), "UNKNOWN")
   ) %>%
   mutate(
      sex           = coalesce(dx_sex, tx_sex),
      end_date_2015 = "2015-12-31",
      end_date_2016 = "2016-12-31",
      end_date_2017 = "2017-12-31",
      end_date_2018 = "2018-12-31",
      end_date_2019 = "2019-12-31",
      end_date_2020 = "2020-12-31",
      end_date_2021 = "2021-12-31",
      end_date_2022 = "2022-12-31",
      end_date_2023 = "2023-08-31",
   ) %>%
   mutate_at(
      .vars = vars(starts_with("end_date_")),
      ~as.Date(.)
   ) %>%
   mutate(
      plhiv_2015 = if_else(end_date_2015 >= ref_death_date | outcome3mos_2015 == "dead", 0, 1, 1),
      plhiv_2016 = if_else(end_date_2016 >= ref_death_date | outcome3mos_2016 == "dead", 0, 1, 1),
      plhiv_2017 = if_else(end_date_2017 >= ref_death_date | outcome3mos_2017 == "dead", 0, 1, 1),
      plhiv_2018 = if_else(end_date_2018 >= ref_death_date | outcome3mos_2018 == "dead", 0, 1, 1),
      plhiv_2019 = if_else(end_date_2019 >= ref_death_date | outcome3mos_2019 == "dead", 0, 1, 1),
      plhiv_2020 = if_else(end_date_2020 >= ref_death_date | outcome3mos_2020 == "dead", 0, 1, 1),
      plhiv_2021 = if_else(end_date_2021 >= ref_death_date | outcome3mos_2021 == "dead", 0, 1, 1),
      plhiv_2022 = if_else(end_date_2022 >= ref_death_date | outcome3mos_2022 == "dead", 0, 1, 1),
      plhiv_2023 = if_else(end_date_2023 >= ref_death_date | outcome3mos_2023 == "dead", 0, 1, 1),
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "region",
         PERM_PSGC_PROV = "province",
         PERM_PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   left_join(
      y  = ref %>%
         select(
            PERM_PSGC_REG  = PSGC_REG,
            PERM_PSGC_PROV = PSGC_PROV,
            PERM_PSGC_MUNC = PSGC_MUNC,
            aemclass_2019,
            aemclass_2023,
         ),
      by = join_by(PERM_PSGC_REG, PERM_PSGC_PROV, PERM_PSGC_MUNC)
   ) %>%
   select(-region, -province, -muncity) %>%
   ohasis$get_addr(
      c(
         region   = "PERM_PSGC_REG",
         province = "PERM_PSGC_PROV",
         muncity  = "PERM_PSGC_MUNC"
      ),
      "nhsss"
   ) %>%
   select(-starts_with("end_date_"), -overseas_addr, -year, -age, -dx_sex, -tx_sex, -bdate)

pse <- read_dta("C:/Users/johnb/Downloads/2022 PSE 10312023.dta") %>%
   select(-starts_with("PSGC")) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "region",
         PERM_PSGC_PROV = "province",
         PERM_PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   select(-region, -province, -muncity) %>%
   ohasis$get_addr(
      c(
         region   = "PERM_PSGC_REG",
         province = "PERM_PSGC_PROV",
         muncity  = "PERM_PSGC_MUNC"
      ),
      "nhsss"
   ) %>%
   relocate(region, province, muncity, .before = 1)

write_dta(pse, "H:/20231031_pse-final.dta")

write_dta(mega, "H:/20231103_mega-harp_2015-2023-08.dta")