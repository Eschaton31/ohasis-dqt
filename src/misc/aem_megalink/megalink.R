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

lw_conn <- ohasis$conn("lw")
prep    <- list(
   id_reg = dbTable(lw_conn, "ohasis_warehouse", "id_registry", cols = c("PATIENT_ID", "CENTRAL_ID")),
   oh     = dbTable(lw_conn, "ohasis_warehouse", "form_prep"),
   start  = read_dta(hs_data("prep", "prepstart", 2023, 8)),
   reg    = read_dta(hs_data("prep", "reg", 2023, 8)),
   disp   = dbTable(
      lw_conn,
      "ohasis_lake",
      "disp_meds",
      join = list(
         "ohasis_warehouse.form_prep" = list(by = c("REC_ID" = "REC_ID"), cols = "VISIT_DATE", type = "inner")
      )
   )
)
dbDisconnect(lw_conn)

harp$dead <- read_dta(hs_data("harp_dead", "reg", 2023, 8)) %>%
   mutate(
      proxy_death_date = as.Date(ceiling_date(as.Date(paste(sep = '-', year, month, '01')), unit = 'month')) - 1,
      ref_death_date   = if_else(
         condition = is.na(date_of_death),
         true      = proxy_death_date,
         false     = date_of_death
      )
   )

harp$dx <- read_dta(hs_data("harp_full", "reg", 2023, 8))



tx_2005 <- read_dta("H:/_R/library/hiv_tx/art_aemfull_2005-2018.dta")
for (yr in seq(2008, 2015)) {
   mo       <- ifelse(yr <= 2022, 12, 8)
   onart    <- as.name(stri_c("onart", yr))
   outcome  <- as.name(stri_c("outcome3mos_", yr))
   hub      <- as.name(stri_c("hub_", yr))
   end_date <- end_ym(yr, mo)

   tx_2005 %<>%
      mutate(
         {{outcome}} := if_else(artstart_date_2018 <= end_date & {{onart}} == 1, "alive on arv", "lost to follow up"),
         {{hub}}     := toupper(allhub)
      )
}

tx20052018 <- tx_2005 %>%
   pivot_longer(
      cols      = c(starts_with("hub_"), starts_with("outcome3mos_")),
      names_to  = c("var", "year"),
      names_sep = "_"
   ) %>%
   pivot_wider(
      id_cols     = c(sacclcode, year),
      names_from  = var,
      values_from = value
   ) %>%
   mutate(
      branch = ""
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "hub", SUB_FACI_ID = "branch")
   ) %>%
   mutate(
      FACI = FACI_ID,
      SUB  = SUB_FACI_ID
   ) %>%
   filter(year < 2015) %>%
   ohasis$get_faci(
      list(realhub = c("FACI", "SUB")),
      "code",
      c("txreg", "txprov", "txmunc")
   ) %>%
   select(-realhub) %>%
   ohasis$get_faci(
      list(hub_name = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   ) %>%
   filter(!is.na(outcome3mos)) %>%
   pivot_wider(
      id_cols     = sacclcode,
      names_from  = year,
      values_from = c(outcome3mos, hub_name, txreg, txprov, txmunc)
   )

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
   ) %>%
   left_join(
      y  = tx20052018,
      by = join_by(sacclcode)
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
   left_join(
      y  = harp$tx$`2023` %>%
         select(art_id, art_bdate = birthdate),
      by = join_by(art_id)
   ) %>%
   select(
      -starts_with("art_id_"),
      -starts_with("idnum_"),
      -starts_with("sex_"),
   ) %>%
   full_join(
      y          = harp$dx %>%
         mutate(
            rep_date = end_ym(year, month),
         ) %>%
         select(
            idnum,
            rep_date,
            class2022,
            region,
            province,
            muncity,
            bdate,
            transmit,
            sexhow,
            dx_sex = sex,
            age,
            year,
            month,
            class2022,
            dead,
            mort,
            everonart
         ),
      by         = join_by(idnum),
      na_matches = "never"
   ) %>%
   select(-any_of("ref_death_date")) %>%
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
      birthdate     = coalesce(bdate, art_bdate),
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
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         TX_PSGC_REG  = "txreg_2023",
         TX_PSGC_PROV = "txprov_2023",
         TX_PSGC_MUNC = "txmunc_2023"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   mutate(
      use_tx            = if_else(is.na(PERM_PSGC_MUNC) & !is.na(TX_PSGC_MUNC), 1, 0, 0),
      TX_PERM_PSGC_REG  = if_else(use_tx == 1, TX_PSGC_REG, PERM_PSGC_REG, PERM_PSGC_REG),
      TX_PERM_PSGC_PROV = if_else(use_tx == 1, TX_PSGC_PROV, PERM_PSGC_PROV, PERM_PSGC_PROV),
      TX_PERM_PSGC_MUNC = if_else(use_tx == 1, TX_PSGC_MUNC, PERM_PSGC_MUNC, PERM_PSGC_MUNC),
   ) %>%
   left_join(
      y  = ref %>%
         select(
            TX_PERM_PSGC_REG  = PSGC_REG,
            TX_PERM_PSGC_PROV = PSGC_PROV,
            TX_PERM_PSGC_MUNC = PSGC_MUNC,
            aemclass_2019_art = aemclass_2019,
            aemclass_2023_art = aemclass_2023,
         ),
      by = join_by(TX_PERM_PSGC_REG, TX_PERM_PSGC_PROV, TX_PERM_PSGC_MUNC)
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
   ohasis$get_addr(
      c(
         region_art   = "TX_PERM_PSGC_REG",
         province_art = "TX_PERM_PSGC_PROV",
         muncity_art  = "TX_PERM_PSGC_MUNC"
      ),
      "nhsss"
   ) %>%
   select(-contains("PSGC"), -overseas_addr, -age, -dx_sex, -tx_sex, -bdate) %>%
   # mutate_at(
   #    .vars = vars(ref_death_date),
   #    ~if_else(. == "1900-01-31", as.Date("2011-12-31"), ., .)
   # ) %>%
   mutate_at(
      .vars = vars(starts_with("end_date_")),
      ~as.Date(.)
   ) %>%
   left_join(
      y  = hs_data("harp_tx", "outcome", 2023, 8) %>%
         read_dta(col_select = c(art_id, baseline_vl, vlp12m, vl_date, vl_result)),
      by = join_by(art_id)
   )


for (yr in seq(1984, 2023)) {
   mo        <- ifelse(yr <= 2022, 12, 8)
   yrdead    <- as.name(stri_c("dead_", yr))
   yrpresume <- as.name(stri_c("presumedead_", yr))
   yrage     <- as.name(stri_c("end_age_", yr))
   end_date  <- end_ym(yr, mo)

   mega %<>% mutate({{yrage}} := NA_integer_)
   mega %<>%
      mutate(
         {{yrdead}}    := if_else(ref_death_date <= end_date, 1, 0, 0),
         {{yrpresume}} := if_else({{yrdead}} == 1 | (is.na(everonart) & interval(rep_date, end_date) / years(1) >= 10), 1, 0, 0),
         {{yrage}}     := coalesce(calc_age(birthdate, end_date), {{yrage}})
      )
}
for (yr in seq(2015, 2023)) {
   yrplhiv   <- as.name(stri_c("plhiv_", yr))
   yrdead    <- as.name(stri_c("dead_", yr))
   yrpresume <- as.name(stri_c("presumedead_", yr))
   mega %<>%
      mutate(
         {{yrdead}}    := if_else({{yrdead}} == 1 & {{yrplhiv}} == 1, 0, {{yrdead}}),
         {{yrdead}}    := if_else({{yrdead}} == 1 & {{yrplhiv}} == 0, 1, {{yrdead}}),
         {{yrpresume}} := if_else({{yrpresume}} == 1 & {{yrplhiv}} == 1, 0, {{yrpresume}}),
         {{yrpresume}} := if_else({{yrpresume}} == 1 & {{yrplhiv}} == 0, 1, {{yrpresume}}),
      )
}


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

file <- "H:/20231113_mega-harp_2008-2023-08_dead-presumed-vl.dta"
mega %>%
   select(
      sacclcode,
      art_id,
      idnum,
      year,
      month,
      sex,
      birthdate,
      art_bdate,
      rep_date,
      class2022,
      transmit,
      sexhow,
      dead,
      mort,
      everonart,
      ref_death_date,
      region,
      province,
      muncity,
      aemclass_2019,
      aemclass_2023,
      region_art,
      province_art,
      muncity_art,
      aemclass_2019_art,
      aemclass_2023_art,
      vl_date,
      vl_result,
      baseline_vl,
      vlp12m,
      ends_with("1984"),
      ends_with("1985"),
      ends_with("1986"),
      ends_with("1987"),
      ends_with("1988"),
      ends_with("1989"),
      ends_with("1990"),
      ends_with("1991"),
      ends_with("1992"),
      ends_with("1993"),
      ends_with("1994"),
      ends_with("1995"),
      ends_with("1996"),
      ends_with("1997"),
      ends_with("1998"),
      ends_with("1999"),
      ends_with("2000"),
      ends_with("2001"),
      ends_with("2002"),
      ends_with("2003"),
      ends_with("2004"),
      ends_with("2005"),
      ends_with("2006"),
      ends_with("2007"),
      ends_with("2008"),
      ends_with("2009"),
      ends_with("2010"),
      ends_with("2011"),
      ends_with("2012"),
      ends_with("2013"),
      ends_with("2014"),
      ends_with("2015"),
      ends_with("2016"),
      ends_with("2017"),
      ends_with("2018"),
      ends_with("2019"),
      ends_with("2020"),
      ends_with("2021"),
      ends_with("2022"),
      ends_with("2023")
   ) %>%
   mutate(
      aemclass_2019     = if_else(is.na(aemclass_2019) & muncity != "UNKNOWN", "c", aemclass_2019, aemclass_2019),
      aemclass_2023     = if_else(is.na(aemclass_2023) & muncity != "UNKNOWN", "c", aemclass_2023, aemclass_2023),
      aemclass_2019_art = if_else(is.na(aemclass_2019_art) & muncity_art != "UNKNOWN", "c", aemclass_2019_art, aemclass_2019_art),
      aemclass_2023_art = if_else(is.na(aemclass_2023_art) & muncity_art != "UNKNOWN", "c", aemclass_2023_art, aemclass_2023_art),
   ) %>%
   format_stata() %>%
   write_dta(file)
compress_stata(file)


prep_per_yr <- prep$oh %>%
   left_join(prep$disp %>% select(REC_ID, DISP_TOTAL, DISP_DATE), join_by(REC_ID)) %>%
   get_cid(prep$id_reg, PATIENT_ID) %>%
   filter(!is.na(MEDICINE_SUMMARY)) %>%
   filter(VISIT_DATE <= "2023-08-31") %>%
   arrange(desc(LATEST_NEXT_DATE)) %>%
   distinct(CENTRAL_ID, VISIT_DATE, .keep_all = TRUE) %>%
   mutate(
      prep_yr    = year(VISIT_DATE),
      prep_total = coalesce(DISP_TOTAL, 30) / 30,
      prep_total = abs(floor(prep_total))
   ) %>%
   filter(prep_total %in% seq(1, 14)) %>%
   group_by(CENTRAL_ID, prep_yr) %>%
   summarise(
      prep_total = sum(prep_total, na.rm = TRUE)
   ) %>%
   ungroup() %>%
   pivot_wider(
      id_cols      = CENTRAL_ID,
      names_from   = prep_yr,
      values_from  = prep_total,
      names_prefix = "prep_"
   )

prep_reg <- prep$reg %>%
   get_cid(prep$id_reg, PATIENT_ID)

try <- prep_reg %>%
   left_join(
      y  = prep$start %>%
         select(prepstart_date, prep_id, prepstart_plan = curr_prep_plan, prepstart_type = curr_prep_type),
      by = join_by(prep_id)
   ) %>%
   left_join(
      y  = read_dta(hs_data("prep", "outcome", 2023, 8), col_select = c(prep_id, prep_reg, prep_prov, prep_munc)),
      by = join_by(prep_id)
   ) %>%
   select(-curr_reg, -curr_munc, -curr_prov) %>%
   left_join(
      y  = prep_addr %>%
         select(prep_id, curr_reg, curr_prov, curr_munc),
      by = join_by(prep_id)
   ) %>%
   left_join(
      y  = prep_per_yr %>%
         inner_join(
            prep_reg %>%
               select(CENTRAL_ID, prep_id) %>%
               left_join(prep$start %>% select(-CENTRAL_ID), join_by(prep_id)) %>%
               filter(!is.na(prepstart_date)) %>%
               select(CENTRAL_ID),
            join_by(CENTRAL_ID)
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      permcurr_reg  = case_when(
         curr_reg == "UNKNOWN" ~ perm_reg,
         curr_reg == "OVERSEAS" ~ perm_reg,
         curr_reg == "" ~ perm_reg,
         TRUE ~ curr_reg
      ),
      permcurr_prov = case_when(
         curr_reg == "UNKNOWN" ~ perm_prov,
         curr_reg == "OVERSEAS" ~ perm_prov,
         curr_reg == "" ~ perm_prov,
         TRUE ~ curr_prov
      ),
      permcurr_munc = case_when(
         curr_reg == "UNKNOWN" ~ perm_munc,
         curr_reg == "OVERSEAS" ~ perm_munc,
         curr_reg == "" ~ perm_munc,
         TRUE ~ curr_munc
      )
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "permcurr_reg",
         PERM_PSGC_PROV = "permcurr_prov",
         PERM_PSGC_MUNC = "permcurr_munc"
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
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PREP_PSGC_REG  = "prep_reg",
         PREP_PSGC_PROV = "prep_prov",
         PREP_PSGC_MUNC = "prep_munc"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   mutate(
      use_prep            = if_else(permcurr_munc %in% c("", "UNKNOWN", "OVERSEAS") & !is.na(PREP_PSGC_MUNC), 1, 0, 0),
      PREP_PERM_PSGC_REG  = if_else(use_prep == 1, PREP_PSGC_REG, PERM_PSGC_REG, PERM_PSGC_REG),
      PREP_PERM_PSGC_PROV = if_else(use_prep == 1, PREP_PSGC_PROV, PERM_PSGC_PROV, PERM_PSGC_PROV),
      PREP_PERM_PSGC_MUNC = if_else(use_prep == 1, PREP_PSGC_MUNC, PERM_PSGC_MUNC, PERM_PSGC_MUNC),
   ) %>%
   left_join(
      y  = ref %>%
         select(
            PREP_PERM_PSGC_REG  = PSGC_REG,
            PREP_PERM_PSGC_PROV = PSGC_PROV,
            PREP_PERM_PSGC_MUNC = PSGC_MUNC,
            aemclass_2019_prep  = aemclass_2019,
            aemclass_2023_prep  = aemclass_2023,
         ),
      by = join_by(PREP_PERM_PSGC_REG, PREP_PERM_PSGC_PROV, PREP_PERM_PSGC_MUNC)
   ) %>%
   select(-permcurr_reg, -permcurr_prov, -permcurr_munc) %>%
   ohasis$get_addr(
      c(
         currperm_reg  = "PERM_PSGC_REG",
         currperm_prov = "PERM_PSGC_PROV",
         currperm_munc = "PERM_PSGC_MUNC"
      ),
      "nhsss"
   ) %>%
   ohasis$get_addr(
      c(
         region_prep   = "PREP_PERM_PSGC_REG",
         province_prep = "PREP_PERM_PSGC_PROV",
         muncity_prep  = "PREP_PERM_PSGC_MUNC"
      ),
      "nhsss"
   )


prep_start <- prep$start %>%
   select(prep_id, -any_of(setdiff(names(try), "prep_id"))) %>%
   left_join(
      y  = try,
      by = join_by(prep_id)
   )

prep_start

write_dta(prep_start %>% format_stata(), "H:/20231109_prepstart_2023-08_AEM.dta")

# cascade dataset with aemclass
dx <- harp$dx %>%
   mutate(
      rep_age = calc_age(bdate, "2023-08-31")
   )


