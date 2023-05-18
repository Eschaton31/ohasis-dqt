dr             <- new.env()
dr$aem20230418 <- new.env()

local(envir = dr$aem20230418, {
   harp      <- list()
   harp$dead <- read_dta(ohasis$get_data("harp_dead", "2022", "12")) %>%
      mutate(
         proxy_death_date = as.Date(ceiling_date(as.Date(paste(sep = '-', year, month, '01')), unit = 'month')) - 1,
         ref_death_date   = if_else(
            condition = is.na(date_of_death),
            true      = proxy_death_date,
            false     = date_of_death
         )
      )

   for (yr in seq(2015, 2022))
      harp$tx[[as.character(yr)]] <- hs_data("harp_tx", "outcome", yr, 12) %>%
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
               'curr_age'
            ))
         ) %>%
         mutate(
            year                = yr,
            death_transout_date = if (!("death_transout_date" %in% colnames(.))) NA_character_ else as.character(death_transout_date),
            transin_date        = if (!("transin_date" %in% colnames(.))) NA_Date_ else transin_date,
            age                 = if (yr >= 2022) curr_age else age,
            outcome30dy         = hiv_tx_outcome(outcome, latest_nextpickup, as.Date(stri_c(yr, "-12-31")), 30),
            outcome3mos         = hiv_tx_outcome(outcome, latest_nextpickup, as.Date(stri_c(yr, "-12-31")), 3, "months"),
         )

   if (yr >= 2022) {
      harp$tx[[as.character(yr)]] %<>%
         left_join(
            y  = hs_data("harp_tx", "reg", yr, 12) %>%
               read_dta(col_select = c("confirmatory_code", "art_id")) %>%
               mutate(
                  sacclcode = if_else(!is.na(confirmatory_code), str_replace_all(confirmatory_code, "[^[:alnum:]]", ""), NA_character_)
               ),
            by = join_by(art_id)
         )
   }

   rm(yr)
})

local(envir = dr$aem20230418, {
   flat          <- list()
   flat$dead_dta <- harp$tx$`2022` %>%
      left_join(
         y  = harp$dead %>%
            select(idnum, ref_death_date, age_death = age),
         by = "idnum"
      ) %>%
      mutate(
         sex       = if_else(sex == "", "(no data)", sex, sex),
         age_death = if_else(is.na(age_death), age, age_death, age_death),
         # ref_death_date = if_else(!is.na(ref_death_date), ref_death_date, ref_death_date),
         Age_Band  = case_when(
            age_death >= 0 & age_death < 5 ~ "01_0-4",
            age_death >= 5 & age_death < 10 ~ "02_5-9",
            age_death >= 10 & age_death < 15 ~ "03_10-14",
            age_death >= 15 & age_death < 20 ~ "04_15-19",
            age_death >= 20 & age_death < 25 ~ "05_20-24",
            age_death >= 25 & age_death < 30 ~ "06_25-29",
            age_death >= 30 & age_death < 35 ~ "07_30-34",
            age_death >= 35 & age_death < 40 ~ "08_35-39",
            age_death >= 40 & age_death < 45 ~ "09_40-44",
            age_death >= 45 & age_death < 50 ~ "10_45-49",
            age_death >= 50 & age_death < 55 ~ "11_50-54",
            age_death >= 55 & age_death < 60 ~ "12_55-59",
            age_death >= 60 & age_death < 65 ~ "13_60-64",
            age_death >= 65 & age_death < 1000 ~ "14_65+",
            TRUE ~ "99_(no data)"
         ),
      )

   flat$dead <- flat$dead_dta %>%
      filter(outcome == "dead") %>%
      group_by(sex, Age_Band) %>%
      summarise(
         `2012` = sum(if_else(year(ref_death_date) == 2012, 1, 0, 0)),
         `2013` = sum(if_else(year(ref_death_date) == 2013, 1, 0, 0)),
         `2014` = sum(if_else(year(ref_death_date) == 2014, 1, 0, 0)),
         `2015` = sum(if_else(year(ref_death_date) == 2015, 1, 0, 0)),
         `2016` = sum(if_else(year(ref_death_date) == 2016, 1, 0, 0)),
         `2017` = sum(if_else(year(ref_death_date) == 2017, 1, 0, 0)),
         `2018` = sum(if_else(year(ref_death_date) == 2018, 1, 0, 0)),
         `2019` = sum(if_else(year(ref_death_date) == 2019, 1, 0, 0)),
         `2020` = sum(if_else(year(ref_death_date) == 2020, 1, 0, 0)),
         `2022` = sum(if_else(year(ref_death_date) == 2022, 1, 0, 0)),
      )

   flat$presumed <- flat$dead_dta %>%
      filter(stri_detect_fixed(outcome, "lost to "), !is.na(age)) %>%
      group_by(Age_Band) %>%
      filter(row_number() <= .1 * n()) %>%
      ungroup() %>%
      select(-Age_Band, -ref_death_date) %>%
      mutate(
         ref_death_date = latest_nextpickup,
         Age_Band       = case_when(
            age >= 0 & age < 5 ~ "01_0-4",
            age >= 5 & age < 10 ~ "02_5-9",
            age >= 10 & age < 15 ~ "03_10-14",
            age >= 15 & age < 20 ~ "04_15-19",
            age >= 20 & age < 25 ~ "05_20-24",
            age >= 25 & age < 30 ~ "06_25-29",
            age >= 30 & age < 35 ~ "07_30-34",
            age >= 35 & age < 40 ~ "08_35-39",
            age >= 40 & age < 45 ~ "09_40-44",
            age >= 45 & age < 50 ~ "10_45-49",
            age >= 50 & age < 55 ~ "11_50-54",
            age >= 55 & age < 60 ~ "12_55-59",
            age >= 60 & age < 65 ~ "13_60-64",
            age >= 65 & age < 1000 ~ "14_65+",
            TRUE ~ "99_(no data)"
         )
      ) %>%
      bind_rows(
         flat$dead_dta %>%
            filter(outcome == "dead")
      ) %>%
      group_by(sex, Age_Band) %>%
      summarise(
         `2012` = sum(if_else(year(ref_death_date) == 2012, 1, 0, 0)),
         `2013` = sum(if_else(year(ref_death_date) == 2013, 1, 0, 0)),
         `2014` = sum(if_else(year(ref_death_date) == 2014, 1, 0, 0)),
         `2015` = sum(if_else(year(ref_death_date) == 2015, 1, 0, 0)),
         `2016` = sum(if_else(year(ref_death_date) == 2016, 1, 0, 0)),
         `2017` = sum(if_else(year(ref_death_date) == 2017, 1, 0, 0)),
         `2018` = sum(if_else(year(ref_death_date) == 2018, 1, 0, 0)),
         `2019` = sum(if_else(year(ref_death_date) == 2019, 1, 0, 0)),
         `2020` = sum(if_else(year(ref_death_date) == 2020, 1, 0, 0)),
         `2022` = sum(if_else(year(ref_death_date) == 2022, 1, 0, 0)),
      )

   flat$ltfu_new <- bind_rows(harp$tx) %>%
      filter(stri_detect_fixed(outcome, "lost to ")) %>%
      mutate(
         sex      = case_when(
            sex == "" ~ "(no data)",
            TRUE ~ StrLeft(sex, 1)
         ),
         ltfu_new = if_else(
            (latest_nextpickup > as.Date(paste0(year - 1, "-09-30")) & latest_nextpickup <= as.Date(paste0(year, "-09-30")))
               & year(artstart_date) < year,
            1,
            0,
            0
         ),
         Age_Band = case_when(
            age >= 0 & age < 5 ~ "01_0-4",
            age >= 5 & age < 10 ~ "02_5-9",
            age >= 10 & age < 15 ~ "03_10-14",
            age >= 15 & age < 20 ~ "04_15-19",
            age >= 20 & age < 25 ~ "05_20-24",
            age >= 25 & age < 30 ~ "06_25-29",
            age >= 30 & age < 35 ~ "07_30-34",
            age >= 35 & age < 40 ~ "08_35-39",
            age >= 40 & age < 45 ~ "09_40-44",
            age >= 45 & age < 50 ~ "10_45-49",
            age >= 50 & age < 55 ~ "11_50-54",
            age >= 55 & age < 60 ~ "12_55-59",
            age >= 60 & age < 65 ~ "13_60-64",
            age >= 65 & age < 1000 ~ "14_65+",
            TRUE ~ "99_(no data)"
         )
      )

})

base <- dr$aem20230418$harp$tx$`2022` %>%
   rename(
      regimen_2022     = latest_regimen,
      outcome30dy_2022 = outcome30dy,
      outcome3mos_2022 = outcome3mos,
   )

for (yr in seq(2015, 2021)) {
   name_arv  <- stri_c("regimen_", yr)
   name_30dy <- stri_c("outcome30dy_", yr)
   name_3mos <- stri_c("outcome3mos_", yr)
   base %<>%
      left_join(
         y = dr$aem20230418$harp$tx[[as.character(yr)]] %>%
            mutate(
               sacclcode = if (yr <= 2016) saccl_code else sacclcode
            ) %>%
            select(
               sacclcode,
               {{name_arv}}  := latest_regimen,
               {{name_30dy}} := outcome30dy,
               {{name_3mos}} := outcome3mos,
            )
      )
}

base %>%
   format_stata() %>%
   write_dta("H:/20230512_onart_2015-2022_forAEM.dta")