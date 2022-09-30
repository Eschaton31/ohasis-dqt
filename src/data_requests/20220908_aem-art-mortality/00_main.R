dr             <- new.env()
dr$aem20220908 <- new.env()

local(envir = dr$aem20220908, {
   harp      <- list()
   harp$dead <- read_dta(ohasis$get_data("harp_dead", "2021", "12")) %>%
      mutate(
         proxy_death_date = as.Date(ceiling_date(as.Date(paste(sep = '-', year, month, '01')), unit = 'month')) - 1,
         ref_death_date   = if_else(
            condition = is.na(date_of_death),
            true      = proxy_death_date,
            false     = date_of_death
         )
      )

   for (yr in seq(2015, 2021))
      harp$tx[[as.character(yr)]] <- read_dta(hs_data("harp_tx", "outcome", yr, 12)) %>%
         mutate(
            year                = yr,
            death_transout_date = if (yr != 2021) NA_Date_ else death_transout_date,
            transin_date        = if (yr != 2021) NA_Date_ else transin_date,
         ) %>%
         select(
            any_of(
               c(
                  'idnum',
                  'outcome',
                  'artstart_date',
                  'latest_ffupdate',
                  'latest_nextpickup',
                  'death_transout_date',
                  'age',
                  'sex',
                  'year'
               )
            )
         )

   rm(yr)
})

local(envir = dr$aem20220908, {
   flat          <- list()
   flat$dead_dta <- harp$tx$`2021` %>%
      left_join(
         y  = harp$dead %>%
            select(idnum, ref_death_date, age_death = age),
         by = "idnum"
      ) %>%
      mutate(
         sex            = if_else(sex == "", "(no data)", sex, sex),
         age_death      = if_else(is.na(age_death), age, age_death, age_death),
         ref_death_date = if_else(!is.na(ref_death_date), ref_death_date, death_transout_date, ref_death_date),
         Age_Band       = case_when(
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
         `2021` = sum(if_else(year(ref_death_date) == 2021, 1, 0, 0)),
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
         `2021` = sum(if_else(year(ref_death_date) == 2021, 1, 0, 0)),
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
      ) %>%
      group_by(sex, Age_Band) %>%
      summarise(
         `2012` = sum(if_else(ltfu_new == 1 & year == 2012, 1, 0, 0)),
         `2013` = sum(if_else(ltfu_new == 1 & year == 2013, 1, 0, 0)),
         `2014` = sum(if_else(ltfu_new == 1 & year == 2014, 1, 0, 0)),
         `2015` = sum(if_else(ltfu_new == 1 & year == 2015, 1, 0, 0)),
         `2016` = sum(if_else(ltfu_new == 1 & year == 2016, 1, 0, 0)),
         `2017` = sum(if_else(ltfu_new == 1 & year == 2017, 1, 0, 0)),
         `2018` = sum(if_else(ltfu_new == 1 & year == 2018, 1, 0, 0)),
         `2019` = sum(if_else(ltfu_new == 1 & year == 2019, 1, 0, 0)),
         `2020` = sum(if_else(ltfu_new == 1 & year == 2020, 1, 0, 0)),
         `2021` = sum(if_else(ltfu_new == 1 & year == 2021, 1, 0, 0)),
      )
})