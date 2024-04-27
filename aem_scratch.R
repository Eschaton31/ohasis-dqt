mega_final <- mega %>%
   mutate(
      aemclass_2019 = if_else(is.na(aemclass_2019) & muncity != "UNKNOWN", "c", aemclass_2019, aemclass_2019),
      aemclass_2023 = if_else(is.na(aemclass_2023) & muncity != "UNKNOWN", "c", aemclass_2023, aemclass_2023),
   ) %>%
   left_join(
      y = dx %>%
         select(idnum, rep_age),
      by = join_by(idnum)
   ) %>%
   mutate(
      cur_age = coalesce(end_age_2023, rep_age)
   )

catb        <- list()
catb$`2019` <- list(
   est        = 36563,
   plhiv      = mega_final %>%
      filter(aemclass_2023 == "b", plhiv_2023 == 1, cur_age >= 15) %>%
      nrow(),
   everonart  = mega_final %>%
      filter(aemclass_2023 == "b", plhiv_2023 == 1, cur_age >= 15, everonart == 1) %>%
      nrow(),
   onart      = mega_final %>%
      filter(aemclass_2023 == "b", plhiv_2023 == 1, cur_age >= 15, outcome3mos_2023 == "alive on arv") %>%
      nrow(),
   vltest     = mega_final %>%
      filter(aemclass_2023 == "b", plhiv_2023 == 1, cur_age >= 15, outcome3mos_2023 == "alive on arv", is.na(baseline_vl), !is.na(vlp12m)) %>%
      nrow(),
   vlsuppress = mega_final %>%
      filter(aemclass_2023 == "b", plhiv_2023 == 1, cur_age >= 15, outcome3mos_2023 == "alive on arv", is.na(baseline_vl), !is.na(vlp12m), vl_result < 1000) %>%
      nrow()
)

catc <- list()
catc$`2019` <- list(
   est        = 25926,
   plhiv      = mega_final %>%
      filter(aemclass_2023 == "c", plhiv_2023 == 1, cur_age >= 15) %>%
      nrow(),
   everonart  = mega_final %>%
      filter(aemclass_2023 == "c", plhiv_2023 == 1, cur_age >= 15, everonart == 1) %>%
      nrow(),
   onart      = mega_final %>%
      filter(aemclass_2023 == "c", plhiv_2023 == 1, cur_age >= 15, outcome3mos_2023 == "alive on arv") %>%
      nrow(),
   vltest     = mega_final %>%
      filter(aemclass_2023 == "c", plhiv_2023 == 1, cur_age >= 15, outcome3mos_2023 == "alive on arv", is.na(baseline_vl), !is.na(vlp12m)) %>%
      nrow(),
   vlsuppress = mega_final %>%
      filter(aemclass_2023 == "c", plhiv_2023 == 1, cur_age >= 15, outcome3mos_2023 == "alive on arv", is.na(baseline_vl), !is.na(vlp12m), vl_result < 1000) %>%
      nrow()
)