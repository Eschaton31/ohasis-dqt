hts_initial <- read_dta("C:/Users/Bene-G16/Downloads/Miscellaneous/reach_2025-04-initial.dta")
hts_debug   <- read_dta("C:/Users/Bene-G16/Downloads/Miscellaneous/reach_2025-04-debug.dta")

check <- hts_initial %>%
   anti_join(hts_debug, join_by(REC_ID))

check <- hts_debug %>%
   anti_join(hts_initial, join_by(REC_ID))

check %>%
   mutate(
      ym = format(T1_DATE, "%Y.%m")
   ) %>%
   tab(ym)

get_names(check, "DATE")
