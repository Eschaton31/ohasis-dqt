harp <- read_dta(hs_data("harp_full", "reg", 2023, 12)) %>%
   dxlab_to_id(
      c("HARP_FACI", "HARP_SUB_FACI"),
      c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
      ohasis$ref_faci
   )

tx <- read_dta(hs_data("harp_tx", "outcome", 2023, 12))

dx_clients <- harp %>%
   mutate(
      tat_enroll = floor(interval(reactive_date, artstart_date) / days(1)),


      sdart      = if_else(tat_enroll <= 1, 1, 0, 0),
      rai        = if_else(tat_enroll <= 7, 1, 0, 0),
      `>1week`   = if_else(tat_enroll > 7, 1, 0, 0),

      onart      = coalesce(onart, 0),
      onart      = as.character(onart)
   )

prc_sdart <- dx_clients %>%
   filter(sdart == 1) %>%
   distinct(HARP_FACI, HARP_SUB_FACI)

not_sdart <- dx_clients %>%
   filter(sdart == 0) %>%
   distinct(HARP_FACI, HARP_SUB_FACI) %>%
   anti_join(prc_sdart, join_by(HARP_FACI))


l2c_sdart_y <- dx_clients %>%
   filter(year >= 2018) %>%
   filter(HARP_FACI %in% prc_sdart$HARP_FACI) %>%
   # group_by(HARP_FACI, HARP_SUB_FACI) %>%
   summarise(
      dx        = n(),
      everonart = sum(!is.na(everonart), na.rm = TRUE)
   ) %>%
   mutate(
      perc_enroll = (everonart / dx) * 100
   )

l2c_sdart_n <- dx_clients %>%
   filter(year >= 2018) %>%
   filter(HARP_FACI %in% not_sdart$HARP_FACI) %>%
   # group_by(HARP_FACI, HARP_SUB_FACI) %>%
   summarise(
      dx        = n(),
      everonart = sum(!is.na(everonart), na.rm = TRUE)
   ) %>%
   mutate(
      perc_enroll = (everonart / dx) * 100
   )

## vl suppression --------------------------------------------------------------

tx2023_12 <- read_dta(hs_data("harp_tx", "outcome", 2023, 12))
periods   <- c(
   "2020_06",
   "2020_12",
   "2021_06",
   "2021_12",
   "2022_06",
   "2022_12",
   "2023_06",
   "2023_12"
)

final_06 <- tx2023_12 %>% select(idnum, art_id, artstart_date)
final_12 <- tx2023_12 %>% select(idnum, art_id, artstart_date)
for (period in periods) {
   yr <- StrLeft(period, 4)
   mo <- StrRight(period, 2)
   final_06 %<>%
      left_join(
         y          = read_dta(hs_data("harp_tx", "outcome", yr, mo)) %>%
            select(
               idnum,
               outcome,
               latest_ffupdate,
               latest_nextpickup,
               vlp12m
            ),
         by         = join_by(idnum),
         na_matches = "never"
      ) %>%
      mutate(
         start_ffup      = interval(artstart_date, latest_ffupdate) / months(1),
         start_ffup      = floor(start_ffup),
         is_06           = if_else(start_ffup <= 6, 1, 0, 0),
         outcome         = hiv_tx_outcome(outcome, latest_nextpickup, end_ym(yr, mo), 30),
         outcome         = if_else(is_06 == 0, NA_character_, outcome, outcome),
         latest_ffupdate = if_else(is_06 == 0, NA_Date_, latest_ffupdate, latest_ffupdate),
         vlsup           = if_else(outcome == "alive on arv" & vlp12m == 1, 1, 0, 0),
         vltest          = if_else(outcome == "alive on arv" & !is.na(vlp12m), 1, 0, 0),
      ) %>%
      select(-vlp12m) %>%
      rename_all(
         ~case_when(
            . == "outcome" ~ paste0("out", str_replace(period, "_", "")),
            . == "latest_ffupdate" ~ paste0("ffup", str_replace(period, "_", "")),
            . == "latest_nextpickup" ~ paste0("pickup", str_replace(period, "_", "")),
            . == "vlsup" ~ paste0("vlsup", str_replace(period, "_", "")),
            . == "vltest" ~ paste0("vltest", str_replace(period, "_", "")),
            TRUE ~ .
         )
      )
}

for (period in periods) {
   final_12 %<>%
      left_join(
         y          = read_dta(hs_data("harp_tx", "outcome", yr, mo)) %>%
            select(
               idnum,
               outcome,
               latest_ffupdate,
               latest_nextpickup,
               vlp12m
            ),
         by         = join_by(idnum),
         na_matches = "never"
      ) %>%
      mutate(
         start_ffup      = interval(artstart_date, latest_ffupdate) / months(1),
         start_ffup      = floor(start_ffup),
         is_06           = if_else(start_ffup <= 12, 1, 0, 0),
         outcome         = hiv_tx_outcome(outcome, latest_nextpickup, end_ym(yr, mo), 30),
         outcome         = if_else(is_06 == 0, NA_character_, outcome, outcome),
         latest_ffupdate = if_else(is_06 == 0, NA_Date_, latest_ffupdate, latest_ffupdate),
         vlsup           = if_else(outcome == "alive on arv" & vlp12m == 1, 1, 0, 0),
         vltest          = if_else(outcome == "alive on arv" & !is.na(vlp12m), 1, 0, 0),
      ) %>%
      select(-vlp12m) %>%
      rename_all(
         ~case_when(
            . == "outcome" ~ paste0("out", str_replace(period, "_", "")),
            . == "latest_ffupdate" ~ paste0("ffup", str_replace(period, "_", "")),
            . == "latest_nextpickup" ~ paste0("pickup", str_replace(period, "_", "")),
            . == "vlsup" ~ paste0("vlsup", str_replace(period, "_", "")),
            . == "vltest" ~ paste0("vltest", str_replace(period, "_", "")),
            TRUE ~ .
         )
      )
}

outcome_at06 <- final_06 %>%
   pivot_longer(
      cols      = starts_with("out"),
      names_to  = "outcome",
      values_to = "value"
   ) %>%
   filter(!is.na(value)) %>%
   group_by(idnum) %>%
   summarise(
      outcome_any = paste0(collapse = ",", unique(sort(value))),
   ) %>%
   ungroup()

vl_at06 <- final_06 %>%
   pivot_longer(
      cols      = starts_with("vlsup"),
      names_to  = "vlsup",
      values_to = "value"
   ) %>%
   filter(!is.na(value)) %>%
   group_by(idnum) %>%
   summarise(
      sup_any = sum(value, na.rm = TRUE),
   ) %>%
   ungroup()

test_at06 <- final_06 %>%
   pivot_longer(
      cols      = starts_with("vltest"),
      names_to  = "vltest",
      values_to = "value"
   ) %>%
   filter(!is.na(value)) %>%
   group_by(idnum) %>%
   summarise(
      test_any = sum(value, na.rm = TRUE),
   ) %>%
   ungroup()

outcome_at12 <- final_12 %>%
   pivot_longer(
      cols      = starts_with("out"),
      names_to  = "outcome",
      values_to = "value"
   ) %>%
   filter(!is.na(value)) %>%
   group_by(idnum) %>%
   summarise(
      outcome_any = paste0(collapse = ",", unique(sort(value)))
   ) %>%
   ungroup()


vl_at12 <- final_12 %>%
   pivot_longer(
      cols      = starts_with("vlsup"),
      names_to  = "vlsup",
      values_to = "value"
   ) %>%
   filter(!is.na(value)) %>%
   group_by(idnum) %>%
   summarise(
      sup_any = sum(value, na.rm = TRUE),
   ) %>%
   ungroup()

test_at12 <- final_12 %>%
   pivot_longer(
      cols      = starts_with("vltest"),
      names_to  = "vltest",
      values_to = "value"
   ) %>%
   filter(!is.na(value)) %>%
   group_by(idnum) %>%
   summarise(
      test_any = sum(value, na.rm = TRUE),
   ) %>%
   ungroup()

analysis_06 <- dx_clients %>%
   left_join(
      y  = outcome_at06,
      by = join_by(idnum)
   ) %>%
   left_join(
      y  = vl_at06,
      by = join_by(idnum)
   ) %>%
   mutate(
      retained = if_else(outcome_any == "alive on arv", "1", "0", "0")
   )

analysis_06 <- dx_clients %>%
   left_join(
      y  = outcome_at06,
      by = join_by(idnum)
   ) %>%
   left_join(
      y  = vl_at06,
      by = join_by(idnum)
   ) %>%
   left_join(
      y  = test_at06,
      by = join_by(idnum)
   ) %>%
   mutate(
      retained = if_else(outcome_any == "alive on arv", "1", "0", "0")
   )

analysis_12 <- dx_clients %>%
   left_join(
      y  = outcome_at12,
      by = join_by(idnum)
   ) %>%
   left_join(
      y  = vl_at12,
      by = join_by(idnum)
   ) %>%
   left_join(
      y  = test_at12,
      by = join_by(idnum)
   ) %>%
   mutate(
      retained = if_else(outcome_any == "alive on arv", "1", "0", "0")
   )
