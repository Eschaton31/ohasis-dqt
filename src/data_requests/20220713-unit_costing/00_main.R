aem_cost <- list()
for (i  in seq(12, 1)) {
   mo                  <- stri_pad_left(i, 2, "0")
   aem_cost$harp[[mo]] <- read_dta(ohasis$get_data("harp_tx-outcome", "2021", mo), col_select = c("idnum", "art_reg1", "latest_ffupdate")) %>%
	  filter(!is.na(idnum)) %>%
	  mutate(
		 tle      = if_else(
			condition = stri_detect_fixed(art_reg1, "tdf") &
			   stri_detect_fixed(art_reg1, "3tc") &
			   stri_detect_fixed(art_reg1, "efv"),
			true      = 1,
			false     = 0,
			missing   = 0
		 ),
		 tld      = if_else(
			condition = stri_detect_fixed(art_reg1, "tdf") &
			   stri_detect_fixed(art_reg1, "3tc") &
			   stri_detect_fixed(art_reg1, "dtg"),
			true      = 1,
			false     = 0,
			missing   = 0
		 ),
		 tld_date = if_else(
			condition = stri_detect_fixed(art_reg1, "tdf") &
			   stri_detect_fixed(art_reg1, "3tc") &
			   stri_detect_fixed(art_reg1, "dtg"),
			true      = latest_ffupdate,
			false     = NA_Date_,
			missing   = NA_Date_
		 ),
		 tle_date = if_else(
			condition = stri_detect_fixed(art_reg1, "tdf") &
			   stri_detect_fixed(art_reg1, "3tc") &
			   stri_detect_fixed(art_reg1, "efv"),
			true      = latest_ffupdate,
			false     = NA_Date_,
			missing   = NA_Date_
		 ),
		 month    = mo
	  ) %>%
	  select(-art_reg1, -latest_ffupdate)
}

aem_cost$tld <- bind_rows(aem_cost$harp) %>%
   group_by(idnum) %>%
   summarise(
	  months_tld = sum(tld, na.rm = TRUE),
	  first_tld  = suppress_warnings(min(tld_date, na.rm = TRUE), "returning [\\-]*Inf"),
   ) %>%
   ungroup() %>%
   left_join(
	  y  = read_dta(ohasis$get_data("harp_tx-outcome", "2021", "12"), col_select = c("idnum", "baseline_vl", "vlp12m", "vl_date", "artstart_date")),
	  by = "idnum"
   ) %>%
   mutate(
	  newonart    = if_else(year(artstart_date) == 2021, 1, 0, 0),
	  since_tld   = floor(interval(first_tld, as.Date("2021-12-31")) / months(1)),
	  since_tld_c = case_when(
		 since_tld <= 1 ~ "1) 1 mo",
		 since_tld %in% seq(2, 3) ~ "2) 2-3 mos",
		 since_tld %in% seq(4, 6) ~ "3) 4-6 mos",
		 since_tld %in% seq(7, 12) ~ "4) 6-12 mos",
		 since_tld %in% seq(13, 1000) ~ "5) 1yr +",
		 TRUE ~ "non-tld"
	  )
   ) %>%
   filter(!is.na(artstart_date))

aem_cost$tle <- bind_rows(aem_cost$harp) %>%
   group_by(idnum) %>%
   summarise(
	  months_tle = sum(tle, na.rm = TRUE),
	  first_tle  = suppress_warnings(min(tle_date, na.rm = TRUE), "returning [\\-]*Inf"),
   ) %>%
   ungroup() %>%
   left_join(
	  y  = read_dta(ohasis$get_data("harp_tx-outcome", "2021", "12"), col_select = c("idnum", "baseline_vl", "vlp12m", "vl_date", "artstart_date")),
	  by = "idnum"
   ) %>%
   mutate(
	  newonart    = if_else(year(artstart_date) == 2021, 1, 0, 0),
	  since_tle   = floor(interval(first_tle, as.Date("2021-12-31")) / months(1)),
	  since_tle_c = case_when(
		 since_tle <= 1 ~ "1) 1 mo",
		 since_tle %in% seq(2, 3) ~ "2) 2-3 mos",
		 since_tle %in% seq(4, 6) ~ "3) 4-6 mos",
		 since_tle %in% seq(7, 12) ~ "4) 6-12 mos",
		 since_tle %in% seq(13, 1000) ~ "5) 1yr +",
		 TRUE ~ "non-tle"
	  )
   ) %>%
   filter(!is.na(artstart_date))

aem_cost$dx <- read_dta(ohasis$get_data("harp_tx-outcome", "2021", "12"), col_select = c("idnum", "baseline_vl", "vlp12m", "vl_date", "artstart_date", "art_reg1")) %>%
   filter(!is.na(idnum)) %>%
   left_join(
	  y  = read_dta(ohasis$get_data("harp_dx", "2021", "12"), col_select = c("idnum", "year")),
	  by = "idnum"
   ) %>%
   mutate(
	  tle = if_else(
		 condition = stri_detect_fixed(art_reg1, "tdf") &
			stri_detect_fixed(art_reg1, "3tc") &
			stri_detect_fixed(art_reg1, "efv"),
		 true      = 1,
		 false     = 0,
		 missing   = 0
	  ),
	  tld = if_else(
		 condition = stri_detect_fixed(art_reg1, "tdf") &
			stri_detect_fixed(art_reg1, "3tc") &
			stri_detect_fixed(art_reg1, "dtg"),
		 true      = 1,
		 false     = 0,
		 missing   = 0
	  ),
   ) %>%
   filter(year >= 2020, tle == 1)

.tab(aem_cost$tld %>% filter(is.na(baseline_vl), months_tld > 0, vlp12m == 1, first_tld <= vl_date, year(artstart_date) == 2021 & month(artstart_date) <= 6), c(since_tld_c, vlp12m))
.tab(aem_cost$tle %>% filter(is.na(baseline_vl), months_tle > 0, vlp12m == 1, first_tle <= vl_date, year(artstart_date) == 2021 & month(artstart_date) <= 6), c(since_tle_c, vlp12m))
.tab(aem_cost$dx %>% filter(is.na(baseline_vl), vlp12m == 1, year(artstart_date) == 2021 & month(artstart_date) <= 6), vlp12m)