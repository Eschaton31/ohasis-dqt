full <- read_dta(hs_data("harp_full", "reg", 2023, 12))

dx_migrate <- full %>%
   mutate(
      migrate   = case_when(
         region != "UNKNOWN" & region != dx_region ~ 1,
         TRUE ~ 0
      ),
      everonart = if_else(everonart == 1, "Yes", "No", "No"),
      onart     = if_else(onart == 1, "Yes", "No", "No"),

      agegrp    = gen_agegrp(age, "harp")
   )

# TPP: 2022 Diagnosed individuals status by end of 2023

# for dx migrations: distribution across regions
dx_migrate %>%
   group_by(region) %>%
   summarise(
      dx_ever  = n(),
      dx_2022  = sum(year == 2022),
      migrated = sum(year == 2022 & migrate == 1)
   ) %>%
   ungroup() %>%
   mutate(
      perc_migrate = (migrated / dx_2022)
   ) %>%
   as_tibble() %>%
   arrange(desc(dx_ever)) %>%
   write_clip()

dx_migrate %>%
   filter(migrate == 1, year == 2022) %>%
   group_by(region) %>%
   summarise(migrate = n()) %>%
   write_clip()

dx_migrate %>%
   filter(year == 2022) %>%
   filter(migrate == 0) %>%
   group_by(agegrp) %>%
   summarise(dx = n()) %>%
   write_clip()

dx_migrate %>%
   filter(year == 2022) %>%
   group_by(agegrp) %>%
   summarise(
      nonmigrant = sum(migrate == 0),
      migrated   = sum(migrate == 1)
   ) %>%
   write_clip()

dx_migrate %>%
   filter(year == 2022) %>%
   group_by(class2022) %>%
   summarise(
      nonmigrant = sum(migrate == 0),
      migrated   = sum(migrate == 1)
   ) %>%
   write_clip()


# for dx migrations: is there impact in initiation?
dx_migrate %>%
   filter(year == 2022) %>%
   group_by(migrate) %>%
   summarise(
      never   = sum(everonart == "No"),
      started = sum(everonart == "Yes"),
   ) %>%
   write_clip()
tab(migrate, cross_tab = everonart, cross_return = "freq+row")

# for dx migrations: is there impact in current art status?
dx_migrate %>%
   filter(year == 2022) %>%
   filter(everonart == "Yes") %>%
   group_by(migrate) %>%
   summarise(
      ltfu  = sum(onart == "No"),
      onart = sum(onart == "Yes"),
   ) %>%
   write_clip()

dx_migrate %>%
   filter(year == 2022) %>%
   filter(everonart == "Yes") %>%
   tab(migrate, cross_tab = onart, cross_return = "freq+row", )

wf   <- waterfall_linelist("2022-12-31", "2023-12-31")
plot <- waterfall_aggregate(
   wf$old %>% filter(curr_age %in% seq(15, 17)),
   wf$new %>% filter(curr_age %in% seq(15, 17)),
   "2022 v 2023 among 15-17 yo (2022)"
)
plot_bmp(plot, "H:/20240614_wf_1517_2022v2023.bmp")