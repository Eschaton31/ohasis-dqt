##  For AEM Subnational --------------------------------------------------------

ref <- psgc_aem(ohasis$ref_addr)
dx  <- list(
   `2021` = read_dta(hs_data("harp_full", "reg", 2021, 12)) %>% mutate(end_ref = as.Date("2021-12-31")),
   `2022` = read_dta(hs_data("harp_full", "reg", 2022, 12)) %>% mutate(end_ref = as.Date("2022-12-31")),
   `2023` = read_dta(hs_data("harp_full", "reg", 2023, 6)) %>% mutate(end_ref = as.Date("2023-06-30"))
)
dx  <- lapply(dx, function(data, ref) {
   data %<>%
      select(-starts_with("PSGC")) %>%
      harp_addr_to_id(
         ohasis$ref_addr,
         c(
            PERM_REG  = "region",
            PERM_PROV = "province",
            PERM_MUNC = "muncity"
         ),
         aem_sub_ntl = TRUE
      ) %>%
      select(-region, -province, -muncity) %>%
      left_join(
         y  = ref$addr %>%
            select(
               PERM_REG  = PSGC_REG,
               PERM_PROV = PSGC_PROV,
               PERM_MUNC = PSGC_MUNC,
               region    = NHSSS_REG,
               province  = NHSSS_PROV,
               muncity   = NHSSS_AEM
            ) %>%
            mutate_at(
               .vars = vars(starts_with("PERM_")),
               ~str_replace_all(., "^PH", "")
            ),
         by = join_by(PERM_REG, PERM_PROV, PERM_MUNC)
      ) %>%
      mutate(
         muncity     = if_else(PERM_MUNC == "072250000", "TALISAY", muncity, muncity),
         mortality   = if_else(dead == 1 | outcome == "dead" | mort == 1, 1, 0, 0),
         dx          = if_else(!is.na(idnum), 1, 0, 0),
         dx_plhiv    = if_else(!is.na(idnum) & dead == 0, 1, 0, 0),
         plhiv       = if_else(mortality == 0, 1, 0, 0),

         onart       = case_when(
            outcome == "alive on arv" ~ 1,
            TRUE ~ 0
         ),
         dx15        = if_else(plhiv == 1 & cur_age >= 15, 1, 0, 0),
         dx          = if_else(plhiv == 1, 1, 0, 0),
         everonart   = if_else(plhiv == 1 & everonart == 1, 1, 0, 0),
         onart       = if_else(plhiv == 1 & onart == 1, 1, 0, 0),
         vl_tested   = if_else(plhiv == 1 &
                                  onart == 1 &
                                  is.na(baseline_vl) &
                                  !is.na(vlp12m), 1, 0, 0),
         vl_suppress = if_else(plhiv == 1 &
                                  onart == 1 &
                                  is.na(baseline_vl) &
                                  vlp12m == 1, 1, 0, 0),
      )
}, ref = ref)

dx$`2021` %>%
   group_by(region, province, muncity) %>%
   summarise_at(
      .vars = vars(dx15, dx, everonart, onart, vl_tested, vl_suppress),
      list(
         `2021` = ~sum(.)
      )
   ) %>%
   ungroup() %>%
   full_join(
      y  = dx$`2022` %>%
         group_by(region, province, muncity) %>%
         summarise_at(
            .vars = vars(dx15, dx, everonart, onart, vl_tested, vl_suppress),
            list(
               `2022` = ~sum(.)
            )
         ) %>%
         ungroup(),
      by = join_by(region, province, muncity)
   ) %>%
   ungroup() %>%
   full_join(
      y  = dx$`2023` %>%
         group_by(region, province, muncity) %>%
         summarise_at(
            .vars = vars(dx15, dx, everonart, onart, vl_tested, vl_suppress),
            list(
               `2023` = ~sum(.)
            )
         ) %>%
         ungroup(),
      by = join_by(region, province, muncity)
   ) %>%
   mutate(
      id = str_c(region, province, muncity)
   ) %>%
   select(
      region,
      province,
      muncity,
      id,
      starts_with("dx15_"),
      starts_with("dx_"),
      starts_with("everonart"),
      starts_with("onart"),
      starts_with("vl_tested"),
      starts_with("vl_suppress"),
   ) %>%
   write_sheet("1I-KM6JP8E1kaT_VwAJnMcqliQDjjdfhtIV2ytBoQKC4", "runs")
