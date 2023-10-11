hts_where       <- r"(REC_ID IN (SELECT SOURCE_REC FROM ohasis_warehouse.rec_link))"
forms           <- list()
forms$form_a    <- lw_conn %>%
   dbTable(
      "ohasis_warehouse",
      "form_a",
      where     = hts_where,
      raw_where = TRUE
   )
forms$form_hts  <- lw_conn %>%
   dbTable(
      "ohasis_warehouse",
      "form_hts",
      where     = hts_where,
      raw_where = TRUE
   )
forms$form_cfbs <- lw_conn %>%
   dbTable(
      "ohasis_warehouse",
      "form_cfbs",
      where     = hts_where,
      raw_where = TRUE
   )
dbDisconnect(lw_conn)

prep <- read_dta(hs_data("prep", "reg", 2023, 6)) %>%
   left_join(
      y = bind_rows(forms$form_hts, forms$form_a, forms$form_cfbs) %>%
         distinct(REC_ID, .keep_all = TRUE) %>%
         mutate(
            ADDR_REG  = coalesce(PERM_PSGC_REG, CURR_PSGC_REG),
            ADDR_PROV = coalesce(PERM_PSGC_PROV, CURR_PSGC_PROV),
            ADDR_MUNC = coalesce(PERM_PSGC_MUNC, CURR_PSGC_MUNC),
         ) %>%
         select(
            HTS_REC = REC_ID,
            ADDR_REG,
            ADDR_PROV,
            ADDR_MUNC
         )
   ) %>%
   ohasis$get_addr(
      c(
         perm_curr_reg  = "ADDR_REG",
         perm_curr_prov = "ADDR_PROV",
         perm_curr_munc = "ADDR_MUNC"
      ),
      "nhsss"
   ) %>%
   mutate_at(
      .vars = vars(starts_with("perm_curr_")),
      ~coalesce(., "UNKNOWN")
   )

prep %>%
   format_stata() %>%
   write_dta("D:/20230710_reg-prep_2023-06_with_address.dta")
