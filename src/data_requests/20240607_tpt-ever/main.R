conn <- ohasis$conn("lw")

tpt_all <- QB$new(conn)$
   from("ohasis_warehouse.form_art_bc")$
   select(PATIENT_ID,
          REC_ID,
          TB_IPT_STATUS,
          TB_IPT_OUTCOME,
          TB_IPT_OUTCOME_OTHER,
          TB_IPT_START_DATE,
          TB_IPT_END_DATE)$
   whereNotNull("TB_IPT_STATUS", "or")$
   whereNotNull("TB_IPT_OUTCOME", "or")$
   whereNotNull("TB_IPT_OUTCOME_OTHER", "or")$
   whereNotNull("TB_IPT_START_DATE", "or")$
   whereNotNull("TB_IPT_END_DATE", "or")$
   get()

# tpt_ever <- QB$new(conn)$from("ohasis_warehouse.tpt_ever")$get()
id_reg <- QB$new(conn)$from("ohasis_warehouse.id_registry")$select("CENTRAL_ID", "PATIENT_ID")$get()

dbDisconnect(conn)

tpt_ever <- tpt_all %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      keep = case_when(
         TB_IPT_STATUS == "0_Not on IPT" ~ 0,
         TRUE ~ 1
      )
   ) %>%
   filter(keep == 1) %>%
   distinct(CENTRAL_ID) %>%
   mutate(ever_ipt = 1)

tx_202406 <- hs_data("harp_tx", "reg", 2024, 6) %>%
   read_dta(col_select = c(art_id, PATIENT_ID)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(
      y  = hs_data("harp_tx", "outcome", 2024, 6) %>%
         read_dta(col_select = c(art_id, outcome, onart)),
      by = join_by(art_id)
   )

art_ever_tpt <- tx_202406 %>%
   left_join(
      y  = tpt_ever,
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate_at(
      .vars = vars(ever_ipt),
      ~coalesce(., as.integer(0))
   )

art_ever_tpt %>% tab(onart, no_tb, ever_ipt)

art_ever_tpt %>%
   filter(onart == 1) %>%
   tab(ever_no_tb)

art_ever_tpt %>%
   tab(ever_ipt)

art_ever_tpt %>%
   filter(onart == 1) %>%
   tab(ever_ipt)

art_ever_tpt %>%
   filter(onart == 1) %>%
   tab(ever_no_tb, ever_ipt)

art_ever_tpt %>%
   filter(onart == 1, ever_no_tb == 1) %>%
   tab(ever_ipt)
