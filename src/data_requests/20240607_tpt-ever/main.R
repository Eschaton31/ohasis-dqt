conn <- ohasis$conn("lw")

tpt_ever <- QB$new(conn)$from("ohasis_warehouse.tpt_ever")$get()
id_reg   <- QB$new(conn)$from("ohasis_warehouse.id_registry")$select("CENTRAL_ID", "PATIENT_ID")$get()

dbDisconnect(conn)

tx_202312 <- hs_data("harp_tx", "reg", 2023, 12) %>%
   read_dta(col_select = c(art_id, PATIENT_ID)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(
      y  = hs_data("harp_tx", "outcome", 2023, 12) %>%
         read_dta(col_select = c(art_id, outcome, onart)),
      by = join_by(art_id)
   )

art_ever_tpt <- tx_202312 %>%
   left_join(
      y = tpt_ever,
      by = join_by(CENTRAL_ID == CID)
   ) %>%
   mutate_at(
      .vars = vars(no_tb, ever_ipt),
      ~coalesce(., as.integer(0))
   ) %>%
   rename(
      ever_no_tb = no_tb
   )

art_ever_tpt %>% tab(onart, no_tb, ever_ipt)

art_ever_tpt %>%
   filter(onart == 1) %>%
   tab(ever_no_tb)

art_ever_tpt %>%
   filter(onart == 1) %>%
   tab(ever_ipt)

art_ever_tpt %>%
   filter(onart == 1) %>%
   tab(ever_no_tb, ever_ipt)

art_ever_tpt %>%
   filter(onart == 1, ever_no_tb == 1) %>%
   tab(ever_ipt)