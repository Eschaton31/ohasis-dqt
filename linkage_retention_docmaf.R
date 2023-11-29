full <- hs_data("harp_full", "reg", 2023, 8) %>%
   read_dta() %>%
   mutate(
      overseas_addr       = case_when(
         muncity == "OUT OF COUNTRY" ~ 1,
         TRUE ~ 0
      ),

      end_date            = as.Date("2023-08-31"),

      reactive_date       = coalesce(blood_extract_date, specimen_receipt_date, test_date, t0_date, visit_date, confirm_date) %>% as.Date(),

      tat_reactive_enroll = interval(reactive_date, artstart_date) / days(1),
      tat_confirm_enroll  = interval(confirm_date, artstart_date) / days(1),

      muncity             = case_when(
         muncity == "PINAMUNGAHAN" & province == "CEBU" ~ "PINAMUNGAJAN",
         muncity == "SAN JUAN" & province == "BULACAN" ~ "MALOLOS",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ muncity
      ),
      province            = case_when(
         muncity == "UNKNOWN" & province == "NCR" ~ "UNKNOWN",
         province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ province
      ),
      region              = case_when(
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ region
      ),

      mortality           = if_else(
         (dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"),
         0,
         1,
         1
      ),


      dx                  = if_else(!is.na(idnum), 1, 0, 0),
      dx_plhiv            = if_else(!is.na(idnum) &
                                       (dead != 1 | is.na(dead)) &
                                       (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      plhiv               = if_else((dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      everonart           = if_else(everonart == 1, 1, 0, 0),
      everonart_plhiv     = if_else(everonart == 1 & outcome != "alive on arv", 1, 0, 0),
      ononart             = if_else(onart == 1, 1, 0, 0),


      baseline_vl_new     = if_else(
         condition = floor(interval(artstart_date, vl_date) / months(1)) < 6,
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      vl_tested           = if_else(
         onart == 1 &
            (is.na(baseline_vl_new) | baseline_vl_new == 0) &
            !is.na(vlp12m),
         1,
         0,
         0
      ),
      vl_suppressed       = if_else(
         onart == 1 &
            (is.na(baseline_vl_new) | baseline_vl_new == 0) &
            vlp12m == 1,
         1,
         0,
         0
      ),
   )