conn      <- connect("ohasis-lw")
id_reg    <- QB$new(conn)$from("ohasis_warehouse.id_registry")$select("CENTRAL_ID", "PATIENT_ID")$get()
form_prep <- QB$new(conn)$
   from("ohasis_warehouse.form_prep")$
   whereNotNull("MEDICINE_SUMMARY")$
   get()
dbDisconnect(conn)

prep_disp <- form_prep %>%
   filter(VISIT_DATE %between% c("2021-01-01", "2024-07-31")) %>%
   convert_prep("code") %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      NEXT_DATE   = as.Date(LATEST_NEXT_DATE),
      NEXT_DATE   = coalesce(NEXT_DATE, VISIT_DATE %m+% days(30)),
      days_prep   = interval(VISIT_DATE, NEXT_DATE) / days(1),
      months_prep = interval(VISIT_DATE, NEXT_DATE) / months(1),
      arv_worth   = case_when(
         days_prep == 0 ~ 'none',
         days_prep > 0 & days_prep <= 30 ~ '01mos of PrEP',
         days_prep > 31 & days_prep <= 60 ~ '02mos of PrEP',
         days_prep > 61 & days_prep <= 90 ~ '03mos of PrEP',
         days_prep > 91 & days_prep <= 180 ~ '06mos of PrEP',
         days_prep > 181 & days_prep <= 365.25 ~ '12mos PrEP',
         days_prep > 365.25 ~ '>12mos of PrEP',
         TRUE ~ '7_(no data)'
      ),
   )

prep       <- read_dta(hs_data("prep", "reg", 2024, 7)) %>%
   select(prep_id, PATIENT_ID) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(
      y = read_dta(hs_data("prep", "outcome", 2024, 7), col_select = c(prep_id, prepstart_date)),
      by = join_by(prep_id)
   )
prep_years <- prep_disp %>%
   mutate(
      year = year(VISIT_DATE)
   ) %>%
   arrange(VISIT_DATE) %>%
   group_by(CENTRAL_ID, year) %>%
   summarise(
      dispense = 1,
      bottles  = floor(sum(months_prep, na.rm = TRUE))
   ) %>%
   ungroup() %>%
   arrange(year) %>%
   pivot_wider(
      names_from  = year,
      values_from = c(dispense, bottles)
   ) %>%
   inner_join(
      y  = prep %>%
         filter(!is.na(prepstart_date)) %>%
         select(CENTRAL_ID),
      by = join_by(CENTRAL_ID)
   )

disp_period <- prep %>%
   left_join(
      y = prep_years,
      by = join_by(CENTRAL_ID)
   )

flow_dta(disp_period, "prep", "disp", 2024, 7)