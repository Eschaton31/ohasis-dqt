lw_conn   <- ohasis$conn("lw")
id_reg    <- QB$new(lw_conn)$from("ohasis_warehouse.id_registry")$select(CENTRAL_ID, PATIENT_ID)$get()
form_prep <- QB$new(lw_conn)$from("ohasis_warehouse.form_prep")$get()
dbDisconnect(lw_conn)

oh_prep <- form_prep %>%
   filter(VISIT_DATE <= "2024-03-31") %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      days_to_pickup = abs(as.numeric(difftime(LATEST_NEXT_DATE, VISIT_DATE, units = "days"))),
      months_prep    = floor(days_to_pickup / 30),
      arv_worth      = case_when(
         days_to_pickup == 0 ~ '0_No ARVs',
         days_to_pickup > 0 & days_to_pickup <= 30 ~ '1_1mos of PrEP',
         days_to_pickup > 31 & days_to_pickup <= 60 ~ '2_2mos of PrEP',
         days_to_pickup > 61 & days_to_pickup <= 90 ~ '3_3mos of PrEP',
         days_to_pickup > 91 & days_to_pickup <= 180 ~ '4_4-6mos of PrEP',
         days_to_pickup > 181 & days_to_pickup <= 365.25 ~ '5_6-12mos PrEP',
         days_to_pickup > 365.25 ~ '6_>12mos of PrEP',
         TRUE ~ '7_(no data)'
      ),
   )

nh_prep <- hs_data("prep", "reg", 2024, 3) %>%
   read_dta() %>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(
      hs_data("prep", "outcome", 2024, 3) %>%
         read_dta(col_select = c(prep_id, prepstart_date, prep_type, prep_reg))
   ) %>%
   arrange(prepstart_date) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   arrange(prep_id)

prep_enroll <- nh_prep %>%
   filter(!is.na(prepstart_date)) %>%
   left_join(
      y  = oh_prep %>%
         filter(!is.na(MEDICINE_SUMMARY)),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      days_after_start = abs(as.numeric(difftime(VISIT_DATE, prepstart_date, units = "days"))),
      prep_ct          = case_when(
         days_after_start == 0 ~ 'prep_enroll',
         days_after_start > 0 & days_after_start <= 90 ~ 'refill_03mos',
         days_after_start >= 91 & days_after_start <= 180 ~ 'refill_06mos',
         days_after_start >= 181 & days_after_start <= 365.25 ~ 'refill_12mos',
         days_after_start > 365.25 ~ 'refill_yr_after',
         TRUE ~ 'no_data'
      ),
   ) %>%
   mutate(
      prepstart_yr = year(prepstart_date),
      prepstart_mo = month(prepstart_date),
   ) %>%
   arrange(VISIT_DATE) %>%
   distinct(CENTRAL_ID, prepstart_yr, prepstart_mo, prep_ct, .keep_all = TRUE) %>%
   pivot_wider(
      id_cols     = c(CENTRAL_ID, sex, prepstart_date, prep_reg, contains("_risk_"), starts_with("kp_"), prepstart_yr, prepstart_mo, prep_reg),
      names_from  = prep_ct,
      values_from = months_prep
   ) %>%
   mutate(
      ct_enroll = prep_enroll,
      ct_03mos  = prep_enroll + coalesce(refill_03mos, 0),
      ct_06mos  = prep_enroll +
         coalesce(refill_03mos, 0) +
         coalesce(refill_06mos, 0),
      ct_12mos  = prep_enroll +
         coalesce(refill_03mos, 0) +
         coalesce(refill_06mos, 0) +
         coalesce(refill_12mos, 0),
   ) %>%
   mutate_at(
      .vars = vars(starts_with("ct_")),
      ~case_when(
         . <= 1 ~ "01mos PrEP",
         . <= 2 ~ "02mos PrEP",
         . <= 3 ~ "03mos PrEP",
         . <= 4 ~ "04mos PrEP",
         . <= 5 ~ "05mos PrEP",
         . <= 6 ~ "06mos PrEP",
         . <= 7 ~ "07mos PrEP",
         . <= 8 ~ "08mos PrEP",
         . <= 9 ~ "09mos PrEP",
         . <= 10 ~ "10mos PrEP",
         . <= 11 ~ "11mos PrEP",
         . <= 12 ~ "12mos PrEP",
         . > 12 ~ "12mos+ PrEP",
      )
   ) %>%
   left_join(
      y  = oh_prep %>%
         filter(!is.na(MEDICINE_SUMMARY)) %>%
         filter(year(VISIT_DATE) == 2021) %>%
         group_by(CENTRAL_ID) %>%
         summarise(months_2021 = sum(months_prep, na.rm = TRUE)) %>%
         ungroup(),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = oh_prep %>%
         filter(!is.na(MEDICINE_SUMMARY)) %>%
         filter(year(VISIT_DATE) == 2022) %>%
         group_by(CENTRAL_ID) %>%
         summarise(months_2022 = sum(months_prep, na.rm = TRUE)) %>%
         ungroup(),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = oh_prep %>%
         filter(!is.na(MEDICINE_SUMMARY)) %>%
         filter(year(VISIT_DATE) == 2023) %>%
         group_by(CENTRAL_ID) %>%
         summarise(months_2023 = sum(months_prep, na.rm = TRUE)) %>%
         ungroup(),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = oh_prep %>%
         filter(!is.na(MEDICINE_SUMMARY)) %>%
         filter(year(VISIT_DATE) == 2024) %>%
         group_by(CENTRAL_ID) %>%
         summarise(months_2024 = sum(months_prep, na.rm = TRUE)) %>%
         ungroup(),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      given_2021 = if_else(!is.na(months_2021), 1, 0, 0),
      given_2022 = if_else(!is.na(months_2022), 1, 0, 0),
      given_2023 = if_else(!is.na(months_2023), 1, 0, 0),
      given_2024 = if_else(!is.na(months_2024), 1, 0, 0),

      msm        = case_when(
         sex == "MALE" & stri_detect_fixed(prep_risk_sexwithm, "yes") ~ 1,
         sex == "MALE" & stri_detect_fixed(hts_risk_sexwithm, "yes") ~ 1,
         # sex == "MALE" & kp_msm == 1 ~ 1,
         TRUE ~ 0
      ),
   )

prep_disp <- nh_prep %>%
   filter(!is.na(prepstart_date)) %>%
   left_join(
      y  = oh_prep %>%
         filter(!is.na(MEDICINE_SUMMARY)) %>%
         filter(year(VISIT_DATE) == 2021) %>%
         distinct(CENTRAL_ID) %>%
         mutate(prep2021 = 1),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = oh_prep %>%
         filter(!is.na(MEDICINE_SUMMARY)) %>%
         filter(year(VISIT_DATE) == 2022) %>%
         distinct(CENTRAL_ID) %>%
         mutate(prep2022 = 1),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = oh_prep %>%
         filter(!is.na(MEDICINE_SUMMARY)) %>%
         filter(year(VISIT_DATE) == 2023) %>%
         distinct(CENTRAL_ID) %>%
         mutate(prep2023 = 1),
      by = join_by(CENTRAL_ID)
   )

# avg months stayed on prep
prep_enroll %>%
   filter(prepstart_date < "2023-10-01", ct_06mos != "(no data)") %>%
   tab(ct_06mos)


prep_enroll %>%
   filter(prepstart_date < "2023-10-01") %>%
   tab(given_2021, given_2022, given_2023, given_2024)

prep_enroll %>%
   filter(prepstart_date < "2023-10-01", ct_06mos != "(no data)") %>%
   mutate(
      ct_06 = case_when(
         StrLeft(ct_06mos, 6) == "12mos+" ~ 13,
         TRUE ~ parse_number(StrLeft(ct_06mos, 2))
      )
   ) %>%
   tabstat(ct_06)
# seroconvert

prep_enroll %>%
   left_join(
      y  = harp %>%
         select(CENTRAL_ID, confirm_date),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      convert = if_else(confirm_date > prepstart_date, 1, 0, 0)
   ) %>%
   tab(convert)

prep_start <- read_dta("H:/_R/library/prep/20240513_prepstart_2024-03.dta") %>%
   get_cid(id_reg, PATIENT_ID)
prep_enroll %>%
   left_join(
      y  = prep_start %>%
         select(
            CENTRAL_ID,
            curr_reg
         ) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE),
      by = join_by(CENTRAL_ID)
   ) %>%
   filter(given_2024 == 1) %>%
   # tab(curr_reg, cross_tab = prep_reg, cross_return = "freq") %>%
   summarise(
      given_2024 = n(),
      start_2024 = sum(prep_new)
   ) %>%
   adorn_totals() %>%
   write_clip()


prep_enroll %>%
   filter(given_2023 == 1) %>%
   tab(prep_reg) %>%
   summarise(
      given_2024 = n(),
      start_2024 = sum(prep_new)
   ) %>%
   adorn_totals()
