lw_conn <- ohasis$conn("lw")
min     <- "2020-01-01"
max     <- "2024-03-31"

forms <- QB$new(lw_conn)
forms$where(function(query = QB$new(lw_conn)) {
   query$whereBetween('RECORD_DATE', c(min, max), "or")
   query$whereBetween('DATE_CONFIRM', c(min, max), "or")
   query$whereBetween('T0_DATE', c(min, max), "or")
   query$whereBetween('T1_DATE', c(min, max), "or")
   query$whereBetween('T2_DATE', c(min, max), "or")
   query$whereBetween('T3_DATE', c(min, max), "or")
   query$whereNested
})

forms$from("ohasis_warehouse.form_hts")
hts <- forms$get()

forms$from("ohasis_warehouse.form_a")
a <- forms$get()

cfbs <- QB$new(lw_conn)$
   from("ohasis_warehouse.form_cfbs")$
   whereBetween("RECORD_DATE", c(min, max))$
   whereBetween("TEST_DATE", c(min, max), "or")$
   get()

id_reg <- QB$new(lw_conn)$
   from("ohasis_warehouse.id_registry")$
   select(PATIENT_ID, CENTRAL_ID)$
   get()

dbDisconnect(lw_conn)

testing <- process_hts(hts, a, cfbs) %>% get_cid(id_reg, PATIENT_ID)
prep    <- read_dta(hs_data("prep", "outcome", 2024, 3)) %>%
   get_cid(id_reg, PATIENT_ID)
harp    <- read_dta(hs_data("harp_dx", "reg", 2024, 3)) %>%
   get_cid(id_reg, PATIENT_ID)

## 1) Unique Results per clients -----------------------------------------------

first_nr <- testing %>%
   select(
      CENTRAL_ID,
      hts_date,
      hts_result,
      hts_modality
   ) %>%
   filter(hts_date %within% interval(min, max)) %>%
   filter(hts_result == "NR") %>%
   arrange(hts_date) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE)

test_r <- testing %>%
   select(
      CENTRAL_ID,
      hts_date,
      hts_result,
      hts_modality
   ) %>%
   filter(hts_date %within% interval(min, max)) %>%
   filter(hts_result == "R")

seroconvert <- first_nr %>%
   select(
      CENTRAL_ID,
      first_nr_date = hts_date,
   ) %>%
   left_join(
      y  = testing %>%
         filter(hts_result != "(no data)") %>%
         select(
            CENTRAL_ID,
            retest_date = hts_date
         ) %>%
         filter(retest_date %within% interval(min, max)),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      retest_after_nr = interval(first_nr_date, retest_date) / days(1),
      remove_retest   = if_else(retest_after_nr < 30, 1, 0),
   ) %>%
   mutate_at(
      .vars = vars(retest_after_nr, retest_after_nr),
      ~if_else(remove_retest == 1, as.na(.), ., .)
   ) %>%
   arrange(retest_date) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   left_join(
      y  = test_r %>%
         select(
            CENTRAL_ID,
            r_date = hts_date,
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      r_after_nr = interval(first_nr_date, r_date) / days(1),
      remove_r   = if_else(r_after_nr < 30, 1, 0),
   ) %>%
   mutate_at(
      .vars = vars(r_date, r_after_nr),
      ~if_else(remove_r == 1, as.na(.), ., .)
   ) %>%
   arrange(r_date) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   left_join(
      y  = harp %>%
         select(
            CENTRAL_ID,
            confirm_date
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = prep %>%
         filter(!is.na(prepstart_date)) %>%
         select(
            CENTRAL_ID,
            prepstart_date
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      converted  = if_else(!is.na(r_after_nr), 1, 0, 0),
      converted  = if_else(confirm_date <= first_nr_date %m+% days(30), 0, converted, converted),

      everonprep = if_else(!is.na(prepstart_date), 1, 0, 0)
   )

# nr tested since 2020 to Mar 2024 converted
seroconvert %>%
   tab(converted)

# converted distribution prep vs no prep
seroconvert %>%
   filter(converted == 1) %>%
   tab(everonprep)


# nr tested since 2020 to Mar 2024 converted
seroconvert %>%
   filter(converted == 1, everonprep == 1) %>%
   left_join(
      y = prep_enroll %>%
         select(
            CENTRAL_ID,
            ct_12mos
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y = oh_prep %>%
         filter(!is.na(MEDICINE_SUMMARY)) %>%
         group_by(CENTRAL_ID) %>%
         summarise(last_disp_prep = max(VISIT_DATE, na.rm = TRUE)),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      diff = interval(last_disp_prep, confirm_date) / days(1),
      diff = if_else(diff < 0, NA_integer_, as.integer(diff), as.integer(diff))
   ) %>%
   tabstat(diff)