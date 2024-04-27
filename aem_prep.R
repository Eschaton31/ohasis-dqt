lw_conn   <- ohasis$conn("lw")
dbname    <- "ohasis_warehouse"
id_reg    <- dbTable(lw_conn, dbname, "id_registry", cols = c("CENTRAL_ID", "PATIENT_ID"))
form_prep <- dbTable(lw_conn, dbname, "form_prep")
rec_link  <- dbTable(lw_conn, dbname, "rec_link")

min       <- "2021-01-01"
max       <- "2023-12-31"
hts_where <- glue(r"(
   (RECORD_DATE BETWEEN '{min}' AND '{max}') OR
      (DATE(DATE_CONFIRM) BETWEEN '{min}' AND '{max}') OR
      (DATE(T3_DATE) BETWEEN '{min}' AND '{max}') OR
      (DATE(T2_DATE) BETWEEN '{min}' AND '{max}') OR
      (DATE(T1_DATE) BETWEEN '{min}' AND '{max}') OR
      (DATE(T0_DATE) BETWEEN '{min}' AND '{max}')
   )")
cbs_where <- glue(r"(
   (RECORD_DATE BETWEEN '{min}' AND '{max}') OR
      (DATE(TEST_DATE) BETWEEN '{min}' AND '{max}')
   )")

forms           <- list()
forms$form_a    <- lw_conn %>%
   dbTable(
      dbname,
      "form_a",
      where     = hts_where,
      raw_where = TRUE
   )
forms$form_hts  <- lw_conn %>%
   dbTable(
      dbname,
      "form_hts",
      where     = hts_where,
      raw_where = TRUE
   )
forms$form_cfbs <- lw_conn %>%
   dbTable(
      dbname,
      "form_cfbs",
      where     = cbs_where,
      raw_where = TRUE
   )

dbDisconnect(lw_conn)


hts  <- process_hts(forms$form_hts, forms$form_a, forms$form_cfbs) %>%
   get_cid(id_reg, PATIENT_ID)
prep <- process_prep(form_prep, hts, rec_link) %>%
   get_cid(id_reg, PATIENT_ID)

tests <- hts %>%
   filter(hts_result != "(no data)")

nr <- hts %>%
   arrange(hts_date) %>%
   filter(hts_result == "NR")


first_nr <- nr %>% distinct(CENTRAL_ID, .keep_all = TRUE)
r        <- hts %>%
   arrange(hts_date) %>%
   filter(hts_result == "R")

prepstart <- hs_data("prep", "reg", 2023, 8) %>%
   read_dta(col_select = c(prep_id, PATIENT_ID)) %>%
   left_join(
      y  = hs_data("prep", "prepstart", 2023, 8) %>%
         read_dta(col_select = -any_of(c("PATIENT_ID", "CENTRAL_ID"))),
      by = join_by(prep_id)
   ) %>%
   get_cid(id_reg, PATIENT_ID)

retest_nr <- first_nr %>%
   left_join(
      y  = tests %>%
         select(
            CENTRAL_ID,
            retest_date   = hts_date,
            retest_result = hts_result
         ),
      by = join_by(CENTRAL_ID, hts_date < retest_date)
   ) %>%
   mutate(
      tat_retest = interval(hts_date, retest_date) / days(1),
      tat_retest = floor(tat_retest),
   ) %>%
   mutate(
      tat_retest  = case_when(
         tat_retest %in% seq(0, 30) ~ "1) w/in 1mo",
         tat_retest %in% seq(31, 60) ~ "2) w/in 2mos",
         tat_retest %in% seq(61, 90) ~ "3) w/in 3mos",
         tat_retest %in% seq(91, 180) ~ "4) w/in 6mos",
         tat_retest %in% seq(181, 365) ~ "5) w/in 1yr",
         tat_retest %in% seq(366, 730) ~ "6) w/in 2yrs",
         tat_retest %in% seq(731, 1095) ~ "7) w/in 3yrs",
         !is.na(retest_result) ~ "8) more than 3yrs"
      ),
      retest_prio = case_when(
         retest_result == "R" ~ 1,
         retest_result == "IND" ~ 2,
         retest_result == "NR" ~ 3,
         TRUE ~ 999
      )
   ) %>%
   arrange(retest_prio, retest_date) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   left_join(
      y  = prepstart %>%
         filter(!is.na(prepstart_date)) %>%
         select(CENTRAL_ID) %>%
         mutate(
            everonprep = 1
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      everonprep = coalesce(everonprep, 0)
   )

retest_nr %>% tab(hts_result, retest_result, tat_retest)
retest_nr %>% get_dupes(CENTRAL_ID)

retest_prep <- prepstart %>%
   filter(!is.na(prepstart_date)) %>%
   left_join(
      y  = tests %>%
         select(
            CENTRAL_ID,
            retest_date   = hts_date,
            retest_result = hts_result
         ),
      by = join_by(CENTRAL_ID, prepstart_date < retest_date)
   ) %>%
   mutate(
      tat_retest = interval(hts_date, retest_date) / days(1),
      tat_retest = floor(tat_retest),
   ) %>%
   mutate(
      tat_retest  = case_when(
         tat_retest %in% seq(0, 30) ~ "1) w/in 1mo",
         tat_retest %in% seq(31, 60) ~ "2) w/in 2mos",
         tat_retest %in% seq(61, 90) ~ "3) w/in 3mos",
         tat_retest %in% seq(91, 180) ~ "4) w/in 6mos",
         tat_retest %in% seq(181, 365) ~ "5) w/in 1yr",
         tat_retest %in% seq(366, 730) ~ "6) w/in 2yrs",
         tat_retest %in% seq(731, 1095) ~ "7) w/in 3yrs",
         !is.na(retest_result) ~ "8) more than 3yrs"
      ),
      retest_prio = case_when(
         retest_result == "R" ~ 1,
         retest_result == "IND" ~ 2,
         retest_result == "NR" ~ 3,
         TRUE ~ 999
      )
   ) %>%
   arrange(retest_prio, retest_date) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   arrange(prep_id)

dir <- "D:/AEM/PrEP"
retest_nr %>%
   format_stata() %>%
   remove_pii() %>%
   write_dta(file.path(dir, "20231115_retest-nr_2021-2023-08.dta"))
retest_prep %>%
   format_stata() %>%
   remove_pii() %>%
   write_dta(file.path(dir, "20231115_retest-prep_2021-2023-08.dta"))

compress_stata("20231115_retest-nr_2021-2023-08.dta")
compress_stata("20231115_retest-prep_2021-2023-08.dta")

##  prep exploratory -----------------------------------------------------------

lw_conn <- ohasis$conn("lw")
prep    <- list(
   id_reg = dbTable(lw_conn, "ohasis_warehouse", "id_registry", cols = c("PATIENT_ID", "CENTRAL_ID")),
   oh     = dbTable(lw_conn, "ohasis_warehouse", "form_prep"),
   start  = read_dta(hs_data("prep", "prepstart", 2023, 8)),
   reg    = read_dta(hs_data("prep", "reg", 2023, 8)),
   disp   = dbTable(
      lw_conn,
      "ohasis_lake",
      "disp_meds",
      join = list(
         "ohasis_warehouse.form_prep" = list(by = c("REC_ID" = "REC_ID"), cols = "VISIT_DATE", type = "inner")
      )
   )
)
dbDisconnect(lw_conn)

prep_dispensing <- prep$oh %>%
   left_join(prep$disp %>% select(REC_ID, DISP_TOTAL, DISP_DATE), join_by(REC_ID)) %>%
   get_cid(prep$id_reg, PATIENT_ID) %>%
   filter(!is.na(MEDICINE_SUMMARY)) %>%
   filter(VISIT_DATE <= "2023-08-31") %>%
   arrange(desc(LATEST_NEXT_DATE)) %>%
   distinct(CENTRAL_ID, VISIT_DATE, .keep_all = TRUE) %>%
   mutate(
      prep_yr    = year(VISIT_DATE),
      prep_total = coalesce(DISP_TOTAL, 30) / 30,
      prep_total = abs(floor(prep_total))
   ) %>%
   filter(prep_total %in% seq(1, 14))


prep_start_dispensing <- prepstart %>%
   left_join(
      y  = prep_dispensing %>%
         select(
            CENTRAL_ID,
            prepstart_date = VISIT_DATE,
            prep_total
         ),
      by = join_by(CENTRAL_ID, prepstart_date)
   ) %>%
   distinct(prep_id, .keep_all = TRUE)

prep_longitudinal <- prepstart %>%
   left_join(
      y  = prep_dispensing %>%
         select(
            CENTRAL_ID,
            prepstart_visit_date = VISIT_DATE,
            prepstart_type       = PREP_TYPE,
            prepstart_shift      = PREP_SHIFT,
            prepstart_dispensed  = DISP_TOTAL
         ),
      by = join_by(CENTRAL_ID, prepstart_date == prepstart_visit_date)
   ) %>%
   arrange(prep_id, prepstart_date) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   mutate(
      prepstart_dispensed = case_when(
         prepstart_dispensed == -330 ~ 30,
         is.na(prepstart_dispensed) & !is.na(prepstart_date) ~ 30,
         TRUE ~ prepstart_dispensed
      )
   ) %>%
   left_join(
      y  = prep_dispensing %>%
         select(
            CENTRAL_ID,
            x2nd_visit_date = VISIT_DATE,
            x2nd_type       = PREP_TYPE,
            x2nd_shift      = PREP_SHIFT,
            x2nd_dispensed  = DISP_TOTAL
         ),
      by = join_by(CENTRAL_ID, prepstart_date < x2nd_visit_date)
   ) %>%
   arrange(prep_id, x2nd_visit_date) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   left_join(
      y  = prep_dispensing %>%
         select(
            CENTRAL_ID,
            x3rd_visit_date = VISIT_DATE,
            x3rd_type       = PREP_TYPE,
            x3rd_shift      = PREP_SHIFT,
            x3rd_dispensed  = DISP_TOTAL
         ),
      by = join_by(CENTRAL_ID, x2nd_visit_date < x3rd_visit_date)
   ) %>%
   arrange(prep_id, x3rd_visit_date) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   mutate(
      x2nd_ref_date = prepstart_date %m+% days(prepstart_dispensed),
      x3rd_ref_date = x2nd_visit_date %m+% days(x2nd_dispensed),

      x2nd_interval = interval(x2nd_ref_date, x2nd_visit_date) / days(1),
      x3rd_interval = interval(x3rd_ref_date, x3rd_visit_date) / days(1),

      x2nd_interval = floor(x2nd_interval),
      x3rd_interval = floor(x3rd_interval),

      x2nd_ontime   = if_else(x2nd_interval %in% seq(-10, 7), 1, 0, 0),
      x3rd_ontime   = if_else(x3rd_interval %in% seq(-10, 7), 1, 0, 0),
   )