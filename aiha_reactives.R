lw_conn <- ohasis$conn("lw")
min     <- "2023-10-01"
max     <- "2024-03-31"
faci    <- "990005"

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
forms$where(function(query = QB$new(lw_conn)) {
   query$where('FACI_ID', faci, boolean = "or")
   query$where('SERVICE_FACI', faci, boolean = "or")
   query$whereNested
})

forms$from("ohasis_warehouse.form_hts")
hts <- forms$get()

forms$from("ohasis_warehouse.form_a")
a <- forms$get()

cfbs <- QB$new(lw_conn)$
   from("ohasis_warehouse.form_cfbs")$
   limit(0)$
   get()

testing <- process_hts(hts, a, cfbs)

id_reg <- QB$new(lw_conn)$
   from("ohasis_warehouse.id_registry")$
   select(PATIENT_ID, CENTRAL_ID)$
   get()

confirm <- QB$new(lw_conn)$
   from("ohasis_lake.px_hiv_testing AS test")$
   join("ohasis_lake.px_pii AS pii", "test.REC_ID", "=", "pii.REC_ID")$
   whereNotNull("CONFIRM_RESULT")$
   select("pii.PATIENT_ID", "test.*")$
   get()

dx <- hs_data("harp_dx", "reg", 2023, 12) %>%
   read_dta(col_select = c(PATIENT_ID, confirm_date, labcode2)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   select(-PATIENT_ID)

tx <- hs_data("harp_tx", "reg", 2023, 12) %>%
   read_dta(col_select = c(PATIENT_ID, artstart_date)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   select(-PATIENT_ID)

prep <- hs_data("prep", "outcome", 2023, 12) %>%
   read_dta(col_select = c(PATIENT_ID, prepstart_date)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   select(-PATIENT_ID)

pos     <- confirm %>%
   filter(str_detect(toupper(CONFIRM_RESULT), "POSITIVE")) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   arrange(DATE_CONFIRM) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(CENTRAL_ID, CONFIRM_CODE, DATE_CONFIRM)
ind     <- confirm %>%
   filter(str_detect(toupper(CONFIRM_RESULT), "INDETERMINATE")) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   arrange(desc(DATE_CONFIRM)) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(CENTRAL_ID, CONFIRM_CODE, DATE_CONFIRM)
neg     <- confirm %>%
   filter(str_detect(toupper(CONFIRM_RESULT), "NEGATIVE")) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   arrange(desc(DATE_CONFIRM)) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(CENTRAL_ID, CONFIRM_CODE, DATE_CONFIRM)
pending <- confirm %>%
   filter(str_detect(toupper(CONFIRM_RESULT), "PENDING")) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   arrange(desc(DATE_CONFIRM)) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(CENTRAL_ID, CONFIRM_CODE, DATE_CONFIRM)


hts <- process_hts(hts, a, cfbs) %>%
   # filter(hts_result == "R") %>%
   get_cid(id_reg, PATIENT_ID)


codes <- dx %>%
   select(CENTRAL_ID, CONFIRM_CODE = labcode2, CONFIRM_DATE = confirm_date) %>%
   mutate(
      RESULT = "Positive"
   ) %>%
   bind_rows(
      pos %>%
         select(CENTRAL_ID, CONFIRM_CODE, CONFIRM_DATE = DATE_CONFIRM) %>%
         mutate(
            RESULT = "Positive"
         )
   ) %>%
   bind_rows(
      neg %>%
         select(CENTRAL_ID, CONFIRM_CODE, CONFIRM_DATE = DATE_CONFIRM) %>%
         mutate(
            RESULT = "Negative"
         )
   ) %>%
   bind_rows(
      ind %>%
         select(CENTRAL_ID, CONFIRM_CODE, CONFIRM_DATE = DATE_CONFIRM) %>%
         mutate(
            RESULT = "Indeterminate"
         )
   ) %>%
   bind_rows(
      pending %>%
         select(CENTRAL_ID, CONFIRM_CODE, CONFIRM_DATE = DATE_CONFIRM) %>%
         mutate(
            RESULT = "Pending"
         )
   ) %>%
   distinct(CENTRAL_ID, RESULT, .keep_all = TRUE) %>%
   pivot_wider(
      id_cols     = CENTRAL_ID,
      names_from  = RESULT,
      values_from = CONFIRM_CODE
   )

dates <- dx %>%
   select(CENTRAL_ID, CONFIRM_CODE = labcode2, CONFIRM_DATE = confirm_date) %>%
   mutate(
      RESULT = "Positive"
   ) %>%
   bind_rows(
      pos %>%
         select(CENTRAL_ID, CONFIRM_CODE, CONFIRM_DATE = DATE_CONFIRM) %>%
         mutate(
            RESULT = "Positive"
         )
   ) %>%
   bind_rows(
      neg %>%
         select(CENTRAL_ID, CONFIRM_CODE, CONFIRM_DATE = DATE_CONFIRM) %>%
         mutate(
            RESULT = "Negative"
         )
   ) %>%
   bind_rows(
      ind %>%
         select(CENTRAL_ID, CONFIRM_CODE, CONFIRM_DATE = DATE_CONFIRM) %>%
         mutate(
            RESULT = "Indeterminate"
         )
   ) %>%
   bind_rows(
      pending %>%
         select(CENTRAL_ID, CONFIRM_CODE, CONFIRM_DATE = DATE_CONFIRM) %>%
         mutate(
            RESULT = "Pending"
         )
   ) %>%
   distinct(CENTRAL_ID, RESULT, .keep_all = TRUE) %>%
   pivot_wider(
      id_cols     = CENTRAL_ID,
      names_from  = RESULT,
      values_from = CONFIRM_DATE
   )


results <- codes %>%
   full_join(dates, join_by(CENTRAL_ID)) %>%
   rename(
      POSITIVE_CODE      = Positive.x,
      POSITIVE_DATE      = Positive.y,
      NEGATIVE_CODE      = Negative.x,
      NEGATIVE_DATE      = Negative.y,
      IDNETERMINATE_CODE = Indeterminate.x,
      IDNETERMINATE_DATE = Indeterminate.y,
      PENDING_CODE       = Pending.x,
      PENDING_DATE       = Pending.y,
   ) %>%
   select(
      CENTRAL_ID,
      starts_with("POSITIVE"),
      starts_with("NEGATIVE"),
      starts_with("INDETERMINATE"),
      starts_with("PENDING"),
   )

new_r <- hts %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   left_join(
      y  = results,
      by = join_by(CENTRAL_ID)
   ) %>%
   convert_hts("nhsss")

new_r <- hts %>%
   mutate(
      hts_priority = case_when(
         CONFIRM_RESULT %in% c(1, 2, 3) ~ 1,
         hts_result != "(no data)" & hts_modality == "FBT" ~ 3,
         hts_result != "(no data)" & hts_modality == "CBS" ~ 4,
         hts_result != "(no data)" & hts_modality == "FBS" ~ 5,
         hts_result != "(no data)" & hts_modality == "ST" ~ 6,
         TRUE ~ 9999
      )
   ) %>%
   arrange(CENTRAL_ID, hts_priority) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   left_join(
      y  = results,
      by = join_by(CENTRAL_ID)
   ) %>%
   convert_hts("nhsss")

new_r %>%
   filter(is.na(pos)) %>%
   tab(CLIENT_MOBILE)
new_r %>%
   filter(CREATED_BY == "Tarbosa, Manndy Brett") %>%
   tab(pos)

aiha_reach <- new_r %>%
   mutate_at(
      .vars = vars(ends_with("DATE")),
      ~as.Date(.)
   ) %>%
   mutate(
      RECORD_DATE = if_else(RECORD_DATE < -25567, T0_DATE, RECORD_DATE, RECORD_DATE),
      T0_DATE     = if_else(hts_result != "(no data)", RECORD_DATE, T0_DATE, T0_DATE),
   ) %>%
   arrange(RECORD_DATE) %>%
   select(
      CENTRAL_ID,
      REACH_DATE       = RECORD_DATE,
      SCREENING_DATE   = T0_DATE,
      SCREENING_RESULT = hts_result,
      CREATED_BY,
      HTS_PROVIDER,
      UIC,
      FIRST,
      MIDDLE,
      LAST,
      SUFFIX,
      starts_with("POSITIVE"),
      starts_with("NEGATIVE"),
      starts_with("INDETERMINATE"),
      starts_with("PENDING"),
      HTS_REG
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(toupper(.))
   ) %>%
   mutate(
      is_pos = if_else(!is.na(POSITIVE_CODE), "Confirmed Positive", "Not confirmed", "Not confirmed")
   ) %>%
   mutate(
      REG = case_when(
         HTS_PROVIDER == "ALAPAR, JANMAY" ~ "6",
         HTS_PROVIDER == "BATALUNA, RHEA LEE" ~ "7",
         HTS_PROVIDER == "BAYOG, ROY OPINA" ~ "6",
         HTS_PROVIDER == "BITAMOR, JOVAN" ~ "6",
         HTS_PROVIDER == "BORDAMONTE, ROMEO LOSBAÑES, JR" ~ "6",
         HTS_PROVIDER == "CUNANAN, JOHN CARLO" ~ "6",
         HTS_PROVIDER == "DULLEGUEZ, JOSHUA" ~ "6",
         HTS_PROVIDER == "ESTIMAR, JOHNMEL MINERVA" ~ "6",
         HTS_PROVIDER == "FABIANO, JOHN ALEXIS SOLINAP" ~ "6",
         HTS_PROVIDER == "LACSON, RAYMUND" ~ "6",
         HTS_PROVIDER == "LAURENTE, LIONEL" ~ "6",
         HTS_PROVIDER == "OBIDOS, JUGIE VIOLATA" ~ "6",
         HTS_PROVIDER == "SULLA, SYNDY" ~ "7",
         HTS_PROVIDER == "SULLA, SYNDY ANN" ~ "7",
         HTS_PROVIDER == "TAMON, ROLAND" ~ "6",
         HTS_PROVIDER == "TARBOSA, MANNDY BRETT" ~ "7",
      )
   ) %>%
   left_join(
      y  = tx %>%
         select(CENTRAL_ID, ART_START_DATE = artstart_date),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = prep %>%
         select(CENTRAL_ID, PREP_START_DATE = prepstart_date),
      by = join_by(CENTRAL_ID)
   )

aiha_reach %>%
   mutate(
      pos = if_else(!is.na(POSITIVE_CODE), 1, 0, 0)
   ) %>%
   filter(SCREENING_RESULT == "R") %>%
   tab(SCREENING_RESULT, pos)
aiha_reach %>%
   View('pos')


aiha_reach %>%
   mutate(
      is_pos = if_else(!is.na(POSITIVE_CODE), "Confirmed Positive", "Not confirmed", "Not confirmed")
   ) %>%
   filter(SCREENING_RESULT == "R") %>%
   mutate(
      REG = case_when(
         HTS_PROVIDER == "ALAPAR, JANMAY" ~ "6",
         HTS_PROVIDER == "BATALUNA, RHEA LEE" ~ "7",
         HTS_PROVIDER == "BAYOG, ROY OPINA" ~ "6",
         HTS_PROVIDER == "BITAMOR, JOVAN" ~ "6",
         HTS_PROVIDER == "BORDAMONTE, ROMEO LOSBAÑES, JR" ~ "6",
         HTS_PROVIDER == "CUNANAN, JOHN CARLO" ~ "6",
         HTS_PROVIDER == "DULLEGUEZ, JOSHUA" ~ "6",
         HTS_PROVIDER == "ESTIMAR, JOHNMEL MINERVA" ~ "6",
         HTS_PROVIDER == "FABIANO, JOHN ALEXIS SOLINAP" ~ "6",
         HTS_PROVIDER == "LACSON, RAYMUND" ~ "6",
         HTS_PROVIDER == "LAURENTE, LIONEL" ~ "6",
         HTS_PROVIDER == "OBIDOS, JUGIE VIOLATA" ~ "6",
         HTS_PROVIDER == "SULLA, SYNDY" ~ "7",
         HTS_PROVIDER == "SULLA, SYNDY ANN" ~ "7",
         HTS_PROVIDER == "TAMON, ROLAND" ~ "6",
         HTS_PROVIDER == "TARBOSA, MANNDY BRETT" ~ "7",
      )
   ) %>%
   # View('pos')
   tab(REG, cross_tab = is_pos, cross_return = "freq+row")

write_flat_file(list(AIHA = aiha_reach), "D:/20240408_aiha-reach-confirmatory_results (Oct 2023 - Mar 2024).xlsx")

hts_aiha <- testing %>%
   left_join(
      results
   )

write_flat_file(list(AIHA = new_r %>%
   mutate_at(
      .vars = vars(ends_with("DATE")),
      ~as.Date(.)
   ) %>%
   mutate(
      RECORD_DATE = if_else(RECORD_DATE < -25567, T0_DATE, RECORD_DATE, RECORD_DATE),
      T0_DATE     = if_else(hts_result != "(no data)", RECORD_DATE, T0_DATE, T0_DATE),
      hts_date    = if_else(hts_result != "(no data)", RECORD_DATE, hts_date, hts_date),
   ) %>%
   arrange(RECORD_DATE)), "D:/20240408_aiha-reach (Oct 2023 - Mar 2024).xlsx")

new_r %>%
   mutate_at(
      .vars = vars(ends_with("DATE")),
      ~as.Date(.)
   ) %>%
   mutate(
      RECORD_DATE = if_else(RECORD_DATE < -25567, T0_DATE, RECORD_DATE, RECORD_DATE),
      T0_DATE     = if_else(hts_result != "(no data)", RECORD_DATE, T0_DATE, T0_DATE),
      hts_date    = if_else(hts_result != "(no data)", RECORD_DATE, hts_date, hts_date),
   ) %>%
   arrange(RECORD_DATE) %>%
   write_xlsx("D:/20240408_aiha-reach (Oct 2023 - Mar 2024).xlsx")