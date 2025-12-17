min  <- "2024-10-01"
max  <- "2025-10-31"
faci <- c('060274', '070163', '180002', '990005')

# forms <- QB$new(lw_conn)
# forms$where(function(query = QB$new(lw_conn)) {
#    query$whereBetween('RECORD_DATE', c(min, max), "or")
#    query$whereBetween('date_confirm', c(min, max), "or")
#    query$whereBetween('T0_DATE', c(min, max), "or")
#    query$whereBetween('T1_DATE', c(min, max), "or")
#    query$whereBetween('T2_DATE', c(min, max), "or")
#    query$whereBetween('T3_DATE', c(min, max), "or")
#    query$whereNested
# })
# forms$where(function(query = QB$new(lw_conn)) {
#    query$where('FACI_ID', faci, boolean = "or")
#    query$where('SERVICE_FACI', faci, boolean = "or")
#    query$whereNested
# })
#
# forms$from("ohasis_warehouse.form_hts")
# hts <- forms$get()
#
# forms$from("ohasis_warehouse.form_a")
# a <- forms$get()
#
# cfbs <- QB$new(lw_conn)$
#    from("ohasis_warehouse.form_cfbs")$
#    limit(0)$
#    get()

forms <- get_hts(min, max, faci)

id_reg <- update_idreg()

testing <- process_hts(forms$hts, forms$a, forms$cfbs) %>%
   get_cid(id_reg, patient_id)

lw_conn <- connect('mariadb-lw')
confirm <- QB$new(lw_conn)$
   from("ohasis_lake.px_hiv_confirmatory AS test")$
   join("ohasis_lake.px_demographics AS pii", "test.rec_id", "=", "pii.rec_id")$
   whereNotNull("confirm_result")$
   select("pii.patient_id", "test.*")$
   get() %>%
   get_cid(id_reg, patient_id)

dx <- QB$new(lw_conn)$
   from('harp_dx.reg_202510')$
   select(patient_id, confirm_date, labcode2)$
   get() %>%
   get_cid(id_reg, patient_id) %>%
   select(-patient_id)

tx <- QB$new(lw_conn)$
   from('harp_tx.reg_202510')$
   select(patient_id, artstart_date)$
   get() %>%
   get_cid(id_reg, patient_id) %>%
   select(-patient_id)

prep <- QB$new(lw_conn)$
   from('prep.outcome_202507')$
   select(patient_id, prepstart_date)$
   get() %>%
   get_cid(id_reg, patient_id) %>%
   select(-patient_id)

pos     <- confirm %>%
   filter(str_detect(toupper(confirm_result), "POSITIVE")) %>%
   get_cid(id_reg, patient_id) %>%
   arrange(date_confirm) %>%
   distinct(central_id, .keep_all = TRUE) %>%
   select(central_id, confirm_code, date_confirm)
ind     <- confirm %>%
   filter(str_detect(toupper(confirm_result), "INDETERMINATE")) %>%
   get_cid(id_reg, patient_id) %>%
   arrange(desc(date_confirm)) %>%
   distinct(central_id, .keep_all = TRUE) %>%
   select(central_id, confirm_code, date_confirm)
neg     <- confirm %>%
   filter(str_detect(toupper(confirm_result), "NEGATIVE")) %>%
   get_cid(id_reg, patient_id) %>%
   arrange(desc(date_confirm)) %>%
   distinct(central_id, .keep_all = TRUE) %>%
   select(central_id, confirm_code, date_confirm)
pending <- confirm %>%
   filter(str_detect(toupper(confirm_result), "PENDING")) %>%
   get_cid(id_reg, patient_id) %>%
   arrange(desc(date_confirm)) %>%
   distinct(central_id, .keep_all = TRUE) %>%
   select(central_id, confirm_code, date_confirm)


hts <- process_hts(forms$hts, forms$a, forms$cfbs) %>%
   # filter(hts_result == "R") %>%
   get_cid(id_reg, patient_id)


codes <- dx %>%
   select(central_id, confirm_code = labcode2, confirm_date = confirm_date) %>%
   mutate(
      result = "Positive"
   ) %>%
   bind_rows(
      pos %>%
         select(central_id, confirm_code, confirm_date = date_confirm) %>%
         mutate(
            result = "Positive"
         )
   ) %>%
   bind_rows(
      neg %>%
         select(central_id, confirm_code, confirm_date = date_confirm) %>%
         mutate(
            result = "Negative"
         )
   ) %>%
   bind_rows(
      ind %>%
         select(central_id, confirm_code, confirm_date = date_confirm) %>%
         mutate(
            result = "Indeterminate"
         )
   ) %>%
   bind_rows(
      pending %>%
         select(central_id, confirm_code, confirm_date = date_confirm) %>%
         mutate(
            result = "Pending"
         )
   ) %>%
   distinct(central_id, result, .keep_all = TRUE) %>%
   pivot_wider(
      id_cols     = central_id,
      names_from  = result,
      values_from = confirm_code
   )

dates <- dx %>%
   select(central_id, confirm_code = labcode2, confirm_date = confirm_date) %>%
   mutate(
      result = "Positive"
   ) %>%
   bind_rows(
      pos %>%
         select(central_id, confirm_code, confirm_date = date_confirm) %>%
         mutate(
            result = "Positive"
         )
   ) %>%
   bind_rows(
      neg %>%
         select(central_id, confirm_code, confirm_date = date_confirm) %>%
         mutate(
            result = "Negative"
         )
   ) %>%
   bind_rows(
      ind %>%
         select(central_id, confirm_code, confirm_date = date_confirm) %>%
         mutate(
            result = "Indeterminate"
         )
   ) %>%
   bind_rows(
      pending %>%
         select(central_id, confirm_code, confirm_date = date_confirm) %>%
         mutate(
            result = "Pending"
         )
   ) %>%
   distinct(central_id, result, .keep_all = TRUE) %>%
   pivot_wider(
      id_cols     = central_id,
      names_from  = result,
      values_from = confirm_date
   )

results <- codes %>%
   full_join(dates, join_by(central_id)) %>%
   rename(
      positive_code      = Positive.x,
      positive_date      = Positive.y,
      negative_code      = Negative.x,
      negative_date      = Negative.y,
      idneterminate_code = Indeterminate.x,
      idneterminate_date = Indeterminate.y,
      pending_code       = Pending.x,
      pending_date       = Pending.y,
   ) %>%
   select(
      central_id,
      starts_with("positive"),
      starts_with("negative"),
      starts_with("indeterminate"),
      starts_with("pending"),
   )

new_r <- hts %>%
   distinct(central_id, .keep_all = TRUE) %>%
   left_join(
      y  = results,
      by = join_by(central_id)
   ) %>%
   convert_hts("nhsss")

new_r <- hts %>%
   mutate(
      hts_priority = case_when(
         confirm_result %in% c(1, 2, 3) ~ 1,
         hts_result != "(no data)" & hts_modality == "FBT" ~ 3,
         hts_result != "(no data)" & hts_modality == "CBS" ~ 4,
         hts_result != "(no data)" & hts_modality == "FBS" ~ 5,
         hts_result != "(no data)" & hts_modality == "ST" ~ 6,
         TRUE ~ 9999
      )
   ) %>%
   arrange(central_id, hts_priority) %>%
   distinct(central_id, .keep_all = TRUE) %>%
   left_join(
      y  = results,
      by = join_by(central_id)
   ) %>%
   convert_hts("nhsss")

new_r %>%
   filter(is.na(pos)) %>%
   tab(client_mobile)
new_r %>%
   filter(created_by == "Tarbosa, Manndy Brett") %>%
   tab(pos)

aiha_reach <- new_r %>%
   mutate_at(
      .vars = vars(ends_with("date")),
      ~as.Date(.)
   ) %>%
   mutate(
      record_date = if_else(record_date < -25567, t0_date, record_date, record_date),
      t0_date     = if_else(hts_result != "(no data)", record_date, t0_date, t0_date),
   ) %>%
   arrange(record_date) %>%
   select(
      central_id,
      reach_date       = record_date,
      screening_date   = t0_date,
      screening_result = hts_result,
      created_by,
      hts_provider,
      uic,
      first,
      middle,
      last,
      suffix,
      starts_with("positive"),
      starts_with("negative"),
      starts_with("indeterminate"),
      starts_with("pending"),
      hts_reg
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(toupper(.))
   ) %>%
   mutate(
      is_pos = if_else(!is.na(positive_code), "Confirmed Positive", "Not confirmed", "Not confirmed")
   ) %>%
   mutate(
      reg = case_when(
         hts_provider == "ALAPAR, JANMAY" ~ "6",
         hts_provider == "BATALUNA, RHEA LEE" ~ "7",
         hts_provider == "BAYOG, ROY OPINA" ~ "6",
         hts_provider == "BITAMOR, JOVAN" ~ "6",
         hts_provider == "BORDAMONTE, ROMEO LOSBAÑES, JR" ~ "6",
         hts_provider == "CUNANAN, JOHN CARLO" ~ "6",
         hts_provider == "DULLEGUEZ, JOSHUA" ~ "6",
         hts_provider == "ESTIMAR, JOHNMEL MINERVA" ~ "6",
         hts_provider == "FABIANO, JOHN ALEXIS SOLINAP" ~ "6",
         hts_provider == "LACSON, RAYMUND" ~ "6",
         hts_provider == "LAURENTE, LIONEL" ~ "6",
         hts_provider == "OBIDOS, JUGIE VIOLATA" ~ "6",
         hts_provider == "SULLA, SYNDY" ~ "7",
         hts_provider == "SULLA, SYNDY ANN" ~ "7",
         hts_provider == "TAMON, ROLAND" ~ "6",
         hts_provider == "TARBOSA, MANNDY BRETT" ~ "7",
      )
   ) %>%
   left_join(
      y  = tx %>%
         select(central_id, art_start_date = artstart_date),
      by = join_by(central_id)
   ) %>%
   left_join(
      y  = prep %>%
         select(central_id, prep_start_date = prepstart_date),
      by = join_by(central_id)
   )

aiha_reach %>%
   mutate(
      pos = if_else(!is.na(positive_code), 1, 0, 0)
   ) %>%
   filter(screening_result == "R") %>%
   tab(screening_result, pos)
aiha_reach %>%
   View('pos')


aiha_reach %>%
   mutate(
      is_pos = if_else(!is.na(positive_code), "Confirmed Positive", "Not confirmed", "Not confirmed")
   ) %>%
   filter(screening_result == "R") %>%
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

write_flat_file(list(AIHA = aiha_reach), "H:/20251212_aiha-reach-confirmatory_results (Oct 2024 - Oct 2025).xlsx")

hts_aiha <- testing %>%
   left_join(
      results
   )

write_flat_file(list(AIHA = new_r %>%
   mutate_at(
      .vars = vars(ends_with("date")),
      ~as.Date(.)
   ) %>%
   mutate(
      record_date = if_else(record_date < -25567, t0_date, record_date, record_date),
      t0_date     = if_else(hts_result != "(no data)", record_date, t0_date, t0_date),
      hts_date    = if_else(hts_result != "(no data)", record_date, hts_date, hts_date),
   ) %>%
   arrange(record_date)), "H:/20251212_aiha-reach (Oct 2024 - Oct 2025).xlsx")

new_r %>%
   mutate_at(
      .vars = vars(ends_with("DATE")),
      ~as.Date(.)
   ) %>%
   mutate(
      record_date = if_else(record_date < -25567, t0_date, record_date, record_date),
      t0_date     = if_else(hts_result != "(no data)", record_date, t0_date, t0_date),
      hts_date    = if_else(hts_result != "(no data)", record_date, hts_date, hts_date),
   ) %>%
   arrange(record_date) %>%
   write_xlsx("D:/20251210_aiha-reach (Oct 2024 - Oct 2025).xlsx")