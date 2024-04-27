source("src/misc/ly_extracts/site_list.R")

con <- ohasis$conn("lw")

forms <- QB$new(con)
forms$where(function(query = QB$new(con)) {
   query$whereBetween('RECORD_DATE', c(min, max), "or")
   query$whereBetween('DATE_CONFIRM', c(min, max), "or")
   query$whereBetween('T0_DATE', c(min, max), "or")
   query$whereBetween('T1_DATE', c(min, max), "or")
   query$whereBetween('T2_DATE', c(min, max), "or")
   query$whereBetween('T3_DATE', c(min, max), "or")
   query$whereNested
})
forms$where(function(query = QB$new(con)) {
   query$whereIn('FACI_ID', sites$FACI_ID, boolean = "or")
   query$whereIn('SERVICE_FACI', sites$FACI_ID, boolean = "or")
   query$whereIn('CREATED_BY', staff$STAFF_ID, boolean = "or")
   query$whereIn('SERVICE_BY', staff$STAFF_ID, boolean = "or")
   query$whereNested
})

forms$from("ohasis_warehouse.form_hts")
hts <- forms$get()

forms$from("ohasis_warehouse.form_a")
a <- forms$get()

cfbs <- QB$new(con)$
   from("ohasis_warehouse.form_cfbs")$
   limit(0)$
   get()

id_reg <- QB$new(con)$
   from("ohasis_warehouse.id_registry")$
   select(PATIENT_ID, CENTRAL_ID)$
   get()

dbDisconnect(con)

testing <- process_hts(hts, a, cfbs) %>%
   get_cid(id_reg, PATIENT_ID)

ly_test <- testing %>%
   get_latest_pii(
      "CENTRAL_ID",
      c(
         "BIRTHDATE",
         "SEX",
         "UIC",
         "CURR_PSGC_REG",
         "CURR_PSGC_PROV",
         "CURR_PSGC_MUNC",
         "PERM_PSGC_REG",
         "PERM_PSGC_PROV",
         "PERM_PSGC_MUNC"
      )
   ) %>%
   convert_hts("name") %>%
   mutate(
      site_sort    = case_when(
         HTS_FACI %like% "LoveYourself" ~ 1,
         TRUE ~ 9999
      ),
      hts_priority = case_when(
         CONFIRM_RESULT %in% c(1, 2, 3) ~ 1,
         hts_result != "(no data)" & FORM_VERSION %in% c("Form A (v2017)", "HTS Form (v2021)") ~ 2,
         hts_result != "(no data)" & hts_modality == "FBT" ~ 3,
         hts_result != "(no data)" & hts_modality == "CBS" ~ 4,
         hts_result != "(no data)" & hts_modality == "FBS" ~ 5,
         hts_result != "(no data)" & hts_modality == "ST" ~ 6,
         TRUE ~ 9999
      )
   ) %>%
   arrange(site_sort, hts_priority) %>%
   distinct(CENTRAL_ID, hts_date, hts_modality, .keep_all = TRUE)

write_dta(format_stata(testing), "D:/20240419_hts-ly_20231216-20240331.dta")
