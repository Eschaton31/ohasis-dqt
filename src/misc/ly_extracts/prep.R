source("src/misc/ly_extracts/site_list.R")

con   <- ohasis$conn("lw")
forms <- QB$new(con)
forms$whereBetween('VISIT_DATE', c(min, max))
forms$where(function(query = QB$new(con)) {
   query$whereIn('FACI_ID', sites$FACI_ID, boolean = "or")
   query$whereIn('SERVICE_FACI', sites$FACI_ID, boolean = "or")
   query$whereIn('CREATED_BY', staff$STAFF_ID, boolean = "or")
   query$whereIn('SERVICE_BY', staff$STAFF_ID, boolean = "or")
   query$whereNested
})

forms$from("ohasis_warehouse.form_prep")
form_prep <- forms$get()

rec_link <- QB$new(con)$from("ohasis_warehouse.rec_link")$get()
dbDisconnect(con)


ly_prep <- process_prep(form_prep, testing, rec_link) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      # name
      STANDARD_FIRST  = stri_trans_general(FIRST, "latin-ascii"),
      name            = str_squish(stri_c(LAST, ", ", FIRST, " ", MIDDLE, " ", SUFFIX)),

      # Age
      AGE             = coalesce(AGE, AGE_MO / 12),
      AGE_DTA         = calc_age(BIRTHDATE, VISIT_DATE),

      # tag those without PREP_FACI
      use_record_faci = if_else(is.na(SERVICE_FACI), 1, 0, 0),
      SERVICE_FACI    = if_else(use_record_faci == 1, FACI_ID, SERVICE_FACI),
   ) %>%
   ohasis$get_faci(
      list(PREP_HUB = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
      "name",
      c("PREP_REG", "PREP_PROV", "PREP_MUNC")
   )

ly_prep %>%
   mutate(
      prep_plan = case_when(
         prep_plan == "paid" ~ "FREE",
         prep_plan == "free" ~ "PAID",
      )
   ) %>%
   format_stata() %>%
   write_dta("D:/20240419_prep-ly_20231216-20240331.dta")

ly_prep %>%
   tab(PREP_HUB)

ly_prep %>%
   group_by(PREP_HUB) %>%
   summarise(
      earliest = min(VISIT_DATE, na.rm = TRUE),
      latest   = max(VISIT_DATE, na.rm = TRUE),
   )