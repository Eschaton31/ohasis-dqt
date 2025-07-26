conn   <- connect('local')
tables <- c('px_test', 'px_test_hiv', 'inventories', 'inventory_transactions')
for (table in tables) {
   data <- QB$new(conn)$from(stri_c('ohasis.', table))$get()
   file <- file.path("H:", stri_c(table, '.dta'))
   write_dta(format_stata(data), file)
   compress_stata(file)
}
dbDisconnect(conn)

users  <- QB$new(`oh-live`)$from("ohasis_interim.users")$get()
export <- ohasis$ref_staff %>%
   inner_join(users %>% select(STAFF_ID = USER_ID, USER_NAME, LAST_LOGIN)) %>%
   inner_join(ohasis$ref_faci %>%
                 filter(FACI_NHSSS_REG == "6") %>%
                 distinct(FACI_ID)) %>%
   filter(USER_NAME != '', is.na(DELETED_AT)) %>%
   mutate(SUB_FACI_ID = NA_character_) %>%
   ohasis$get_faci(
      list(facility = c("FACI_ID", "SUB_FACI_ID")),
      "name"
   ) %>%
   select(
      facility,
      username      = USER_NAME,
      staff_name    = STAFF_NAME,
      designation   = STAFF_DESIG,
      prc           = PRC_LICENSE,
      post_nominals = POST_NOMINAL,
      email         = EMAIL,
      mobile        = MOBILE,
      landline      = LANDLINE,
      created_at    = CREATED_AT,
      updated_at    = UPDATED_AT,
      last_login    = LAST_LOGIN,
   ) %>%
   arrange(facility, username)

write_xlsx(export,"H:/20250725_reg6-active_users.xlsx")
