con        <- ohasis$conn("lw")
ly_clients <- QB$new(con)$from("ohasis_lake.ly_clients")$get()
dbDisconnect(con)

match_var <- function(...) {
   clients <- ly_clients %>%
      mutate(
         CLIENT_MOBILE = str_replace(CLIENT_MOBILE, "^0", ""),
         # LEFT_EMAIL    = substr(CLIENT_EMAIL, 1, 6),
         LEFT_EMAIL    = nysiis(CLIENT_EMAIL),
      )
   non     <- clients %>%
      filter(is.na(CENTRAL_ID)) %>%
      filter(if_all(c(...), ~!is.na(.)))

   with <- clients %>%
      filter(!is.na(CENTRAL_ID)) %>%
      filter(if_all(c(...), ~!is.na(.)))

   non %>%
      select(row_id, ...) %>%
      inner_join(
         y  = with %>%
            select(CENTRAL_ID, ...) %>%
            distinct(),
         by = join_by(...)
      ) %>%
      distinct(row_id, .keep_all = TRUE) %>%
      return()
}

match <- match_var(CLIENT_MOBILE, CLIENT_CODE, LEFT_EMAIL)

con <- ohasis$conn("lw")
dbxUpsert(con, Id(schema = "ohasis_lake", table = "ly_clients"), match %>% select(row_id, CENTRAL_ID), "row_id")
dbDisconnect(con)
