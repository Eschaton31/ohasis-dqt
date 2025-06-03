users <- OhasisMigration$new('users', 'ohasis-lw', 'local')
users$download()

match <- users$data$from %>%
   rename_all(tolower) %>%
   add_missing_columns(users$data$to) %>%
   select(any_of(names(users$data$to)))
pb    <- progress_bar$new(
   format = ":current of :total rows | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed",
   total  = nrow(match),
   width  = 100,
   clear  = FALSE
)
for (i in seq_len(nrow(match))) {
   file <- na_if(match[i,]$display_pic, '')
   file <- file.path("Z:/xampp/htdocs/interim/img/user", basename(file))
   if (!is.na(file) && file.exists(file)) {
      match[i,]$display_pic <- knitr::image_uri(file)
   }
   pb$tick(1)
}

users$upload(match, "user_id")

model_has_roles <- users$data$from %>%
   filter(!is.na(ROLE)) %>%
   mutate(
      model_type = 'App\\Models\\User',
      role_id    = case_when(
         ROLE == 1 ~ 1,
         ROLE == 2 ~ 2,
         # ROLE == 3 ~ 22,
         ROLE == 4 ~ 15,
         ROLE == 5 ~ 16,
         ROLE == 6 ~ 19,
         ROLE == 7 ~ 13,
         ROLE == 8 ~ 14,
         ROLE == 9 ~ 13,
         ROLE == 10 ~ 14,
         ROLE == 11 ~ 13,
         ROLE == 12 ~ 14,
      )
   ) %>%
   filter(!is.na(role_id)) %>%
   select(
      role_id,
      model_type,
      model_id = USER_ID
   )

conn <- connect('local')
dbxUpsert(conn, "model_has_roles", model_has_roles, names(model_has_roles))
dbDisconnect(conn)