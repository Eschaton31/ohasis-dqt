if (!exists("dr"))
   dr <- new.env()

dr$`20220511-iit` <- new.env()

local(envir = dr$`20220511-iit`, {
   data <- list()

   for (i in c("2019", "2020", "2021")) {
      for (j in c("03", "06", "09", "12"))
         data[[glue("{i}{j}")]] <- read_dta(ohasis$get_data("harp_tx-outcome", i, j)) %>%
            mutate_if(
               .predicate = is.character,
               ~str_squish(.) %>%
                  if_else(. == "", NA_character_, ., .)
            ) %>%
            mutate(
               diff    = floor(difftime(ceiling_date(as.Date(paste(sep = "-", i, j, "01")), unit = "month") - 1, latest_nextpickup, units = "days")),
               outcome = case_when(
                  outcome == "alive on arv" ~ "onart",
                  outcome == "lost to follow up" ~ "ltfu",
                  outcome == "trans out" ~ "transout",
                  TRUE ~ outcome
               )
            ) %>%
            mutate(
               .after      = outcome,
               outcome_new = case_when(
                  outcome == "dead" ~ "dead",
                  outcome == "stopped" ~ "stopped",
                  diff > 28 ~ "ltfu",
                  diff <= 28 ~ "onart",
                  outcome == "ltfu" ~ "ltfu",
                  TRUE ~ "(no data)"
               )
            ) %>%
            select(
               idnum,
               sex,
               birthdate,
               age,
               artstart_date,
               sacclcode,
               "hub{{i}}{{j}}"         := hub,
               "ffup{{i}}{{j}}"        := latest_ffupdate,
               "pickup{{i}}{{j}}"      := latest_nextpickup,
               "outcome{{i}}{{j}}"     := outcome,
               "outcome_new{{i}}{{j}}" := outcome_new
            ) %>%
            rename_all(
               ~stri_replace_all_fixed(., "\"", "")
            )
   }

   rm(i, j)
})

df <- dr$`20220511-iit`$data$`202112` %>%
   select(
      idnum,
      sex,
      birthdate,
      age,
      artstart_date,
      sacclcode,
      hub202112,
      ffup202112,
      pickup202112,
      outcome202112,
      outcome_new202112,
   )

for_merge <- names(dr$`20220511-iit`$data)
for_merge <- for_merge[for_merge != "202112"]
for_merge <- sort(for_merge, decreasing = TRUE)

for (i in for_merge)
   df <- df %>%
      left_join(
         y  = dr$`20220511-iit`$data[[i]] %>%
            select(
               sacclcode,
               ends_with(i)
            ),
         by = "sacclcode"
      )

df %>%
   left_join(
      y  = read_dta(
         ohasis$get_data("harp_dx", "2021", "12"),
         col_select = c("idnum", "transmit", "sexhow", "region", "province", "muncity", "dxlab_standard", "confirmlab", "confirm_date")
      ),
      by = "idnum"
   )

write_dta(
   df %>%
      left_join(
         y  = read_dta(
            ohasis$get_data("harp_dx", "2021", "12"),
            col_select = c("idnum", "transmit", "sexhow", "region", "province", "muncity", "dxlab_standard", "confirmlab", "confirm_date", "self_identity")
         ),
         by = "idnum"
      ),
   "H:/20220622_iit_2019-2021.dta"
)