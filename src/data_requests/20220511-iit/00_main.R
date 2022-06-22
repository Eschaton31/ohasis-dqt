if (!exists("dr"))
   dr <- new.env()

dr$`20220511-iit` <- new.env()

local(envir = dr$`20220511-iit`, {
   data <- list()

   for (i in c("2018", "2019", "2020", "2021"))
      data[[i]] <- read_dta(ohasis$get_data("harp_tx-outcome", i, "12")) %>%
         mutate_if(
            .predicate = is.character,
            ~str_squish(.) %>%
               if_else(. == "", NA_character_, ., .)
         )

   rm(i)
})

df <- dr$`20220511-iit`$data$`2021` %>%
   select(
      idnum,
      sex,
      birthdate,
      age,
      artstart_date,
      # idnum2021   = idnum,
      sacclcode,
      ffup2021    = latest_ffupdate,
      pickup2021  = latest_nextpickup,
      outcome2021 = outcome,
   ) %>%
   left_join(
      y  = dr$`20220511-iit`$data$`2020` %>%
         select(
            # idnum2020   = idnum,
            sacclcode,
            ffup2020    = latest_ffupdate,
            pickup2020  = latest_nextpickup,
            outcome2020 = outcome,
         ),
      by = "sacclcode"
   ) %>%
   left_join(
      y  = dr$`20220511-iit`$data$`2019` %>%
         select(
            # idnum2019   = idnum,
            sacclcode,
            ffup2019    = latest_ffupdate,
            pickup2019  = latest_nextpickup,
            outcome2019 = outcome,
         ),
      by = "sacclcode"
   ) %>%
   left_join(
      y  = dr$`20220511-iit`$data$`2018` %>%
         select(
            # idnum2018   = idnum,
            sacclcode,
            ffup2018    = latest_ffupdate,
            pickup2018  = latest_nextpickup,
            outcome2018 = outcome,
         ),
      by = "sacclcode"
   ) %>%
   mutate_at(
      .vars = vars(outcome2018, outcome2019, outcome2020, outcome2021),
      ~case_when(
         . == "alive on arv" ~ "onart",
         . == "lost to follow up" ~ "ltfu",
         . == "trans out" ~ "transout",
         TRUE ~ .
      )
   ) %>%
   mutate(
      .after          = outcome2021,
      outcome_new2021 = case_when(
         outcome2021 == "dead" ~ "dead",
         outcome2021 == "stopped" ~ "stopped",
         floor(difftime(as.Date("2021-12-31"), pickup2021, units = "days")) > 28 ~ "ltfu",
         floor(difftime(as.Date("2021-12-31"), pickup2021, units = "days")) <= 28 ~ "onart",
         outcome2021 == "ltfu" ~ "ltfu",
         TRUE ~ "(no data)"
      )
   ) %>%
   mutate(
      .after          = outcome2020,
      outcome_new2020 = case_when(
         outcome2020 == "dead" ~ "dead",
         outcome2020 == "stopped" ~ "stopped",
         floor(difftime(as.Date("2020-12-31"), pickup2020, units = "days")) > 28 ~ "ltfu",
         floor(difftime(as.Date("2020-12-31"), pickup2020, units = "days")) <= 28 ~ "onart",
         outcome2020 == "ltfu" ~ "ltfu",
         TRUE ~ "(no data)"
      )
   ) %>%
   mutate(
      .after          = outcome2019,
      outcome_new2019 = case_when(
         outcome2019 == "dead" ~ "dead",
         outcome2019 == "stopped" ~ "stopped",
         floor(difftime(as.Date("2019-12-31"), pickup2019, units = "days")) > 28 ~ "ltfu",
         floor(difftime(as.Date("2019-12-31"), pickup2019, units = "days")) <= 28 ~ "onart",
         outcome2019 == "ltfu" ~ "ltfu",
         TRUE ~ "(no data)"
      )
   ) %>%
   mutate(
      .after          = outcome2018,
      outcome_new2018 = case_when(
         outcome2018 == "dead" ~ "dead",
         outcome2018 == "stopped" ~ "stopped",
         floor(difftime(as.Date("2018-12-31"), pickup2018, units = "days")) > 28 ~ "ltfu",
         floor(difftime(as.Date("2018-12-31"), pickup2018, units = "days")) <= 28 ~ "onart",
         outcome2018 == "ltfu" ~ "ltfu",
         TRUE ~ "(no data)"
      )
   )

write_dta(
   df,
   "H:/20220622_iit_2018-2021.dta"
)