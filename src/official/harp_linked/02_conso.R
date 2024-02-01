local(envir = nhsss$harp_linked, {
   data$final <- data$dx %>%
      left_join(
         y  = data$tx$outcome,
         by = "idnum"
      ) %>%
      left_join(
         y  = data$tx$reg,
         by = "art_id"
      ) %>%
      left_join(
         y  = data$dead,
         by = "idnum"
      ) %>%
      select(-any_of(c("sacclcode", "firstname", "middle", "last", "name_suffix", "name"))) %>%
      distinct(idnum, .keep_all = TRUE) %>%
      mutate(
         dead          = case_when(
            mort == 1 ~ 1,
            outcome == "dead" ~ 1,
         ),
         ffupdif       = if_else(
            condition = outcome %in% c("alive on arv", "trans out", "lost to follow up") &
               dead == 1 &
               !is.na(date_of_death),
            true      = interval(latest_ffupdate, date_of_death) / days(1),
            false     = as.numeric(NA)
         ),
         dead          = case_when(
            outcome %in% c("alive on arv", "trans out") & (is.na(ffupdif) | ffupdif < 0) ~ as.numeric(NA),
            outcome %in% c("alive on arv", "trans out") & ffupdif == 0 ~ 1,
            outcome == "lost to follow up" & ffupdif < 0 ~ as.numeric(NA),
            TRUE ~ dead
         ),
         onart         = case_when(
            dead == 1 & onart == 1 ~ 0,
            TRUE ~ onart
         ),
         art_outcome   = case_when(
            onart == 1 & everonart == 1 ~ "alive",
            dead == 1 & everonart == 1 ~ "dead",
            everonart == 1 ~ "ltfu",
            TRUE ~ NA_character_
         ),
         cur_age       = age + (as.numeric(yr) - year),
         reactive_date = coalesce(blood_extract_date, specimen_receipt_date, test_date, t0_date, visit_date, confirm_date) %>% as.Date(),
      ) %>%
      select(
         -any_of(c(
            "firstname",
            "first",
            "middle",
            "last"
         ))
      )

   # if (mo %in% c("03", "06", "09", "12")) {
   data$final <- data$final %>%
      left_join(
         y  = hs_data("harp_tx", "outcome", yr, mo) %>%
            read_dta(
               col_select = c(
                  art_id,
                  vl_date,
                  vl_result,
                  vl_suppressed,
                  baseline_vl,
                  vlp12m
               )
            ) %>%
            zap_missing() %>%
            rename(
               suppressed_latest = vl_suppressed
            ),
         by = "art_id"
      )
   # }
})

write_dta(format_stata(nhsss$harp_linked$data$final), nhsss$harp_linked$file)

date_vars <- names(nhsss$harp_linked$data$final %>% select_if(.predicate = is.Date))
date_vars <- paste(collapse = " ", date_vars)
stata(glue(r"(
u "{nhsss$harp_linked$file}", clear

ds, has(type string)
foreach var in `r(varlist)' {{
   loc type : type `var'
   loc len = substr("`type'", 4, 1000)

   cap form `var' %-`len's
}}

form {date_vars} %tdCCYY-NN-DD
compress
sa "{nhsss$harp_linked$file}", replace
)"))
rm(date_vars)