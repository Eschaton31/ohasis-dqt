##  Primary Controller for the IHBSS 2022 Linkage ------------------------------

if (!exists("ihbss"))
   ihbss <- new.env()

ihbss$`2022`            <- new.env()
ihbss$`2022`$odk$config <- read_sheet("1SNB43JjGOB-uEivT5n8bfGpxiTEKkhNM7izo-KCQKPY", "forms")

# setup
ihbss$`2022`$odk$submissions <- list()
ihbss$`2022`$odk$data        <- list()
for (i in seq_len(nrow(ihbss$`2022`$odk$config))) {
   config <- ihbss$`2022`$odk$config[i,]
   form   <- paste(sep = " - ", config$City, config$Language)

   ruODK::ru_setup(
      svc     = config$OData,
      tz      = "Asia/Hong_Kong",
      verbose = FALSE
   )

   check_dir(file.path("H:/_R/library/ihbss2022", form))
   tryCatch({
      ihbss$`2022`$odk$submissions[[form]] <- ruODK::submission_list()
   },
      error = function(e) .log_info("{e}")
   )

   ihbss$`2022`$odk$data[[form]] <- ruODK::odata_submission_get(download = TRUE, local_dir = file.path("H:/_R/library/ihbss2022", form)) %>%
      mutate(Form = form)
}

# list projects & forms
ihbss$`2022`$odk$submissions <- ruODK::submission_list()
ihbss$`2022`$odk$data        <- ruODK::odata_submission_get(download = TRUE, local_dir = file.path(" H:/_R/library/ihbss2022", form))

ruODK::user_list()
subs <- bind_rows(ihbss$`2022`$odk$submissions)
data <- bind_rows(ihbss$`2022`$odk$data)

data %>%
   select(
      id,
      d8 = d_8_months_with_anal_sex_act
   ) %>%
   separate(
      d8,
      sep  = " ",
      into = c("m01", "m02", "m03", "m04", "m05", "m06", "m07", "m08", "m09", "m10", "m11", "m12")
   ) %>%
   pivot_longer(
      cols      = starts_with("m"),
      names_to  = "num",
      values_to = "month"
   ) %>%
   filter(!is.na(month)) %>%
   arrange(month) %>%
   mutate(
      month = month.abb[as.numeric(month)],
      num   = 1,
   ) %>%
   pivot_wider(
      id_cols      = id,
      names_from   = month,
      values_from  = num,
      names_prefix = "d_8_"
   )