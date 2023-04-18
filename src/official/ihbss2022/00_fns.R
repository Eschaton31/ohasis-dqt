ihbss_mos <- function(data, col, varname) {
   data %>%
      select(
         id,
         months = col
      ) %>%
      separate(
         months,
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
         names_prefix = paste0(varname, "_")
      )
}

ihbss_rename <- function(data, survey) {
   data %<>%
      rename_all(
         ~stri_replace_first_regex(., "_([0-9])", "$1")
      ) %>%
      rowwise() %>%
      mutate(
         .after = sq1_rid,
         cn     = str_squish(strsplit(sq1_rid, "\\-")[[1]][3]),
      ) %>%
      ungroup() %>%
      relocate(n_casette, sq2_current_age, .after = sq1_rid) %>%
      mutate(
         .after          = cn,
         cn              = if_else(!is.na(cn), cn, "", ""),
         c1              = paste0(cn, "1"),
         c2              = paste0(cn, "2"),
         c3              = paste0(cn, "3"),
         recruiter       = StrLeft(cn, nchar(cn) - 1),
         sq2_current_age = floor(sq2_current_age),
         Age_Band        = case_when(
            sq2_current_age < 15 ~ "<15>",
            sq2_current_age %in% seq(15, 17) ~ "15-17",
            sq2_current_age %in% seq(18, 24) ~ "18-24",
            sq2_current_age %in% seq(25, 35) ~ "25-35",
            sq2_current_age > 35 ~ "35+",
            TRUE ~ "(no data)"
         ),
      ) %>%
      mutate(
         .after       = int1_ihbss_site,
         site_decoded = case_when(
            int1_ihbss_site == "100" ~ "National Capital Region",
            int1_ihbss_site == "101" ~ "Angeles City",
            int1_ihbss_site == "102" ~ "Baguio City",
            int1_ihbss_site == "104" ~ "Cagayan de Oro City",
            int1_ihbss_site == "105" ~ "Cebu City",
            int1_ihbss_site == "106" ~ "Davao City",
            int1_ihbss_site == "107" ~ "General Santos City",
            int1_ihbss_site == "108" ~ "Iloilo City",
            int1_ihbss_site == "111" ~ "Puerto Princesa City",
            int1_ihbss_site == "113" ~ "Tuguegarao City",
            int1_ihbss_site == "114" ~ "Zamboanga City",
            int1_ihbss_site == "117" ~ "Batangas City",
            int1_ihbss_site == "122" ~ "Bacolod City",
            int1_ihbss_site == "123" ~ "Bacoor City",
            int1_ihbss_site == "301" ~ "Naga City",
            int1_ihbss_site == "300" ~ "Dasmariñas City",
            TRUE ~ as.character(int1_ihbss_site)
         )
      ) %>%
      # time start
      mutate(
         .after     = start,
         start_date = format(start, "%Y-%m-%d"),
         start_time = format(start, "%H:%M:%S"),
      ) %>%
      # time end
      mutate(
         .after   = end,
         end_date = format(end, "%Y-%m-%d"),
         end_time = format(end, "%H:%M:%S"),
      )

   if (survey == "msm") {
      data %<>%
         mutate(
            path_dta = file.path(Sys.getenv("IHBSS_2022_LOCAL"), stri_c("MSM - ", City), "data"),
            path_sig = file.path(Sys.getenv("IHBSS_2022_LOCAL"), stri_c("MSM - ", City), "signature"),
            path_cst = file.path(Sys.getenv("IHBSS_2022_LOCAL"), stri_c("MSM - ", City), "cassette"),
         )
   }

   return(data)
}

ihbss_monitoring <- function(ss, sheets, survey, channels) {
   # write current monitoring
   monitoring_list <- names(sheets)
   invisible(lapply(monitoring_list, function(issue, sheets, ss) {
      # write new data
      sheet_write(
         sheets[[issue]],
         ss,
         issue
      )
   }, sheets, ss))

   empty_sheets <- ""
   curr_sheets  <- sheet_names(ss)
   for (issue in monitoring_list)
      if (nrow(sheets[[issue]]) == 0 & issue %in% curr_sheets)
         empty_sheets <- append(empty_sheets, issue)

   for (issue in curr_sheets)
      if (!(issue %in% monitoring_list))
         empty_sheets <- append(empty_sheets, issue)

   # delete if existing sheet no longer has values in new run
   if (length(empty_sheets[-1]) > 0)
      sheet_delete(ss, empty_sheets[-1])

   # re-fit data
   for (issue in setdiff(curr_sheets, empty_sheets))
      range_autofit(ss, issue)

   # acquire sheet_id
   slack_by   <- (slackr_users() %>% filter(name == Sys.getenv("SLACK_PERSONAL")))$id
   drive_link <- paste0("https://docs.google.com/spreadsheets/d/", ss, "/|GSheets Link: Daily Monitoring - ", toupper(survey))
   slack_msg  <- glue(">*IHBSS 2022*\n>Monitoring sheets for `{survey}` have been updated by <@{slack_by}>.\n><{drive_link}>")
   if (is.null(channels)) {
      slackr_msg(slack_msg, mrkdwn = "true")
   } else {
      for (channel in channels)
         slackr_msg(slack_msg, mrkdwn = "true", channel = channel)
   }
}
