##  Flag issues ----------------------------------------------------------------

ihbss_flag_issues <- function(data, survey) {
   log_info("Checcking issues for the {green(toupper(survey))} survey.")
   issues                    <- list()
   issues[["Duplicate RID"]] <- data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      get_dupes(sq1_rid)

   issues[["RID Typo"]] <- data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      filter(
         !stri_detect_regex(sq1_rid, "^[M|W|F|P]-") |
            stri_count_fixed(sq1_rid, "-") != 2 |
            stri_detect_fixed(sq1_rid, " ") |
            is.na(sq1_rid)
      )

   issues[["Rejected Forms"]] <- data %>%
      filter(review_state == "rejected")

   issues[["Incomplete Test Result"]] <- data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      filter_at(
         .vars           = vars(n1_hiv, n2_syph, n3_hepb),
         .vars_predicate = any_vars(is.na(.))
      ) %>%
      relocate(n1_hiv, n2_syph, n3_hepb, .after = sq1_rid)

   issues[["Missing Cassette"]] <- data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      filter(
         is.na(n_casette)
      ) %>%
      relocate(n_casette, .after = City)

   if (survey != "medtech") {
      issues[["Missing Signature"]] <- data %>%
         filter(is.na(review_state) | review_state != "rejected") %>%
         filter(
            is.na(consent_consent_sign)
         ) %>%
         relocate(consent_consent_sign, .after = City)

      issues[["Source Stranger"]] <- data %>%
         filter(is.na(review_state) | review_state != "rejected") %>%
         filter(str_left(sq1_source_coupon, 1) == "1") %>%
         relocate(sq1_source_coupon, .after = sq1_rid)

      issues[["No Recruiter"]] <- data %>%
         filter(is.na(review_state) | review_state != "rejected") %>%
         filter(
            recruiter_exist == "N"
         )
   }

   # tab variables
   tabvars         <- vars(
      Age_Band,
      b1_self_identity,
      c4_age_first_sex_with_male,
      e8_ways_finding_most_common,
      k4_tested_ever,
      n1_hiv,
      n2_syph,
      n3_hepb,
      d12_cdmuse_last_anal
   )
   dta_list        <- lapply(tabvars, function(var, data) {
      var_list <- list()
      var_name <- substr(deparse(var), 2, 1000)
      if (var_name %in% names(data)) {
         for (site in unique(data$City)) {
            filtered <- data %>%
               filter(is.na(review_state) | review_state != "rejected") %>%
               filter(City == site)

            if (nrow(filtered) > 0) {
               var_list[[site]] <- filtered %>%
                  mutate(
                     !!var := if_else(
                        is.na(!!var) | !!var == "999",
                        "(no data)",
                        as.character(!!var),
                        as.character(!!var)
                     )
                  ) %>%
                  tab(!!var, as_df = TRUE) %>%
                  mutate(
                     City    = NA_character_,
                     .before = 1
                  ) %>%
                  mutate(
                     `Percent`      = format(`Percent` * 100, digits = 2),
                     `Cum. Percent` = format(`Cum. Percent` * 100, digits = 2),
                  ) %>%
                  add_row(City = NA_character_)

               var_list[[site]][1, "City"] <- site
            }
         }
      }  else {
         var_list <- tibble()
      }

      return(bind_rows(var_list))
   }, data)
   names(dta_list) <- stri_replace_all_fixed(as.character(tabvars), "~", "tab: ")

   issues <- modifyList(issues, dta_list)
   return(issues)
}

ihbss_monitoring <- function(ss, sheets, channels, survey) {
   # write current monitoring
   monitoring_list <- names(sheets)
   invisible(lapply(monitoring_list, function(issue, sheets, ss) {
      # write new data
      if (nrow(as.data.frame(sheets[[issue]])) > 0)
         sheets[[issue]] %>%
            mutate_if(
               .predicate = is.labelled,
               ~as_factor(.)
            ) %>%
            sheet_write(
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
   slack_msg  <- glue(">*IHBSS 2022*\n>Monitoring sheets for `{toupper(survey)}` have been updated by <@{slack_by}>.\n><{drive_link}>")
   if (is.null(channels)) {
      slackr_msg(slack_msg, mrkdwn = "true")
   } else {
      for (channel in channels)
         slackr_msg(slack_msg, mrkdwn = "true", channel = channel)
   }
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), survey) {
   monitoring   <- ihbss_flag_issues(envir$odk$data[[survey]], survey)
   ss_sheet     <- as_id(envir$gdrive$monitoring[[survey]])
   ss_archive   <- as_id(envir$gdrive$monitoring$archive)
   archive_name <- paste0(format(Sys.time(), "%Y.%m.%d"), "_monitoring-", survey)

   # archive current version of monitoring
   drive_cp(ss_sheet, ss_archive, archive_name, overwrite = TRUE)
   ihbss_monitoring(ss_sheet, monitoring, c("@cjtinaja", "@jrpalo"), survey)
}
