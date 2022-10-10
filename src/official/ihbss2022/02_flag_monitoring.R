ihbss$`2022`$conso$initial$monitoring                    <- list()
ihbss$`2022`$conso$initial$monitoring[['Duplicate RID']] <- ihbss$`2022`$conso$initial$data %>%
   filter(is.na(review_state) | review_state != "rejected") %>%
   get_dupes(sq1_rid)

ihbss$`2022`$conso$initial$monitoring[['RID Typo']] <- ihbss$`2022`$conso$initial$data %>%
   filter(is.na(review_state) | review_state != "rejected") %>%
   filter(
      !stri_detect_regex(sq1_rid, "^M-") |
         stri_count_fixed(sq1_rid, "-") != 2 |
         stri_detect_fixed(sq1_rid, " ")
   )

ihbss$`2022`$conso$initial$monitoring[['Source Stranger']] <- ihbss$`2022`$conso$initial$data %>%
   filter(is.na(review_state) | review_state != "rejected") %>%
   filter(StrLeft(sq1_source_coupon, 1) == "1") %>%
   relocate(sq1_source_coupon, .after = sq1_rid)

ihbss$`2022`$conso$initial$monitoring[['Incomplete Test Result']] <- ihbss$`2022`$conso$initial$data %>%
   filter(is.na(review_state) | review_state != "rejected") %>%
   filter_at(
      .vars           = vars(n1_hiv, n2_syph, n3_hepb),
      .vars_predicate = any_vars(is.na(.))
   ) %>%
   relocate(n1_hiv, n2_syph, n3_hepb, .after = sq1_rid)

ihbss$`2022`$conso$initial$monitoring[['No Recruiter']] <- ihbss$`2022`$conso$initial$data %>%
   filter(is.na(review_state) | review_state != "rejected") %>%
   filter(
      recruiter_exist == "N"
   )

ihbss$`2022`$conso$initial$monitoring[['Rejected Forms']] <- ihbss$`2022`$conso$initial$data %>%
   filter(review_state == "rejected")

ihbss$`2022`$conso$initial$monitoring[['Site-City Mismatch']] <- ihbss$`2022`$conso$initial$data %>%
   filter(is.na(review_state) | review_state != "rejected") %>%
   filter(
      City != site_decoded
   ) %>%
   relocate(site_decoded, int1_ihbss_site, .after = City)

# tab variables
tabvars <- vars(
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
for (var in tabvars) {
   dta_list <- list()

   for (site in unique(ihbss$`2022`$conso$initial$data$City)) {
      dta_list[[site]] <- ihbss$`2022`$conso$initial$data %>%
         filter(is.na(review_state) | review_state != "rejected") %>%
         filter(City == site) %>%
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

      dta_list[[site]][1, 'City'] <- site
   }
   ihbss$`2022`$conso$initial$monitoring[[paste0("tab: ", quo_name(var))]] <- dta_list %>%
      bind_rows()
}
tabvars <- vars(
   n1_hiv,
   n2_syph,
   n3_hepb,
)
# for (var in tabvars) {
#    if (quo_name(var) == "n1_hiv")
#       data <- ihbss$`2022`$conso$initial$data %>%
#          filter(c4_age_first_sex_with_male != 999)
#    else
#       data <- ihbss$`2022`$conso$initial$data
#
#    ref <- data %>%
#       group_by(City, int1_ihbss_site) %>%
#       summarise(
#          Total = n()
#       ) %>%
#       ungroup()
#
#    ihbss$`2022`$conso$initial$monitoring[[paste0("tab: ", quo_name(var))]] <- data %>%
#       mutate(
#          !!var := if_else(
#             is.na(!!var) | !!var == "999",
#             "(no data)",
#             as.character(!!var),
#             as.character(!!var)
#          )
#       ) %>%
#       group_by(City, int1_ihbss_site, !!var) %>%
#       summarise(
#          Value = n()
#       ) %>%
#       ungroup() %>%
#       pivot_wider(
#          id_cols     = c(City, int1_ihbss_site),
#          names_from  = !!var,
#          values_from = Value
#       ) %>%
#       left_join(ref) %>%
#       mutate(
#          RR = (`1_reactive` / Total) * 100
#       )
# }

# special
# var <- quo(d12_cdmuse_last_anal)
#
# ihbss$`2022`$conso$initial$monitoring[[paste0("tab: ", quo_name(var))]] <- ihbss$`2022`$conso$initial$data %>%
#    filter(c4_age_first_sex_with_male != 999) %>%
#    mutate(
#       !!var := if_else(
#          is.na(!!var) | !!var == "999",
#          "(no data)",
#          as.character(!!var),
#          as.character(!!var)
#       )
#    ) %>%
#    group_by(City, int1_ihbss_site, !!var) %>%
#    summarise(
#       Value = n()
#    ) %>%
#    ungroup() %>%
#    pivot_wider(
#       id_cols     = c(City, int1_ihbss_site),
#       names_from  = !!var,
#       values_from = Value
#    )

# archive current version of monitoring
drive_cp(
   as_id(ihbss$`2022`$gdrive$monitoring),
   as_id(ihbss$`2022`$gdrive$monitoring_archive),
   paste0(format(Sys.time(), "%Y.%m.%d"), "_monitoring"),
   overwrite = TRUE
)

# write current monitoring
monitoring_list <- names(ihbss$`2022`$conso$initial$monitoring)
invisible(lapply(monitoring_list, function(issue) {
   # write new data
   sheet_write(
      ihbss$`2022`$conso$initial$monitoring[[issue]],
      ihbss$`2022`$gdrive$monitoring,
      issue
   )
   # re-fit data
   range_autofit(ihbss$`2022`$gdrive$monitoring, issue)
}))

empty_sheets <- ""
curr_sheets  <- sheet_names(ihbss$`2022`$gdrive$monitoring)
for (issue in monitoring_list)
   if (nrow(ihbss$`2022`$conso$initial$monitoring[[issue]]) == 0 &
      issue %in% curr_sheets)
      empty_sheets <- append(empty_sheets, issue)

for (issue in curr_sheets)
   if (!(issue %in% monitoring_list))
      empty_sheets <- append(empty_sheets, issue)

# delete if existing sheet no longer has values in new run
if (length(empty_sheets[-1]) > 0)
   sheet_delete(ihbss$`2022`$gdrive$monitoring, empty_sheets[-1])

rm(curr_sheets, empty_sheets, issue, monitoring_list, data, ref, var, tabvars, dta_list, site)