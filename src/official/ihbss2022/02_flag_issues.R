ihbss$`2022`$conso$initial$issues <- list()
ihbss$`2022`$conso$initial$issues[['Duplicate RID']] <- ihbss$`2022`$conso$initial$data %>%
   get_dupes(sq1_rid)

ihbss$`2022`$conso$initial$issues[['RID Typo']] <- ihbss$`2022`$conso$initial$data %>%
   filter(
      !stri_detect_regex(sq1_rid, "^M-") |
         stri_count_fixed(sq1_rid, "-") != 2
   )

ihbss$`2022`$conso$initial$issues[['Source Stranger']] <- ihbss$`2022`$conso$initial$data %>%
   filter(StrLeft(sq1_source_coupon, 1) == "1") %>%
   relocate(sq1_source_coupon, .after = sq1_rid)

ihbss$`2022`$conso$initial$issues[['Incomplete Test Result']] <- ihbss$`2022`$conso$initial$data %>%
   filter_at(
      .vars           = vars(n1_hiv, n2_syph, n3_hepb),
      .vars_predicate = any_vars(is.na(.))
   ) %>%
   relocate(n1_hiv, n2_syph, n3_hepb, .after = sq1_rid)


# archive current version of issues
drive_cp(
   as_id(ihbss$`2022`$gdrive$issues),
   as_id(ihbss$`2022`$gdrive$issues_archive),
   paste0(format(Sys.time(), "%Y.%m.%d"), "_issues"),
   overwrite = TRUE
)

# write current issues
issues_list <- names(ihbss$`2022`$conso$initial$issues)
invisible(lapply(issues_list, function(issue) {
   # write new data
   sheet_write(
      ihbss$`2022`$conso$initial$issues[[issue]],
      ihbss$`2022`$gdrive$issues,
      issue
   )
   # re-fit data
   range_autofit(ihbss$`2022`$gdrive$issues, issue)
}))

empty_sheets <- ""
curr_sheets  <- sheet_names(ihbss$`2022`$gdrive$issues)
for (issue in issues_list)
   if (nrow(ihbss$`2022`$conso$initial$issue[[issue]]) == 0 &
      issue %in% curr_sheets)
      empty_sheets <- append(empty_sheets, issue)

for (issue in curr_sheets)
   if (!(issue %in% issues_list))
      empty_sheets <- append(empty_sheets, issue)

# delete if existing sheet no longer has values in new run
if (length(empty_sheets[-1]) > 0)
   sheet_delete(ihbss$`2022`$gdrive$issues, empty_sheets[-1])

rm(curr_sheets, empty_sheets, issue, issues_list)