##  MSM ------------------------------------------------------------------------
local(envir = ihbss$`2022`, {
   conso$msm$monitoring                    <- list()
   conso$msm$monitoring[['Duplicate RID']] <- conso$msm$data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      get_dupes(sq1_rid)

   conso$msm$monitoring[['RID Typo']] <- conso$msm$data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      filter(
         !stri_detect_regex(sq1_rid, "^M-") |
            stri_count_fixed(sq1_rid, "-") != 2 |
            stri_detect_fixed(sq1_rid, " ")
      )

   conso$msm$monitoring[['Source Stranger']] <- conso$msm$data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      filter(StrLeft(sq1_source_coupon, 1) == "1") %>%
      relocate(sq1_source_coupon, .after = sq1_rid)

   conso$msm$monitoring[['Incomplete Test Result']] <- conso$msm$data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      filter_at(
         .vars           = vars(n1_hiv, n2_syph, n3_hepb),
         .vars_predicate = any_vars(is.na(.))
      ) %>%
      relocate(n1_hiv, n2_syph, n3_hepb, .after = sq1_rid)

   conso$msm$monitoring[['No Recruiter']] <- conso$msm$data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      filter(
         recruiter_exist == "N"
      )

   conso$msm$monitoring[['Rejected Forms']] <- conso$msm$data %>%
      filter(review_state == "rejected")

   conso$msm$monitoring[['Site-City Mismatch']] <- conso$msm$data %>%
      filter(is.na(review_state) | review_state != "rejected") %>%
      filter(
         City != site_decoded
      ) %>%
      relocate(site_decoded, int1_ihbss_site, .after = City)


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
      for (site in unique(data$City)) {
         var_list[[site]] <- data %>%
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

         var_list[[site]][1, 'City'] <- site
      }

      return(bind_rows(var_list))
   }, ihbss$`2022`$conso$msm$data)
   names(dta_list) <- stri_replace_all_fixed(as.character(tabvars), "~", "tab: ")

   conso$msm$monitoring <- modifyList(conso$msm$monitoring, dta_list)
   rm(dta_list, tabvars)
})

# archive current version of monitoring
drive_cp(
   as_id(ihbss$`2022`$gdrive$monitoring$msm),
   as_id(ihbss$`2022`$gdrive$monitoring$archive),
   paste0(format(Sys.time(), "%Y.%m.%d"), "_monitoring-msm"),
   overwrite = TRUE
)

ihbss_monitoring(ihbss$`2022`$gdrive$monitoring$msm, ihbss$`2022`$conso$msm$monitoring, "msm", c("@cjtinaja", "@jrpalo"))
