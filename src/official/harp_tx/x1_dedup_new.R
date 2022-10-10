##  Prepare dataset for initial deduplication ----------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Preparing dataset for initial deduplication.")
dedup_new <- dedup_prep(
   data         = nhsss$harp_tx$reg.converted$data,
   name_f       = first,
   name_m       = middle,
   name_l       = last,
   name_s       = suffix,
   uic          = uic,
   birthdate    = birthdate,
   code_confirm = confirmatory_code,
   code_px      = px_code,
   phic         = philhealth_no,
   philsys      = philsys_id
)

##  Grouped identifiers for deduplication --------------------------------------

.log_info("Deduplicating based on grouped identifiers.")
nhsss$harp_tx$dedup_new <- list()
group_pii               <- list(
   "UIC.Base"            = "uic",
   "UIC.Fixed"           = "UIC_SORT",
   "Name.Base"           = c("FIRST", "LAST", "birthdate"),
   "Name.Fixed"          = c("FIRST_NY", "LAST_NY", "birthdate"),
   "Name.Partial"        = c("FIRST_A", "LAST_A", "birthdate"),
   "YMName.Base"         = c("FIRST", "LAST", "BIRTH_YR", "BIRTH_MO"),
   "YDName.Fixed"        = c("FIRST", "LAST", "BIRTH_YR", "BIRTH_DY"),
   "MDName.Partial"      = c("FIRST", "LAST", "BIRTH_MO", "BIRTH_DY"),
   "YMNameClean.Base"    = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_MO"),
   "YDNameClean.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_DY"),
   "MDNameClean.Partial" = c("FIRST_NY", "LAST_NY", "BIRTH_MO", "BIRTH_DY")
)
invisible(
   lapply(seq_along(group_pii), function(i) {
      dedup_name <- names(group_pii)[[i]]
      dedup_id   <- group_pii[[i]]

      # tag duplicates based on grouping
      df <- dedup_new %>%
         filter_at(
            .vars           = vars(dedup_id),
            .vars_predicate = all_vars(!is.na(.))
         ) %>%
         get_dupes(dedup_id) %>%
         filter(dupe_count > 0) %>%
         group_by(across(all_of(dedup_id))) %>%
         mutate(
            # generate a group id to identify groups of duplicates
            group_id = cur_group_id(),
         ) %>%
         ungroup() %>%
         mutate(DUP_IDS = paste(collapse = ', ', dedup_id))

      # if any found, include in list for review
      if (nrow(df) > 0)
         .GlobalEnv$nhsss$harp_tx$dedup_new[[dedup_name]] <- df
   })
)

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
gdrive_validation(nhsss$harp_tx, "dedup_new", ohasis$ym, list_name = NULL)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
