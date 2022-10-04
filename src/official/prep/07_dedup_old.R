##  Prepare dataset for art registry deduplication -----------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Preparing dataset for art registry deduplication.")
data_new <- nhsss$prep$reg.converted$data %>%
   mutate(
      byr = if_else(
         condition = !is.na(birthdate),
         true      = year(birthdate),
         false     = NA_integer_
      ) %>% as.numeric(),
      bmo = if_else(
         condition = !is.na(birthdate),
         true      = year(birthdate),
         false     = NA_integer_
      ) %>% as.numeric(),
      bdy = if_else(
         condition = !is.na(birthdate),
         true      = year(birthdate),
         false     = NA_integer_
      ) %>% as.numeric(),
   )

data_old <- nhsss$prep$official$old_reg %>%
   mutate(
      byr = if_else(
         condition = !is.na(birthdate),
         true      = year(birthdate),
         false     = NA_integer_
      ) %>% as.numeric(),
      bmo = if_else(
         condition = !is.na(birthdate),
         true      = year(birthdate),
         false     = NA_integer_
      ) %>% as.numeric(),
      bdy = if_else(
         condition = !is.na(birthdate),
         true      = year(birthdate),
         false     = NA_integer_
      ) %>% as.numeric(),
   )

##  Patient Record Linkage Algorithm -------------------------------------------

.log_info("Passing dataset through the record linkage algorithm.")
nhsss$prep$dedup_old <- list()
reclink_df              <- fastLink(
   dfA              = data_new,
   dfB              = data_old,
   varnames         = c(
      "first",
      "middle",
      "last",
      "byr",
      "bmo",
      "bdy"
   ),
   stringdist.match = c(
      "first",
      "middle",
      "last"
   ),
   partial.match    = c(
      "first",
      "middle",
      "last"
   ),
   numeric.match    = c(
      "byr",
      "bmo",
      "bdy"
   ),
   threshold.match  = 0.95,
   cut.a            = 0.90,
   cut.p            = 0.85,
   dedupe.matches   = FALSE,
   n.cores          = 4
)

if (length(reclink_df$matches$inds.a) > 0) {
   reclink_matched <- getMatches(
      dfA         = data_new,
      dfB         = data_old,
      fl.out      = reclink_df,
      combine.dfs = FALSE
   )

   reclink_review <- reclink_matched$dfA.match %>%
      mutate(
         MATCH_ID = row_number()
      ) %>%
      select(
         MATCH_ID,
         MASTER_RID          = REC_ID,
         MASTER_PID          = PATIENT_ID,
         MASTER_CID          = CENTRAL_ID,
         MASTER_FIRST        = first,
         MASTER_MIDDLE       = middle,
         MASTER_LAST         = last,
         MASTER_SUFFIX       = suffix,
         MASTER_BIRTHDATE    = birthdate,
         MASTER_UIC          = uic,
         MASTER_INITIALS     = initials,
         posterior
      ) %>%
      left_join(
         y  = reclink_matched$dfB.match %>%
            mutate(
               MATCH_ID = row_number()
            ) %>%
            select(
               MATCH_ID,
               USING_RID          = REC_ID,
               USING_PID          = PATIENT_ID,
               USING_CID          = CENTRAL_ID,
               USING_PREP_ID       = prep_id,
               USING_FIRST        = first,
               USING_MIDDLE       = middle,
               USING_LAST         = last,
               USING_SUFFIX       = suffix,
               USING_BIRTHDATE    = birthdate,
               USING_UIC          = uic,
               USING_INITIALS     = initials,
               posterior
            ),
         by = "MATCH_ID"
      ) %>%
      select(-posterior.y) %>%
      rename(posterior = posterior.x) %>%
      unite(
         col   = "MASTER_FMS",
         sep   = " ",
         MASTER_FIRST,
         MASTER_MIDDLE,
         MASTER_SUFFIX,
         na.rm = TRUE
      ) %>%
      unite(
         col   = "MASTER_NAME",
         sep   = ", ",
         MASTER_LAST,
         MASTER_FMS,
         na.rm = TRUE
      ) %>%
      unite(
         col   = "USING_FMS",
         sep   = " ",
         USING_FIRST,
         USING_MIDDLE,
         USING_SUFFIX,
         na.rm = TRUE
      ) %>%
      unite(
         col   = "USING_NAME",
         sep   = ", ",
         USING_LAST,
         USING_FMS,
         na.rm = TRUE
      ) %>%
      arrange(desc(posterior)) %>%
      relocate(posterior, .before = MATCH_ID) %>%
      # Additional sift through of matches
      mutate(
         # levenshtein
         LV       = stringsim(MASTER_NAME, USING_NAME, method = 'lv'),
         # jaro-winkler
         JW       = stringsim(MASTER_NAME, USING_NAME, method = 'jw'),
         # qgram
         QGRAM    = stringsim(MASTER_NAME, USING_NAME, method = 'qgram', q = 3),
         AVG_DIST = (LV + QGRAM + JW) / 3,
      ) %>%
      # choose 60% and above match
      filter(AVG_DIST >= 0.60, !is.na(posterior))

   # assign to global env
   nhsss$prep$dedup_old$reclink <- reclink_review %>%
      mutate(
         Bene = NA_character_,
         Meg  = NA_character_,
         Kath = NA_character_,
      )
}

##  Grouped identifiers for deduplication --------------------------------------

.log_info("Preparing dataset for pii dedup.")
dedup_new <- dedup_prep(
   data         = nhsss$prep$official$new_reg %>% mutate(CONFIRMATORY_CODE = NA_character_),
   name_f       = first,
   name_m       = middle,
   name_l       = last,
   name_s       = suffix,
   uic          = uic,
   birthdate    = birthdate,
   code_confirm = CONFIRMATORY_CODE,
   code_px      = px_code,
   phic         = philhealth_no,
   philsys      = philsys_id
)

.log_info("Deduplicating based on grouped identifiers.")
nhsss$prep$dedup_new <- list()
group_pii               <- list(
   "UIC.Base"            = "uic",
   "UIC.Fixed"           = "UIC_SORT",
   "ConfirmCode.Base"    = "CONFIRMATORY_CODE",
   "ConfirmCode.Fixed"   = "CONFIRM_SIEVE",
   "PxCode.Base"         = "PATIENT_CODE",
   "PxCode.Fixed"        = "PXCODE_SIEVE",
   "PxConfirm.Base"      = c("PATIENT_CODE", "CONFIRMATORY_CODE"),
   "PxConfirm.Fixed"     = c("PXCODE_SIEVE", "CONFIRM_SIEVE"),
   "ConfirmUIC.Base"     = c("CONFIRMATORY_CODE", "UIC"),
   "ConfirmUIC.Fixed"    = c("CONFIRM_SIEVE", "UIC"),
   "PxUIC.Base"          = c("PATIENT_CODE", "UIC"),
   "PxUIC.Fixed"         = c("PXCODE_SIEVE", "UIC"),
   "PxBD.Base"           = c("PATIENT_CODE", "birthdate"),
   "PxBD.Fixed"          = c("PXCODE_SIEVE", "birthdate"),
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
      .GlobalEnv$nhsss$prep$dedup_old[[dedup_name]] <- df
   })
)

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
gdrive_validation(nhsss$prep, "dedup_old", ohasis$ym)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
