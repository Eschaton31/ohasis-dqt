##  Prepare dataset for art registry deduplication -----------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Preparing dataset for art registry deduplication.")
data_new <- nhsss$harp_tx$reg.converted$data %>%
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

data_old <- nhsss$harp_tx$official$old_reg %>%
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
nhsss$harp_tx$dedup_old <- list()
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
         MASTER_CONFIRMATORY = confirmatory_code,
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
               USING_ART_ID       = art_id,
               USING_FIRST        = first,
               USING_MIDDLE       = middle,
               USING_LAST         = last,
               USING_SUFFIX       = suffix,
               USING_BIRTHDATE    = birthdate,
               USING_CONFIRMATORY = confirmatory_code,
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
   nhsss$harp_tx$dedup_old$reclink <- reclink_review %>%
      mutate(
         Bene = NA_character_,
         Meg  = NA_character_,
         Kath = NA_character_,
      )
}

##  Grouped identifiers for deduplication --------------------------------------

.log_info("Preparing dataset for pii dedup.")
dedup_new <- nhsss$harp_tx$official$new_reg %>%
   mutate(
      # standardize PII
      LAST              = stri_trans_toupper(last),
      MIDDLE            = stri_trans_toupper(middle),
      FIRST             = stri_trans_toupper(first),
      SUFFIX            = stri_trans_toupper(suffix),
      UIC               = stri_trans_toupper(uic),
      CONFIRMATORY_CODE = stri_trans_toupper(confirmatory_code),
      PATIENT_CODE      = stri_trans_toupper(px_code),

      # get components of birthdate
      BIRTH_YR          = year(birthdate),
      BIRTH_MO          = month(birthdate),
      BIRTH_DY          = day(birthdate),

      # extract parent info from uic
      UIC_MOM           = if_else(!is.na(UIC), substr(UIC, 1, 2), NA_character_),
      UIC_DAD           = if_else(!is.na(UIC), substr(UIC, 3, 4), NA_character_),

      # variables for first 3 letters of names
      FIRST_A           = if_else(!is.na(FIRST), substr(FIRST, 1, 3), NA_character_),
      MIDDLE_A          = if_else(!is.na(MIDDLE), substr(MIDDLE, 1, 3), NA_character_),
      LAST_A            = if_else(!is.na(LAST), substr(LAST, 1, 3), NA_character_),

      LAST              = if_else(is.na(LAST), MIDDLE, LAST),
      MIDDLE            = if_else(is.na(MIDDLE), LAST, MIDDLE),

      # clean ids
      CONFIRM_SIEVE     = if_else(!is.na(CONFIRMATORY_CODE), str_replace_all(CONFIRMATORY_CODE, "[^[:alnum:]]", ""), NA_character_),
      PXCODE_SIEVE      = if_else(!is.na(PATIENT_CODE), str_replace_all(PATIENT_CODE, "[^[:alnum:]]", ""), NA_character_),
      FIRST_S           = if_else(!is.na(FIRST), str_replace_all(FIRST, "[^[:alnum:]]", ""), NA_character_),
      MIDDLE_S          = if_else(!is.na(MIDDLE), str_replace_all(MIDDLE, "[^[:alnum:]]", ""), NA_character_),
      LAST_S            = if_else(!is.na(LAST), str_replace_all(LAST, "[^[:alnum:]]", ""), NA_character_),
      PHIC              = if_else(!is.na(philhealth_no), str_replace_all(philhealth_no, "[^[:alnum:]]", ""), NA_character_),

      # code standard names
      FIRST_NY          = if_else(!is.na(FIRST_S), nysiis(FIRST_S, stri_length(FIRST_S)), NA_character_),
      MIDDLE_NY         = if_else(!is.na(MIDDLE_S), nysiis(MIDDLE_S, stri_length(MIDDLE_S)), NA_character_),
      LAST_NY           = if_else(!is.na(LAST_S), nysiis(LAST_S, stri_length(LAST_S)), NA_character_),
   )

# genearte UIC w/o 1 parent, 2 combinations
dedup_new_uic <- dedup_new %>%
   filter(!is.na(UIC)) %>%
   select(
      CENTRAL_ID,
      UIC_MOM,
      UIC_DAD
   ) %>%
   pivot_longer(
      cols      = starts_with('UIC'),
      names_to  = 'UIC',
      values_to = 'FIRST_TWO'
   ) %>%
   arrange(CENTRAL_ID, FIRST_TWO) %>%
   group_by(CENTRAL_ID) %>%
   mutate(UIC = row_number()) %>%
   ungroup() %>%
   pivot_wider(
      id_cols      = CENTRAL_ID,
      names_from   = UIC,
      names_prefix = 'UIC_',
      values_from  = FIRST_TWO
   )

dedup_new %<>%
   left_join(
      y  = dedup_new_uic,
      by = 'CENTRAL_ID'
   ) %>%
   mutate(
      UIC_SORT = if_else(
         condition = !is.na(UIC),
         true      = paste0(UIC_1, UIC_2, substr(uic, 5, 14)),
         false     = NA_character_
      )
   )

.log_info("Deduplicating based on grouped identifiers.")
nhsss$harp_tx$dedup_new <- list()
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
      .GlobalEnv$nhsss$harp_tx$dedup_old[[dedup_name]] <- df
   })
)

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "dedup_old"
if (length(nhsss$harp_tx[[data_name]]) > 0)
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$harp_tx[[data_name]],
      drive_path  = paste0(nhsss$harp_tx$gdrive$path$report, "Validation/"),
      surv_name   = "HARP Tx"
   )

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
