##  Prepare dataset for initial deduplication ----------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

log_info("Preparing dataset for initial deduplication.")
dedup_new <- nhsss$harp_dx$converted$data %>%
   mutate(
      # standardize PII
      LAST              = stri_trans_toupper(last),
      MIDDLE            = stri_trans_toupper(middle),
      FIRST             = stri_trans_toupper(name),
      SUFFIX            = stri_trans_toupper(name_suffix),
      UIC               = stri_trans_toupper(uic),
      CONFIRMATORY_CODE = stri_trans_toupper(labcode),

      # get components of birthdate
      BIRTH_YR          = year(bdate),
      BIRTH_MO          = month(bdate),
      BIRTH_DY          = day(bdate),

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
      FIRST_S           = if_else(!is.na(FIRST), str_replace_all(FIRST, "[^[:alnum:]]", ""), NA_character_),
      MIDDLE_S          = if_else(!is.na(MIDDLE), str_replace_all(MIDDLE, "[^[:alnum:]]", ""), NA_character_),
      LAST_S            = if_else(!is.na(LAST), str_replace_all(LAST, "[^[:alnum:]]", ""), NA_character_),
      PHIC              = if_else(!is.na(philhealth), str_replace_all(philhealth, "[^[:alnum:]]", ""), NA_character_),

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
      id_cols      = c('CENTRAL_ID', 'UIC'),
      names_from   = 'UIC',
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

##  Grouped identifiers for deduplication --------------------------------------

log_info("Deduplicating based on grouped identifiers.")
nhsss$harp_dx$dedup_new <- list()
group_pii               <- list(
   "UIC.Base"            = "uic",
   "UIC.Fixed"           = "UIC_SORT",
   "Name.Base"           = c("FIRST", "LAST", "bdate"),
   "Name.Fixed"          = c("FIRST_NY", "LAST_NY", "bdate"),
   "Name.Partial"        = c("FIRST_A", "LAST_A", "bdate"),
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
         .GlobalEnv$nhsss$harp_dx$dedup_new[[dedup_name]] <- df
   })
)

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "dedup_new"
if (length(nhsss$harp_dx[[data_name]]) > 0)
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$harp_dx[[data_name]],
      drive_path  = paste0(nhsss$harp_dx$gdrive$path$report, "Validation/")
   )

log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
