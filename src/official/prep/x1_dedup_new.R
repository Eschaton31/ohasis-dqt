##  Grouped identifiers for deduplication --------------------------------------

dedup_group_ids <- function(data) {
   dedup_new <- list()
   group_pii <- list(
      "UIC.Base"           = "uic",
      "UIC.Fixed"          = "UIC_SORT",
      "PhilHealth.Fixed"   = "PHIC",
      "PhilSys.Fixed"      = "PHILSYS",
      "ConfirmCode.Base"   = "CONFIRMATORY_CODE",
      "ConfirmCode.Fixed"  = "CONFIRM_SIEVE",
      "PxCode.Base"        = "PATIENT_CODE",
      "PxCode.Fixed"       = "PXCODE_SIEVE",
      "PxConfirm.Base"     = c("PATIENT_CODE", "CONFIRMATORY_CODE"),
      "PxConfirm.Fixed"    = c("PXCODE_SIEVE", "CONFIRM_SIEVE"),
      "ConfirmUIC.Base"    = c("CONFIRMATORY_CODE", "UIC"),
      "ConfirmUIC.Fixed"   = c("CONFIRM_SIEVE", "UIC"),
      "PxUIC.Base"         = c("PATIENT_CODE", "UIC"),
      "PxUIC.Fixed"        = c("PXCODE_SIEVE", "UIC_SORT"),
      "FirstUIC.Base"      = c("FIRST_SIEVE", "UIC"),
      "FirstUIC.Fixed"     = c("FIRST_NY", "UIC_SORT"),
      "FirstUIC.Partial"   = c("FIRST_A", "UIC_SORT"),
      "FirstUIC.Sort"      = c("NAMESORT_FIRST", "UIC_SORT"),
      "PxBD.Base"          = c("PATIENT_CODE", "birthdate"),
      "PxBD.Fixed"         = c("PXCODE_SIEVE", "birthdate"),
      "Name.Base"          = c("FIRST_SIEVE", "LAST_SIEVE", "birthdate"),
      "Name.Fixed"         = c("FIRST_NY", "LAST_NY", "birthdate"),
      "Name.Partial"       = c("FIRST_A", "LAST_A", "birthdate"),
      "Name.Sort"          = c("NAMESORT_FIRST", "NAMESORT_LAST", "birthdate"),
      "YM.BD-Name.Base"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Base"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Base"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Sort"    = c("NAMESORT_FIRST", "NAMESORT_LAST", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Sort"    = c("NAMESORT_FIRST", "NAMESORT_LAST", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Sort"    = c("NAMESORT_FIRST", "NAMESORT_LAST", "BIRTH_MO", "BIRTH_DY")
   )
   for (i in seq_len(length(group_pii))) {
      dedup_name <- names(group_pii)[[i]]
      dedup_id   <- group_pii[[i]]

      # tag duplicates based on grouping
      df <- data %>%
         filter(if_all(any_of(dedup_id), ~!is.na(.))) %>%
         get_dupes(all_of(dedup_id)) %>%
         filter(dupe_count > 0) %>%
         group_by(across(all_of(dedup_id))) %>%
         mutate(
            # generate a group id to identify groups of duplicates
            grp_id = str_c(collapse = ",", sort(prep_id)),
         ) %>%
         ungroup() %>%
         mutate(DUP_IDS = paste(collapse = ', ', dedup_id))

      # if any found, include in list for review
      dedup_new[[dedup_name]] <- df
   }

   all_dedup <- combine_validations(data, dedup_new, c("grp_id", "CENTRAL_ID"))
   dedup_new <- list(group_dedup = all_dedup)

   return(dedup_new)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   data <- p$official$new_reg %>%
      filter(year == p$params$yr, month == p$params$mo) %>%
      mutate(confirmatory_code = NA_character_) %>%
      dedup_prep(
         first,
         middle,
         last,
         suffix,
         uic,
         birthdate,
         confirmatory_code,
         px_code,
         philhealth_no,
         philsys_id
      )

   step$check <- dedup_group_ids(data)
   flow_validation(p, "dedup_new", p$params$ym, upload = vars$upload)
   log_success("Done.")
}
