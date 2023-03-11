##  Grouped identifiers for deduplication --------------------------------------

dedup_group_ids <- function(data) {
   dedup_new <- list()
   group_pii <- list(
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
            group_id = cur_group_id(),
         ) %>%
         ungroup() %>%
         mutate(DUP_IDS = paste(collapse = ', ', dedup_id))

      # if any found, include in list for review
      dedup_new[[dedup_name]] <- df
   }

   return(dedup_new)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      data <- read_rds(file.path(wd, "reg.converted.RDS"))
      data <- dedup_prep(
         data,
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

      check <- dedup_group_ids(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_tx, "dedup_new", ohasis$ym))
}
