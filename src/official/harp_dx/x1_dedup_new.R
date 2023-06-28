##  Grouped identifiers for deduplication --------------------------------------

dedup_group_ids <- function(data) {
   dedup_new <- list()
   group_pii <- list(
      "UIC.Base"           = "uic",
      "UIC.Fixed"          = "UIC_SORT",
      "FirstUIC.Base"      = c("FIRST_SIEVE", "UIC"),
      "FirstUIC.Fixed"     = c("FIRST_NY", "UIC_SORT"),
      "FirstUIC.Partial"   = c("FIRST_A", "UIC_SORT"),
      "Name.Base"          = c("FIRST_SIEVE", "LAST_SIEVE", "bdate"),
      "Name.Fixed"         = c("FIRST_NY", "LAST_NY", "bdate"),
      "Name.Partial"       = c("FIRST_A", "LAST_A", "bdate"),
      "YM.BD-Name.Base"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Base"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Base"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_MO", "BIRTH_DY")
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
      data <- read_rds(file.path(wd, "converted.RDS"))
      data <- dedup_prep(
         data %>%
            mutate(philsys = NA_character_),
         firstname,
         middle,
         last,
         name_suffix,
         uic,
         bdate,
         labcode,
         pxcode,
         philhealth,
         philsys
      )

      check <- dedup_group_ids(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_dx, "dedup_new", ohasis$ym))
}
