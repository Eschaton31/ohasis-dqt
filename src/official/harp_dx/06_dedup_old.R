##  Prepare dataset for registry deduplication ---------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Preparing dataset for registry deduplication.")
data_new <- nhsss$harp_dx$converted$data %>%
   mutate(
      byr = if_else(
         condition = !is.na(bdate),
         true      = year(bdate),
         false     = NA_integer_
      ) %>% as.numeric(),
      bmo = if_else(
         condition = !is.na(bdate),
         true      = year(bdate),
         false     = NA_integer_
      ) %>% as.numeric(),
      bdy = if_else(
         condition = !is.na(bdate),
         true      = year(bdate),
         false     = NA_integer_
      ) %>% as.numeric(),
   )

data_old <- nhsss$harp_dx$official$old %>%
   mutate(
      byr = if_else(
         condition = !is.na(bdate),
         true      = year(bdate),
         false     = NA_integer_
      ) %>% as.numeric(),
      bmo = if_else(
         condition = !is.na(bdate),
         true      = year(bdate),
         false     = NA_integer_
      ) %>% as.numeric(),
      bdy = if_else(
         condition = !is.na(bdate),
         true      = year(bdate),
         false     = NA_integer_
      ) %>% as.numeric(),
   )

##  Patient Record Linkage Algorithm -------------------------------------------

.log_info("Passing dataset through the record linkage algorithm.")
nhsss$harp_dx$dedup_old <- list()
reclink_df              <- fastLink(
   dfA              = data_new,
   dfB              = data_old,
   varnames         = c(
      "firstname",
      "middle",
      "last",
      "byr",
      "bmo",
      "bdy"
   ),
   stringdist.match = c(
      "firstname",
      "middle",
      "last"
   ),
   partial.match    = c(
      "firstname",
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
         MASTER_FIRST        = firstname,
         MASTER_MIDDLE       = middle,
         MASTER_LAST         = last,
         MASTER_SUFFIX       = name_suffix,
         MASTER_BIRTHDATE    = bdate,
         MASTER_CONFIRMATORY = labcode,
         MASTER_UIC          = uic,
         MASTER_INITIALS     = pxcode,
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
               USING_IDNUM        = idnum,
               USING_FIRST        = firstname,
               USING_MIDDLE       = middle,
               USING_LAST         = last,
               USING_SUFFIX       = name_suffix,
               USING_BIRTHDATE    = bdate,
               USING_CONFIRMATORY = labcode,
               USING_UIC          = uic,
               USING_PATIENT_CODE = pxcode,
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
   nhsss$harp_dx$dedup_old$reclink <- reclink_review
}


##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "dedup_old"
if (length(nhsss$harp_dx[[data_name]]) > 0)
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$harp_dx[[data_name]],
      drive_path  = paste0(nhsss$harp_dx$gdrive$path$report, "Validation/"),
      surv_name = "Dx"
   )

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
