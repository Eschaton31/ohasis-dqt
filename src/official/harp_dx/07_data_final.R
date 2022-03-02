##  Append w/ old Registry -----------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

log_info("Appending with the previous registry.")
nhsss$harp_dx$official$new <- nhsss$harp_dx$official$old %>%
   mutate(consent_test = as.integer(consent_test)) %>%
   bind_rows(nhsss$harp_dx$converted$data) %>%
   arrange(idnum) %>%
   mutate(
      drop_notyet     = 0,
      drop_duplicates = 0,
   )

##  Tag data to be reported later on and duplicates for dropping ---------------

log_info("Tagging duplicates and postponed reports.")
for (drop_var in c("drop_notyet", "drop_duplicates"))
   if (drop_var %in% names(nhsss$harp_dx$corr))
      for (i in seq_len(nrow(nhsss$harp_dx$corr[[drop_var]]))) {
         record_id <- nhsss$harp_dx$corr[[drop_var]][i, "REC_ID"]

         # tag based on record id
         nhsss$harp_dx$official$new %<>%
            mutate(
               !!drop_var := if_else(
                  condition = REC_ID == record_id,
                  true      = 1,
                  false     = !!drop_var
               )
            )
      }

##  Subsets for documentation --------------------------------------------------

log_info("Generating subsets based on tags.")
nhsss$harp_dx$official$dropped_notyet <- nhsss$harp_dx$official$new %>%
   filter(drop_notyet == 1)

nhsss$harp_dx$official$dropped_duplicates <- nhsss$harp_dx$official$new %>%
   filter(drop_duplicates == 1)

##  Drop using taggings --------------------------------------------------------

log_info("Finalizing dataset.")
nhsss$harp_dx$official$new %<>%
   mutate(
      drop        = drop_duplicates + drop_notyet,
      who_staging = as.integer(who_staging)
   ) %>%
   filter(drop == 0) %>%
   select(-drop, -drop_duplicates, -drop_notyet, -mot)

log_info("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
