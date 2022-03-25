##  Append w/ old Registry -----------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Appending with the previous registry.")
nhsss$harp_tx$official$new_outcome <- nhsss$harp_tx$outcome.converted$data %>%
   arrange(art_id) %>%
   mutate(
      drop_notyet     = 0,
      drop_duplicates = 0,
   ) %>%
   select(
      art_id,
      idnum,
      sex,
      curr_age,
      hub = curr_hub,
      tx_reg,
      tx_prov,
      tx_munc,
      pubpriv,
      class             = curr_class,
      outcome           = curr_outcome,
      latest_ffupdate   = curr_ffup,
      latest_nextpickup = curr_pickup,
      latest_regimen    = curr_regimen,
      art_reg           = curr_artreg,
      line              = curr_line,
   )

##  Tag data to be reported later on and duplicates for dropping ---------------

.log_info("Tagging duplicates and postponed reports.")
for (drop_var in c("drop_notyet", "drop_duplicates"))
   if (drop_var %in% names(nhsss$harp_tx$corr))
      for (i in seq_len(nrow(nhsss$harp_tx$corr[[drop_var]]))) {
         record_id <- nhsss$harp_tx$corr[[drop_var]][i, "REC_ID"]

         # tag based on record id
         nhsss$harp_tx$official$new %<>%
            mutate(
               !!drop_var := if_else(
                  condition = REC_ID == record_id,
                  true      = 1,
                  false     = !!drop_var
               )
            )
      }

##  Subsets for documentation --------------------------------------------------

.log_info("Generating subsets based on tags.")
nhsss$harp_tx$official$dropped_notyet <- nhsss$harp_tx$official$new %>%
   filter(drop_notyet == 1)

nhsss$harp_tx$official$dropped_duplicates <- nhsss$harp_tx$official$new %>%
   filter(drop_duplicates == 1)

##  Drop using taggings --------------------------------------------------------

.log_info("Finalizing dataset.")
nhsss$harp_tx$official$new %<>%
   mutate(
      drop        = drop_duplicates + drop_notyet,
      who_staging = as.integer(who_staging)
   ) %>%
   filter(drop == 0) %>%
   select(-drop, -drop_duplicates, -drop_notyet, -mot)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
