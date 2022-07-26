##  Append w/ old Registry -----------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Appending with the previous registry.")
nhsss$harp_dx$official$new <- nhsss$harp_dx$official$old %>%
   mutate(consent_test = as.integer(consent_test)) %>%
   bind_rows(nhsss$harp_dx$converted$data) %>%
   arrange(idnum) %>%
   mutate(
	  drop_notyet     = 0,
	  drop_duplicates = 0,
   )

##  Tag data to be reported later on and duplicates for dropping ---------------

.log_info("Tagging duplicates and postponed reports.")
for (drop_var in c("drop_notyet", "drop_duplicates"))
   if (drop_var %in% names(nhsss$harp_dx$corr))
	  nhsss$harp_dx$official$new %<>%
		 left_join(
			y = nhsss$harp_dx$corr[[drop_var]] %>%
			   select(REC_ID) %>%
			mutate(drop = 1),
			by = "REC_ID"
		 ) %>%
		 mutate(
			drop_var := if_else(
			   condition = drop == 1,
			   true      = 1,
			   false     = !!drop_var,
			   missing   = !!drop_var
			)
		 ) %>%
		 select(-drop)

##  Subsets for documentation --------------------------------------------------

.log_info("Generating subsets based on tags.")
nhsss$harp_dx$official$dropped_notyet <- nhsss$harp_dx$official$new %>%
   filter(drop_notyet == 1)

nhsss$harp_dx$official$dropped_duplicates <- nhsss$harp_dx$official$new %>%
   filter(drop_duplicates == 1)

##  Drop using taggings --------------------------------------------------------

.log_info("Finalizing dataset.")
nhsss$harp_dx$official$new %<>%
   mutate(
	  labcode2    = if_else(
		 condition = is.na(labcode2),
		 true      = labcode,
		 false     = labcode2,
		 missing   = labcode2
	  ),
	  drop        = drop_duplicates + drop_notyet,
	  who_staging = as.integer(who_staging)
   ) %>%
   filter(drop == 0) %>%
   select(-drop, -drop_duplicates, -drop_notyet, -mot)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
