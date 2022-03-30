##  Append w/ old Registry -----------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Finalizing outcomes.")
nhsss$harp_tx$official$new_outcome <- nhsss$harp_tx$outcome.converted$data %>%
   arrange(art_id) %>%
   mutate(
      hub               = if_else(
         condition = use_db == 1,
         true      = curr_hub,
         false     = prev_hub,
      ),
      latest_ffupdate   = if_else(
         condition = use_db == 1,
         true      = curr_ffup,
         false     = prev_ffup,
      ),
      latest_nextpickup = if_else(
         condition = use_db == 1,
         true      = curr_pickup,
         false     = prev_pickup,
      ),
      latest_regimen    = if_else(
         condition = use_db == 1,
         true      = curr_regimen,
         false     = prev_regimen,
      ),
      art_reg           = if_else(
         condition = use_db == 1,
         true      = curr_artreg,
         false     = prev_artreg,
      ),
      line              = if_else(
         condition = use_db == 1,
         true      = curr_line,
         false     = prev_line,
      ),
      curr_outcome      = case_when(
         prev_outcome == "dead" ~ "dead",
         TRUE ~ curr_outcome
      ),
      newonart          = if_else(
         condition = year(artstart_date) == as.numeric(ohasis$yr) &
            month(artstart_date) == as.numeric(ohasis$mo),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      onart             = if_else(
         condition = curr_outcome == "alive on arv",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      curr_class        = if_else(
         condition = is.na(curr_class),
         true      = prev_class,
         false     = curr_class
      ),
   ) %>%
   select(
      CENTRAL_ID,
      art_id,
      idnum,
      sex,
      curr_age,
      hub,
      sathub,
      tx_reg,
      tx_prov,
      tx_munc,
      pubpriv,
      artstart_date,
      class   = curr_class,
      outcome = curr_outcome,
      latest_ffupdate,
      latest_nextpickup,
      latest_regimen,
      art_reg,
      line,
      newonart,
      onart
   ) %>%
   distinct_all() %>%
   arrange(art_id)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
