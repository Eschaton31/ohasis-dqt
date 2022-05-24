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
      branch            = if_else(
         condition = use_db == 1,
         true      = curr_branch,
         false     = prev_branch,
      ),
      sathub            = if_else(
         condition = use_db == 1,
         true      = curr_sathub,
         false     = prev_sathub,
      ),
      transhub          = if_else(
         condition = use_db == 1,
         true      = curr_transhub,
         false     = prev_transhub,
      ),
      realhub          = if_else(
         condition = use_db == 1,
         true      = curr_realhub,
         false     = prev_transhub,
      ),
      realhub_branch          = if_else(
         condition = use_db == 1,
         true      = curr_realhub_branch,
         false     = prev_transhub_branch,
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
      REC_ID,
      CENTRAL_ID,
      art_id,
      idnum,
      sex,
      curr_age,
      hub,
      branch,
      sathub,
      transhub,
      realhub,
      realhub_branch,
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
   arrange(art_id) %>%
   mutate(central_id = CENTRAL_ID)

.log_info("Performing late validation cleanings.")
if ("new_outcome" %in% names(nhsss$harp_tx$corr))
   nhsss$harp_tx$official$new_outcome <- .cleaning_list(nhsss$harp_tx$official$new_outcome, nhsss$harp_tx$corr$new_outcome, "CENTRAL_ID", "character")

nhsss$harp_tx$official$new_outcome %<>%
   select(-central_id)
.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
