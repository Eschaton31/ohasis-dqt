##  Output Stata Datasets ------------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Checking output directory.")
output_version      <- format(Sys.time(), "%Y%m%d")
output_name.reg     <- paste0(output_version, '_reg-art_', ohasis$yr, '-', ohasis$mo)
output_name.outcome <- paste0(output_version, '_onart_', ohasis$yr, '-', ohasis$mo)
output_dir          <- file.path("archive", ohasis$ym, ohasis$output_title, "harp_tx")

nhsss$harp_tx$official$file_reg     <- file.path(output_dir, paste0(output_name.reg, ".dta"))
nhsss$harp_tx$official$file_outcome <- file.path(output_dir, paste0(output_name.outcome, ".dta"))
check_dir(output_dir)

# write main file
.log_info("Saving in Stata data format.")
write_dta(
   data = format_stata(nhsss$harp_tx$official$new_reg),
   path = nhsss$harp_tx$official$file_reg
)
write_dta(
   data = format_stata(nhsss$harp_tx$official$new_outcome),
   path = nhsss$harp_tx$official$file_outcome
)

.log_info("Uploading datasets to the NHSSS Cloud.")
kwb.nextcloud::upload_file(
   nhsss$harp_tx$official$file_reg,
   "/DQT/library/harp_tx"
)
kwb.nextcloud::upload_file(
   nhsss$harp_tx$official$file_outcome,
   "/DQT/library/harp_tx"
)

# write subsets if existing
for (drop_var in c("dropped_notyet", "dropped_duplicates", "dropped_notart"))
   if (nrow(nhsss$harp_tx$official[[drop_var]]) > 0) {
      output_name.reg <- paste0(output_version, "_", drop_var, "_", ohasis$yr, '-', ohasis$mo)
      write_dta(
         data = format_stata(nhsss$harp_tx$official[[drop_var]]),
         path = file.path(output_dir, paste0(output_name.reg, ".dta"))
      )
   }

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
