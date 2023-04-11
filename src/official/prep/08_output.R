##  Output Stata Datasets ------------------------------------------------------

output_dta <- function(official) {
   log_info("Checking output directory.")
   output_version      <- format(Sys.time(), "%Y%m%d")
   output_name.reg     <- paste0(output_version, '_reg-prep_', ohasis$yr, '-', ohasis$mo)
   output_name.outcome <- paste0(output_version, '_onprep_', ohasis$yr, '-', ohasis$mo)
   output_dir          <- Sys.getenv("PREP")
   output_file.reg     <- file.path(output_dir, paste0(output_name.reg, ".dta"))
   output_file.outcome <- file.path(output_dir, paste0(output_name.outcome, ".dta"))
   check_dir(output_dir)

   # write main file
   log_info("Saving in Stata data format.")
   official$new_reg %>%
      format_stata() %>%
      write_dta(output_file.reg)
   official$new_outcome %>%
      format_stata() %>%
      write_dta(output_file.outcome)

   # write subsets if existing
   for (drop_var in c("dropped_notyet", "dropped_duplicates", "dropped_notart"))
      if (nrow(official[[drop_var]]) > 0) {
         output_name <- paste0(output_version, "_", drop_var, "_", ohasis$yr, '-', ohasis$mo)
         output_file <- file.path(output_dir, paste0(output_name, ".dta"))
         official[[drop_var]] %>%
            format_stata() %>%
            write_dta(output_file)
      }
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- .GlobalEnv$nhsss$prep
   output_dta(p$official)
}