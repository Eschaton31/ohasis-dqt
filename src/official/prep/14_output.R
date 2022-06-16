##  Output Stata Datasets ------------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Checking output directory.")
output_version      <- format(Sys.time(), "%Y%m%d")
output_name.reg     <- paste0(output_version, '_reg-prep_', ohasis$yr, '-', ohasis$mo)
output_name.outcome <- paste0(output_version, '_onprep_', ohasis$yr, '-', ohasis$mo)
output_dir          <- file.path("archive", ohasis$ym, ohasis$output_title, "prep")

nhsss$prep$official$file_reg     <- file.path(output_dir, paste0(output_name.reg, ".dta"))
nhsss$prep$official$file_outcome <- file.path(output_dir, paste0(output_name.outcome, ".dta"))
check_dir(output_dir)

# write main file
.log_info("Saving in Stata data format.")
write_dta(
   data = nhsss$prep$official$new_reg,
   path = nhsss$prep$official$file_reg
)
write_dta(
   data = nhsss$prep$official$new_outcome,
   path = nhsss$prep$official$file_outcome
)

# write subsets if existing
# for (drop_var in c("dropped_notyet", "dropped_duplicates", "dropped_notprep"))
#    if (nrow(nhsss$prep$official[[drop_var]]) > 0) {
#       output_name.reg <- paste0(output_version, "_", drop_var, "_", ohasis$yr, '-', ohasis$mo)
#       write_dta(
#          data = nhsss$prep$official[[drop_var]],
#          path = file.path(output_dir, paste0(output_name.reg, ".dta"))
#       )
#    }

.log_info("Finalizing formats.")
# variable->label pairs
var_df <- nhsss$prep$corr$stata_labels$lab_val
for (file in list.files(output_dir, "*.dta", full.names = TRUE)) {
   # initialize empty stata commands
   stataCMD <- ""

   # use file
   stataCMD <- glue(r"(u "{file}", clear)")

   # format and save file
   stataCMD <- glue(paste0(stataCMD, "\n", r"(
ds, has(type string)
foreach var in `r(varlist)' {{
   loc type : type `var'
   loc len = substr("`type'", 4, 1000)

   cap form `var' %-`len's
}}

form *date* %tdCCYY-NN-DD
compress
sa "{file}", replace
   )"))

   # run command
   stata(stataCMD)
}

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
