##  Output Stata Datasets ------------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Checking output directory.")
output_version <- format(Sys.time(), "%Y%m%d")
output_name    <- paste0(output_version, '_mort_', ohasis$yr, '-', ohasis$mo)
output_dir     <- file.path("archive", ohasis$ym, ohasis$output_title, "harp_dead")

nhsss$harp_dead$official$new_file <- file.path(output_dir, paste0(output_name, ".dta"))
check_dir(output_dir)

# write main file
.log_info("Saving in Stata data format.")
write_dta(
   data = nhsss$harp_dead$official$new,
   path = nhsss$harp_dead$official$new_file
)

# write subsets if existing
for (drop_var in c("dropped_notyet", "dropped_duplicates"))
   if (nrow(nhsss$harp_dead$official[[drop_var]]) > 0) {
      output_name <- paste0(output_version, "_", drop_var, "_", ohasis$yr, '-', ohasis$mo)
      write_dta(
         data = nhsss$harp_dead$official[[drop_var]],
         path = file.path(output_dir, paste0(output_name, ".dta"))
      )
   }

##  Stata Labels ---------------------------------------------------------------

.log_info("Creating `lab def` do-files.")
# key->value pairs
label_df   <- nhsss$harp_dead$corr$stata_labels$lab_def
label_list <- unique(label_df$label_name)
for (var in label_list) {
   # initialize empty stata commands
   label_name <- ""

   # get value->label pairing
   df <- label_df %>% filter(label_name == var)
   for (i in seq_len(nrow(df))) {
      value <- df[i, "value"] %>% as.integer()
      label <- df[i, "label"] %>% as.character()

      label_name <- paste0(label_name, '
lab def ', var, ' ', value, ' "', label, '", add')
   }

   # stata create label file
   label_name <- paste0(label_name, '
lab save ', var, ' using "', file.path(output_dir, paste0('Labels-', var, '.do')), '", replace')

   # run command
   stata(label_name)
}

.log_info("Attaching labels to variables.")
# variable->label pairs
var_df <- nhsss$harp_dead$corr$stata_labels$lab_val
for (file in list.files(output_dir, "*.dta", full.names = TRUE)) {
   # initialize empty stata commands
   stataCMD <- ""

   # use file
   stataCMD <- glue(r"(u "{file}", clear)")
   #
   # # run label do-files
   # for (do_file in list.files(output_dir, "*.do", full.names = TRUE))
   #    stataCMD <- glue(paste0(stataCMD, "\n", r"(do "{do_file}")"))
   #
   # # label values
   # for (var in seq_len(nrow(var_df))) {
   #    variable   <- var_df[var, "variable"] %>% as.character()
   #    label_name <- var_df[var, "label_name"] %>% as.character()
   #
   #    stataCMD <- glue(paste0(stataCMD, "\n", r"(lab val {variable} {label_name})"))
   # }

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
