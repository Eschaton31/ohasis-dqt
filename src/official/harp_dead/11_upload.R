##  Upload archive to nextcloud ------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# check if current month dir exists
.log_info("Checking directories exist in cloud.")
dir_archive <- "/DQT/archive"
dir_report  <- file.path(dir_archive, ohasis$ym)
dir_run     <- file.path(dir_report, ohasis$output_title)
if (!(glue("{ohasis$ym}/") %in% list_files(dir_archive)))
   create_folder(dir_report)

# check if current run dir exists
if (!(glue("{ohasis$output_title}/") %in% list_files(dir_report)))
   create_folder(dir_run)

# upload files
.log_info("Uploading archives.")
zip_dir <- file.path("archive", ohasis$ym, ohasis$output_title)
for (zip_file in list.files(pattern = "\\.", zip_dir, full.names = TRUE)) {
   upload_file(zip_file, dir_run)
}

##  Upload archive to Dropbox --------------------------------------------------

.log_info("Uploading dataset to Dropbox.")
data_dta <- nhsss$harp_dead$official$new_file
data_zip <- stri_replace_last_fixed(data_dta, ".dta", ".zip")
zip::zip(zipfile = data_zip, files = data_dta)
drop_upload(
   data_zip,
   "DQT/Data Factory/HARP Dead"
)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))