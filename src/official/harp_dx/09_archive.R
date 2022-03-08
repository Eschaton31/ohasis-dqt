##  Archiving Output -----------------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

log_info("Checking archive directory.")
zip_dir <- file.path("archive", ohasis$ym, ohasis$output_title)
check_dir(zip_dir)

log_info("Backing up scripts.")
dir_copy(file.path("src"), file.path(zip_dir, "src"), overwrite = TRUE)

log_info("Placing files inside archive.")
for (files_dir in list.files(zip_dir, full.names = TRUE)) {
   files_dir  <- tools::file_path_as_absolute(files_dir)
   files_path <- strsplit(files_dir, "/")
   zip_file   <- paste0(files_path[[1]][length(files_path[[1]])], '.zip')

   for_archive <- dir(files_dir, full.names = TRUE)
   if (length(for_archive) > 0) {
      zip::zip(
         zipfile = file.path(tools::file_path_as_absolute(zip_dir), zip_file),
         files   = for_archive,
         mode    = 'cherry-pick',
      )
      # delete original files to preserve space
      # unlink(files_dir, recursive = TRUE, force = TRUE)
   }
}

log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
