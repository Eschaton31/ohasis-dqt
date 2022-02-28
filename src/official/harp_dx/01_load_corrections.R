##  Get the files used for correcting data -------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# drive path
drive_dx_path <- "~/DQT/Data Factory/HARP Dx/"
drive_dx_data <- drive_ls(drive_dx_path)

# list of correction files
log_info("Getting list of correction datasets.")

log_info("Downloading sheets for all reporting periods.")
corr_list <- drive_ls((drive_dx_data %>% filter(name == ".all"))$id)
for (i in seq_len(nrow(corr_list))) {
   corr_id                         <- corr_list[i,]$id
   corr_name                       <- corr_list[i,]$name
   nhsss$harp_dx$corr[[corr_name]] <- read_sheet(corr_id)
}

# create monthly folder if not exists
log_info("Downloading sheets for this reporting period.")
drive_dx_curr_path <- paste0(drive_dx_path, ohasis$yr, ".", ohasis$mo, "/")
drive_dx_curr_data <- drive_dx_data %>% filter(name == paste0(ohasis$yr, ".", ohasis$mo))
if (nrow(drive_dx_curr_data) == 0)
   drive_mkdir(stri_replace_last_fixed(drive_dx_curr_path, "/", ""))

# create cleaning folder if not exists
drive_dx_data      <- drive_ls(drive_dx_curr_path)
drive_dx_curr_path <- paste0(drive_dx_curr_path, "Cleaning/")
drive_dx_curr_data <- drive_dx_data %>% filter(name == "Cleaning")
if (nrow(drive_dx_curr_data) == 0)
   drive_mkdir(stri_replace_last_fixed(drive_dx_curr_path, "/", ""))

corr_list <- drive_ls(drive_dx_curr_path)
if (nrow(corr_list) > 0) {
   for (i in seq_len(nrow(corr_list))) {
      corr_id                         <- corr_list[i,]$id
      corr_name                       <- corr_list[i,]$name
      nhsss$harp_dx$corr[[corr_name]] <- read_sheet(corr_id)
   }
}

# rm(corr_list, i, corr_id, corr_name)
log_info("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
