##  Cloud functions used in processing data ------------------------------------

# get list of encoded
get_ei <- function(reporting = NULL) {
   main_path  <- "~/DQT/Documentation/Encoding/"
   main_drive <- drive_ls(main_path)

   df <- data.frame()
   # download everything if no reporting period specified
   if (is.null(reporting)) {
	  for (reporting in main_drive$name) {
		 if (StrIsNumeric(reporting)) {
			list_ei <- drive_ls(paste0(main_path, reporting, "/"))
			list_ei %<>% filter(name != "FOR ENCODING")

			for (ei in seq_len(nrow(list_ei))) {
			   ei_df <- read_sheet(list_ei[ei, "id"] %>% as.character()) %>%
				  mutate_all(
					 ~as.character(.)
				  ) %>%
				  mutate(
					 encoder = list_ei[ei, "name"] %>% as.character()
				  )

			   df <- bind_rows(df, ei_df)
			}
		 }
	  }
   } else {
	  if (StrIsNumeric(reporting)) {
		 list_ei <- drive_ls(paste0(main_path, reporting, "/"))
		 list_ei %<>% filter(name != "FOR ENCODING")

		 for (ei in seq_len(nrow(list_ei))) {
			ei_df <- read_sheet(list_ei[ei, "id"] %>% as.character()) %>%
			   mutate_all(
				  ~as.character(.)
			   ) %>%
			   mutate(
				  encoder = list_ei[ei, "name"] %>% as.character()
			   )

			df <- bind_rows(df, ei_df)
		 }
	  }
   }

   return(df)
}

# get drive endpoint
gdrive_endpoint <- function(surv_name = NULL, report_period = NULL) {
   path         <- list()
   path$primary <- glue(r"(~/DQT/Data Factory/{surv_name}/)")
   path$report  <- glue(r"({path$primary}{report_period}/)")

   # create folders if not exists
   drive_folders <- list(
	  c(path$primary, report_period),
	  c(path$report, "Cleaning"),
	  c(path$report, "Validation")
   )
   invisible(
	  lapply(drive_folders, function(folder) {
		 parent <- folder[1] # parent dir
		 path   <- folder[2] # name of dir to be checked

		 # get sub-folders
		 dribble <- drive_ls(parent)

		 # create folder if not exists
		 if (nrow(dribble %>% filter(name == path)) == 0)
			drive_mkdir(paste0(parent, path))
	  })
   )

   return(path)
}

# load correction data
gdrive_correct <- function(drive_path = NULL, report_period = NULL) {
   corr <- list()

   # drive path
   primary_files <- drive_ls(paste0(drive_path, ".all/"))
   report_files  <- drive_ls(paste0(drive_path, report_period, "/Cleaning/"))

   # list of correction files
   .log_info("Getting list of correction datasets.")

   .log_info("Downloading sheets for all reporting periods.")
   if (nrow(primary_files) > 0) {
	  for (i in seq_len(nrow(primary_files))) {
		 corr_id     <- primary_files[i,]$id
		 corr_name   <- primary_files[i,]$name
		 corr_sheets <- sheet_names(corr_id)

		 if (length(corr_sheets) > 1) {
			corr[[corr_name]] <- list()
			for (sheet in corr_sheets)
			   corr[[corr_name]][[sheet]] <- read_sheet(corr_id, sheet)
		 } else {
			corr[[corr_name]] <- read_sheet(corr_id)
		 }
	  }
   }

   # create monthly folder if not exists
   .log_info("Downloading sheets for this reporting period.")

   if (nrow(report_files) > 0) {
	  for (i in seq_len(nrow(report_files))) {
		 corr_id           <- report_files[i,]$id
		 corr_name         <- report_files[i,]$name
		 corr[[corr_name]] <- read_sheet(corr_id)
	  }
   }

   return(corr)
}