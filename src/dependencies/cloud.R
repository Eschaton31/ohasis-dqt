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
            list_ei <- drive_ls(paste0(main_path, reporting, "/EI/"))
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
         list_ei <- drive_ls(paste0(main_path, reporting, "/EI ./"))
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

# get list of encoded forms
get_encoded <- function(reporting = NULL, module = NULL) {
   main_path  <- as_id("18hh6GZzjnNBidMg9sxOworj2IhbwTUak")
   main_drive <- drive_ls(main_path)

   df_list <- list()
   # download everything if no reporting period specified
   if (!is.null(reporting)) {
      if (StrIsNumeric(reporting)) {
         surveillances <- drive_ls(as_id(main_drive[main_drive$name == reporting,]$id))

         list_ei <- drive_ls(as_id(surveillances[surveillances$name == module,]$id))

         for (ei in seq_len(nrow(list_ei))) {
            ss_id   <- as_id(list_ei[ei,]$id)
            ss_name <- list_ei[ei,]$name
            sheets  <- sheet_names(ss_id)
            sheets  <- sheets[!stri_detect_fixed(sheets, "LEGENDS")]
            forms   <- sheets[stri_detect_fixed(sheets, "FORM") |
                                 stri_detect_fixed(sheets, "DISPENSE") |
                                 stri_detect_fixed(sheets, "DISCONTINUE") |
                                 stri_detect_fixed(sheets, "documentation")]
            sheets  <- c(setdiff(sheets, names(df_list)), forms)

            for (sheet in sheets) {
               ei_df <- read_sheet(ss_id, sheet = sheet, col_types = "c") %>%
                  mutate_all(
                     ~as.character(.)
                  ) %>%
                  mutate(
                     encoder   = ss_name,
                     ss        = ss_id,
                     sheet_row = row_number()
                  )

               if (is.null(df_list[[sheet]]))
                  df_list[[sheet]] <- ei_df
               else
                  df_list[[sheet]] <- bind_rows(df_list[[sheet]], ei_df)
            }
         }
      }
   }

   return(df_list)
}

# get drive endpoint
gdrive_endpoint <- function(surv_name = NULL, report_period = NULL) {
   path         <- list()
   path$primary <- glue(r"(~/DQT/Data Factory/{surv_name}/)")
   path$report  <- glue(r"({path$primary}{report_period}/)")

   path$primary <- (drive_ls("~/DQT", "Data Factory") %>%
      drive_ls(surv_name))$id
   path$report  <- (drive_ls(path$primary, report_period))$id

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
         dribble <- drive_ls(as_id(parent))

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
   primary_files <- drive_ls(drive_ls(as_id(drive_path$primary), ".all")$id)
   report_files  <- drive_ls( drive_ls(as_id(drive_path$report), "Cleaning")$id)

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

# unified data factory
gdrive_correct2 <- function(parent = NULL, report_period = NULL, surv_name = NULL, speed = TRUE) {
   # re-initialize
   corr <- list()

   # drive path
   data_cleaning <- as_id("1rk5kVc7MxtWfE-Ju_kJSMkviSLvr0h6C")
   period_all    <- drive_ls(as_id("109AGqRwhu_zdL2CrDTy3eCSQky2Mf8fM"))

   # get period data
   periods   <- drive_ls(data_cleaning)
   clean_all <- as_id(period_all[period_all$name == surv_name,]$id)
   clean_all <- (if (length(clean_all) > 0) drive_ls(clean_all) else data.frame())
   clean_now <- as_id(periods[periods$name == report_period,]$id)
   clean_now <- (if (length(clean_now) > 0) drive_ls(clean_now) else data.frame())

   # list of correction files
   .log_info("Downloading corrections across all periods.")
   if (nrow(clean_all) > 0) {
      for (i in seq_len(nrow(clean_all))) {
         corr_id     <- as_id(clean_all[i,]$id)
         corr_name   <- clean_all[i,]$name
         corr_sheets <- sheet_names(corr_id)

         if (speed) {
            if (length(corr_sheets) > 1) {
               corr[[corr_name]] <- list()
               for (sheet in corr_sheets)
                  corr[[corr_name]][[sheet]] <- range_speedread(corr_id, sheet, show_col_types = FALSE)
            } else {
               corr[[corr_name]] <- range_speedread(corr_id, show_col_types = FALSE)
            }
         } else {
            if (length(corr_sheets) > 1) {
               corr[[corr_name]] <- list()
               for (sheet in corr_sheets)
                  corr[[corr_name]][[sheet]] <- read_sheet(corr_id, sheet)
            } else {
               corr[[corr_name]] <- read_sheet(corr_id)
            }
         }
      }
   }

   # create monthly folder if not exists
   .log_info("Downloading corrections for this period.")
   if (nrow(clean_now) > 0) {
      for (i in seq_len(nrow(clean_now))) {
         corr_id     <- as_id(clean_now[i,]$id)
         corr_name   <- clean_now[i,]$name
         corr_sheets <- sheet_names(corr_id)
         sheet       <- corr_sheets[corr_sheets == surv_name]

         if (length(sheet) != 0) {
            if (speed)
               corr[[corr_name]] <- range_speedread(corr_id, sheet, show_col_types = FALSE)
            else
               corr[[corr_name]] <- read_sheet(corr_id, sheet)
         }
      }
   }

   parent[[surv_name]]$corr <- corr
   return(parent)
}

# validatioins
gdrive_validation <- function(data_env = NULL,
                              process_step = NULL,
                              report_period = NULL,
                              channels = NULL,
                              list_name = "check") {
   # re-intiialize
   data_validation <- as_id("1JOCJPjIsdrys_uaFPI3AIElfkMHzrayh")
   surv_name       <- strsplit(deparse(substitute(data_env)), "\\$")[[1]]
   surv_name       <- surv_name[length(surv_name)]
   empty_sheets    <- ""
   corr_status     <- "old"

   if (is.null(list_name))
      corr_list <- data_env[[process_step]]
   else
      corr_list <- data_env[[process_step]][[list_name]]

   if (length(corr_list) > 0) {
      # get period data
      periods   <- drive_ls(data_validation)
      valid_now <- as_id(periods[periods$name == report_period,]$id)
      if (length(valid_now) == 0) {
         valid_now <- as_id(drive_mkdir(report_period, data_validation)$id)
      }

      # get surveillance endpoints
      surveillances <- drive_ls(valid_now)
      gd_surv       <- as_id(surveillances[surveillances$name == surv_name,]$id)
      if (length(gd_surv) == 0) {
         gd_surv <- as_id(drive_mkdir(surv_name, valid_now)$id)
      }

      # get steps & write data
      steps   <- drive_ls(gd_surv)
      gd_step <- as_id(steps[steps$name == process_step,]$id)
      if (length(gd_step) == 0) {
         # create as new if not existing
         corr_status <- "new"
         drive_rm(paste0("~/", process_step))
         gd_step <- as_id(gs4_create(process_step, sheets = corr_list))
         drive_mv(gd_step, gd_surv, overwrite = TRUE)
      }
      gd_archive <- as_id(steps[steps$name == "Archive",]$id)
      if (length(gd_archive) == 0) {
         gd_archive <- as_id(drive_mkdir("Archive", gd_surv)$id)
      }

      # archive current
      drive_cp(
         gd_step,
         gd_archive,
         paste0(format(Sys.time(), "%Y.%m.%d"), "_", process_step),
         overwrite = TRUE
      )

      .log_info("Uploading to GSheets..")

      # list of validations
      issues_list <- names(corr_list)
      sheets_list <- sheet_names(gd_step)

      # acquire sheet_id
      slack_by   <- (slackr_users() %>% filter(name == Sys.getenv("SLACK_PERSONAL")))$id
      drive_link <- paste0("https://docs.google.com/spreadsheets/d/", gd_step, "/|GSheets Link: ", process_step)
      slack_msg  <- glue(">*{surv_name}*\n>Conso validation sheets for `{process_step}` have been updated by <@{slack_by}>.\n><{drive_link}>")
      for (issue in issues_list) {
         # add issue
         if (nrow(corr_list[[issue]]) > 0) {
            if (corr_status == "old")
               sheet_write(corr_list[[issue]], gd_step, issue)
            else
               range_autofit(gd_step, issue)
         }
      }

      # delete list of empty dataframes from sheet
      .log_info("Deleting empty sheets.")
      for (issue in issues_list)
         if (nrow(corr_list[[issue]]) == 0 & issue %in% sheets_list)
            empty_sheets <- append(empty_sheets, issue)
      for (issue in sheets_list)
         if (!(issue %in% issues_list))
            empty_sheets <- append(empty_sheets, issue)

      # delete if existing sheet no longer has values in new run
      if (length(empty_sheets[-1]) > 0)
         sheet_delete(gd_step, empty_sheets[-1])

      # log in slack
      if (is.null(channels)) {
         slackr_msg(slack_msg, mrkdwn = "true")
      } else {
         for (channel in channels)
            slackr_msg(slack_msg, mrkdwn = "true", channel = channel)
      }
      return(gd_step)
   }
}

drive_upload_folder <- function(folder, drive_path) {
   # Only call fs::dir_info once in order to avoid weirdness if the contents of the folder is changing
   contents       <- fs::dir_info(folder, type = c("file", "dir"))
   dirs_to_upload <- contents %>%
      dplyr::filter(type == "directory") %>%
      pull(path)

   # Directly upload the files
   uploaded_files <- contents %>%
      dplyr::filter(type == "file") %>%
      pull(path) %>%
      purrr::map_dfr(googledrive::drive_upload, path = drive_path, overwrite = FALSE)

   # Create the next level down of directories
   tryCatch({
      dirs_to_upload %>%
         fs::path_rel(folder) %>%
         purrr::map(., googledrive::drive_mkdir, path = drive_path, overwrite = FALSE) %>%
         # Recursively call this function
         purrr::map2_dfr(dirs_to_upload, ., drive_upload_folder) %>%
         # return a dribble of what's been uploaded
         dplyr::bind_rows(uploaded_files) %>%
         invisible()
   },
      error = function(e) {
         dirs_to_upload %>%
            fs::path_rel(folder) %>%
            # purrr::map(., googledrive::drive_update, file = drive_path) %>%
            # Recursively call this function
            purrr::map2_dfr(dirs_to_upload, ., drive_upload_folder) %>%
            # return a dribble of what's been uploaded
            dplyr::bind_rows(uploaded_files) %>%
            invisible()
      }
   )
}