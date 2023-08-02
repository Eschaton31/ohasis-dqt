# import functions
source("db_helper.R")
source("stata.R")
source("process_hts.R")
source("process_prep.R")
source("process_vl.R")
source("gender_identity.R")
source("check.R")
source("oh_data.R")
source("facility_id.R")
source("address.R")
source("pii.R")

# pipeline function -> call files based on structure
pipeline <- function(system, parent, step = NULL, group = "official") {
   # get system name as character
   p_name <- stri_replace_all_fixed(deparse(substitute(parent)), '"', "")
   s_name <- stri_replace_all_fixed(deparse(substitute(system)), '"', "")

   # define datasets
   if (!exists(p_name, envir = .GlobalEnv))
      .GlobalEnv[[p_name]] <- list()

   if (!(s_name %in% names(.GlobalEnv[[p_name]])))
      .GlobalEnv[[p_name]][[s_name]] <- new.env()

   # download current state of environment
   envir    <- .GlobalEnv[[p_name]][[s_name]]
   # define working directory
   envir$wd <- file.path(getwd(), "..", group, s_name)

   # get steps from working directory
   steps    <- ""
   file_dir <- ""
   files    <- list.files(envir$wd, "*.R")
   for (file in files) {
      step_no           <- substr(file, 1, 2)
      step_name         <- substr(file, 4, nchar(file) - 2)
      step_name         <- stri_replace_all_fixed(step_name, "_", " ")
      steps[step_no]    <- step_name
      file_dir[step_no] <- file
   }
   steps <- steps[steps != "" & steps != "main"]
   steps <- c(steps, "0" = "cancel")

   if (is.null(step) || !(step %in% names(file_dir))) {
      choose <- input(
         prompt  = "Which step will you be running?",
         options = steps
      )
   } else {
      choose <- step
   }

   if (choose != "0")
      source(file.path(envir$wd, file_dir[choose]))

   # re-assign w/ changes
   .GlobalEnv[[p_name]][[s_name]] <- envir
}

# flow
flow <- function(flow_env, tasks, wd = getwd(), parent_env = globalenv()) {
   parent_name <- deparse(substitute(parent_env))
   if (!(parent_name %in% c("globalenv()", ".GlobalEnv"))) {
      if (!exists(parent_name, envir = .GlobalEnv)) {
         .GlobalEnv[[parent_name]] <- new.env()
      }
   }

   # check if environment exists
   env_name <- deparse(substitute(flow_env))
   if (!exists(env_name, envir = parent_env))
      parent_env[[env_name]] <- new.env()

   assign("wd", wd, parent_env[[env_name]])
   assign("tasks", tasks, parent_env[[env_name]])
   local(envir = parent_env[[env_name]], {
      # convert into separate environments
      for (i in seq_len(length(tasks))) {
         orig_key <- names(tasks)
         orig_val <- tasks

         names(tasks)[[i]] <- paste0(orig_key[[i]], "_", orig_val[[i]])
         tasks[[i]]        <- paste0(orig_key[[i]], "_", orig_val[[i]])
      }
      rm(i, orig_key, orig_val)

      # get methods
      steps <- lapply(tasks, function(step) {
         src <- file.path(wd, paste0(step, ".R"))
         env <- new.env()
         local(envir = env, source(src, local = TRUE))
         return(env)
      })
      rm(tasks)
   })
}


# validatioins
flow_validation <- function(data_env = NULL,
                            process_step = NULL,
                            report_period = NULL,
                            channels = NULL,
                            list_name = "check",
                            upload = NULL) {
   # re-intiialize
   local_drive_quiet()
   local_gs4_quiet()

   data_validation <- as_id("1JOCJPjIsdrys_uaFPI3AIElfkMHzrayh")
   surv_name       <- strsplit(environment_name(data_env), "\\$")[[1]]
   surv_name       <- surv_name[length(surv_name)]

   data_env      <- data_env$steps
   process_name  <- c()
   process_names <- names(data_env)
   process_step  <- gsub("converted", "convert", process_step)
   if (!grepl("dedup", process_step) & !grepl("pdf", process_step))
      process_name <- process_names[grepl(paste0("data_", process_step), process_names)]

   if (length(process_name) == 0)
      process_name <- process_names[grepl(process_step, process_names)]

   if (is.null(list_name)) {
      corr_list <- data_env[[process_name]]
   } else {
      corr_list <- data_env[[process_name]][[list_name]]
   }

   if (length(corr_list) > 0) {
      upload <- ifelse(
         !is.null(upload),
         upload,
         input(
            prompt  = glue("Re-upload gsheet validations for {green(surv_name)}-{green(process_step)}?"),
            options = c("1" = "yes", "2" = "no"),
            default = "2"
         )
      )
      if (upload == "1") {
         log_info("Loading endpoints.")
         corr_status <- "old"

         # get period data
         valid_now <- as_id((drive_ls(data_validation, pattern = report_period))$id)
         if (length(valid_now) == 0) {
            valid_now <- as_id(drive_mkdir(report_period, data_validation)$id)
         }

         # get surveillance endpoints
         gd_surv <- as_id((drive_ls(valid_now, pattern = surv_name))$id)
         if (length(gd_surv) == 0) {
            gd_surv <- as_id(drive_mkdir(surv_name, valid_now)$id)
         }

         # get steps & write data
         gd_step <- as_id((drive_ls(gd_surv, pattern = process_step))$id)
         if (length(gd_step) == 0) {
            # create as new if not existing
            corr_status <- "new"
            drive_rm(paste0("~/", process_step))
            gd_step <- as_id(gs4_create(process_step))
            drive_mv(gd_step, gd_surv, overwrite = TRUE)
         }
         gd_archive <- as_id((drive_ls(gd_surv, pattern = "Archive"))$id)
         if (length(gd_archive) == 0) {
            gd_archive <- as_id(drive_mkdir("Archive", gd_surv)$id)
         }

         # archive current
         # drive_cp(
         #    gd_step,
         #    gd_archive,
         #    paste0(format(Sys.time(), "%Y.%m.%d"), "_", process_step),
         #    overwrite = TRUE
         # )

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
               log_info("Uploadinng {green(issue)}.")
               corr_list[[issue]] %>%
                  mutate_if(
                     .predicate = is.labelled,
                     ~as_factor(.)
                  ) %>%
                  sheet_write(gd_step, issue)
            }
         }

         log_info("Deleting empty sheets.")
         empty_sheets <- names(corr_list[sapply(corr_list, nrow) == 0])
         empty_sheets <- append(intersect(sheets_list, empty_sheets), setdiff(sheets_list, issues_list))
         empty_sheets <- empty_sheets[empty_sheets != "Validations done"]

         # delete if existing sheet no longer has values in new run
         if (length(empty_sheets) > 0) {
            if (length(setdiff(sheets_list, empty_sheets)) == 0) {
               sheet_write(tibble(MSG = "Validations empty."), gd_step, "Validations done")
               sheet_delete(gd_step, empty_sheets)
            } else {
               sheet_delete(gd_step, empty_sheets)
            }
         }
         if (corr_status == "new")
            sheet_autofit(gd_step)

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
}

# register pipelines in working directory
flow_register <- function() {
   files <- fs::dir_info(file.path(getwd(), "src"), recurse = TRUE, regexp = "[/]+_init.") %>%
      arrange(path)
   invisible(lapply(files$path, source))
}