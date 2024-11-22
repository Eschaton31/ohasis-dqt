# import functions
source("db_helper.R")
source("stata.R")
source("process_hts.R")
source("process_prep.R")
source("process_vl.R")
source("process_cd4.R")
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
         current_list <- sheet_names(gd_step)
         upload_list  <- corr_list[which(lapply(corr_list, nrow) != 0)]
         delete_list  <- setdiff(current_list, names(upload_list))

         # acquire sheet_id
         slack_by   <- (slackr_users() %>% filter(name == Sys.getenv("SLACK_PERSONAL")))$id
         drive_link <- paste0("https://docs.google.com/spreadsheets/d/", gd_step, "/|", surv_name, "/", process_step)
         slack_msg  <- glue(r"(
         *[<{drive_link}>]* Validation sheets updated by <@{slack_by}>
         )")
         for (issue in names(upload_list)) {
            log_info("Uploading {green(issue)}.")
            upload_list[[issue]] %>%
               mutate_if(
                  .predicate = is.labelled,
                  ~to_character(.)
               ) %>%
               sheet_write(gd_step, issue)
         }

         # delete if existing sheet no longer has values in new run
         log_info("Deleting empty sheets.")
         if (length(delete_list) > 0) {
            if (length(upload_list) == 0) {
               sheet_write(tibble(MSG = "Validations empty."), gd_step, "Validations done")
            }

            sheet_delete(gd_step, delete_list)
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

# new validation in mariadb
flow_validation <- function(data_env = NULL,
                            process_step = NULL,
                            report_period = NULL,
                            channels = NULL,
                            list_name = "check",
                            upload = NULL) {
   # re-intiialize
   surv_name <- strsplit(environment_name(data_env), "\\$")[[1]]
   surv_name <- surv_name[length(surv_name)]

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

         # acquire sheet_id
         drive_link <- paste0(surv_name, "/", process_step)
         slack_msg  <- glue(r"(
         *`{drive_link}`* Validation sheets updated by <@{ohasis$slack_id}>
         )")

         ym      <- report_period %>% str_replace("\\.", "")
         lw_conn <- ohasis$conn("lw")
         for (issue in names(corr_list)) {
            db     <- "nhsss_validations"
            table  <- paste0(surv_name, "-", process_step, "-", issue)
            schema <- Id(schema = db, table = table)

            log_info("Uploading {green(table)}.")

            upload <- corr_list[[issue]]
            cols   <- names(upload)
            if ("last" %in% cols & "LAST" %in% cols) upload %<>% select(-LAST)
            if ("first" %in% cols & "FIRST" %in% cols) upload %<>% select(-FIRST)
            if ("middle" %in% cols & "MIDDLE" %in% cols) upload %<>% select(-MIDDLE)
            if ("suffix" %in% cols & "SUFFIX" %in% cols) upload %<>% select(-SUFFIX)
            if ("birthdate" %in% cols & "BIRTHDATE" %in% cols) upload %<>% select(-BIRTHDATE)
            if ("uic" %in% cols & "UIC" %in% cols) upload %<>% select(-UIC)
            if ("patient_code" %in% cols & "PATIENT_CODE" %in% cols) upload %<>% select(-PATIENT_CODE)
            if ("philsys_id" %in% cols & "PHILSYS_ID" %in% cols) upload %<>% select(-PHILSYS_ID)
            if ("confirmatory_code" %in% cols & "CONFIRMATORY_CODE" %in% cols) upload %<>% select(-CONFIRMATORY_CODE)
            if ("philhealth_no" %in% cols & "PHILHEALTH_NO" %in% cols) upload %<>% select(-PHILHEALTH_NO)

            id_col <- switch(issue,
                             reclink     = "MATCH_ID",
                             merge       = c("MASTER_CID", "USING_CID"),
                             all_issues  = "REC_ID",
                             tabstat     = "VARIABLE",
                             group_dedup = c("grp_id", "CENTRAL_ID"),
                             "CENTRAL_ID")

            if (dbExistsTable(lw_conn, schema)) {
               dbRemoveTable(lw_conn, schema)
            }
            ohasis$upsert(lw_conn, db, table, upload, id_col)

            # update version
            version <- tibble(
               surveillance = surv_name,
               indicator    = process_step,
               issue        = issue,
               period       = report_period,
               last_update  = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
            )
            ohasis$upsert(lw_conn, db, "version", version, c("surveillance", "indicator", "issue"))
         }
         dbDisconnect(lw_conn)

         # log in slack
         if (is.null(channels)) {
            slackr_msg(slack_msg, mrkdwn = "true")
         } else {
            for (channel in channels)
               slackr_msg(slack_msg, mrkdwn = "true", channel = channel)
         }
      }
   }
}

# register pipelines in working directory
flow_register <- function() {
   files <- fs::dir_info(file.path(getwd(), "src"), recurse = TRUE, regexp = "[/]+_init.") %>%
      arrange(path)
   invisible(lapply(files$path, source))
}

flow_dta <- function(data, surv_name, type, yr, mo) {
   yr <- stri_pad_left(yr, 4, "0")
   mo <- stri_pad_left(mo, 2, "0")

   db     <- surv_name
   table  <- stri_c(type, "_", yr, mo)
   schema <- Id(schema = db, table = table)
   id_col <- switch(
      surv_name,
      harp_dx   = "idnum",
      harp_tx   = "art_id",
      harp_dead = "mort_id",
      harp_full = "idnum",
      prep      = "prep_id",
      tbhiv     = "art_id"
   )


   log_info("Uploading {green(table)}.")
   lw_conn <- ohasis$conn("lw")
   if (dbExistsTable(lw_conn, schema)) {
      dbRemoveTable(lw_conn, schema)
   }
   ohasis$upsert(
      lw_conn,
      db,
      table,
      data %>%
         mutate_if(
            .predicate = is.labelled,
            ~to_character(.)
         ),
      id_col
   )

   version <- tibble(
      type        = type,
      period      = stri_c(yr, ".", mo),
      last_update = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   )
   ohasis$upsert(lw_conn, surv_name, "version", version, c("type", "period"))

   dbDisconnect(lw_conn)
}

flow_corr <- function(report_period = NULL, surv_name = NULL) {
   # re-initialize
   corr <- list()

   # list of correction files
   log_info("Downloading corrections.")
   con                  <- ohasis$conn("lw")
   corr$label_values    <- QB$new(con)$
      from("nhsss_stata.label_values")$
      where("system", surv_name)$
      get()
   corr$label_variables <- QB$new(con)$
      from("nhsss_stata.label_variables")$
      where("system", surv_name)$
      get()

   # non duplicates
   table_space <- Id(schema = surv_name, table = "non_dupes")
   if (dbExistsTable(con, table_space)) {
      table_name     <- stri_c(surv_name, ".", "non_dupes")
      corr$non_dupes <- QB$new(con)$
         from(table_name)$
         get()
   }

   # classd
   table_space <- Id(schema = surv_name, table = "corr_classd")
   if (dbExistsTable(con, table_space)) {
      table_name       <- stri_c(surv_name, ".", "corr_classd")
      corr$corr_classd <- QB$new(con)$
         from(table_name)$
         get()
   }

   for (tbl in c("corr_reg", "corr_outcome", "corr_defer", "corr_drop")) {
      table_space <- Id(schema = surv_name, table = tbl)
      if (dbExistsTable(con, table_space)) {
         table_name  <- stri_c(surv_name, ".", tbl)
         corr[[tbl]] <- QB$new(con)$
            from(table_name)$
            where("period", report_period)$
            get()
      }
   }
   dbDisconnect(con)

   log_success("Done.")
   return(corr)
}

infer_type <- function(value, format) {
   return(
      switch(
         format,
         character = as.character(value),
         numeric   = as.numeric(value),
         double    = as.numeric(value),
         integer   = as.integer(value),
         Date      = as.Date(value),
      )
   )
}

apply_corrections <- function(data, corr, id_name) {
   id_col  <- as.name(id_name)
   id_type <- typeof(data[[id_name]])

   variables            <- mutate(corr, new_value = na_if(new_value, "NULL"))
   variables[[id_name]] <- infer_type(variables[[id_name]], id_type)

   variables <- split(variables, ~variable)
   variables <- lapply(variables, function(corrections) {
      format <- corrections[1,]$format
      corrections %<>%
         select({{id_col}}, new_value)

      corrections[['new_value']] <- infer_type(corrections[['new_value']], format)

      return(corrections)
   })

   for (variable in names(variables)) {
      variable <- as.name(variable)
      data %<>%
         select(-matches("NEW_VALUE", ignore.case = FALSE)) %>%
         left_join(
            y  = variables[[variable]],
            by = join_by({{id_col}})
         ) %>%
         mutate(
            {{variable}} := coalesce(new_value, {{variable}})
         ) %>%
         select(-new_value)
   }

   return(data)
}

combine_validations <- function(data_src, corr_list, row_ids) {
   appended <- corr_list %>%
      bind_rows(.id = "sheet_name") %>%
      filter(sheet_name != "tabstat") %>%
      mutate(
         issue = 1
      )

   cols     <- names(appended)
   col_id   <- intersect(names(data_src), row_ids)
   combined <- appended %>%
      select(all_of(row_ids), sheet_name, issue) %>%
      distinct() %>%
      pivot_wider(
         id_cols      = all_of(row_ids),
         names_from   = sheet_name,
         values_from  = issue,
         names_prefix = "issue_"
      ) %>%
      left_join(
         y  = data_src %>%
            select(any_of(cols)),
         by = col_id
      ) %>%
      relocate(starts_with("issue_"), .after = tail(names(.), 1))

   return(combined)
}

hs_download <- function(sys, type, yr, mo) {
   yr <- as.character(yr)
   mo <- stri_pad_left(mo, 2, "0")

   table_version <- stri_c(sys, ".version")
   table_period  <- stri_c(yr, ".", mo)
   table_data    <- stri_c(sys, ".", type, "_", yr, mo)


   con     <- ohasis$conn("lw")
   version <- QB$new(con)$
      from(table_version)$
      where("type", type)$
      where("period", table_period)$
      get()
   data    <- QB$new(con)$
      from(table_data)$
      get()
   dbDisconnect(con)

   local_version <- format(as.POSIXct(version$last_update), "%Y%m%d")
   local_path    <- Sys.getenv(toupper(sys))

   name <- case_when(
      sys == "harp_tx" & type == "outcome" ~ "onart-vl",
      sys == "harp_tx" & type == "reg" ~ "reg-art",
      sys == "prep" & type == "outcome" ~ "onprep",
      sys == "prep" & type == "reg" ~ "reg-prep",
      sys == "harp_dead" & type == "reg" ~ "mort",
      sys == "harp_full" & type == "reg" ~ "harp",
      TRUE ~ type
   )

   local_file <- stri_c(local_version, "_", name, "_", yr, "-", mo, ifelse(sys == "harp_full", "_wVL", ""), ".dta")
   local_file <- file.path(local_path, local_file)

   write_dta(format_stata(data), local_file)
}
