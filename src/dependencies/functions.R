##  Helper functions -----------------------------------------------------------

# taking user input
input <- function(prompt = NULL, options = NULL, default = NULL, max.char = NULL, is.num = NULL) {
   # options must all be quoted
   # options must be of the format: integer = definition (i.e., "1" = "yes")

   prompt      <- paste0("\n", blue(prompt))
   default_txt <- ""
   if (!is.null(default))
      default_txt <- paste0("\nDefault: ", underline(magenta(default)))

   if (!is.null(options)) {
      key     <- names(options)
      val     <- stri_trans_totitle(options)
      options <- paste(collapse = "\n", paste0(underline(green(key)), " ", cyan(val)))
      prompt  <- paste0(prompt, "\n", options)
   }
   prompt <- paste0(prompt, "\n", default_txt, "\nPress <RETURN> to continue: ")
   prompt <- glue(prompt)

   # get user input
   # cat(prompt, "\n")
   data <- gtools::ask(prompt)

   # check if is integer
   if (data != "" & !is.null(options)) {
      # while (!StrIsNumeric(data))
      #    data <- gtools::ask("Please enter a valid selection: ")

      while (!(data %in% key))
         data <- gtools::ask("Please choose a value from selection: ")
   }

   # if empty, use default
   if (data == "" & !is.null(default))
      data <- default

   # if no default, throw error
   if (data == "" & is.null(default))
      while (data == "")
         data <- gtools::ask("Please provide an input: ")


   # check if max characters defined
   if (!is.null(max.char) && nchar(data) > max.char)
      while (nchar(data) > max.char)
         data <- gtools::ask("Input exceeds the maximum number of characters: ")

   # return value
   return(data)
}

# directory checker-creator
check_dir <- function(dir) {
   if (!dir.exists(file.path(dir))) {
      dir.create(file.path(dir), TRUE, TRUE)
      .log_info("Directory successfully created.")
   } else {
      .log_warn("Directory already exists.\n")
   }
}

# logger
.log_info <- function(msg = NULL) {
   .log_type <- "INFO" %>% stri_pad_right(7, " ")
   log       <- bold(blue(.log_type)) %+% magenta(glue(' [{format(Sys.time(), "%Y-%m-%d %H:%M:%S")}]'))
   msg       <- glue(msg, .envir = parent.frame(1))
   cat(log, msg, "\n")
}

.log_success <- function(msg = NULL) {
   .log_type <- "SUCCESS" %>% stri_pad_right(7, " ")
   log       <- bold(green(.log_type)) %+% magenta(glue(' [{format(Sys.time(), "%Y-%m-%d %H:%M:%S")}]'))
   msg       <- glue(msg, .envir = parent.frame(1))
   cat(log, msg, "\n")
}

.log_warn <- function(msg = NULL) {
   .log_type <- "WARN" %>% stri_pad_right(7, " ")
   log       <- bold(yellow(.log_type)) %+% magenta(glue(' [{format(Sys.time(), "%Y-%m-%d %H:%M:%S")}]'))
   msg       <- glue(msg, .envir = parent.frame(1))
   cat(log, msg, "\n")
}

.log_error <- function(msg = NULL) {
   .log_type <- "ERROR" %>% stri_pad_right(7, " ")
   log       <- bold(red(.log_type)) %+% magenta(glue(' [{format(Sys.time(), "%Y-%m-%d %H:%M:%S")}]'))
   msg       <- glue(msg, .envir = parent.frame(1))
   cat(log, msg, "\n")
}

# sheets cleaning per id
.cleaning_list <- function(data_to_clean = NULL, cleaning_list = NULL, corr_id_name = NULL, corr_id_type = NULL) {
   data    <- data_to_clean %>% filter(!is.na({{corr_id_name}}))
   # for (i in seq_len(nrow(cleaning_list))) {
   #
   #    # load idnum and name of variable
   #    df       <- cleaning_list[i,] %>% as.data.frame()
   #    id       <- paste0("df[,'", corr_id_name, "'] %>% as.", corr_id_type, '()')
   #    id       <- eval(parse(text = id))
   #    eb_id    <- tolower(corr_id_name) %>% as.symbol()
   #    variable <- df$VARIABLE %>% as.symbol()
   #
   #    # evaluate data type of variable
   #    if (df$NEW_VALUE == "NULL")
   #       value <- paste0('as.', df$FORMAT, '(NA)')
   #    else
   #       value <- paste0("'", df$NEW_VALUE, "'", ' %>% as.', df$FORMAT, '()')
   #
   #    value <- eval(parse(text = value))
   #
   #    # update data
   #    data %<>%
   #       mutate(
   #          !!variable := if_else(
   #             condition = !!eb_id == id,
   #             true      = value,
   #             false     = !!variable,
   #             missing   = !!variable
   #          )
   #       )
   # }
   eb_id   <- tolower(corr_id_name)
   id_type <- typeof(data[[eb_id]])
   id_type <- if_else(id_type == "double", "numeric", id_type)
   for (var in unique(cleaning_list$VARIABLE)) {
      var_clean <- cleaning_list %>%
         filter(VARIABLE == var) %>%
         rowwise() %>%
         mutate(
            {{corr_id_name}} := eval(parse(text = glue("as.{id_type}({corr_id_name})"))),
            NEW_VALUE        = eval(parse(text = glue("as.{FORMAT}('{NEW_VALUE}')"))),
         ) %>%
         ungroup() %>%
         select(
            {{eb_id}} := {{corr_id_name}},
            NEW_VALUE
         ) %>%
         mutate(
            update = 1,
         )
      data %<>%
         left_join(
            y  = var_clean,
            by = eb_id
         ) %>%
         mutate(
            {{var}} := if_else(
               condition = update == 1,
               true      = NEW_VALUE,
               false     = !!as.symbol(var),
               missing   = !!as.symbol(var)
            )
         ) %>%
         select(-update, -NEW_VALUE)
   }
   return(data)
}

# upload to gdrive/gsheets validations
.validation_gsheets <- function(data_name = NULL, parent_list = NULL, drive_path = NULL, surv_name = NULL, channels = NULL) {
   .log_info("Uploading to GSheets..")
   slack_by     <- (slackr_users() %>% filter(name == Sys.getenv("SLACK_PERSONAL")))$id
   empty_sheets <- ""
   gsheet       <- paste0(data_name, "_", format(Sys.time(), "%Y.%m.%d"))
   drive_file   <- drive_get(paste0(drive_path, gsheet))

   # list of validations
   issues_list <- names(parent_list)

   # create as new if not existing
   corr_status <- "old"
   if (nrow(drive_file) == 0) {
      corr_status <- "new"
      drive_rm(paste0("~/", gsheet))
      gs4_create(gsheet, sheets = parent_list)
      drive_mv(drive_get(paste0("~/", gsheet))$id %>% as_id(), drive_path, overwrite = TRUE)
   }

   # acquire sheet_id
   drive_file <- drive_get(paste0(drive_path, gsheet))
   drive_link <- paste0("https://docs.google.com/spreadsheets/d/", drive_file$id, "/|GSheets Link: ", gsheet)
   slack_msg  <- glue(">*{surv_name}*\n>Conso validation sheets for `{data_name}` have been updated by <@{slack_by}>.\n><{drive_link}>")
   for (issue in issues_list) {
      # add issue
      if (nrow(parent_list[[issue]]) > 0) {
         if (corr_status == "old")
            sheet_write(parent_list[[issue]], drive_file$id, issue)
         else
            range_autofit(drive_file$id, issue)
      }
   }

   # delete list of empty dataframes from sheet
   .log_info("Deleting empty sheets.")
   for (issue in issues_list)
      if (nrow(parent_list[[issue]]) == 0 & issue %in% sheet_names(drive_file$id))
         empty_sheets <- append(empty_sheets, issue)
   for (issue in sheet_names(drive_file$id))
      if (!(issue %in% issues_list))
         empty_sheets <- append(empty_sheets, issue)

   # delete if existing sheet no longer has values in new run
   if (length(empty_sheets[-1]) > 0)
      sheet_delete(drive_file$id, empty_sheets[-1])

   # log in slack
   if (is.null(channels)) {
      slackr_msg(slack_msg, mrkdwn = "true")
   } else {
      for (channel in channels)
         slackr_msg(slack_msg, mrkdwn = "true", channel = channel)
   }
}

# download entire dropbox folder
remove_trailing_slashes <- function(x) gsub("/*$", "", x)

download_folder <- function(
   path,
   local_path,
   dtoken = rdrop2::drop_auth(),
   unzip = TRUE,
   overwrite = FALSE,
   progress = interactive(),
   verbose = interactive()
) {
   if (unzip && dir.exists(local_path))
      stop("a directory already exists at ", local_path)
   if (!unzip && file.exists(local_path))
      stop("a file already exists at ", local_path)

   path              <- remove_trailing_slashes(path)
   local_path        <- remove_trailing_slashes(local_path)
   local_parent      <- dirname(local_path)
   original_dir_name <- basename(path)
   download_path     <- if (unzip) tempfile("dir") else local_path

   if (!dir.exists(local_parent)) stop("target parent directory ", local_parent, " not found")

   url <- "https://content.dropboxapi.com/2/files/download_zip"
   req <- httr::POST(
      url = url,
      httr::config(token = dtoken),
      httr::add_headers(
         `Dropbox-API-Arg` = jsonlite::toJSON(list(path = paste0("/", path)),
                                              auto_unbox = TRUE)),
      if (progress) httr::progress(),
      httr::write_disk(download_path, overwrite)
   )
   httr::stop_for_status(req)
   if (verbose) {
      size        <- file.size(download_path)
      class(size) <- "object_size"
      base::message(sprintf("Downloaded %s to %s: %s on disk", path,
                            download_path, format(size, units = "auto")))
   }
   if (unzip) {
      if (verbose) base::message("Unzipping file...")
      new_dir_name <- basename(local_path)
      unzip_path   <- tempfile("dir")
      unzip(download_path, exdir = unzip_path)
      file.rename(file.path(unzip_path, original_dir_name),
                  file.path(unzip_path, new_dir_name))
      file.copy(file.path(unzip_path, new_dir_name),
                local_parent,
                recursive = TRUE)
   }

   TRUE
}

# function to extract information from pdf in sections
pdf_section <- function(data, x_seq, y_seq) {
   df <- data %>%
      filter(
         x %in% x_seq,
         y %in% y_seq
      ) %>%
      group_by(y) %>%
      summarise(
         text = paste0(collapse = " ", text)
      )
   return(df)
}

# clear environment function
clear_env <- function(...) {
   env <- ls(envir = .GlobalEnv)
   if (!exists("currEnv", envir = .GlobalEnv))
      currEnv <- env[env != "currEnv"]

   exclude <- as.character(match.call(expand.dots = FALSE)$`...`)

   remove <- setdiff(env, exclude)
   remove <- setdiff(remove, lsf.str(envir = .GlobalEnv))
   remove <- setdiff(remove, .protected)
   rm(list = remove, envir = .GlobalEnv)
}

remove_code <- function(var) {
   if_else(
      condition = !is.na(var) & stri_detect_fixed(var, '_'),
      true      = substr(var, stri_locate_first_fixed(var, '_') + 1, nchar(var)),
      false     = var
   )
}

keep_code <- function(var) {
   if_else(
      condition = !is.na(var) & stri_detect_fixed(var, '_'),
      true      = substr(var, 1, stri_locate_first_fixed(var, '_') - 1),
      false     = var
   )
}

get_names <- function(parent, pattern = NULL) {
   if (!is.null(pattern))
      names(parent)[grepl(pattern, names(parent))]
   else
      names(parent)
}

chunk_df <- function(data = NULL, chunk_size = NULL) {
   # upsert data
   n_rows   <- nrow(data)
   n_chunks <- rep(1:ceiling(n_rows / chunk_size), each = chunk_size)[seq_len(n_rows)]
   chunked  <- split(data, n_chunks)

   return(chunked)
}