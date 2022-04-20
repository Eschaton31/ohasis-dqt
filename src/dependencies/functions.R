##  Helper functions -----------------------------------------------------------

# taking user input
input <- function(prompt = NULL, options = NULL, default = NULL, max.char = NULL, is.num = NULL) {
   # options must all be quoted
   # options must be of the format: integer = definition (i.e., "1" = "yes")

   if (!is.null(options) && !is.null(default)) {
      key     <- names(options)
      val     <- stri_trans_totitle(options)
      options <- paste(collapse = "\n", paste0(key, ") ", val))
      prompt  <- paste0(prompt, "\n", options, "\n\nDefault: ", default, "\nPress <RETURN> to continue: ")
   } else if (!is.null(default)) {
      prompt <- paste0(prompt, "\n\nDefault: ", default, "\nPress <RETURN> to continue: ")
   }

   # get user input
   # cat(prompt, "\n")
   data <- gtools::ask(prompt %>% cyan())

   # check if is integer
   if (data != "" & !is.null(options)) {
      while (!StrIsNumeric(data))
         data <- gtools::ask("Please enter a valid selection: ")

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

# stata univar tab
.tab <- function(dataframe, column, nrows = 100L) {
   # column <- enquo(column)

   tab_df <- dataframe %>%
      dplyr::group_by(
         across({{column}})
      ) %>%
      dplyr::summarise(
         `Freq.` = n()
      ) %>%
      ungroup() %>%
      dplyr::mutate(
         `Cum. Freq.`   = num(cumsum(`Freq.`), notation = "dec"),
         Percent        = num(round((`Freq.` / sum(`Freq.`)), 10), notation = "dec", label = "%", digits = 2, scale = 100),
         `Cum. Percent` = num(round(cumsum(freq = `Freq.` / sum(`Freq.`)), 10), label = "%", digits = 2, scale = 100),
         `Freq.`        = num(`Freq.`, notation = "dec"),
      ) %>%
      dplyr::bind_rows(
         dataframe %>%
            dplyr::summarise(
               `Freq.` = n()
            ) %>%
            dplyr::mutate(
               `Cum. Freq.`   = num(cumsum(`Freq.`), notation = "dec"),
               Percent        = num(round((`Freq.` / sum(`Freq.`)), 10), notation = "dec", label = "%", digits = 2, scale = 100),
               `Cum. Percent` = num(round(cumsum(freq = `Freq.` / sum(`Freq.`)), 10), label = "%", digits = 2, scale = 100),
               `Freq.`        = num(`Freq.`, notation = "dec"),
            )
      )

   tab_df[nrow(tab_df), 1] <- "TOTAL"

   tab_df %>%
      print(n = Inf)
}

# sheets cleaning per id
.cleaning_list <- function(data_to_clean = NULL, cleaning_list = NULL, corr_id_name = NULL, corr_id_type = NULL) {
   data <- data_to_clean
   for (i in seq_len(nrow(cleaning_list))) {

      # load idnum and name of variable
      df       <- cleaning_list[i,]
      id       <- paste0("df['", corr_id_name, "'] %>% as.", corr_id_type, '()')
      id       <- eval(parse(text = id))
      eb_id    <- tolower(corr_id_name) %>% as.symbol()
      variable <- df$VARIABLE %>% as.symbol()

      # evaluate data type of variable
      if (df$NEW_VALUE == "NULL")
         value <- paste0('as.', df$FORMAT, '(NA)')
      else
         value <- paste0("'", df$NEW_VALUE, "'", ' %>% as.', df$FORMAT, '()')

      value <- eval(parse(text = value))

      # update data
      data %<>%
         mutate(
            !!variable := if_else(
               condition = !!eb_id == id,
               true      = value,
               false     = !!variable
            )
         )
   }
   return(data)
}

# upload to gdrive/gsheets validations
.validation_gsheets <- function(data_name = NULL, parent_list = NULL, drive_path = NULL, surv_name = NULL) {
   .log_info("Uploading to GSheets..")
   empty_sheets <- ""
   gsheet       <- paste0(data_name, "_", format(Sys.time(), "%Y.%m.%d"))
   drive_file   <- drive_get(paste0(drive_path, gsheet))
   drive_link   <- paste0("https://docs.google.com/spreadsheets/d/", drive_file$id, "/|GSheets Link: ", gsheet)
   slack_msg    <- glue(">*{surv_name}*\n>Conso validation sheets for `{data_name}` have been updated.\n><{drive_link}>")

   # list of validations
   issues_list <- names(parent_list)

   # create as new if not existing
   if (nrow(drive_file) == 0) {
      drive_rm(paste0("~/", gsheet))
      gs4_create(gsheet, sheets = parent_list)
      drive_mv(drive_get(paste0("~/", gsheet))$id %>% as_id(), drive_path, overwrite = TRUE)

      # acquire sheet_id
      drive_file <- drive_get(paste0(drive_path, gsheet))
      drive_link <- paste0("https://docs.google.com/spreadsheets/d/", drive_file$id, "/|GSheets Link: ", gsheet)
      slack_msg  <- glue(">*{surv_name}*\n>Conso validation sheets for `{data_name}` have been updated.\n><{drive_link}>")
   } else {
      for (issue in issues_list) {
         # add issue
         if (nrow(parent_list[[issue]]) > 0) {
            sheet_write(parent_list[[issue]], drive_file$id, issue)
            range_autofit(drive_file$id, issue)
         }
      }
   }

   # delete list of empty dataframes from sheet
   .log_info("Deleting empty sheets.")
   for (issue in issues_list)
      if (nrow(parent_list[[issue]]) == 0 & issue %in% sheet_names(drive_file$id))
         empty_sheets <- append(empty_sheets, issue)

   # delete if existing sheet no longer has values in new run
   if (length(empty_sheets[-1]) > 0)
      sheet_delete(drive_file$id, empty_sheets[-1])

   # log in slack
   slackr_msg(slack_msg, mrkdwn = "true")
}

# download entire dropbox folder
remove_trailing_slashes <- function(x) gsub("/*$", "", x)

download_folder <- function(path,
                            local_path,
                            dtoken = rdrop2::drop_auth(),
                            unzip = TRUE,
                            overwrite = FALSE,
                            progress = interactive(),
                            verbose = interactive()) {
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

# ohasis patient_id
oh_px_id <- function(db_conn = NULL, faci_id = NULL) {
   letter <- substr(stri_rand_shuffle(paste(collapse = "", LETTERS[seq_len(130)])), 1, 1)
   number <- substr(stri_rand_shuffle(strrep("0123456789", 5)), 1, 3)

   randomized <- stri_rand_shuffle(paste0(letter, number))
   patient_id <- paste0(format(Sys.time(), "%Y%m%d"), faci_id, randomized)

   pid_query <- dbSendQuery(db_conn, glue("SELECT PATIENT_ID FROM `ohasis_interim`.`px_info` WHERE PATIENT_ID = '{patient_id}'"))
   pid_count <- dbFetch(pid_query)
   dbClearResult(pid_query)

   while (nrow(pid_count) > 0) {
      letter <- substr(stri_rand_shuffle(strrep(LETTERS, 5)), 1, 1)
      number <- substr(stri_rand_shuffle(strrep("0123456789", 5)), 1, 3)

      randomized <- stri_rand_shuffle(paste0(letter, number))
      patient_id <- paste0(format(Sys.time(), "%Y%m%d"), faci_id, randomized)

      pid_query <- dbSendQuery(db_conn, glue("SELECT PATIENT_ID FROM `ohasis_interim`.`px_info` WHERE PATIENT_ID = '{patient_id}'"))
      pid_count <- dbFetch(pid_query)
      dbClearResult(pid_query)
   }

   return(patient_id)
}