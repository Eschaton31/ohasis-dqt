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
   data <- gtools::ask(prompt)

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
      log_info("Directory successfully created.")
   } else {
      log_warn("Directory already exists.\n")
   }
}

# stata univar tab
.tab <- function(dataframe, column, nrows = 100L) {
   column <- enquo(column)

   dataframe %>%
      dplyr::group_by(
         !!column
      ) %>%
      dplyr::summarise(
         `Freq.` = n()
      ) %>%
      dplyr::mutate(
         !!column       := as.character(!!column),
         `Cum. Freq.`   = formatC((cumsum(`Freq.`)), big.mark = ",") %>%
            stringr::str_pad(
               .,
               width = max(nchar(.)),
               side  = "left",
               pad   = " "
            ),
         Percent        = str_pad(
            paste0(formatC(round((`Freq.` / sum(`Freq.`)), 4) * 100, digits = 2, format = "f"), "%"),
            width = 6,
            side  = "left",
            pad   = " "
         ),
         `Cum. Percent` = str_pad(
            paste0(formatC(round(cumsum(freq = `Freq.` / sum(`Freq.`)), 4) * 100, format = "f", digits = 2), "%"),
            width = 7,
            side  = "left",
            pad   = " "
         ),
         `Freq.`        = formatC(`Freq.`, big.mark = ",") %>%
            stringr::str_pad(
               .,
               width = max(nchar(.)),
               side  = "left",
               pad   = " "
            ),
      ) %>%
      dplyr::bind_rows(
         dataframe %>%
            dplyr::summarise(
               `Freq.` = n()
            ) %>%
            dplyr::mutate(
               !!column       := 'TOTAL',
               `Cum. Freq.`   = formatC((cumsum(`Freq.`)), big.mark = ",") %>%
                  stringr::str_pad(
                     .,
                     width = max(nchar(.)),
                     side  = "left",
                     pad   = " "
                  ),
               Percent        = str_pad(
                  paste0(formatC(round((`Freq.` / sum(`Freq.`)), 4) * 100, digits = 2, format = "f"), "%"),
                  width = 6,
                  side  = "left",
                  pad   = " "
               ),
               `Cum. Percent` = str_pad(
                  paste0(formatC(round(cumsum(freq = `Freq.` / sum(`Freq.`)), 4) * 100, format = "f", digits = 2), "%"),
                  width = 7,
                  side  = "left",
                  pad   = " "
               ),
               `Freq.`        = formatC(`Freq.`, big.mark = ",") %>%
                  stringr::str_pad(
                     .,
                     width = max(nchar(.)),
                     side  = "left",
                     pad   = " "
                  ),
            )
      ) %>%
      print(n = Inf)
}

# sheets cleaning per id
.cleaning_list <- function(data_to_clean = NULL, cleaning_list = NULL, corr_id_name = NULL, corr_id_type = NULL) {
   for (i in seq_len(nrow(cleaning_list))) {

      # load idnum and name of variable
      df       <- cleaning_list[i,]
      id       <- paste0("df['", corr_id_name, "'] %>% as.", corr_id_type, '()')
      id       <- eval(parse(text = id))
      variable <- df$VARIABLE %>% as.symbol()

      # evaluate data type of variable
      value <- paste0("'", df$NEW_VALUE, "'", ' %>% as.', df$FORMAT, '()')
      value <- eval(parse(text = value))

      # update data
      data_to_clean %<>%
         mutate(
            !!variable := if_else(
               condition = idnum == id,
               true      = value,
               false     = !!variable
            )
         )

      return(data_to_clean)
   }
}

# upload to gdrive/gsheets validations
.validation_gsheets <- function(data_name = NULL, parent_list = NULL, drive_path = NULL) {
   log_info("Uploading to GSheets..")
   empty_sheets <- ""
   gsheet       <- paste0(data_name, "_", format(Sys.time(), "%Y.%m.%d"))
   drive_file   <- drive_get(paste0(drive_path, gsheet))
   drive_link   <- paste0("https://docs.google.com/spreadsheets/d/", drive_file$id, "/|GSheets Link: ", gsheet)
   slack_msg    <- paste0(">HARP Dx conso validation sheets for `", data_name, "` have been updated.\n><", drive_link, ">")

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
      slack_msg  <- paste0(">HARP Dx conso validation sheets for `", data_name, "` have been updated.\n><", drive_link, ">")
   } else {
      for (issue in issues_list) {
         # add issue
         if (nrow(parent_list[[issue]]) > 0)
            sheet_write(parent_list[[issue]], drive_file$id, issue)
      }
   }

   # delete list of empty dataframes from sheet
   log_info("Deleting empty sheets.")
   for (issue in issues_list)
      if (nrow(parent_list[[issue]]) == 0 & issue %in% sheet_names(drive_file$id))
         empty_sheets <- append(empty_sheets, issue)

   # delete if existing sheet no longer has values in new run
   if (length(empty_sheets[-1]) > 0)
      sheet_delete(drive_file$id, empty_sheets[-1])

   # log in slack
   slackr_msg(slack_msg, mrkdwn = "true")
}