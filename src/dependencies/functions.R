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