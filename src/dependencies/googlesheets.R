# get excel/sheet column number of letter
xlcolconv <- function(col) {
   # test: 1 = A, 26 = Z, 27 = AA, 703 = AAA
   if (is.character(col)) {
      # codes from https://stackoverflow.com/a/34537691/2292993
      s             <- col
      # Uppercase
      s_upper       <- toupper(s)
      # Convert string to a vector of single letters
      s_split       <- unlist(strsplit(s_upper, split = ""))
      # Convert each letter to the corresponding number
      s_number      <- sapply(s_split, function(x) {which(LETTERS == x)})
      # Derive the numeric value associated with each letter
      numbers       <- 26^((length(s_number) - 1):0)
      # Calculate the column number
      column_number <- sum(s_number * numbers)
      return(column_number)
   } else {
      n       <- col
      letters <- ''
      while (n > 0) {
         r       <- (n - 1) %% 26  # remainder
         letters <- paste0(intToUtf8(r + utf8ToInt('A')), letters) # ascii
         n       <- (n - 1) %/% 26 # quotient
      }
      return(letters)
   }
}

# format column background color
range_write_color <- function(ss, sheet, column, color) {
   # get sheet properties
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.get",
      params   = list(spreadsheetId = ss)
   )
   val <- googlesheets4::request_make(req)
   res <- httr::content(val)
   for (i in seq_len(length(res$sheets))) {
      sheet_name <- res$sheets[[i]]$properties$title

      if (sheet_name == sheet)
         sheet_id <- res$sheets[[i]]$properties$sheetId
   }

   if (!stri_detect_regex(color, "^#"))
      color <- paste0("#", color)

   rgb    <- col2rgb(color)
   col_id <- xlcolconv(column) - 1
   req    <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            repeatCell = list(
               range  = list(
                  sheetId          = sheet_id,
                  startRowIndex    = 0,
                  startColumnIndex = col_id,
                  endColumnIndex   = col_id + 1
               ),
               cell   = list(
                  userEnteredFormat = list(
                     backgroundColorStyle = list(
                        rgbColor = list(
                           red   = rgb[[1]] / 255,
                           green = rgb[[2]] / 255,
                           blue  = rgb[[3]] / 255
                        )
                     )
                  )
               ),
               fields = "userEnteredFormat"
            )
         )
      )
   )
   googlesheets4::request_make(req)
}

dedup_by <- function(ss, sheet, col_start, col_end) {
   col_formula <- col_start

   # columns must be letters
   col_start <- xlcolconv(col_start) - 1
   col_end   <- xlcolconv(col_end) - 1

   # get sheet properties
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.get",
      params   = list(spreadsheetId = ss)
   )
   val <- googlesheets4::request_make(req)
   res <- httr::content(val)
   for (i in seq_len(length(res$sheets))) {
      sheet_name <- res$sheets[[i]]$properties$title

      if (sheet_name == sheet)
         sheet_id <- res$sheets[[i]]$properties$sheetId
   }

   # write to sheet
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            setDataValidation = list(
               rule  = list(
                  condition    = list(
                     type   = 'ONE_OF_LIST',
                     values = list(
                        list(userEnteredValue = "Y"),
                        list(userEnteredValue = "N")
                     )
                  ),
                  showCustomUi = TRUE
               ),
               range = list(
                  sheetId          = sheet_id,
                  # startRowIndex    = row_start,
                  # endRowIndex      = row_end,
                  startColumnIndex = col_start,
                  endColumnIndex   = col_end
               )
            )
         )
      )
   )
   googlesheets4::request_make(req)

   # conditional formatting
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            deleteConditionalFormatRule = list(
               index   = 0,
               sheetId = sheet_id
            )
         )
      )
   )
   googlesheets4::request_make(req)
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            deleteConditionalFormatRule = list(
               index   = 0,
               sheetId = sheet_id
            )
         )
      )
   )
   googlesheets4::request_make(req)
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            addConditionalFormatRule = list(
               rule = list(
                  ranges      = list(
                     sheetId          = sheet_id,
                     # startRowIndex    = row_start,
                     # endRowIndex      = row_end,
                     startColumnIndex = col_start,
                     endColumnIndex   = col_end
                  ),
                  booleanRule = list(
                     condition = list(
                        type   = "CUSTOM_FORMULA",
                        values = list(userEnteredValue = glue(r"(={col_formula}1 = "Y")"))
                     ),
                     format    = list(
                        backgroundColorStyle = list(
                           rgbColor = list(
                              red   = 97 / 255,
                              green = 226 / 255,
                              blue  = 148 / 255
                           )
                        )
                     )
                  )
               )
            )
         )
      )
   )
   googlesheets4::request_make(req)
   req <- googlesheets4::request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params   = list(
         spreadsheetId = ss,
         requests      = list(
            addConditionalFormatRule = list(
               rule = list(
                  ranges      = list(
                     sheetId          = sheet_id,
                     # startRowIndex    = row_start,
                     # endRowIndex      = row_end,
                     startColumnIndex = col_start,
                     endColumnIndex   = col_end
                  ),
                  booleanRule = list(
                     condition = list(
                        type   = "CUSTOM_FORMULA",
                        values = list(userEnteredValue = glue(r"(={col_formula}1 = "N")"))
                     ),
                     format    = list(
                        backgroundColorStyle = list(
                           rgbColor = list(
                              red   = 189 / 255,
                              green = 147 / 255,
                              blue  = 216 / 255
                           )
                        )
                     )
                  )
               )
            )
         )
      )
   )
   googlesheets4::request_make(req)
}

sheet_autofit <- function(ss) {
   local_gs4_quiet()
   ss     <- as_id(ss)
   sheets <- sheet_names(ss)
   invisible(lapply(sheets, function (sheet) {
      log_info("Resizing = {green(sheet)}.")
      range_autofit(ss, sheet)
   }))
   log_success("Done.")
}
