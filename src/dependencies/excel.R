read_worksheet <- function(file, sheet, password = NULL, ...) {
   # read workbook using password
   if (!is.null(password) && !is.na(password)) {
      wb <- XLConnect::loadWorkbook(file, password = password)
   } else {
      wb <- XLConnect::loadWorkbook(file)
   }

   data <- XLConnect::readWorksheet(wb, sheet, colTypes = XLC$DATA_TYPE.STRING, ...)
   data <- as_tibble(data)

   rm(wb)
   XLConnect::xlcFreeMemory()

   return(data)
}

write_flat_file <- function(sheet_data, file) {
   xlsx          <- list()
   xlsx$wb       <- createWorkbook()
   xlsx$hs       <- createStyle(
      fontName       = "Calibri",
      fontSize       = 10,
      halign         = "center",
      valign         = "center",
      textDecoration = "bold",
      fgFill         = "#ffe699"
   )
   xlsx$hs_disag <- createStyle(
      fontName       = "Calibri",
      fontSize       = 10,
      halign         = "center",
      valign         = "center",
      textDecoration = "bold",
      fgFill         = "#92d050"
   )
   xlsx$cs       <- createStyle(
      fontName = "Calibri",
      fontSize = 10,
      numFmt   = openxlsx_getOp("numFmt", "COMMA")
   )

   ## Sheet 1
   nsheet <- length(sheet_data)
   for (i in 1:nsheet) {
      addWorksheet(xlsx$wb, names(sheet_data)[i])
      writeData(xlsx$wb, sheet = i, x = sheet_data[[i]])
      addStyle(xlsx$wb, sheet = i, xlsx$hs, rows = 1, cols = seq_len(ncol(sheet_data[[i]])), gridExpand = TRUE)
      addStyle(xlsx$wb, sheet = i, xlsx$cs, rows = 2:(nrow(sheet_data[[i]]) + 1), cols = seq_len(ncol(sheet_data[[i]])), gridExpand = TRUE)
      setColWidths(xlsx$wb, sheet = i, cols = seq_len(ncol(sheet_data[[i]])), widths = 'auto')
      setRowHeights(xlsx$wb, sheet = i, rows = seq_len(nrow(sheet_data[[i]]) + 1), heights = 14)
      freezePane(xlsx$wb, sheet = i, firstRow = TRUE)
   }

   saveWorkbook(xlsx$wb, file, overwrite = TRUE)
}
