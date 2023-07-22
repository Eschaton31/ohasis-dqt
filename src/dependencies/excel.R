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
