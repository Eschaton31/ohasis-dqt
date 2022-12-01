# format stata exports
format_stata <- function(data) {
   # convert to integers
   vars <- colnames(select_if(data, .predicate = is.numeric))
   for (var in vars) {
      test_int <- any(data[[var]] %% 1 != 0)
      test_int <- ifelse(is.na(test_int), FALSE, test_int)
      if (test_int == TRUE) storage.mode(data[[var]]) <- "integer"
   }

   # format dates
   vars <- colnames(select_if(data, .predicate = is.Date))
   for (var in vars) {
      storage.mode(data[[var]])            <- "integer"
      attributes(data[[var]])$format.stata <- "%tdCCYY-NN-DD"
   }

   # format strings
   vars <- colnames(select_if(data, .predicate = is.character))
   for (var in vars) {
      char_fmt                             <- max(nchar(data[[var]]), na.rm = TRUE)
      attributes(data[[var]])$format.stata <- paste0("%-", char_fmt, "s")
   }

   return(data)
}
