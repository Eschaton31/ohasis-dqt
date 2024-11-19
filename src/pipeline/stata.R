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
      char_fmt                             <- suppress_warnings(max(nchar(data[[var]]), na.rm = TRUE), "returning [\\-]*Inf")
      attributes(data[[var]])$format.stata <- paste0("%-", char_fmt, "s")
   }

   data %<>%
      rename_all(
         ~if_else(nchar(.) > 32, str_left(., 32), ., .)
      )

   return(data)
}

compress_stata <- function(file) {
   if (Sys.getenv("STATA_PATH") != "") {
      # format and save file
      stataCMD <- glue(r"(
u "{file}", clear

ds, has(type string)
foreach var in `r(varlist)' {{
   loc type : type `var'
   loc len = substr("`type'", 4, 1000)

   cap form `var' %-`len's
}}

form *date* %tdCCYY-NN-DD
compress

sa "{file}", replace
   )")

      stata(stataCMD, stata.echo = FALSE)
   }
}