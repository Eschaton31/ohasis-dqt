export_excel <- function(data, file) {
   xlsx                <- list()
   xlsx$wb             <- createWorkbook()
   xlsx$style          <- list()
   xlsx$style$header   <- createStyle(
      fontName       = "Calibri",
      fontSize       = 11,
      halign         = "center",
      valign         = "center",
      textDecoration = "bold",
      fgFill         = "#ffe699"
   )
   xlsx$style$cells    <- createStyle(
      fontName = "Calibri",
      fontSize = 11,
      numFmt   = openxlsx_getOp("numFmt", "COMMA")
   )
   xlsx$style$datetime <- createStyle(
      fontName = "Calibri",
      fontSize = 11,
      numFmt   = "yyyy-mm-dd hh:mm:ss"
   )
   xlsx$style$date     <- createStyle(
      fontName = "Calibri",
      fontSize = 11,
      numFmt   = "yyyy-mm-dd"
   )

   ## Sheet 1
   addWorksheet(xlsx$wb, "Sheet1")
   writeData(xlsx$wb, sheet = 1, x = data)

   style_cols <- seq_len(ncol(data))
   style_rows <- 2:(nrow(data) + 1)
   addStyle(xlsx$wb, sheet = 1, xlsx$style$header, rows = 1, cols = style_cols, gridExpand = TRUE)

   # format date/time
   all_cols <- names(data)
   cols     <- names(data %>% select_if(is.POSIXct))
   indices  <- c()
   for (col in cols) {
      index   <- grep(col, all_cols)
      indices <- c(indices, index)
   }
   addStyle(xlsx$wb, sheet = 1, xlsx$style$datetime, rows = style_rows, cols = indices, gridExpand = TRUE)

   cols    <- names(data %>% select_if(is.Date))
   indices <- c()
   for (col in cols) {
      index   <- grep(col, all_cols)
      indices <- c(indices, index)
   }
   addStyle(xlsx$wb, sheet = 1, xlsx$style$date, rows = style_rows, cols = indices, gridExpand = TRUE)

   setColWidths(xlsx$wb, 1, cols = style_cols, widths = rep("auto", ncol(data)))
   setRowHeights(xlsx$wb, 1, rows = seq_len(nrow(data) + 1), heights = 14)
   freezePane(xlsx$wb, 1, firstRow = TRUE)

   saveWorkbook(xlsx$wb, file, overwrite = TRUE)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   data <- p$official$recency %>% %>%
      remove_pii() %>%
      mutate_if(
         .predicate = is.labelled,
         ~as_factor(.)
      ) %>%
      select(
         -any_of(c(
            "CREATED_BY",
            "UPDATED_BY",
            "CLINIC_NOTES",
            "COUNSEL_NOTES",
            "use_curr",
            "AGE_DTA",
            "risks",
            "idnum",
            "male",
            "female",
            "SELF_IDENT_OTHER_SIEVE",
            "VL_ERROR",
            "VL_DROP"
         ))
      )
   data %>% write_sheet("1RN3JFNgWkyDf27qb3pfl_R-R_v-_lOPe8ZU_ZgS3Wtc", "PostProcessed")

   dir       <- Sys.getenv("TRACE_BOX")
   files     <- list(
      final = file.path(dir, "RecencyTesting-PostProcess.xlsx"),
      faci  = file.path(dir, "OHASIS-FacilityIDs.xlsx"),
      json  = file.path(dir, "DataStatus.json")
   )
   json_data <- jsonlite::read_json(files$json)


   json_data$`RecencyTesting-PostProcess` <- list(
      upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      version_date = format(max(as.POSIXct(data$SNAPSHOT), na.rm = TRUE), "%Y-%m-%d %H:%M:%S")
   )
   json_data$`OHASIS-FacilityIDs`         <- list(
      upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      version_date = format(
         as.POSIXct(
            paste0(
               strsplit(ohasis$timestamp, "\\.")[[1]][1], "-",
               strsplit(ohasis$timestamp, "\\.")[[1]][2], "-",
               strsplit(ohasis$timestamp, "\\.")[[1]][3], " ",
               StrLeft(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":",
               substr(strsplit(ohasis$timestamp, "\\.")[[1]][4], 3, 4), ":",
               StrRight(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2)
            )
         ),

         "%Y-%m-%d %H:%M:%S"
      )
   )

   export_excel(data, files$final)
   export_excel(ohasis$ref_faci, files$faci)
   jsonlite::write_json(json_data, files$json, pretty = TRUE, auto_unbox = TRUE)

   log_success("Done.")
}