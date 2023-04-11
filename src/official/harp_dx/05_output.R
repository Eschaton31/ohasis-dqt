##  Stata Labels ---------------------------------------------------------------

label_stata <- function(newdx, stata_labels) {
   labels <- split(stata_labels$lab_def, ~label_name)
   labels <- lapply(labels, function(data) {
      final_labels <- ""
      for (i in seq_len(nrow(data))) {
         value <- data[i, "value"] %>% as.integer()
         label <- data[i, "label"] %>% as.character()

         row_label        <- value
         names(row_label) <- label
         final_labels     <- c(final_labels, row_label)
      }

      return(final_labels[-1])
   })

   for (i in seq_len(nrow(stata_labels$lab_val))) {
      var   <- stata_labels$lab_val[i,]$variable
      label <- stata_labels$lab_val[i,]$label_name

      if (var %in% names(newdx))
         newdx[[var]] <- labelled(
            newdx[[var]],
            labels[[label]]
         )
   }

   return(newdx)
}

##  Output Stata Datasets ------------------------------------------------------

output_dta <- function(official) {
   log_info("Checking output directory.")
   output_version <- format(Sys.time(), "%Y%m%d")
   output_dir     <- Sys.getenv("HARP_DX")
   output_name    <- paste0(output_version, '_reg_', ohasis$yr, '-', ohasis$mo)
   output_file    <- file.path(output_dir, paste0(output_name, ".dta"))
   check_dir(output_dir)

   # write main file
   log_info("Saving in Stata data format.")
   official$new %>%
      format_stata() %>%
      write_dta(output_file)

   # write subsets if existing
   for (drop_var in c("dropped_notyet", "dropped_duplicates"))
      if (nrow(official[[drop_var]]) > 0) {
         output_name <- paste0(output_version, "_", drop_var, "_", ohasis$yr, '-', ohasis$mo)
         output_file <- file.path(output_dir, paste0(output_name, ".dta"))
         official[[drop_var]] %>%
            format_stata() %>%
            write_dta(output_file)
      }
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      p$official$new <- label_stata(p$official$new, p$corr)
      output_dta(p$official)
   })
}