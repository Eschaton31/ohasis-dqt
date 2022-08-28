# pipeline function -> call files based on structure
pipeline <- function(system, parent, step = NULL, group = "official") {
   # get system name as character
   p_name <- stri_replace_all_fixed(deparse(substitute(parent)), '"', "")
   s_name <- stri_replace_all_fixed(deparse(substitute(system)), '"', "")

   # define datasets
   if (!exists(p_name, envir = .GlobalEnv))
      .GlobalEnv[[p_name]] <- list()

   if (!(s_name %in% names(.GlobalEnv[[p_name]])))
      .GlobalEnv[[p_name]][[s_name]] <- new.env()

   # download current state of environment
   envir    <- .GlobalEnv[[p_name]][[s_name]]
   # define working directory
   envir$wd <- file.path(getwd(), "src", group, s_name)

   # get steps from working directory
   steps    <- ""
   file_dir <- ""
   files    <- list.files(envir$wd, "*.R")
   for (file in files) {
      step_no           <- substr(file, 1, 2)
      step_name         <- substr(file, 4, nchar(file) - 2)
      step_name         <- stri_replace_all_fixed(step_name, "_", " ")
      steps[step_no]    <- step_name
      file_dir[step_no] <- file
   }
   steps <- steps[steps != "" & steps != "main"]
   steps <- c(steps, "0" = "cancel")

   if (is.null(step) || !(step %in% names(file_dir))) {
      choose <- input(
         prompt  = "Which step will you be running?",
         options = steps
      )
   } else {
      choose <- step
   }

   if (choose != "0")
      source(file.path(envir$wd, file_dir[choose]))

   # re-assign w/ changes
   .GlobalEnv[[p_name]][[s_name]] <- envir
}

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
      char_fmt <- max(nchar(data[[var]]), na.rm = TRUE)
      attributes(data[[var]])$format.stata <- paste0("%-", char_fmt, "s")
   }

   return(data)
}