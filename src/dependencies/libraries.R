##  Required Libraries ---------------------------------------------------------

# local() keeps helper variables out of the global environment
local({
   # R_LIBS may list several paths; installs go to the first one
   lib <- strsplit(Sys.getenv("R_LIBS"), .Platform$path.sep)[[1]][1]
   if (is.na(lib) || !dir.exists(lib))
      stop("R_LIBS is not set or does not exist ('", Sys.getenv("R_LIBS"), "'). Set it in .Renviron (see .Renviron.example).")

   # easy loader for libraries
   if (!requireNamespace("pacman", quietly = TRUE))
      install.packages("pacman", lib = lib)
   library(pacman)

   # p_load installs missing packages into the first library path (R_LIBS), then loads
   packages <- trimws(readLines("requirements.txt", warn = FALSE))
   packages <- packages[packages != "" & !startsWith(packages, "#")]

   # install everything missing before loading anything: on Windows, a package
   # whose DLL is already loaded (e.g. rlang via dplyr) can't be updated in-session
   missing <- setdiff(packages, rownames(installed.packages()))
   if (length(missing) > 0)
      install.packages(missing, lib = lib)

   loaded   <- p_load(char = packages)
   if (!all(loaded))
      stop("Failed to install/load: ", paste(names(loaded)[!loaded], collapse = ", "))

   p_load_gh("lmullen/genderdata")

   # nhsss unique functions; always updated, but fall back to the installed copy
   # if GitHub is unreachable (offline / rate-limited) so startup doesn't break
   p_unload("nhsss")
   tryCatch(
      remotes::install_github("Eschaton31/nhsss", upgrade = "never", quiet = TRUE),
      error = function(e) {
         if (!requireNamespace("nhsss", quietly = TRUE))
            stop("nhsss is not installed and could not be fetched from GitHub: ", conditionMessage(e))
         warning("Could not update nhsss, using installed version: ", conditionMessage(e))
      }
   )
   library(nhsss)
})

duckplyr::methods_overwrite()
options(duckdb.materialize_message = FALSE)
