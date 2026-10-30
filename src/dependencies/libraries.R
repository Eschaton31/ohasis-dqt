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

   # install missing / too-old packages before loading anything: on Windows, a package
   # whose DLL is already loaded (e.g. rlang via dplyr) can't be updated in-session.
   # install.packages() never upgrades installed deps, so check version requirements
   # ourselves (reads DESCRIPTION metadata only, loads nothing)
   ip      <- installed.packages()
   ip      <- ip[!duplicated(ip[, "Package"]), , drop = FALSE] # first in .libPaths() wins
   deps    <- tools::package_dependencies(intersect(packages, rownames(ip)), db = ip, recursive = TRUE)
   needed  <- unique(c(packages, unlist(deps)))
   missing <- setdiff(needed, rownames(ip))
   check   <- intersect(needed, rownames(ip))
   reqs    <- unlist(strsplit(ip[check, c("Depends", "Imports", "LinkingTo")], ","))
   reqs    <- regmatches(reqs, regexec("^\\s*([[:alnum:].]+)\\s*\\(\\s*>=\\s*([^) ]+)\\s*\\)", reqs))
   reqs    <- do.call(rbind, Filter(length, reqs))
   outdated <- if (!is.null(reqs)) {
      reqs <- reqs[reqs[, 2] %in% rownames(ip), , drop = FALSE]
      unique(reqs[package_version(ip[reqs[, 2], "Version"]) < package_version(reqs[, 3]), 2])
   }
   if (length(c(missing, outdated)) > 0)
      install.packages(c(missing, outdated), lib = lib)

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
