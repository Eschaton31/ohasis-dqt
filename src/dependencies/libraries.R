##  Required Libraries ---------------------------------------------------------

# easy loader for libraries
if (!require(pacman))
   install.packages("pacman")

# load libraries
library(pacman)

load_packages <- function(path_to_file) {

   # convert arguments to vector
   packages <- readLines(path_to_file)

   # start loop to determine if each package is installed
   for (package in packages) {

      # if package is installed locally, load
      if (package %in% rownames(installed.packages()))
         do.call('p_load', list(package))

         # if package is not installed locally, download, then load
      else {
         install.packages(package, lib = Sys.getenv("R_LIBS"))
         do.call("p_load", list(package))
      }
   }
}

load_packages("requirements.txt")
p_load_gh(
   "hrbrmstr/speedtest",
   "ropensci/tabulizerjars",
   "ropensci/tabulizer",
   "lmullen/genderdata",
   "SymbolixAU/googlePolylines",
   "SymbolixAU/googleway"
)

# nhsss unique functions
p_unload("nhsss")
remotes::install_git("http://130.105.75.3:3000/jrpalo.doh/nhsss.git", upgrade = "never", quiet = TRUE)
require(nhsss)