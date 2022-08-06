##  Required Libraries ---------------------------------------------------------

# easy loader for libraries
if (!require(pacman))
   install.packages("pacman")

# load libraries
library(pacman)

load_packages <- function(...) {
   packages <- as.character(match.call(expand.dots = FALSE)$`...`)
   for (pkg in packages)
	  suppressMessages(p_install(pkg, force = FALSE, character.only = TRUE, lib = Sys.getenv("R_LIBS")))

   # p_update(lib.loc = Sys.getenv("R_LIBS"))
   p_load(char = packages, lib.loc = Sys.getenv("R_LIBS"))
   # do.call("p_load", as.list(substitute(list(...)))[-1L], lib.loc = Sys.getenv("R_LIBS"))
}

load_packages(
   remotes,
   magrittr,
   ggplot2,
   RColorBrewer,
   DescTools,
   car,
   biostat3,
   aod,
   bazar,
   haven,
   readstata13,
   dplyr,
   stringi,
   stringr,
   tidyverse,
   readxl,
   janitor,
   lubridate,
   uuid,
   vroom,
   data.table,
   bannerCommenter,
   profvis,
   odbc,
   RMariaDB,
   RStata,
   vroom,
   furrr,
   writexl,
   clipr,
   dbx,
   fs,
   progress,
   gmailr,
   googledrive,
   googlesheets4,
   rdrop2,
   slackr,
   fastLink,
   stringdist,
   phonics,
   glue,
   googleway,
   crayon,
   docxtractr,
   rJava,
   XLConnect,
   gender,
   openxlsx,
   ruODK,
   quarto
)

p_load_gh(
   "KWB-R/kwb.nextcloud",
   "hrbrmstr/speedtest",
   "ropensci/tabulizerjars",
   "ropensci/tabulizer",
   "lmullen/genderdata",
   "SymbolixAU/googlePolylines",
   "SymbolixAU/googleway"
)