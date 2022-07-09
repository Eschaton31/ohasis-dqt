##  Required Libraries ---------------------------------------------------------

# easy loader for libraries
if (!require(pacman))
   install.packages("pacman")

# load libraries
library(pacman)

p_install(
   force = FALSE,
   remotes
)

p_load(
   magrittr,
   ggplot2,
   RColorBrewer,
   DescTools,
   car,
   biostat3,
   aod,
   bazar,
   haven,
   dplyr,
   stringr,
   stringi,
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
   XLConnect,
   gender,
   openxlsx,
   ruODK,
   quarto
)

p_load_gh(
   update = TRUE,
   "KWB-R/kwb.nextcloud",
   "hrbrmstr/speedtest",
   "ropensci/tabulizerjars",
   "ropensci/tabulizer",
   "lmullen/genderdata",
   "SymbolixAU/googlePolylines",
   "SymbolixAU/googleway"
)