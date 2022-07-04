##  Required Libraries ---------------------------------------------------------

# easy loader for libraries
if (!require(pacman))
   install.packages('pacman')

# load libraries
library(pacman)
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
   # logger,
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
   "KWB-R/kwb.nextcloud",
   "hrbrmstr/speedtest"
)

if (!require("remotes"))
   install.packages("remotes")

remotes::install_github(c("ropensci/tabulizerjars", "ropensci/tabulizer"), INSTALL_opts = "--no-multiarch")
remotes::install_github("lmullen/genderdata")

# devtools::install_github("SymbolixAU/googlePolylines")
# devtools::install_github("SymbolixAU/googleway")
