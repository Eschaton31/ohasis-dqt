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
   gender
)

# special libraries
if (!require(remotes))
   install.packages("remotes", repos = "https://cloud.r-project.org")

if (!require(kwb.nextcloud))
   remotes::install_github("KWB-R/kwb.nextcloud")

# devtools::install_github("SymbolixAU/googlePolylines")
# devtools::install_github("SymbolixAU/googleway")
