##------------------------------------------------------------------------------
##  Required Libraries
##------------------------------------------------------------------------------

# easy loader for libraries
if (!require(pacman))
   install.packages('pacman')

# load libraries
library(pacman)
p_load(
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
   logger,
   fs,
   progress,
   gmailr,
   googledrive,
   googlesheets4,
   rdrop2
)