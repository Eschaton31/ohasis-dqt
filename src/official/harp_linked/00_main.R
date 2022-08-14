##  HARP Registry Linkage Controller -------------------------------------------

# define datasets
if (!exists("nhsss"))
   nhsss <- list()

if (!("harp_linked" %in% names(nhsss)))
   nhsss$harp_linked <- new.env()

nhsss$harp_linked$wd <- file.path(getwd(), "src", "official", "harp_linked")

##  Begin linkage of linked dataset --------------------------------------------

source(file.path(nhsss$harp_linked$wd, "01_load_reqs.R"))
source(file.path(nhsss$harp_linked$wd, "02_conso.R"))