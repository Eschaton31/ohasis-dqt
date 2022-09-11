##  HIV Confirmatory Controller ------------------------------------------------

# define datasets
if (!exists("nhsss"))
   nhsss <- list()

if (!("hcr" %in% names(nhsss)))
   nhsss$hcr <- new.env()

nhsss$hcr$wd <- file.path(getwd(), "src", "official", "hcr")

source(file.path(nhsss$hcr$wd, "01_load_reqs.R"))