##  GF Logsheet Controller -----------------------------------------------------

# define datasets
if (!exists("gf"))
   gf <- new.env()

gf$wd <- file.path(getwd(), "src", "official", "dsa_gf")

##  Begin linkage of datasets --------------------------------------------------

source(file.path(gf$wd, "01_load_reqs.R"))
source(file.path(gf$wd, "02-01_loghseet_psfi.R"))
source(file.path(gf$wd, "02-02_loghseet_ohasis.R"))
source(file.path(gf$wd, "02-03_combine.R"))
source(file.path(gf$wd, "03-01_hts.R"))
source(file.path(gf$wd, "04-01_kp6a.R"))
source(file.path(gf$wd, "06_conso_flat.R"))
source(file.path(gf$wd, "07_export.R"))
source(file.path(gf$wd, "08_mail.R"))
