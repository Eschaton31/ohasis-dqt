##  HIV EpiCenter Controller ---------------------------------------------------

# define datasets
if (!exists("epictr"))
   epictr <- new.env()

epictr$wd   <- file.path(getwd(), "src", "official", "hivepicenter")
epictr$data <- list()

source(file.path(epictr$wd, "00_forms.R"))
source(file.path(epictr$wd, "00_refs.R"))
source(file.path(epictr$wd, "01_harp.R"))
source(file.path(epictr$wd, "02_estimates.R"))
source(file.path(epictr$wd, "03.00_epictr_linelist.R"))
source(file.path(epictr$wd, "03.01_epictr_by_res.R"))
source(file.path(epictr$wd, "03.02_epictr_by_dx.R"))
source(file.path(epictr$wd, "03.03_epictr_by_tx.R"))
source(file.path(epictr$wd, "04.01_epctr-est_dx.R"))
source(file.path(epictr$wd, "04.02_epctr-est_tx.R"))
source(file.path(epictr$wd, "04.03_epctr_est_conso.R"))
# source(file.path(epictr$wd, "05.01_epictr_hivtest.R"))
source(file.path(epictr$wd, "06.01_epictr_cascade.R"))
