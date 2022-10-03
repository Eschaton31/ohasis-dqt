Sys.setenv(TZ = "Asia/Hong_Kong")
options(
   # browser = Sys.getenv("BROWSER"),
   browser             = function(url) {
      if (grepl("^https?:", url)) {
         if (!.Call(".jetbrains_processBrowseURL", url)) {
            browseURL(url, .jetbrains$ther_old_browser)
         }
      } else {
         .Call(".jetbrains_showFile", url, url)
      }
   },
   help_type           = "html",
   RStata.StataPath    = Sys.getenv("STATA_PATH"),
   RStata.StataVersion = as.integer(Sys.getenv("STATA_VER")),
   java.parameters     = "-Xmx4g",
   scipen              = 999,
   timeout             = 9999999,
   repos               = c(
      ropensci = "https://ropensci.r-universe.dev",
      kwbr     = "https://kwb-r.r-universe.dev",
      CRAN     = "https://cloud.r-project.org"
   )
)
.protected <- c("gmail", "nhsss", "epic", "ohasis", "gf", "dqai", "dr", "ihbss")