# ensure UTF-8 on any OS; already the default for R >= 4.2 on Windows. if this warns about code page 1252,
# enable "Beta: Use Unicode UTF-8 for worldwide language support" in Windows' system locale settings
if (!l10n_info()[["UTF-8"]]) {
   Sys.setlocale(category = "LC_ALL", locale = "en_US.UTF-8")
   Sys.setlocale(category = "LC_CTYPE", locale = "en_US.UTF-8")
}
options(
   help_type           = "html",
   # needed by rJava (tabulizer); must be set before rJava loads
   java.parameters     = "-Xmx4g",
   scipen              = 999,
   timeout             = 3600,
   repos               = c(
      ropensci = "https://ropensci.r-universe.dev",
      CRAN     = "https://cloud.r-project.org"
   )
)

# show urls/help in the JetBrains IDE; only available inside its R console
if (exists(".jetbrains")) {
   options(
      browser = function(url) {
         if (grepl("^https?:", url)) {
            if (!.Call(".jetbrains_processBrowseURL", url)) {
               browseURL(url, .jetbrains$ther_old_browser)
            }
         } else {
            .Call(".jetbrains_showFile", url, url)
         }
      }
   )
}
