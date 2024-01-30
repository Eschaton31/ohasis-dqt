# Google
# trigger auth on purpose --> store a token in the specified cache
options(
   gargle_oauth_cache = ".secrets",
   gargle_oauth_email = "nhsss@doh.gov.ph",
   gargle_oob_default = FALSE
)
options(browser = Sys.getenv("BROWSER"))
drive_auth(cache = ".secrets")
gs4_auth(cache = ".secrets")

# Dropbox
# trigger auth on purpose --> store a token in the specified cache
# if (!file.exists(".secrets/hivregistry.nec@gmail.com.RDS") & !is.null(drop_acc()$error_summary)) {
#    token <- drop_auth(new_user = TRUE)
#    saveRDS(token, ".secrets/hivregistry.nec@gmail.com.RDS")
#    rm("token")
#
# } else if (file.exists(".secrets/hivregistry.nec@gmail.com.RDS") & !is.null(drop_acc()$error_summary)) {
#    token <- readRDS(".secrets/hivregistry.nec@gmail.com.RDS")
#    drop_acc(dtoken = token)
#    rm("token")
# }
options(
   browser = function(url) {
      if (grepl('^https?:', url)) {
         if (!.Call('.jetbrains_processBrowseURL', url)) {
            browseURL(url, .jetbrains$ther_old_browser)
         }
      } else {
         .Call('.jetbrains_showFile', url, url)
      }
   }
)