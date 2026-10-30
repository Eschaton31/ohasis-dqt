# Google
# trigger auth on purpose --> store a token in the specified cache
options(
   gargle_oauth_cache = ".secrets",
   gargle_oauth_email = "nhsss@doh.gov.ph",
   gargle_oob_default = FALSE
)

google_account <- function(email) {
   # use the user's browser for auth, then restore the IDE browser set in options.R
   old <- options(browser = Sys.getenv("BROWSER"))
   on.exit(options(old))
   drive_auth(email = email, cache = ".secrets")
   gs4_auth(email = email, token = drive_token())
}

google_account("nhsss@doh.gov.ph")

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