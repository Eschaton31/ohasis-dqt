# Google
# trigger auth on purpose --> store a token in the specified cache
options(
   gargle_oauth_cache = ".secrets",
   gargle_oauth_email = "nhsss@doh.gov.ph",
   gargle_oob_default = TRUE
)
drive_auth(cache = ".secrets")
gs4_auth(cache = ".secrets")

# Dropbox
# trigger auth on purpose --> store a token in the specified cache
if (!file.exists(".secrets/hivregistry.nec@gmail.com.RDS")) {
   token <- drop_auth(new_user = TRUE)
   saveRDS(token, ".secrets/hivregistry.nec@gmail.com.RDS")
   rm("token")
} else {
   token <- readRDS(".secrets/hivregistry.nec@gmail.com.RDS")
   drop_acc(dtoken = token)
   rm("token")
}
