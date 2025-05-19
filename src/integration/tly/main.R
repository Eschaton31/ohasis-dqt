source(file.path(getwd(), "src", "integration", "tly", "hts-db-v3.R"))
source(file.path(getwd(), "src", "integration", "tly", "tx-db-v3.R"))

google_account("eb@loveyourself.ph")

hts <- LyHts$new()
# hts$download() # run when changes are made to decking sheets
hts$readAll()
hts$toLogsheet()
hts$writeLogsheet("H:/hts-imports/20250518/20250518_hts-ly.xlsx")


art <- LyArt$new()
# art$downloadArtDb() # run when changes are made to treatment db
# art$downloadArv()   # run when changes are made to arv dispensing
art$readArtDb()
art$readArv()
art$readIds()
art$convert()
art$checkIssues()
# art$addNewPatients() # only run if there are a lot of new patients with no cid
art$getExisting()
art$prepareUpload()
art$deconstructTables()
art$upload()

google_account("nhsss@doh.gov.ph")
