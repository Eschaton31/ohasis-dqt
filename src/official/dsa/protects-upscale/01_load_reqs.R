sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", "Sheet1")
sites %<>%
   filter(site_gf_2024 == 1, !is.na(FACI_ID))

min       <- "2024-01-01"
max       <- "2024-06-30"
