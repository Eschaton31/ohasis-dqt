file_hts  <- file.path("E:/Data Sharing/PROTECTS-UPSCALE", "hts_202501-202512_ahd-class.dta")
file_art  <- file.path("E:/Data Sharing/PROTECTS-UPSCALE", "tx_202501-202512.dta")
file_prep <- file.path("E:/Data Sharing/PROTECTS-UPSCALE", "prep_202501-202512.dta")

testing %>% format_stata %>% write_dta(file_hts)
tx_gf %>% format_stata %>% write_dta(file_art)
prep_gf %>% format_stata %>% write_dta(file_prep)

date     <- format(Sys.time(), '%Y%m%d')
files    <- c(
   file_hts,
   file_art,
   file_prep
)
zip_file <- file.path('O:/My Drive/Data Sharing/PROTECTS-UPSCALE/2025-S2', glue("as_of-{date}.zip"))
zip::zip(zip_file, files, mode = "cherry-pick")
