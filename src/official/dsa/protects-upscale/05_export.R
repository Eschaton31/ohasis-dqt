file_hts <- file.path("H:/Data Sharing/PROTECTS-UPSCALE", "hts_202401-202410_ahd-class.dta")
file_art <- file.path("H:/Data Sharing/PROTECTS-UPSCALE", "tx_202401-202409.dta")
file_prep <- file.path("H:/Data Sharing/PROTECTS-UPSCALE", "prep_202401-202409.dta")

testing %>% format_stata %>% write_dta(file_hts)
tx_gf %>% format_stata %>% write_dta(file_art)
prep_gf %>% format_stata %>% write_dta(file_prep)


