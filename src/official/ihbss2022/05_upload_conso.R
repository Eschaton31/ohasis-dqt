# archive current version of monitoring
file <- paste0(Sys.getenv("IHBSS_2022_LOCAL"), "/Conso/", format(Sys.time(), "%Y.%m.%d"), "_data-msm.xlsx")
write_xlsx(
   ihbss$`2022`$conso$msm$data,
   file
)
write_xlsx(
   ihbss$`2022`$conso$msm$data,
   "G:/.shortcut-targets-by-id/1of-fq1pVocs0Lce3wcJHzr-tYolHubw4/DQT/Data Factory/IHBSS - 2022/Data/Daily Data - MSM.xlsx"
)

drive_file <- drive_ls(as_id(ihbss$`2022`$gdrive$data$main)) %>%
   filter(name == "Daily Data - MSM.xlsx")

drive_cp(
   as_id(drive_file[1,]$id),
   as_id(ihbss$`2022`$gdrive$data_archive$msm),
   paste0(format(Sys.time(), "%Y.%m.%d"), "_data-msm.xlsx"),
   overwrite = TRUE
)

drive_upload(
   file,
   as_id(ihbss$`2022`$gdrive$data$main),
   "Daily Data - MSM.xlsx",
   overwrite = TRUE
)
unlink(file)
rm(file, drive_file)