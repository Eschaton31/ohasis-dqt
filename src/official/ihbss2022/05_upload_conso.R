# archive current version of monitoring
file <- paste0(Sys.getenv("IHBSS_2022_LOCAL"), "/Conso/", format(Sys.time(), "%Y.%m.%d"), "_data.xlsx")
write_xlsx(
   ihbss$`2022`$conso$initial$data,
   file
)

drive_file <- drive_ls(as_id(ihbss$`2022`$gdrive$data)) %>%
   filter(name == "Daily Data.xlsx")

drive_cp(
   as_id(drive_file[1,]$id),
   as_id(ihbss$`2022`$gdrive$data_archive),
   paste0(format(Sys.time(), "%Y.%m.%d"), "_data.xlsx"),
   overwrite = TRUE
)

drive_upload(
   file,
   as_id(ihbss$`2022`$gdrive$data),
   "Daily Data.xlsx",
   overwrite = TRUE
)
unlink(file)
rm(file, drive_file)