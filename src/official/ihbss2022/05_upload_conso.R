# archive current version of monitoring
file <- paste0("C:/data/ihbss2022/Conso/", format(Sys.time(), "%Y.%m.%d"), "_data.xlsx")
write_xlsx(
   ihbss$`2022`$conso$initial$data,
   file
)

drive_cp(
   as_id(ihbss$`2022`$gdrive$data),
   as_id(ihbss$`2022`$gdrive$data_archive),
   paste0(format(Sys.time(), "%Y.%m.%d"), "_data.xlsx"),
   overwrite = TRUE
)

drive_upload(
   file,
   as_id(ihbss$`2022`$gdrive$data_dir),
   "Daily Data.xlsx",
   overwrite = TRUE
)
unlink(file)
rm(file)