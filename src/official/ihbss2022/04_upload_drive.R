# get already uploaded files
drive_files <- dir_info(Sys.getenv("IHBSS_2022_DRIVE"), recurse = TRUE) %>%
   filter(type == "file")

local_files <- dir_info(Sys.getenv("IHBSS_2022_LOCAL"), recurse = TRUE) %>%
   filter(type == "file") %>%
   mutate(
      drive_path = stri_replace_all_fixed(
         path,
         Sys.getenv("IHBSS_2022_LOCAL"),
         Sys.getenv("IHBSS_2022_DRIVE")
      )
   ) %>%
   anti_join(
      y  = drive_files %>% select(drive_path = path),
      by = "drive_path"
   )

pb <- progress_bar$new(format = ":current of :total files | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(local_files), width = 100, clear = FALSE)
pb$tick(0)
for (i in seq_len(nrow(local_files))) {
   if (!dir.exists(dirname(local_files[i,]$drive_path)))
      dir.create(dirname(local_files[i,]$drive_path))

   file_copy(local_files[i,]$path, local_files[i,]$drive_path, overwrite = TRUE)
   pb$tick(1)
}
rm(drive_files, local_files, i, pb)