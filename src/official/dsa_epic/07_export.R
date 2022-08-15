epic$final <- bind_rows(epic$flat) %>%
   mutate_at(
      .vars = vars(`Site City`, `Site Province`, `Site Region`, `Age_Band`),
      ~remove_code(.)
   ) %>%
   arrange(
      Indicator,
      `Site/Organization`,
      `DISAG 2`,
      `DISAG 3`,
      `KP Population`,
      Age_Band
   )

# epic$xlsx$dir      <- glue("H:/Data Sharing/FHI 360/{epic$coverage$type}/{epic$coverage$ym}")
epic$xlsx$dir      <- "H:/Data Sharing/FHI 360/QR/2022.Q3"
epic$xlsx$file     <- glue("EpiC {epic$coverage$type} Indicators ({epic$coverage$ym}).xlsx")
check_dir(epic$xlsx$dir)

epic$xlsx$wb       <- createWorkbook()
epic$xlsx$hs       <- createStyle(
   fontName       = "Calibri",
   fontSize       = 10,
   halign         = "center",
   valign         = "center",
   textDecoration = "bold",
   fgFill         = "#ffe699"
)
epic$xlsx$hs_disag <- createStyle(
   fontName       = "Calibri",
   fontSize       = 10,
   halign         = "center",
   valign         = "center",
   textDecoration = "bold",
   fgFill         = "#92d050"
)
epic$xlsx$cs       <- createStyle(
   fontName = "Calibri",
   fontSize = 10,
   numFmt   = openxlsx_getOp("numFmt", "COMMA")
)

## Sheet 1
addWorksheet(epic$xlsx$wb, "FLAT_PHL")
writeData(epic$xlsx$wb, sheet = 1, x = epic$final)
addStyle(epic$xlsx$wb, sheet = 1, epic$xlsx$hs, rows = 1, cols = seq_len(ncol(epic$final)), gridExpand = TRUE)
addStyle(epic$xlsx$wb, sheet = 1, epic$xlsx$hs_disag, rows = 1, cols = 10:11, gridExpand = TRUE)
addStyle(epic$xlsx$wb, sheet = 1, epic$xlsx$cs, rows = 2:(nrow(epic$final) + 1), cols = seq_len(ncol(epic$final)), gridExpand = TRUE)
setColWidths(epic$xlsx$wb, 1, cols = seq_len(ncol(epic$final)), widths = 'auto')
setRowHeights(epic$xlsx$wb, 1, rows = seq_len(nrow(epic$final) + 1), heights = 14)
freezePane(epic$xlsx$wb, 1, firstRow = TRUE)

saveWorkbook(
   epic$xlsx$wb,
   file.path(epic$xlsx$dir, epic$xlsx$file),
   overwrite = TRUE
)

check_dir(file.path(epic$xlsx$dir, "prev"))
saveRDS(
   epic,
   file.path(epic$xlsx$dir, "prev", "epic.RDS")
)

for (ind in names(epic$linelist)) {
   write_dta(
      epic$linelist[[ind]] %>%
         rename_all(
            ~stri_replace_all_fixed(., " ", "_") %>%
               stri_replace_all_fixed("/", "_") %>%
               stri_replace_all_fixed(".", "_")
         ),
      file.path(epic$xlsx$dir, "prev", glue("{ind}.dta"))
   )

   stata(glue(r"(
u "{epic$xlsx$dir}/prev/{ind}.dta", clear
format_compress
sa "{epic$xlsx$dir}/prev/{ind}.dta", replace
   )"))
}
rm(ind)