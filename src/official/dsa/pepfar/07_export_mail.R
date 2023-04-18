local(envir = pepfar, {
   ip <- list()
   for (program in c("EpiC", "ICAP")) {
      ip_data  <- list()
      partner  <- tolower(program)
      var      <- switch(
         partner,
         epic = "site_epic_2022",
         icap = "site_icap_2023"
      )
      var_name <- as.name(var)

      # flat file
      ip_data$flat <- flat %>%
         filter({{var_name}} == 1) %>%
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
         ) %>%
         select(-starts_with("site_"))

      # line lists
      ip_data$linelist <- lapply(linelist, function(data, var) {
         var_name <- as.name(var)

         data %<>%
            filter({{var_name}} == 1)

         return(data)
      }, var)

      # output
      ip_data$dir      <- file.path(Sys.getenv("DSA"), program, coverage$type, coverage$curr$ym)
      ip_data$file_agg <- file.path(ip_data$dir, stri_c(program, " ", coverage$type, " Indicators (", coverage$curr$ym, ").xlsx"))
      ip_data$file_rds <- file.path(ip_data$dir, stri_c(partner, ".rds"))
      check_dir(ip_data$dir)

      xlsx          <- list()
      xlsx$wb       <- createWorkbook()
      xlsx$hs       <- createStyle(
         fontName       = "Calibri",
         fontSize       = 10,
         halign         = "center",
         valign         = "center",
         textDecoration = "bold",
         fgFill         = "#ffe699"
      )
      xlsx$hs_disag <- createStyle(
         fontName       = "Calibri",
         fontSize       = 10,
         halign         = "center",
         valign         = "center",
         textDecoration = "bold",
         fgFill         = "#92d050"
      )
      xlsx$cs       <- createStyle(
         fontName = "Calibri",
         fontSize = 10,
         numFmt   = openxlsx_getOp("numFmt", "COMMA")
      )

      ## Sheet 1
      addWorksheet(xlsx$wb, "FLAT_PHL")
      writeData(xlsx$wb, sheet = 1, x = ip_data$flat)
      addStyle(xlsx$wb, sheet = 1, xlsx$hs, rows = 1, cols = seq_len(ncol(ip_data$flat)), gridExpand = TRUE)
      addStyle(xlsx$wb, sheet = 1, xlsx$hs_disag, rows = 1, cols = 10:11, gridExpand = TRUE)
      addStyle(xlsx$wb, sheet = 1, xlsx$cs, rows = 2:(nrow(ip_data$flat) + 1), cols = seq_len(ncol(ip_data$flat)), gridExpand = TRUE)
      setColWidths(xlsx$wb, 1, cols = seq_len(ncol(ip_data$flat)), widths = 'auto')
      setRowHeights(xlsx$wb, 1, rows = seq_len(nrow(ip_data$flat) + 1), heights = 14)
      freezePane(xlsx$wb, 1, firstRow = TRUE)

      saveWorkbook(xlsx$wb, ip_data$file_agg, overwrite = TRUE)
      saveRDS(ip_data, ip_data$file_rds)

      for (surv in names(ip_data$linelist)) {
         exp <- ip_data$linelist[[surv]] %>%
            rename_all(
               ~stri_replace_all_fixed(., " ", "_") %>%
                  stri_replace_all_fixed("/", "_") %>%
                  stri_replace_all_fixed(".", "_")
            ) %>%
            format_stata() %>%
            remove_pii()

         exp %>%
            write_dta(file.path(ip_data$dir, glue("{surv}.dta")))
      }

      ip[[program]] <- ip_data
   }
   rm(var, var_name, program, partner, ip_data, surv)
})
