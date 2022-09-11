local(envir = nhsss$hcr, {
   coverage    <- list()
   coverage$mo <- input(prompt = "What is the reporting month for the reports?", max.char = 2)
   coverage$mo <- stri_pad_left(coverage$mo, 2, "0")
   coverage$yr <- input(prompt = "What is the reporting year for the reports?", max.char = 4)

   coverage$ym <- paste(sep = ".", coverage$yr, coverage$mo)
})

check <- input(
   prompt  = glue("Load/reload the {green('Confirmatory Requests')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   nhsss$hcr$requests <-
      read_sheet(
         drive_get(glue("~/DQT/Data Factory/HIV Confirmatory Request/Requests/{nhsss$hcr$coverage$ym}_hcr")),
         "clients"
      ) %>%
         mutate(
            row_id       = paste0(nhsss$hcr$coverage$ym, "_hcr-", stri_pad_left(row_number(), 6, "0")),
            cell_idnum   = paste0("D", row_number() + 1),
            cell_labcode = paste0("E", row_number() + 1),
         )

   nhsss$hcr$requestor <-
      read_sheet(
         drive_get(glue("~/DQT/Data Factory/HIV Confirmatory Request/Requests/{nhsss$hcr$coverage$ym}_hcr")),
         "requestor"
      )
}

check <- input(
   prompt  = glue("Load/reload the {green('HIV Dx Registry')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   nhsss$hcr$harp <-
      hs_data("harp_dx", "reg", as.numeric(format(Sys.time(), "%Y")), format(Sys.time(), "%m")) %>%
         read_dta() %>%
         mutate_if(
            .predicate = is.character,
            ~str_squish(.) %>%
               if_else(. == "", NA_character_, ., .)
         )
}

check <- input(
   prompt  = glue("Load/reload the {green('List of Confirmatory PDFs')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   nhsss$hcr$pdf <-list_files("/Form%20A", pattern = "*.pdf", recursive = TRUE, full_info = TRUE)
}