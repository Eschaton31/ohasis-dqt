invisible(lapply(unique(ihbss$`2022`$conso$msm$data$City), function(city) {
   data <- ihbss$`2022`$conso$msm$data %>%
      filter(City == city)

   write_xlsx(data, file.path(data[1,]$path_dta, paste0(format(Sys.time(), "%Y.%m.%d"), "_data_msm_", tolower(city), ".xlsx")))
}))

dir <- "G:/.shortcut-targets-by-id/1of-fq1pVocs0Lce3wcJHzr-tYolHubw4/DQT/Data Factory/IHBSS - 2022/Data"
ihbss$`2022`$odk$data$msm %>% write_xlsx(file.path(dir, "Daily Data - MSM.xlsx"))
ihbss$`2022`$odk$data$fsw %>% write_xlsx(file.path(dir, "Daily Data - FSW.xlsx"))
ihbss$`2022`$odk$data$pwid_f %>% write_xlsx(file.path(dir, "Daily Data - PWID Females.xlsx"))
ihbss$`2022`$odk$data$pwid_m %>% write_xlsx(file.path(dir, "Daily Data - PWID Males.xlsx"))
ihbss$`2022`$odk$data$medtech %>% write_xlsx(file.path(dir, "Daily Data - MedTech.xlsx"))