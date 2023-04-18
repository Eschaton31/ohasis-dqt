invisible(lapply(unique(ihbss$`2022`$conso$msm$data$City), function(city) {
   data <- ihbss$`2022`$conso$msm$data %>%
      filter(City == city)

   write_xlsx(data, file.path(data[1,]$path_dta, paste0(format(Sys.time(), "%Y.%m.%d"), "_data_msm_", tolower(city), ".xlsx")))
}))