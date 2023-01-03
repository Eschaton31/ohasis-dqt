local(envir = nhsss$harp_linked, {
   mo          <- input(prompt = "What is the reporting month?", max.char = 2)
   mo          <- mo %>% stri_pad_left(width = 2, pad = "0")
   yr          <- input(prompt = "What is the reporting year?", max.char = 4)
   yr          <- yr %>% stri_pad_left(width = 4, pad = "0")
   ym          <- paste0(yr, "-", mo)
   date_linked <- format(Sys.time(), "%Y%m%d")

   dir         <- list()
   dir$output  <- "E:/_R/library/hiv_full/data"
   dir$linkage <- glue("E:/System/HARP/_Cascade/{ym}")

   if (mo %in% c("03", "06", "09", "12"))
      file <- file.path(dir$output, glue("{date_linked}_harp_{ym}_wVL.dta"))
   else
      file <- file.path(dir$output, glue("{date_linked}_harp_{ym}_ram_noVL.dta"))

   file <- file.path(dir$output, glue("{date_linked}_harp_{ym}_wVL.dta"))

   lapply(dir, check_dir)
})

local(envir = nhsss$harp_linked, {
   data            <- list()
   data$tx$reg     <- hs_data("harp_tx", "reg", as.numeric(yr), mo) %>%
      read_dta(col_select = c(art_id, uic, confirmatory_code, px_code)) %>%
      rename(
         uic_art   = uic,
         sacclcode = confirmatory_code
      ) %>%
      zap_missing()
   data$tx$outcome <- hs_data("harp_tx", "outcome", as.numeric(yr), mo) %>%
      read_dta(
         col_select = c(
            art_id,
            hub,
            branch,
            tx_reg,
            tx_prov,
            tx_munc,
            idnum,
            artstart_date,
            latest_ffupdate,
            latest_nextpickup,
            outcome,
            latest_regimen,
            sathub,
            onart,
            line,
            realhub,
            realhub_branch,
            real_reg,
            real_prov,
            real_munc
         )
      ) %>%
      mutate(
         artstart_year  = year(artstart_date),
         artstart_month = month(artstart_date),
         everonart      = 1
      ) %>%
      zap_missing()
   data$dx         <- hs_data("harp_dx", "reg", as.numeric(yr), mo) %>%
      read_dta() %>%
      distinct_all() %>%
      zap_missing()
   data$dead       <- hs_data("harp_dead", "reg", as.numeric(yr), mo) %>%
      read_dta(
         col_select = c(
            mort_id,
            idnum,
            place_of_death_province,
            place_of_death_muncity,
            date_of_death,
            report_date
         )
      ) %>%
      zap_missing() %>%
      mutate(mort = 1)
})