##  prepare dataset for HTS_TST ------------------------------------------------

.log_info("Preparing {green('Linelist')}.")
local(envir = dr$`20220606-kuya_chard`, {
   data$`2`$a <- data$`1`$a %>%
      filter(StrLeft(FINAL_TEST_RESULT, 1) == "2") %>%
      inner_join(
         y  = forms$form_prep %>%
            distinct(CENTRAL_ID) %>%
            mutate(
               screen = 1
            ),
         by = "CENTRAL_ID"
      ) %>%
      left_join(
         y  = harp$prep$new_reg %>%
            select(
               CENTRAL_ID,
               prep_id,
            ),
         by = "CENTRAL_ID"
      ) %>%
      left_join(
         y  = harp$prep$new_outcome %>%
            select(
               prep_id,
               prepstart_date,
               eligible
            ) %>%
            mutate(
               started = if_else(
                  condition = !is.na(prepstart_date),
                  true      = 1,
                  false     = 0,
                  missing   = 0
               )
            ),
         by = "prep_id"
      ) %>%
      mutate(
         eligible = if_else(
            condition = eligible == 1 | started == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         started  = if_else(
            condition = started == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
      )
})

.log_info("Generating {green('Aggregates')}.")
local(envir = dr$`20220606-kuya_chard`, {
   request$`2`$a <- data$`1`$a %>%
      filter(StrLeft(FINAL_TEST_RESULT, 1) == "2") %>%
      arrange(FINAL_TEST_DATE) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      group_by(SITE_REG) %>%
      summarise(
         `Negative / Serodiscordant / Seroconverted` = n()
      ) %>%
      full_join(
         y = data$`2`$a %>%
            arrange(FINAL_TEST_DATE) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE) %>%
            group_by(SITE_REG) %>%
            summarise(
               `Screened for PrEP` = sum(screen),
               `Eligible for PrEP` = sum(eligible),
               `Initiated to PrEP` = sum(started),
            )
      ) %>%
      ungroup() %>%
      adorn_totals() %>%
      # mutate_at(
      #    .vars = vars(
      #       `Negative / Serodiscordant / Seroconverted`,
      #       `Screened for PrEP`,
      #       `Eligible for PrEP`,
      #       `Initiated to PrEP`
      #    ),
      #    ~if_else(
      #       condition = is.na(.),
      #       true      = "-",
      #       false     = format(., big.mark = ",")
      #    )
      # ) %>%
      arrange(SITE_REG)%>%
      rename(Region = 1)

   request$`2`$b <- data$`2`$a %>%
      filter(started == 1) %>%
      select(
         CENTRAL_ID,
         VISIT_DATE = prepstart_date
      ) %>%
      distinct_all() %>%
      left_join(
         y  = forms$form_prep %>%
            mutate(
               use_record_faci = if_else(
                  condition = is.na(SERVICE_FACI),
                  true      = 1,
                  false     = 0
               ),
               SERVICE_FACI    = if_else(
                  condition = use_record_faci == 1,
                  true      = FACI_ID,
                  false     = SERVICE_FACI
               ),
            ) %>%
            select(
               CENTRAL_ID,
               VISIT_DATE,
               SERVICE_FACI,
               SERVICE_SUB_FACI
            ),
         by = c("CENTRAL_ID", "VISIT_DATE")
      ) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)


   request$`2`$b <- ohasis$get_faci(
      request$`2`$b,
      list("SITE_NAME" = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
      "name"
   )

   request$`2`$b %<>%
      group_by(SITE_NAME) %>%
      summarise(
         `Number of PrEP clients initiated to PrEP` = n()
      ) %>%
      ungroup()
   request$`2`$b <- tibble(
         SITE_NAME                                  = "HIV & AIDS Support House (HASH)",
         `Number of PrEP clients initiated to PrEP` = 1311
      ) %>%
      add_row(
         SITE_NAME                                  = "Save And Improve Lives (SAIL) Healthcare - Makati",
         `Number of PrEP clients initiated to PrEP` = 1
      ) %>%
      rename(`Site/Organization` = 1)
})