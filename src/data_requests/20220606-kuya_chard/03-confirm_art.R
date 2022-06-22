##  prepare dataset for HTS_TST ------------------------------------------------

.log_info("Preparing {green('Linelist')}.")
local(envir = dr$`20220606-kuya_chard`, {
   data$`3`$a <- data$`1`$a %>%
      filter(StrLeft(FINAL_TEST_RESULT, 1) == "1") %>%
      left_join(
         y  = harp$dx %>%
            select(
               CENTRAL_ID,
               FORM_REC = REC_ID,
               dxlab_standard
            ) %>%
            mutate(
               dx = 1
            ),
         by = "CENTRAL_ID"
      ) %>%
      left_join(
         y  = harp$tx$new_reg %>%
            select(
               CENTRAL_ID,
               art_id,
               artstart_hub,
               artstart_branch
            ),
         by = "CENTRAL_ID"
      ) %>%
      left_join(
         y  = harp$tx$new_outcome %>%
            select(
               art_id,
               artstart_date
            ) %>%
            mutate(
               everonart = 1
            ),
         by = "art_id"
      ) %>%
      mutate(
         dx        = if_else(
            condition = dx == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         everonart = if_else(
            condition = everonart == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
      )
})

.log_info("Generating {green('Aggregates')}.")
local(envir = dr$`20220606-kuya_chard`, {
   request$`3`$a <- data$`1`$a %>%
      filter(StrLeft(FINAL_TEST_RESULT, 1) == "1") %>%
      arrange(FINAL_TEST_DATE) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      group_by(SITE_REG) %>%
      summarise(
         `Reactive / Serodiscordant / Seroconverted` = n()
      ) %>%
      full_join(
         y = data$`3`$a %>%
            arrange(FINAL_TEST_DATE) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE) %>%
            group_by(SITE_REG) %>%
            summarise(
               `Confirmed Positive` = sum(dx),
               `Initiated to ART`   = sum(everonart),
            )
      ) %>%
      ungroup() %>%
      adorn_totals() %>%
      # mutate_at(
      #    .vars = vars(
      #       `Reactive / Serodiscordant / Seroconverted`,
      #       `Confirmed Positive`,
      #       `Initiated to ART`
      #    ),
      #    ~if_else(
      #       condition = is.na(.),
      #       true      = "-",
      #       false     = format(., big.mark = ",")
      #    )
      # ) %>%
      arrange(SITE_REG)%>%
      rename(Region = 1)

   request$`3`$b <- data$`3`$a %>%
      filter(dx == 1) %>%
      select(
         CENTRAL_ID,
         dxlab_standard
      ) %>%
      distinct_all() %>%
      left_join(
         y  = ohasis$ref_faci %>%
            select(
               dxlab_standard = FACI_NAME_CLEAN,
               SITE_NAME      = FACI_NAME
            ),
         by = "dxlab_standard"
      )

   request$`3`$b %<>%
      group_by(SITE_NAME) %>%
      summarise(
         `Number of clients confirmed to HIV` = n()
      ) %>%
      ungroup() %>%
      arrange(desc(`Number of clients confirmed to HIV`))

   request$`3`$b <- request$`3`$b %>%
      slice(1:5) %>%
      bind_rows(
         request$`3`$b %>%
            slice(-5) %>%
            summarise(
               `Number of clients confirmed to HIV` = sum(`Number of clients confirmed to HIV`)
            ) %>%
            mutate(
               SITE_NAME = "Other Facilities"
            )
      ) %>%
      # mutate_at(
      #    .vars = vars(`Number of clients confirmed to HIV`),
      #    ~if_else(
      #       condition = is.na(.),
      #       true      = "-",
      #       false     = format(., big.mark = ",")
      #    )
      # ) %>%
      rename(`Site/Organization` = 1)

   request$`3`$c <- data$`3`$a %>%
      filter(everonart == 1) %>%
      select(
         CENTRAL_ID,
         hub    = artstart_hub,
         branch = artstart_branch
      ) %>%
      mutate(
         hub    = case_when(
            stri_detect_regex(branch, "^HASH") ~ "HASH",
            stri_detect_regex(branch, "^SAIL") ~ "SAIL",
            stri_detect_regex(branch, "^TLY") ~ "TLY",
            TRUE ~ hub
         ),
         branch = if_else(
            condition = nchar(branch) == 3,
            true      = NA_character_,
            false     = branch
         ),
         branch = case_when(
            hub == "HASH" & is.na(branch) ~ "HASH-QC",
            hub == "TLY" & is.na(branch) ~ "TLY-ANGLO",
            hub == "SHP" & is.na(branch) ~ "SHIP-MAKATI",
            TRUE ~ branch
         ),
      ) %>%
      distinct_all() %>%
      left_join(
         y  = ohasis$ref_faci_code %>%
            mutate(
               FACI_CODE     = case_when(
                  stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
                  stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
                  stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
                  TRUE ~ FACI_CODE
               ),
               SUB_FACI_CODE = if_else(
                  condition = nchar(SUB_FACI_CODE) == 3,
                  true      = NA_character_,
                  false     = SUB_FACI_CODE
               ),
               SUB_FACI_CODE = case_when(
                  FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
                  FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
                  FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
                  TRUE ~ SUB_FACI_CODE
               ),
            ) %>%
            select(
               hub     = FACI_CODE,
               branch  = SUB_FACI_CODE,
               TX_FACI = FACI_NAME,
            ) %>%
            distinct_all(),
         by = c("hub", "branch")
      ) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)

   request$`3`$c %<>%
      group_by(TX_FACI) %>%
      summarise(
         `Number of clients initiated to ART` = n()
      ) %>%
      ungroup() %>%
      arrange(desc(`Number of clients initiated to ART`))

   request$`3`$c <- request$`3`$c %>%
      slice(1:5) %>%
      bind_rows(
         request$`3`$c %>%
            slice(-5) %>%
            summarise(
               `Number of clients initiated to ART` = sum(`Number of clients initiated to ART`)
            ) %>%
            mutate(
               TX_FACI = "Other Facilities"
            )
      ) %>%
      # mutate_at(
      #    .vars = vars(`Number of clients initiated to ART`),
      #    ~if_else(
      #       condition = is.na(.),
      #       true      = "-",
      #       false     = format(., big.mark = ",")
      #    )
      # ) %>%
      rename(`Site/Organization` = 1)
})