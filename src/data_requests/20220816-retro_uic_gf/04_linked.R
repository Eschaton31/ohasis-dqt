dr$linked <- dr$final %>%
   left_join(
      y  = dr$harp$dx %>%
         mutate(confirmed_positive = 1) %>%
         select(
            CENTRAL_ID,
            idnum,
            confirmed_positive,
            confirm_date,
         ),
      by = "CENTRAL_ID"
   ) %>%
   left_join(
      y  = dr$harp$tx$new_reg %>%
         select(
            CENTRAL_ID,
            art_id,
            artstart_date,
         ),
      by = "CENTRAL_ID"
   ) %>%
   left_join(
      y  = dr$harp$tx$new_outcome %>%
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
                  FACI_ID,
                  realhub        = FACI_CODE,
                  realhub_branch = SUB_FACI_CODE,
                  TX_FACI        = FACI_NAME,
                  TX_REGION      = FACI_NAME_REG,
               ) %>%
               distinct_all(),
            by = c("realhub", "realhub_branch")
         ) %>%
         select(
            art_id,
            tx_hub    = TX_FACI,
            tx_region = TX_REGION
         ),
      by = "art_id"
   ) %>%
   left_join(
      y  = dr$harp$prep$new_reg %>%
         select(
            CENTRAL_ID,
            prep_id
         ) %>%
         inner_join(
            y  = dr$harp$prep$new_outcome %>%
               filter(!is.na(prepstart_date)) %>%
               mutate(everonprep = 1) %>%
               select(
                  prep_id,
                  everonprep
               ),
            by = "prep_id"
         ),
      by = "CENTRAL_ID"
   ) %>%
   rename(
      gf_confimed_positive = confirmed_positive.x,
      gf_confim_date       = confirm_date.x,
      gf_artstart_date     = artstart_date.x,
      gf_tx_hub            = tx_hub.x,
      eb_confimed_positive = confirmed_positive.y,
      eb_confim_date       = confirm_date.y,
      eb_artstart_date     = artstart_date.y,
      eb_tx_hub            = tx_hub.y,
   ) %>%
   distinct(ohasis_record, .keep_all = TRUE)
