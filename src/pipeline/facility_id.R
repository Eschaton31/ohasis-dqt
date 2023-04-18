faci_code_to_id <- function(data, ref_faci_code, faci_branch) {
   faci_id     <- names(faci_branch)[1]
   sub_faci_id <- names(faci_branch)[2]

   col_hub <- as.name(faci_branch[[1]])
   col_sub <- as.name(faci_branch[[2]])

   data %<>%
      mutate(
         main_faci   = if_else({{col_hub}} == "", NA_character_, {{col_hub}}, {{col_hub}}),
         branch_faci = if_else({{col_sub}} == "", NA_character_, {{col_sub}}, {{col_sub}}),
         main_faci   = case_when(
            stri_detect_regex(branch_faci, "^HASH") ~ "HASH",
            stri_detect_regex(branch_faci, "^SAIL") ~ "SAIL",
            stri_detect_regex(branch_faci, "^TLY") ~ "TLY",
            TRUE ~ main_faci
         ),
         branch_faci = if_else(
            condition = nchar(branch_faci) == 3,
            true      = NA_character_,
            false     = branch_faci
         ),
         branch_faci = case_when(
            main_faci == "HASH" & is.na(branch_faci) ~ "HASH-QC",
            main_faci == "TLY" & is.na(branch_faci) ~ "TLY-ANGLO",
            main_faci == "SHP" & is.na(branch_faci) ~ "SHIP-MAKATI",
            TRUE ~ branch_faci
         ),
      ) %>%
      left_join(
         y  = ref_faci_code %>%
            distinct(FACI_CODE, SUB_FACI_CODE, .keep_all = TRUE) %>%
            mutate(
               main_faci   = if_else(FACI_CODE == "", NA_character_, FACI_CODE, FACI_CODE),
               branch_faci = if_else(SUB_FACI_CODE == "", NA_character_, SUB_FACI_CODE, SUB_FACI_CODE),
               main_faci   = case_when(
                  stri_detect_regex(branch_faci, "^HASH") ~ "HASH",
                  stri_detect_regex(branch_faci, "^SAIL") ~ "SAIL",
                  stri_detect_regex(branch_faci, "^TLY") ~ "TLY",
                  TRUE ~ main_faci
               ),
               branch_faci = if_else(
                  condition = nchar(branch_faci) == 3,
                  true      = NA_character_,
                  false     = branch_faci
               ),
               branch_faci = case_when(
                  main_faci == "HASH" & is.na(branch_faci) ~ "HASH-QC",
                  main_faci == "TLY" & is.na(branch_faci) ~ "TLY-ANGLO",
                  main_faci == "SHP" & is.na(branch_faci) ~ "SHIP-MAKATI",
                  TRUE ~ branch_faci
               ),
            ) %>%
            distinct(FACI_ID, main_faci, branch_faci, .keep_all = TRUE) %>%
            select(
               {{faci_id}}     := FACI_ID,
               {{sub_faci_id}} := SUB_FACI_ID,
               main_faci,
               branch_faci
            ),
         by = c("main_faci", "branch_faci")
      ) %>%
      select(-main_faci, -branch_faci)

   return(data)
}

dxlab_to_id <- function(data, facility_ids, dx_lab_cols = NULL) {
   faci_id     <- facility_ids[1]
   sub_faci_id <- facility_ids[2]

   dx_reg  <- dx_lab_cols[1]
   dx_prov <- dx_lab_cols[2]
   dx_munc <- dx_lab_cols[3]
   dx_lab  <- dx_lab_cols[4]

   data %<>%
      left_join(
         y = read_sheet("1WiUiB7n5qkvyeARwGV1l1ipuCknDT8wZ6Pt7662J2ms", "Sheet1") %>%
            select(
               {{dx_reg}}      := dx_region,
               {{dx_prov}}     := dx_province,
               {{dx_munc}}     := dx_muncity,
               {{dx_lab}}      := dxlab_standard,
               {{faci_id}}     := FACI_ID,
               {{sub_faci_id}} := SUB_FACI_ID
            )
      )

   return(data)
}
