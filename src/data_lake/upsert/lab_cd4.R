##  CD4 Data -------------------------------------------------------------------

continue <- 0
id_col   <- "REC_ID"
object   <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
   filter(
      (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
         (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
         (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
   ) %>%
   select(
      REC_ID,
      PATIENT_ID,
      CREATED_AT,
      UPDATED_AT,
      DELETED_AT
   )

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      inner_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_labs")) %>%
            filter(
               LAB_TEST == 5,
               !is.na(LAB_DATE),
               LAB_DATE != "0000-00-00"
            ) %>%
            select(
               REC_ID,
               CD4_DATE   = LAB_DATE,
               CD4_RESULT = LAB_RESULT
            ),
         by = 'REC_ID'
      ) %>%
      collect() %>%
      mutate(
         SNAPSHOT   = case_when(
            !is.na(DELETED_AT) ~ DELETED_AT,
            !is.na(UPDATED_AT) ~ UPDATED_AT,
            TRUE ~ CREATED_AT
         ),
         CD4_YR     = year(CD4_DATE),
         CD4_MO     = month(CD4_DATE),
         CD4_RESULT = toupper(CD4_RESULT),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, ",", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "COPIES", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "CUL", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "CEL", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "CELLS", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "CELL", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "C/UL", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "U/CL", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "C/LU", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "C/IL", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "CLUL", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "CU/L", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "/UL", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "`", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "+", ""),
         CD4_RESULT = stri_replace_all_fixed(CD4_RESULT, "'", ""),
         CD4_RESULT = str_squish(CD4_RESULT),
         CD4_RESULT = case_when(
            CD4_RESULT == "UNDETECTABLE" ~ '199',
            CD4_RESULT == "UNDETECTED" ~ '199',
            CD4_RESULT == "UNREMARKABLE" ~ '199',
            CD4_RESULT == "<558/9/21" ~ '54',
            CD4_RESULT == "ABOVE 200 (ESTIMATED ONLY)" ~ '201',
            CD4_RESULT == "BELOW DETECTABLE LEVEL" ~ '199',
            CD4_RESULT == "BELOW DETECTION" ~ '199',
            CD4_RESULT == "BELOW DETECTION LIMIT" ~ '199',
            CD4_RESULT == ">60" ~ '61',
            CD4_RESULT == "=650" ~ '650',
            CD4_RESULT == "2 0" ~ '20',
            CD4_RESULT == "2/9" ~ '29',
            CD4_RESULT == "192 U" ~ '192',
            CD4_RESULT == "[6]" ~ '6',
            CD4_RESULT == "220S" ~ '220',
            CD4_RESULT == "23 LS" ~ '23',
            CD4_RESULT == "266 L" ~ '266',
            CD4_RESULT == "446C" ~ '446',
            CD4_RESULT == "53B" ~ '53',
            CD4_RESULT == "834 230" ~ '834',
            CD4_RESULT == "BELOW RANGE" ~ '199',
            CD4_RESULT == "L16" ~ '16',
            stri_detect_fixed(CD4_RESULT, "LESS THAN") ~ as.character(as.numeric(str_squish(stri_replace_all_fixed(CD4_RESULT, "LESS THAN", "")))),
            stri_detect_fixed(CD4_RESULT, "<") ~ as.character(as.numeric(str_squish(stri_replace_all_fixed(CD4_RESULT, "<", "")))),
            # stri_detect_charclass(CD4_RESULT, "[^[:digit:]]") &
            #    !stri_detect_fixed(CD4_RESULT, ".") &
            #    !stri_detect_fixed(CD4_RESULT, "-") ~ NA_character_,
            TRUE ~ CD4_RESULT
         ),
         ORDER = case_when(
            !is.na(DELETED_AT) ~ 9999,
            TRUE ~ 1
         )
         # CD4_RESULT = as.numeric(CD4_RESULT)
      ) %>%
      filter(!is.na(CD4_RESULT)) %>%
      arrange(PATIENT_ID, ORDER, desc(CD4_DATE)) %>%
      distinct(PATIENT_ID, CD4_MO, CD4_YR, CD4_RESULT, .keep_all = TRUE) %>%
      select(-ORDER)
}