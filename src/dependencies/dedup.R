##  Deduplication functions used in processing data ----------------------------

dedup_prep <- function(
   data = NULL,
   name_f = NULL,
   name_m = NULL,
   name_l = NULL,
   name_s = NULL,
   uic = NULL,
   birthdate = NULL,
   code_confirm = NULL,
   code_px = NULL,
   phic = NULL,
   philsys = NULL
) {
   dedup_new <- data %>%
      mutate(
         LAST              = stri_trans_general(stri_trans_toupper({{name_l}}), "latin-ascii"),
         MIDDLE            = stri_trans_general(stri_trans_toupper({{name_m}}), "latin-ascii"),
         FIRST             = stri_trans_general(stri_trans_toupper({{name_f}}), "latin-ascii"),
         SUFFIX            = stri_trans_general(stri_trans_toupper({{name_s}}), "latin-ascii"),
         UIC               = stri_trans_general(stri_trans_toupper({{uic}}), "latin-ascii"),
         CONFIRMATORY_CODE = stri_trans_general(stri_trans_toupper({{code_confirm}}), "latin-ascii"),
         PATIENT_CODE      = stri_trans_general(stri_trans_toupper({{code_px}}), "latin-ascii"),

         # get components of birthdate
         BIRTH_YR          = year({{birthdate}}),
         BIRTH_MO          = month({{birthdate}}),
         BIRTH_DY          = day({{birthdate}}),

         # extract parent info from uic
         UIC_MOM           = if_else(!is.na(UIC), substr(UIC, 1, 2), NA_character_),
         UIC_DAD           = if_else(!is.na(UIC), substr(UIC, 3, 4), NA_character_),

         # variables for first 3 letters of names
         FIRST_A           = if_else(!is.na(FIRST), substr(FIRST, 1, 3), NA_character_),
         MIDDLE_A          = if_else(!is.na(MIDDLE), substr(MIDDLE, 1, 3), NA_character_),
         LAST_A            = if_else(!is.na(LAST), substr(LAST, 1, 3), NA_character_),

         LAST              = if_else(is.na(LAST), MIDDLE, LAST),
         MIDDLE            = if_else(is.na(MIDDLE), LAST, MIDDLE),

         # clean ids
         CONFIRM_SIEVE     = if_else(!is.na(CONFIRMATORY_CODE), str_replace_all(CONFIRMATORY_CODE, "[^[:alnum:]]", ""), NA_character_),
         PXCODE_SIEVE      = if_else(!is.na(PATIENT_CODE), str_replace_all(PATIENT_CODE, "[^[:alnum:]]", ""), NA_character_),
         FIRST_S           = if_else(!is.na(FIRST), str_replace_all(FIRST, "[^[:alnum:]]", ""), NA_character_),
         MIDDLE_S          = if_else(!is.na(MIDDLE), str_replace_all(MIDDLE, "[^[:alnum:]]", ""), NA_character_),
         LAST_S            = if_else(!is.na(LAST), str_replace_all(LAST, "[^[:alnum:]]", ""), NA_character_),
         PHIC              = if_else(!is.na({{phic}}), str_replace_all({{phic}}, "[^[:alnum:]]", ""), NA_character_),
         PHILSYS           = if_else(!is.na({{philsys}}), str_replace_all({{philsys}}, "[^[:alnum:]]", ""), NA_character_),

         # code standard names
         FIRST_NY          = if_else(!is.na(FIRST_S), nysiis(FIRST_S, stri_length(FIRST_S)), NA_character_),
         MIDDLE_NY         = if_else(!is.na(MIDDLE_S), nysiis(MIDDLE_S, stri_length(MIDDLE_S)), NA_character_),
         LAST_NY           = if_else(!is.na(LAST_S), nysiis(LAST_S, stri_length(LAST_S)), NA_character_),
      )


   # genearte UIC w/o 1 parent, 2 combinations
   dedup_new_uic <- dedup_new %>%
      filter(!is.na(UIC)) %>%
      select(
         CENTRAL_ID,
         UIC_MOM,
         UIC_DAD
      ) %>%
      pivot_longer(
         cols      = starts_with('UIC'),
         names_to  = 'UIC',
         values_to = 'FIRST_TWO'
      ) %>%
      arrange(CENTRAL_ID, FIRST_TWO) %>%
      group_by(CENTRAL_ID) %>%
      mutate(UIC = row_number()) %>%
      ungroup() %>%
      pivot_wider(
         id_cols      = CENTRAL_ID,
         names_from   = UIC,
         names_prefix = 'UIC_',
         values_from  = FIRST_TWO
      )

   dedup_new %<>%
      left_join(
         y  = dedup_new_uic,
         by = 'CENTRAL_ID'
      ) %>%
      mutate(
         UIC_SORT = if_else(
            condition = !is.na(UIC),
            true      = paste0(UIC_1, UIC_2, substr(uic, 5, 14)),
            false     = NA_character_
         )
      )

   return(dedup_new)
}