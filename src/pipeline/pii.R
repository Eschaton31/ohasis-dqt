remove_pii <- function(data) {
   data %<>%
      select(
         -any_of(
            c(
               "UIC",
               "uic",
               "FIRST",
               "first",
               "firstname",
               "MIDDLE",
               "middle",
               "LAST",
               "last",
               "SUFFIX",
               "suffix",
               "name_suffix",
               "PATIENT_CODE",
               "px_code",
               "PHILHEALTH_NO",
               "philhealth_no",
               "PHILHEALTH",
               "philhealth",
               "PHILSYS_ID",
               "philsys_id",
               "PERM_ADDR",
               "CURR_ADDR",
               "BIRTH_ADDR",
               "CLIENT_MOBILE",
               "client_mobile",
               "mobile",
               "CLIENT_EMAIL",
               "client_email",
               "email"
            )
         )
      )
   return(data)
}

clean_pii <- function(pii_col) {
   clean <- str_squish(stri_trans_toupper(pii_col))
   clean <- case_when(
      str_detect(clean, "^[^[:alnum:]]$") ~ NA_character_,
      str_detect(clean, "^AWAITING") ~ NA_character_,
      str_detect(clean, "^PENDING") ~ NA_character_,
      clean == "NOT AVAILABLE" ~ NA_character_,
      clean == "" ~ NA_character_,
      clean == "NONE" ~ NA_character_,
      clean == "N/A" ~ NA_character_,
      clean == "NA" ~ NA_character_,
      clean == "XXX" ~ NA_character_,
      clean == "XX" ~ NA_character_,
      TRUE ~ clean
   )

   return(clean)
}