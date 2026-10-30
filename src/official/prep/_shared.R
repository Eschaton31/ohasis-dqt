##  Helpers shared by the prep steps -------------------------------------------
# Sourced into the prep environment by _init.R, so every step can call these.

# one visit per client; earliest by default, latest if `latest = TRUE`
pick_visit <- function(data, latest = FALSE) {
   data %<>%
      arrange(if (latest) desc(visit_date) else visit_date, desc(latest_next_date), central_id) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      rename(
         prep_faci     = service_faci,
         prep_sub_faci = service_sub_faci,
      )

   return(data)
}

# facility codes/branches and nhsss addresses; `faci_addr` names the facility address columns
convert_faci_addr <- function(data, faci_addr = c("prep_reg", "prep_prov", "prep_munc")) {
   log_info("Converting address & facility data.")
   data %<>%
      ohasis$get_faci(
         list(prep_faci_code = c("prep_faci", "prep_sub_faci")),
         "code",
         faci_addr
      ) %>%
      mutate(
         prep_branch    = if_else(nchar(prep_faci_code) > 3, prep_faci_code, NA_character_),
         prep_faci_code = case_when(
            str_detect(prep_faci_code, "^TLY") ~ "TLY",
            str_detect(prep_faci_code, "^SHIP") ~ "SHP",
            str_detect(prep_faci_code, "^HASH") ~ "HASH",
            str_detect(prep_faci_code, "^SAIL") ~ "SAIL",
            TRUE ~ prep_faci_code
         ),
         prep_branch    = case_when(
            prep_faci_code == "TLY" & is.na(prep_branch) ~ "TLY-ANGLO",
            prep_faci_code == "SHP" & is.na(prep_branch) ~ "SHIP-MAKATI",
            prep_faci_code == "HASH" & is.na(prep_branch) ~ "HASH-QC",
            TRUE ~ prep_branch
         )
      ) %>%
      get_addr(
         c(
            perm_reg  = "perm_reg",
            perm_prov = "perm_prov",
            perm_munc = "perm_munc"
         ),
         "nhsss"
      ) %>%
      get_addr(
         c(
            curr_reg  = "curr_reg",
            curr_prov = "curr_prov",
            curr_munc = "curr_munc"
         ),
         "nhsss"
      ) %>%
      arrange(prep_faci_code, visit_date, latest_next_date)

   return(data)
}

# standardized demographics shared by the registry, outcome and prepstart datasets
tag_demographics <- function(data) {
   data %<>%
      mutate(
         initials                  = str_squish(stri_c(str_left(first, 1), str_left(middle, 1), str_left(last, 1))),
         sex                       = remove_code(stri_trans_toupper(sex)),
         self_identity             = remove_code(stri_trans_toupper(self_ident)),
         self_identity             = case_when(
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = stri_trans_toupper(self_ident_other),
         self_identity_other_sieve = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),

         first_time                = as.integer(keep_code(first_time)),
         first_time                = if_else(first_time == 1, first_time, NA_integer_)
      ) %>%
      generate_gender_identity(sex, self_ident, self_ident_other, gender_identity)

   return(data)
}
