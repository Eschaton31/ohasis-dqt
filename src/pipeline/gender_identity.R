generate_gender_identity <- function(linelist, sex, self_identity, self_identity_text, gender_identity) {
   local_gs4_quiet()

   col_sex <- deparse(substitute(sex))
   col_si  <- deparse(substitute(self_identity))
   ref_gi  <- read_sheet("1LFYk4FGokmq4OzBsU6KW6cJWPUYKagXvR6VvvWu1uNo", "curr_process", .name_repair = "unique_quiet")
   linelist %<>%
      # clean data
      mutate_at(
         .vars = vars({{sex}}, {{self_identity}}),
         ~toupper(remove_code(.))
      ) %>%
      mutate_at(
         .vars = vars({{sex}}, {{self_identity}}),
         ~case_when(
            . %in% c("MALE", "MAN") ~ "MALE",
            . %in% c("FEMALE", "WOMAN") ~ "FEMALE",
            . %in% c("OTHER", "OTHERS") ~ "OTHERS",
            . == "0" ~ NA_character_,
            TRUE ~ .
         )
      ) %>%
      mutate(
         {{self_identity_text}} := if_else({{self_identity}} == "OTHERS", {{self_identity_text}}, NA_character_, NA_character_),
         {{self_identity_text}} := toupper(str_squish({{self_identity_text}})),
         SI_SIEVE               = str_replace_all({{self_identity_text}}, "[^[:alnum:]]", ""),
      ) %>%
      # gender identity
      left_join(
         y  = ref_gi %>%
            mutate(
               SEX        = toupper(remove_code(SEX)),
               SELF_IDENT = toupper(remove_code(SELF_IDENT)),
               SI_SIEVE   = str_replace_all(SELF_IDENT_OTHER, "[^[:alnum:]]", ""),
            ) %>%
            distinct(SEX, SELF_IDENT, SI_SIEVE, .keep_all = TRUE) %>%
            select(
               {{sex}}             := SEX,
               {{self_identity}}   := SELF_IDENT,
               SI_SIEVE,
               {{gender_identity}} := correct_gi
            ),
         by = c(col_sex, col_si, "SI_SIEVE")
      ) %>%
      relocate({{gender_identity}}, .before = {{self_identity}}) %>%
      select(-SI_SIEVE)

   return(linelist)
}