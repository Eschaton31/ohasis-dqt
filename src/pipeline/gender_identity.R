generate_gender_identity <- function(linelist, sex, self_identity, self_identity_text, gender_identity) {
   local_gs4_quiet()

   col_sex <- deparse(substitute(sex))
   col_si  <- deparse(substitute(self_identity))
   ref_gi  <- read_sheet("1LFYk4FGokmq4OzBsU6KW6cJWPUYKagXvR6VvvWu1uNo", "Sheet1")
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
            TRUE ~ .
         )
      ) %>%
      mutate(
         {{self_identity_text}} := toupper(str_squish({{self_identity_text}})),
         SI_SIEVE               = if_else(
            condition = !is.na({{self_identity_text}}),
            true      = str_replace_all({{self_identity_text}}, "[^[:alnum:]]", ""),
            false     = NA_character_
         ),
      ) %>%
      # gender identity
      left_join(
         y  = ref_gi %>%
            mutate(SEX = remove_code(SEX)) %>%
            select(
               {{sex}}             := SEX,
               {{self_identity}}   := self_identity,
               SI_SIEVE            = self_identity_other_sieve,
               {{gender_identity}} := gender_identity
            ),
         by = c(col_sex, col_si, "SI_SIEVE")
      ) %>%
      relocate({{gender_identity}}, .before = {{self_identity}}) %>%
      select(-SI_SIEVE)

   return(linelist)
}