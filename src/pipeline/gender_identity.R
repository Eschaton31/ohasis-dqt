# apply fn to unique values only, then map back (sex / self_ident have very few levels)
gi_map_unique <- function(x, fn) {
   uniq <- unique(x)
   fn(uniq)[match(x, uniq)]
}

gi_clean_sex <- function(x) {
   x <- toupper(remove_code(x))
   case_when(
      x %in% c("MALE", "MAN") ~ "MALE",
      x %in% c("FEMALE", "WOMAN") ~ "FEMALE",
      x %in% c("OTHER", "OTHERS") ~ "OTHERS",
      x == "0" ~ NA_character_,
      TRUE ~ x
   )
}

# missing / punctuation-only text is stored as the literal "NULL" in the reference sheet
gi_sieve <- function(x) {
   x <- str_replace_all(toupper(x), "[^[:alnum:]]", "")
   if_else(is.na(x) | x == "", "NULL", x)
}

# NA sex / self-identity never match (same as the old na_matches = "never")
gi_key <- function(sex, self_identity, sieve) {
   if_else(is.na(sex) | is.na(self_identity), NA_character_, str_c(sex, self_identity, sieve, sep = "|"))
}

get_gi_ref <- function() {
   ref <- cached_sheet("1LFYk4FGokmq4OzBsU6KW6cJWPUYKagXvR6VvvWu1uNo", "curr_process") %>%
      mutate(
         SEX        = gi_clean_sex(SEX),
         SELF_IDENT = gi_clean_sex(SELF_IDENT),
         key        = gi_key(SEX, SELF_IDENT, gi_sieve(SELF_IDENT_OTHER)),
      ) %>%
      filter(!is.na(key), !is.na(correct_gi))

   # same idea as get_reg_dupes(): only look at rows whose key repeats; base R,
   # because the duckplyr equivalent is ~80x slower on a sheet this small
   dup_keys  <- unique(ref$key[duplicated(ref$key)])
   pairs     <- unique(ref[ref$key %in% dup_keys, c("key", "correct_gi")])
   conflicts <- unique(pairs$key[duplicated(pairs$key)])
   if (length(conflicts) > 0)
      log_warn("Gender identity reference has conflicting correct_gi for: {str_c(conflicts, collapse = ', ')}. Using the first row.")

   # base dedup: duckplyr's distinct() does not keep row order, so "first row" would be random
   ref[!duplicated(ref$key), ]
}

generate_gender_identity <- function(linelist, sex, self_identity, self_identity_text, gender_identity) {
   ref_gi <- get_gi_ref()

   linelist %>%
      mutate(
         across(c({{sex}}, {{self_identity}}), ~gi_map_unique(., gi_clean_sex)),
         {{self_identity_text}} := if_else({{self_identity}} == "OTHERS", toupper(str_squish({{self_identity_text}})), NA_character_, NA_character_),
         {{gender_identity}}    := ref_gi$correct_gi[match(gi_key({{sex}}, {{self_identity}}, gi_sieve({{self_identity_text}})), ref_gi$key)],
      ) %>%
      relocate({{gender_identity}}, .before = {{self_identity}})
}
