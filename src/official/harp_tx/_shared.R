##  Helpers shared by the harp_tx steps ----------------------------------------
# Sourced into the harp_tx environment by _init.R, so every step can call these.

# non-ARV medicines recorded alongside the regimen in medicine_summary
non_arv_meds <- c("3HP", "CPT", "INH", "PEN-G", "TMP/SMX")

# drop non-ARV medicines from a "+"-delimited medicine summary; NA if only non-ARVs were given.
# summaries w/o any non-ARV are returned as-is
remove_non_arv <- function(regimen) {
   meds    <- stri_c("\\Q", non_arv_meds, "\\E", collapse = "|")
   pattern <- stri_c("(?i)\\+\\s*(", meds, ")\\s*(?=\\+)")
   padded  <- stri_c("+", regimen, "+")
   has_non <- coalesce(stri_detect_regex(padded, pattern), FALSE)

   cleaned <- padded[has_non] %>%
      stri_replace_all_regex(pattern, "") %>%
      # empty combos left behind, then the padding
      stri_replace_all_regex("\\++", "+") %>%
      stri_replace_all_regex("^\\+|\\+$", "")

   regimen[has_non] <- na_if(cleaned, "")
   regimen
}

# one visit per client; earliest by default, latest if `latest = TRUE`
pick_visit <- function(data, latest = FALSE) {
   data %<>%
      arrange(if (latest) desc(visit_date) else visit_date, desc(latest_next_date), central_id) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      rename(
         art_faci     = service_faci,
         art_sub_faci = service_sub_faci,
      )

   return(data)
}

# facility of service, special clinics, satellite & transient facilities of a visit
tag_visit_faci <- function(data, params) {
   data %>%
      mutate(
         # Age
         age                = coalesce(age, age_mo / 12),
         age_dta            = calc_age(birthdate, visit_date),

         # tag those without art_faci
         use_record_faci    = if_else(is.na(service_faci), 1, 0, 0),
         service_faci       = if_else(use_record_faci == 1, faci_id, service_faci),

         # convert to harp facility
         actual_faci        = service_faci,
         actual_sub_faci    = service_sub_faci,

         # tag special clinics
         special_clinic     = case_when(
            service_faci %in% params$clinics$tly ~ "TLY",
            service_faci %in% params$clinics$sail ~ "SAIL",
            TRUE ~ NA_character_
         ),
         service_faci       = case_when(
            special_clinic == "TLY" ~ "130001",
            special_clinic == "SAIL" ~ "130025",
            TRUE ~ service_faci
         ),

         # satellite
         satellite_faci     = if_else(
            condition = str_left(client_type, 1) == "5",
            true      = faci_disp,
            false     = NA_character_
         ),
         satellite_sub_faci = if_else(
            condition = str_left(client_type, 1) == "5",
            true      = sub_faci_disp,
            false     = NA_character_
         ),

         # transient
         transient_faci     = if_else(
            condition = str_left(client_type, 1) == "6",
            true      = faci_disp,
            false     = NA_character_
         ),
         transient_sub_faci = if_else(
            condition = str_left(client_type, 1) == "6",
            true      = sub_faci_disp,
            false     = NA_character_
         ),
      )
}

# convert facility ids into harp facility codes & branches
convert_faci <- function(data) {
   # TLY / SHIP hubs w/o a branch default to their main branch
   default_branch <- function(branch, faci_code) {
      case_when(
         faci_code == "TLY" & is.na(branch) ~ "TLY-ANGLO",
         faci_code == "SHP" & is.na(branch) ~ "SHIP-MAKATI",
         TRUE ~ branch
      )
   }

   data %>%
      # record faci
      ohasis$get_faci(
         list(faci_code = c("faci_id", "sub_faci_id")),
         "code"
      ) %>%
      # art faci
      ohasis$get_faci(
         list(art_faci_code = c("art_faci", "art_sub_faci")),
         "code",
         c("tx_reg", "tx_prov", "tx_munc")
      ) %>%
      # epic / gf faci
      ohasis$get_faci(
         list(actual_faci_code = c("actual_faci", "actual_sub_faci")),
         "code",
         c("real_reg", "real_prov", "real_munc")
      ) %>%
      # satellite
      ohasis$get_faci(
         list(satellite_faci_code = c("satellite_faci", "satellite_sub_faci")),
         "code"
      ) %>%
      # transient
      ohasis$get_faci(
         list(transient_faci_code = c("transient_faci", "transient_sub_faci")),
         "code"
      ) %>%
      mutate(
         # branch codes are longer than hub codes
         art_branch    = if_else(nchar(art_faci_code) > 3, art_faci_code, NA_character_),
         actual_branch = if_else(nchar(actual_faci_code) > 3, actual_faci_code, NA_character_),
      ) %>%
      mutate_at(
         .vars = vars(ends_with("_faci_code", ignore.case = FALSE)),
         ~case_when(
            str_detect(., "^TLY") ~ "TLY",
            str_detect(., "^SHIP") ~ "SHP",
            str_detect(., "^SAIL") ~ "SAIL",
            TRUE ~ .
         )
      ) %>%
      mutate(
         art_branch    = default_branch(art_branch, art_faci_code),
         actual_branch = default_branch(actual_branch, actual_faci_code),
      ) %>%
      mutate(
         art_branch = case_when(
            special_clinic == "SAIL" ~ actual_branch,
            special_clinic == "TLY" & is.na(actual_branch) ~ "TLY-ANGLO",
            special_clinic == "TLY" & actual_branch == "TLY" ~ "TLY-ANGLO",
            special_clinic == "TLY" & actual_branch != "TLY" ~ actual_branch,
            TRUE ~ art_branch
         ),
      ) %>%
      arrange(art_faci_code, visit_date, latest_next_date)
}
