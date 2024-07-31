hts <- read_dta("H:/hts_2022-2023.dta")

hts_2223 <- hts %>%
   mutate(
      hts_year = year(hts_date)
   ) %>%
   filter(hts_year %in% c(2022, 2023)) %>%
   mutate(

      curr_age = case_when(
         !is.na(BIRTHDATE) ~ calc_age(BIRTHDATE, RECORD_DATE),
         TRUE ~ as.integer(AGE)
      ),
      curr_age = floor(curr_age),
      agegrp   = case_when(
         curr_age < 15 ~ "below 15",
         curr_age %in% seq(15, 17) ~ "15-17",
         curr_age %in% seq(18, 24) ~ "18-24",
         curr_age %in% seq(25, 34) ~ "25-34",
         curr_age %in% seq(35, 1000) ~ "35+",
      )
   )

try <- hts_2223 %>%
   distinct(CENTRAL_ID, hts_date, hts_result, .keep_all = TRUE) %>%
   group_by(hts_year) %>%
   summarise_at(
      .vars = vars(starts_with("REACH_")),
      ~sum(StrLeft(., 1) == "1", na.rm = TRUE)
   )

hts_2223 %>%
   filter(HTS_REG == "National Capital Region (NCR)") %>%
   # filter(hts_result == "R") %>%
   # distinct(CENTRAL_ID, hts_date, hts_result, .keep_all = TRUE) %>%
   group_by(hts_year) %>%
   summarise_at(
      .vars = vars(starts_with("REACH_")),
      ~sum(StrLeft(., 1) == "1", na.rm = TRUE)
   )

ben_modality <- function(data, ...) {
   return(
      data %>%
         group_by(..., hts_year, hts_modality) %>%
         summarise(
            tested   = n(),
            reactive = sum(hts_result == "R", na.rm = TRUE),
         ) %>%
         ungroup() %>%
         mutate(
            rr = coalesce(reactive, 0) / tested
         ) %>%
         arrange(...) %>%
         pivot_wider(
            id_cols     = c(..., hts_year),
            names_from  = hts_modality,
            values_from = c(tested, reactive, rr)
         ) %>%
         pivot_wider(
            id_cols     = c(...),
            names_from  = hts_year,
            values_from = c(contains("tested"), contains("reactive"), contains("rr"))
         ) %>%
         select(
            ...,
            contains("FBT_2022"),
            contains("FBS_2022"),
            contains("CBS_2022"),
            contains("ST_2022"),
            contains("REACH_2022"),
            contains("NA_2022"),
            contains("FBT_2023"),
            contains("FBS_2023"),
            contains("CBS_2023"),
            contains("ST_2023"),
            contains("REACH_2023"),
            contains("NA_2023"),
         )
   )
}

ben_agegrp <- function(data, ...) {
   return(
      data %>%
         group_by(..., hts_year, agegrp) %>%
         summarise(
            tested   = n(),
            reactive = sum(hts_result == "R", na.rm = TRUE),
         ) %>%
         ungroup() %>%
         mutate(
            rr = coalesce(reactive, 0) / tested
         ) %>%
         arrange(...) %>%
         pivot_wider(
            id_cols     = c(..., hts_year),
            names_from  = agegrp,
            values_from = c(tested, reactive, rr)
         ) %>%
         pivot_wider(
            id_cols     = c(...),
            names_from  = hts_year,
            values_from = c(contains("tested"), contains("reactive"), contains("rr"))
         ) %>%
         select(
            ...,
            contains("below"),
            contains("15-17"),
            contains("18-24"),
            contains("25-34"),
            contains("35"),
         )
   )
}


try <- hts_2223 %>%
   # filter(hts_result == "R") %>%
   # distinct(CENTRAL_ID, hts_date, hts_result, .keep_all = TRUE) %>%
   group_by(HTS_REG, HTS_PROV, hts_year, hts_modality) %>%
   summarise(
      tested   = n(),
      reactive = sum(hts_result == "R", na.rm = TRUE),
   ) %>%
   ungroup() %>%
   mutate(
      rr = coalesce(reactive, 0) / tested
   ) %>%
   pivot_wider(
      id_cols =
   )


#
# summarise_at(
#    .vars = vars(starts_with("REACH_")),
#    list(
#       tested   = ~sum(StrLeft(., 1) == "1", na.rm = TRUE),
#       reactive = ~sum(StrLeft(., 1) == "1" & hts_result == "R", na.rm = TRUE),
#       rr       = ~(sum(StrLeft(., 1) == "1" & hts_result == "R", na.rm = TRUE) / sum(StrLeft(., 1) == "1", na.rm = TRUE))
#    )
# ) %>%
#    ungroup() %>%
#    arrange(HTS_REG, HTS_PROV)
try <- ben_modality(hts_2223, HTS_REG)
write_sheet(try, "1Qb23MfRATcDfd5qIGmNWlLnF6363FVRqvWSC4TFW1N4", "modality-reg")

try <- ben_agegrp(hts_2223, HTS_REG)
write_sheet(try, "1Qb23MfRATcDfd5qIGmNWlLnF6363FVRqvWSC4TFW1N4", "agegrp-reg")


hts_2223 %>%
   mutate(
      HTS_PROV = case_when(
         HTS_PROV == "NCR, City of Manila, First District (Not a Province)" ~ "City of Manila",
         HTS_REG == "National Capital Region (NCR)" ~ HTS_MUNC,
         TRUE ~ HTS_PROV
      )
   ) %>%
   group_by(HTS_REG, HTS_PROV, hts_year) %>%
   summarise(
      tested   = n(),
      reactive = sum(hts_result == "R", na.rm = TRUE),
      facis    = n_distinct(HTS_FACI),
   ) %>%
   ungroup() %>%
   mutate(
      rr = coalesce(reactive, 0) / tested
   ) %>%
   arrange(HTS_REG, HTS_PROV) %>%
   pivot_wider(
      id_cols     = c(HTS_REG, HTS_PROV),
      names_from  = hts_year,
      values_from = c(facis, tested, reactive, rr)
   ) %>%
   select(
      HTS_REG, HTS_PROV,
      contains("2022"),
      contains("2023"),
   ) %>%
   write_sheet("1Qb23MfRATcDfd5qIGmNWlLnF6363FVRqvWSC4TFW1N4", "slide3-prov")
