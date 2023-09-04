# extract facility names for OHASISdata
convert_faci_id <- function(linelist, facilities, input_set, return_type = "name", get_faci_addr = FALSE) {
   # input_set format:
   # final_faci = c(faci_id, sub_faci_id)
   final_faci  <- names(input_set)
   faci_id     <- input_set[[1]][1]
   sub_faci_id <- input_set[[1]][2]

   get <- switch(
      return_type,
      nhsss = "FACI_NAME_CLEAN",
      code  = "FACI_CODE",
      name  = "FACI_NAME"
   )

   # check if sub_faci_id col exists
   if (!(sub_faci_id %in% names(linelist)))
      linelist[, sub_faci_id] <- NA_character_

   # convert to names
   faci_id     <- as.name(faci_id)
   sub_faci_id <- as.name(sub_faci_id)

   # clean variables first
   linelist %<>%
      mutate(
         {{faci_id}}     := if_else(
            condition = is.na({{faci_id}}),
            true      = "",
            false     = {{faci_id}},
            missing   = {{faci_id}}
         ),
         {{sub_faci_id}} := case_when(
            is.na({{sub_faci_id}}) ~ "",
            StrLeft({{sub_faci_id}}, 6) != {{faci_id}} ~ "",
            TRUE ~ {{sub_faci_id}}
         )
      ) %>%
      # get referenced data
      left_join(
         y  = facilities %>%
            select(
               {{faci_id}}     := FACI_ID,
               {{sub_faci_id}} := SUB_FACI_ID,
               {{final_faci}}  := {{get}},
               if (get_faci_addr) {
                  any_of(
                     c(
                        "FACI_PSGC_REG",
                        "FACI_PSGC_PROV",
                        "FACI_PSGC_MUNC"
                     )
                  )
               }
            ),
         by = input_set[[1]]
      ) %>%
      # move then rename to old version
      relocate({{final_faci}}, .after = {{sub_faci_id}}) %>%
      # remove id data
      select(-any_of(input_set[[1]]))

   return(linelist)
}

# extract and finalize an ohasis central id variable
get_cid <- function(linelist, cid_list, pid_col) {
   join_col <- deparse(substitute(pid_col))
   # finalize a central_id columns
   linelist %<>%
      select(-matches("CENTRAL_ID", ignore.case = FALSE)) %>%
      left_join(
         y  = cid_list %>%
            select(
               CENTRAL_ID,
               {{pid_col}} := PATIENT_ID
            ),
         by = join_col
      ) %>%
      mutate(
         CENTRAL_ID = if_else(
            condition = coalesce(CENTRAL_ID, "") == "",
            true      = {{pid_col}},
            false     = CENTRAL_ID
         ),
      ) %>%
      relocate(CENTRAL_ID, .before = {{pid_col}})

   return(linelist)
}

calc_age <- function(birthdate, as_of_date = Sys.time(), date_format = "%Y-%m-%d") {
   birthdate  <- as.Date(birthdate, format = date_format)
   as_of_date <- as.Date(as_of_date, format = date_format)
   age        <- if_else(
      condition = !is.na(birthdate),
      true      = as.integer(floor(interval(birthdate, as_of_date) / years(1))),
      false     = NA_integer_
   )

   return(age)
}

gen_agegrp <- function(age, cat_type = c("harp", "5yr")) {
   age <- as.integer(floor(age))

   if (cat_type == "harp") {
      agegrp <- case_when(
         age %in% seq(0, 14) ~ "<15",
         age %in% seq(15, 24) ~ "15-24",
         age %in% seq(25, 34) ~ "25-34",
         age %in% seq(35, 49) ~ "35-49",
         age %in% seq(50, 1000) ~ "50+",
         TRUE ~ "(no data)"
      )
   }

   if (cat_type == "5yr") {
      agegrp <- case_when(
         age %in% seq(0, 4) ~ "<15",
         age %in% seq(5, 9) ~ "<15",
         age %in% seq(10, 14) ~ "<15",
         age %in% seq(15, 17) ~ "15-17",
         age %in% seq(18, 19) ~ "18-19",
         age %in% seq(20, 24) ~ "20-24",
         age %in% seq(25, 29) ~ "25-29",
         age %in% seq(30, 34) ~ "30-34",
         age %in% seq(35, 49) ~ "35-49",
         age %in% seq(50, 54) ~ "50-54",
         age %in% seq(55, 59) ~ "55-59",
         age %in% seq(60, 64) ~ "60-64",
         age %in% seq(65, 69) ~ "65-69",
         age %in% seq(70, 74) ~ "70-74",
         age %in% seq(75, 79) ~ "75-79",
         age %in% seq(80, 1000) ~ "80+",
         TRUE ~ "(no data)"
      )
   }

   return(agegrp)
}

null_dates <- function(date, type = "POSIXct") {
   new_date <- switch(
      type,
      POSIXct = if_else(date == -62169984000, NA_POSIXct_, date, NA_POSIXct_),
      Date    = if_else(date == -719560, NA_Date_, date, NA_Date_),
   )

   return(new_date)
}

fullname_to_components <- function(data, name_col) {
   data %<>%
      mutate(
         MUTATE_NAME = toupper(str_squish({{name_col}})),
         MUTATE_NAME = str_replace(MUTATE_NAME, " ,", ","),
         MUTATE_NAME = str_replace(MUTATE_NAME, "\\s(?=[^,]+,)", "|_"),
         MUTATE_NAME = str_replace(MUTATE_NAME, "(?:\\(.*)", ""),
      ) %>%
      # extract name components
      mutate(
         NAME_1 = str_extract(MUTATE_NAME, "([^ ]+) (.*)", 1),
         NAME_2 = str_extract(MUTATE_NAME, "([^ ]+) (.*)", 2),
      ) %>%
      mutate_at(
         .vars = vars(NAME_1, NAME_2),
         ~str_squish(str_replace(., "\\|_", " "))
      ) %>%
      mutate(
         .after     = NAME_2,
         LastName   = if_else(str_detect(NAME_1, ",$"), NAME_1, NAME_2, NAME_2),
         FirstName  = if_else(str_detect(NAME_1, ",$"), NAME_2, NAME_1, NAME_1),
         MiddleName = if_else(str_detect(FirstName, "\\.$"), str_extract(FirstName, " ([:alnum:]*\\.)$"), NA_character_)
      ) %>%
      mutate(
         LastName   = str_replace(LastName, ",$", ""),
         FirstName  = if_else(!is.na(MiddleName), stri_replace_all_fixed(FirstName, MiddleName, ""), FirstName, FirstName),
         MiddleName = str_squish(MiddleName)
      ) %>%
      select(
         -NAME_1,
         -NAME_2,
         -MUTATE_NAME,
      )

   return(data)
}