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
            condition = is.na(CENTRAL_ID),
            true      = {{pid_col}},
            false     = CENTRAL_ID
         ),
      ) %>%
      relocate(CENTRAL_ID, .before = {{pid_col}})

   return(linelist)
}
