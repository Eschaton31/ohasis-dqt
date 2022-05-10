data <- limiter %>%
   inner_join(
      y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_key_pop")),
      by = "REC_ID"
   ) %>%
   mutate(
      IS_KP = case_when(
         IS_KP == "0" ~ "0_No",
         IS_KP == "1" ~ "1_Yes"
      )
   ) %>%
   pivot_wider(
      id_cols     = REC_ID,
      names_from  = KP,
      values_from = c("IS_KP", "KP_OTHER"),
   ) %>%
   rename_all(
      ~case_when(
         . == "IS_KP_1" ~ "KP_PDL",
         . == "IS_KP_2" ~ "KP_TG",
         . == "IS_KP_3" ~ "KP_PWID",
         . == "IS_KP_5" ~ "KP_MSM",
         . == "IS_KP_6" ~ "KP_SW",
         . == "IS_KP_7" ~ "KP_OFW",
         . == "IS_KP_8" ~ "KP_PARTNER",
         . == "KP_OTHER_8888" ~ "KP_OTHER",
         TRUE ~ .
      )
   ) %>%
   select(
      REC_ID,
      any_of(
         c(
            "KP_PDL",
            "KP_TG",
            "KP_PWID",
            "KP_MSM",
            "KP_SW",
            "KP_OFW",
            "KP_PARTNER",
            "KP_OTHER"
         )
      )
   )