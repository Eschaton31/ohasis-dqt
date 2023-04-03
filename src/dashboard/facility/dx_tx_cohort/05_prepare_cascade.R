gen_cascade <- function(data) {
   cascade    <- list()
   cascade$dx <- data$dx %>%
      mutate(
         linkage_facility = case_when(
            FACI_ID == CURR_FACI ~ "same",
            FACI_ID != CURR_FACI ~ "transfer",
            is.na(CURR_FACI) ~ "(not enrolled)",
            TRUE ~ "(no data)"
         )
      ) %>%
      mutate_at(
         .vars = vars(outcome, outcome_new),
         ~if_else(is.na(.), "(not enrolled)", ., .)
      ) %>%
      group_by(
         FACI_ID,
         dx_age_c1,
         dx_age_c2,
         curr_age_c1,
         curr_age_c2,
         kap_type,
         sex,
         mot,
         linkage_facility,
         outcome,
         outcome_new
      ) %>%
      summarise_at(
         .vars = vars(
            dx,
            dx_plhiv,
            everonart,
            everonart_plhiv,
            onart,
            onart_new,
            artestablish,
            artestablish_new,
            vltested,
            vltested_new,
            vlsuppress,
            vlsuppress_50
         ),
         ~sum(., na.rm = TRUE)
      ) %>%
      pivot_longer(
         cols      = c(
            dx,
            dx_plhiv,
            everonart,
            everonart_plhiv,
            onart,
            onart_new,
            artestablish,
            artestablish_new,
            vltested,
            vltested_new,
            vlsuppress,
            vlsuppress_50
         ),
         names_to  = "indicator",
         values_to = "total"
      )

   cascade$tx <- data$tx %>%
      mutate_at(
         .vars = vars(outcome, outcome_new),
         ~case_when(
            FACI_ID != CURR_FACI ~ "transout - other facility",
            FACI_ID == CURR_FACI ~ .,
            TRUE ~ .
         )
      ) %>%
      group_by(
         FACI_ID,
         dx_age_c1,
         dx_age_c2,
         curr_age_c1,
         curr_age_c2,
         kap_type,
         sex,
         mot,
         linkage_facility,
         outcome,
         outcome_new
      ) %>%
      summarise_at(
         .vars = vars(
            dx,
            dx_plhiv,
            everonart,
            everonart_plhiv,
            onart,
            onart_new,
            artestablish,
            artestablish_new,
            vltested,
            vltested_new,
            vlsuppress,
            vlsuppress_50
         ),
         ~sum(., na.rm = TRUE)
      ) %>%
      pivot_longer(
         cols      = c(
            dx,
            dx_plhiv,
            everonart,
            everonart_plhiv,
            onart,
            onart_new,
            artestablish,
            artestablish_new,
            vltested,
            vltested_new,
            vlsuppress,
            vlsuppress_50
         ),
         names_to  = "indicator",
         values_to = "total"
      )

   cascade$prep <- data$prep %>%
      mutate(
         linkage_facility = case_when(
            FACI_ID == CURR_FACI ~ "same",
            FACI_ID != CURR_FACI ~ "transfer",
            is.na(CURR_FACI) ~ "(not enrolled)",
            TRUE ~ "(no data)"
         )
      ) %>%
      group_by(
         FACI_ID,
         curr_age_c1,
         curr_age_c2,
         kap_type,
         sex,
         linkage_facility,
         prep_type
      ) %>%
      summarise_at(
         .vars = vars(
            prep_screen,
            prep_elig,
            prep_ineligible
         ),
         ~sum(., na.rm = TRUE)
      ) %>%
      pivot_longer(
         cols      = c(
            prep_screen,
            prep_elig,
            prep_ineligible
         ),
         names_to  = "indicator",
         values_to = "total"
      ) %>%
      bind_rows(
         data$nr %>%
            mutate(test_nr = 1) %>%
            rename(
               prep_offer = PREP_OFFER
            ) %>%
            group_by(
               FACI_ID,
               curr_age_c1,
               curr_age_c2,
               kap_type,
               sex,
            ) %>%
            summarise_at(
               .vars = vars(
                  test_nr,
                  prep_offer
               ),
               ~sum(., na.rm = TRUE)
            ) %>%
            pivot_longer(
               cols      = c(
                  test_nr,
                  prep_offer
               ),
               names_to  = "indicator",
               values_to = "total"
            )
      )

   cascade$dx_tx <- cascade$dx %>%
      mutate(
         data_src = "dx"
      ) %>%
      bind_rows(
         cascade$tx %>%
            mutate(
               data_src = "tx"
            )
      )

   return(cascade)
}

.init <- function(envir = parent.env(environment())) {
   p         <- envir
   p$cascade <- gen_cascade(p$data)
}