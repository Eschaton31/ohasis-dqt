check_avg_art_submit <- function(yr, mo) {
   lw_conn  <- ohasis$conn("lw")
   art_data <- tracked_select(lw_conn, glue(r"(
   SELECT COALESCE(SERVICE_FACI, FACI_ID) AS SERVICE_FACI,
          SERVICE_SUB_FACI,
          YEAR(VISIT_DATE)  AS VISIT_YR,
          MONTH(VISIT_DATE) AS VISIT_MO
   FROM ohasis_warehouse.form_art_bc AS art
   WHERE YEAR(VISIT_DATE) = {yr}
     AND MEDICINE_SUMMARY IS NOT NULL
)"), "ART Submissions")
   dbDisconnect(lw_conn)

   months_past <- art_data %>%
      filter(VISIT_MO < mo) %>%
      ohasis$get_faci(
         list(TX_HUB = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
         "code",
         c("TX_REG", "TX_PROV", "TX_MUNC")
      ) %>%
      group_by(TX_REG, TX_HUB, VISIT_MO) %>%
      summarise(
         VISITS = n()
      ) %>%
      ungroup() %>%
      group_by(TX_REG, TX_HUB) %>%
      summarise(
         AVG_VISITS_PASTMOS = floor(mean(VISITS))
      ) %>%
      ungroup()

   months_curr <- art_data %>%
      filter(VISIT_MO == mo) %>%
      ohasis$get_faci(
         list(TX_HUB = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
         "code",
         c("TX_REG", "TX_PROV", "TX_MUNC")
      ) %>%
      group_by(TX_REG, TX_HUB) %>%
      summarise(
         NUM_VISITS_CURRMO = n()
      ) %>%
      ungroup()

   submissions <- months_past %>%
      full_join(months_curr, join_by(TX_REG, TX_HUB)) %>%
      mutate_at(
         .vars = vars(AVG_VISITS_PASTMOS, NUM_VISITS_CURRMO),
         ~as.integer(.)
      )

   submissions %<>%
      mutate(
         NEW_COMPARED_TO_AVG = (NUM_VISITS_CURRMO / AVG_VISITS_PASTMOS),
         SUBMISSION_REMARKS  = case_when(
            NEW_COMPARED_TO_AVG <= .60 ~ "partial",
            is.na(NUM_VISITS_CURRMO) ~ "no submission",
            is.na(AVG_VISITS_PASTMOS) ~ "first time submission",
            TRUE ~ ""
         )
      ) %>%
      rename_all(
         ~case_when(
            . == "NUM_VISITS_CURRMO" ~ stri_c("NUM_VISITS_", yr, stri_pad_left(mo, 2, "0")),
            . == "AVG_VISITS_PASTMOS" ~ stri_c("AVG_VISITS_", yr),
            TRUE ~ .
         )
      )

   return(submissions)
}

submissions <- check_avg_art_submit(2023, 08)
submissions %>%
   as_tibble() %>%
   write_sheet("1QZ8Tb1BcE6djJxFUqPHvz8QENXRDPAx2OXBUdApb7R4", "OHASIS-Encoded")
