flow(
   faci_cohort,
   list(
      "01" = "load_reqs",
      "02" = "prepare_data"
   ),
   file.path(getwd(), "src", "dashboard", "facility", "dx_tx_cohort"),
   db
)