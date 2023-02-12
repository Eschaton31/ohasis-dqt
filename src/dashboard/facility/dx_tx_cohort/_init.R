flow(
   faci_cohort,
   list(
      "01" = "load_reqs",
      "02" = "prepare_harp",
      "03" = "upload_db"
   ),
   file.path(getwd(), "src", "dashboard", "facility", "dx_tx_cohort"),
   db
)