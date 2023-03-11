flow(
   pepfar,
   list(
      "01" = "load_reqs",
      "02" = "prepare_tx",
      "03" = "prepare_prep",
      "04" = "prepare_reach",
      "05" = "aggregate_data",
      "06" = "conso_flat"
   ),
   file.path(getwd(), "src", "official", "dsa", "pepfar"),
   .GlobalEnv
)