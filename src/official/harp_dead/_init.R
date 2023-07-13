flow(
   harp_dead,
   list(
      "01" = "load_reqs",
      "02" = "data_mortality",
      "x1" = "dedup_new",
      "x2" = "dedup_old",
      "x3" = "dedup_dx"
   ),
   file.path(getwd(), "src", "official", "harp_dead"),
   .GlobalEnv
)