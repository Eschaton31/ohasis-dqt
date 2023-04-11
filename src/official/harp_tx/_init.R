flow(
   harp_tx,
   list(
      "01" = "load_reqs",
      "02" = "data_reg.initial",
      "03" = "data_reg.convert",
      "04" = "data_reg.final",
      "05" = "data_outcome.initial",
      "06" = "data_outcome.convert",
      "07" = "data_outcome.final",
      "08" = "output",
      "x1" = "dedup_new",
      "x2" = "dedup_old",
      "x3" = "dedup_dx"
   ),
   file.path(getwd(), "src", "official", "harp_tx"),
   nhsss
)