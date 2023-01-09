flow(
   prep,
   list(
      "01" = "load_reqs",
      "02" = "data_reg.initial",
      "03" = "data_reg.convert",
      "04" = "data_reg.final",
      "05" = "data_outcome.initial",
      "06" = "data_outcome.convert",
      "07" = "data_outcome.final",
      "x1" = "dedup_new",
      "x2" = "dedup_old",
      "x3" = "dedup_dx",
      "x4" = "dedup_tx"
   ),
   file.path(getwd(), "src", "official", "prep"),
   nhsss
)