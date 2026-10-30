flow(
   prep,
   list(
      "01" = "load_reqs",
      "02" = "data_prep_offer",
      "03" = "data_prep_curr",
      "x1" = "dedup_new",
      "x2" = "dedup_old",
      "x3" = "dedup_dx",
      "x4" = "dedup_tx"
   ),
   file.path(getwd(), "src", "official", "prep"),
   .GlobalEnv
)

# helpers used by several steps; step environments inherit from prep
local(envir = prep, source(file.path(wd, "_shared.R"), local = TRUE))