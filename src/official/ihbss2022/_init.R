flow(
   `2022`,
   list(
      "01" = "load_reqs",
      "02" = "download_odk",
      "03" = "flag_monitoring"
   ),
   file.path(getwd(), "src", "official", "ihbss2022"),
   ihbss
)