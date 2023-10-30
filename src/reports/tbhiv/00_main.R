report <- new.env()

report$wd <- file.path(getwd(), "src", "reports", "tbhiv")

report$img     <- list(
   doh = knitr::image_uri(file.path(report$wd, "logo_doh.png")),
   eb  = knitr::image_uri(file.path(report$wd, "logo_eb.png"))
)
report$src     <- list(
   js     = file.path(getwd(), "src", "templates", "paged.polyfill.js"),
   css    = file.path(getwd(), "src", "templates", "nhsss-reports.css"),
   header = file.path(getwd(), "src", "templates", "doh-eb-letterhead.html")
)
report$src     <- lapply(report$src, read_file)
report$src$css <- template_replace(report$src$css, c(logo_doh = report$img$doh, logo_eb = report$img$eb))

report$data <- read_dta("H:/System/HARP/TB-HIV/2023-2ndQr/20230814_tbhiv_2023-S1.dta")
