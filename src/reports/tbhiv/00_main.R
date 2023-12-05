report <- new.env()

report$params <- list(
   min_mo = 7,
   max_mo = 9,
   yr     = 2023
)

report$wd <- file.path(getwd(), "src", "reports", "tbhiv")

report$img  <- list(
   doh = knitr::image_uri(file.path(report$wd, "logo_doh.png")),
   eb  = knitr::image_uri(file.path(report$wd, "logo_eb.png"))
)
report$data <- read_dta("H:/System/HARP/TB-HIV/2023-3rdQr/20231124_tbhiv_2023-Q1.dta") %>%
   mutate(
      ind_onart    = if_else(onart == 1, 1, 0, 0),
      ind_visit    = if_else(ind_onart == 1 & visit == 1, 1, 0, 0),
      ind_screened = if_else(ind_visit == 1 & screened == 1, 1, 0, 0),
      ind_notb     = if_else(ind_screened == 1 & final_new == 1 & withtb != 1, 1, 0, 0),
      ind_tpt      = if_else(ind_notb == 1 & onipt == 1, 1, 0, 0),
   )

##  parmas ---------------------------------------------------------------------

as_of           <- "November 24, 2023"
date_coverage   <- stri_c(month.name[report$params$min_mo], " - ", month.name[report$params$max_mo], " ", report$params$yr)
date_end        <- stri_c(month.name[report$params$max_mo], " ", report$params$yr)
data_population <- "PLHIV on ART who visited a facility within the period"

##  data -----------------------------------------------------------------------

replace                          <- list(
   n_onart         = format(nrow(filter(report$data, ind_onart == 1)), big.mark = ","),
   n_visit         = format(nrow(filter(report$data, ind_visit == 1)), big.mark = ","),
   n_faci          = format(nrow(distinct(report$data, REAL_HUB))),
   screened_denom  = format(nrow(filter(report$data, ind_visit == 1)), big.mark = ","),
   screened_num    = format(nrow(filter(report$data, ind_screened == 1)), big.mark = ","),
   screened_target = "90%",
   tpt_denom       = format(nrow(filter(report$data, ind_notb == 1)), big.mark = ","),
   tpt_num         = format(nrow(filter(report$data, ind_tpt == 1)), big.mark = ","),
   tpt_target      = "80%"
)
replace[["perc_visit"]]          <- stri_c(format((parse_number(replace$n_visit) / parse_number(replace$n_onart)) * 100, digits = 1), "%")
replace[["screened_perc"]]       <- stri_c(format((parse_number(replace$screened_num) / parse_number(replace$screened_denom)) * 100, digits = 1), "%")
replace[["screened_accomplish"]] <- stri_c(format((parse_number(replace$screened_perc) / parse_number(replace$screened_target)) * 100, digits = 1), "%")
replace[["tpt_perc"]]            <- stri_c(format((parse_number(replace$tpt_num) / parse_number(replace$tpt_denom)) * 100, digits = 1), "%")
replace[["tpt_accomplish"]]      <- stri_c(format((parse_number(replace$tpt_perc) / parse_number(replace$tpt_target)) * 100, digits = 1), "%")

##  demographic table ----------------------------------------------------------

table1         <- r"(
 <table class="data-table">
     <colgroup>
         <col style="width: 5%">
         <col style="width: 35%">
         <col style="width: 20%">
         <col style="width: 20%">
         <col style="width: 20%">
     </colgroup>
     <thead>
         <tr>
             <th colspan="5">DEMOGRAPHIC BREAKDOWN</th>
         </tr>
         <tr>
             <th rowspan="2" colspan="2">Demographics</th>
             <th colspan="2">Sex (at birth)</th>
             <th rowspan="2">Total</th>
         </tr>
         <tr style="border-top: 1pt solid #dee0e0">
             <th>Male</th>
             <th>Female</th>
         </tr>
     </thead>
        <tbody class="group">
            <tr>
                <td colspan="2">PLHIV on ART Screened for TB</td>
            </tr>
            {{screened_demog}}
        </tbody>
        <tbody class="group">
            <tr>
                <td colspan="2">New PLHIV on ART given TPT</td>
            </tr>
            {{tpt_demog}}
        </tbody>
     <tfoot>
         <tr></tr>
     </tfoot>
 </table>
)"
screened_demog <- report$data %>%
   filter(ind_screened == 1) %>%
   mutate(
      n_male   = if_else(sex == "MALE", 1, 0, 0),
      n_female = if_else(sex == "FEMALE", 1, 0, 0),
      n_total  = 1
   ) %>%
   group_by(Age_Band) %>%
   summarise_at(
      .vars = vars(n_male, n_female, n_total),
      ~stri_c(r"(<td class="data">)", format(sum(.), big.mark = ","), "</td>")
   ) %>%
   ungroup() %>%
   mutate(
      Age_Band = stri_c(r"(<td></td><td class="text">)", Age_Band, "</td>"),
      latex    = stri_c("<tr>", Age_Band, n_male, n_female, n_total, "</tr>")
   )
tpt_demog      <- report$data %>%
   filter(ind_tpt == 1) %>%
   mutate(
      n_male   = if_else(sex == "MALE", 1, 0, 0),
      n_female = if_else(sex == "FEMALE", 1, 0, 0),
      n_total  = 1
   ) %>%
   group_by(Age_Band) %>%
   summarise_at(
      .vars = vars(n_male, n_female, n_total),
      ~stri_c(r"(<td class="data">)", format(sum(.), big.mark = ","), "</td>")
   ) %>%
   ungroup() %>%
   mutate(
      Age_Band = stri_c(r"(<td></td><td class="text">)", Age_Band, "</td>"),
      latex    = stri_c("<tr>", Age_Band, n_male, n_female, n_total, "</tr>")
   )

table1 <- template_replace(
   table1,
   c(
      screened_demog = stri_c(screened_demog$latex, collapse = ""),
      tpt_demog      = stri_c(tpt_demog$latex, collapse = "")
   )
)

##  facility table -------------------------------------------------------------

table2 <- r"(
<br/>
 <table class="data-table">
     <colgroup>
         <col style="width: 5%">
         <col style="width: 53%">
         <col style="width: 7%">
         <col style="width: 7%">
         <col style="width: 7%">
         <col style="width: 7%">
         <col style="width: 7%">
         <col style="width: 7%">
     </colgroup>
     <thead>
         <tr>
             <th colspan="8">FACILITY BREAKDOWN</th>
         </tr>
         <tr>
             <th rowspan="2" colspan="2">Facility Name</th>
             <th colspan="3">PLHIV on ART Screened for TB</th>
             <th colspan="3">New PLHIV on ART given TPT</th>
         </tr>
         <tr style="border-top: 1pt solid #dee0e0">
             <th>Denom</th>
             <th>Num</th>
             <th>%</th>
             <th>Denom</th>
             <th>Num</th>
             <th>%</th>
         </tr>
     </thead>
     {{facilities}}
     <tfoot>
         <tr></tr>
     </tfoot>
 </table>
)"

region_row <- function(data, region) {
   faci  <- data %>%
      filter(REAL_REG == region)
   latex <- r"(
<tbody class="group">
   <tr>
       <td colspan="8">{{region}}</td>
       {{faci}}
   </tr>
</tbody>
   )"
   latex <- template_replace(latex, c(region = region, faci = stri_c(collapse = "", faci$latex)))

   return(latex)
}

facilities <- report$data %>%
   group_by(REAL_REG, REAL_HUB) %>%
   summarise_at(
      .vars = vars(ind_visit, ind_screened, ind_notb, ind_tpt),
      ~sum(.)
   ) %>%
   ungroup() %>%
   mutate(
      screened_perc = format((ind_screened / ind_visit) * 100, digits = 1),
      screened_perc = parse_number(screened_perc),
      screened_perc = if_else(ind_screened < ind_visit & screened_perc == 100, 99, screened_perc, screened_perc),
      screened_perc = stri_c(r"(<td class="data">)", coalesce(stri_c(screened_perc, "%"), "-"), "</td>"),

      tpt_perc      = format((ind_tpt / ind_notb) * 100, digits = 1),
      tpt_perc      = parse_number(tpt_perc),
      tpt_perc      = if_else(ind_tpt < ind_notb & tpt_perc == 100, 99, tpt_perc, tpt_perc),
      tpt_perc      = stri_c(r"(<td class="data">)", coalesce(stri_c(tpt_perc, "%"), "-"), "</td>"),

      REAL_HUB      = stri_c(r"(<td></td><td class="text">)", REAL_HUB, "</td>"),
   ) %>%
   mutate_at(
      .vars = vars(ind_visit, ind_screened, ind_notb, ind_tpt),
      ~stri_c(r"(<td class="data">)", format(., big.mark = ","), "</td>")
   ) %>%
   mutate(
      latex         = stri_c("<tr>", REAL_HUB, ind_visit, ind_screened, screened_perc, ind_notb, ind_tpt, tpt_perc, "</tr>"),
   )

regions <- unique(facilities$REAL_REG)
regions <- lapply(regions, region_row, data = facilities)

table2 <- template_replace(table2, c(facilities = stri_c(collapse = "", regions)))

##  replace with data ----------------------------------------------------------

report$src     <- list(
   js     = file.path(getwd(), "src", "templates", "paged.polyfill.js"),
   css    = file.path(getwd(), "src", "templates", "nhsss-reports.css"),
   header = file.path(getwd(), "src", "templates", "doh-eb-letterhead.html")
)
report$src     <- lapply(report$src, read_file)
report$src$css <- template_replace(report$src$css, c(logo_doh = report$img$doh, logo_eb = report$img$eb))

header       <- stri_c("<style>", report$src$css, "</style>")
html_content <- stri_c(table1, table2)

html <- template_replace(
   read_file(file.path(report$wd, "template.html")),
   c(
      js         = file.path(getwd(), "src", "reports", "tbhiv", "paged.polyfill.js"),
      header     = header,
      letterhead = read_file(file.path(getwd(), "src", "templates", "doh-eb-letterhead.html"))
   )
)
html <- template_replace(
   html,
   c(
      as_of           = as_of,
      date_coverage   = date_coverage,
      date_end        = date_end,
      data_population = data_population
   )
)
html <- template_replace(html, replace)
html <- template_replace(html, c(html_content = html_content))
write_file(html, "H:/try_tbhiv.html")
