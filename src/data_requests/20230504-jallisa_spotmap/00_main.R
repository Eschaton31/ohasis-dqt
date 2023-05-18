pacman::p_load(
   dplyr,
   sf,
   viridis,
   leaflet,
   leaflegend,
   RColorBrewer,
   mapview,
   webshot2,
   nngeo
)
dr     <- new.env()
epictr <- new.env()

# set params
dr$dirs      <- list()
dr$dirs$refs <- file.path(getwd(), "src", "official", "hivepicenter")
dr$dirs$wd   <- file.path(getwd(), "src", "data_requests", "20230504-jallisa_spotmap")

# get references
source(file.path(dr$dirs$refs, "00_refs.R"))
source(file.path(dr$dirs$refs, "02_estimates.R"))

dr$sites$kap_se <- read_dta("C:/Users/johnb/Downloads/20230223_projected_psebymuncity.dta") %>%
   select(
      region       = 1,
      province     = 3,
      muncity      = 4,
      kap_pop_2024 = forbene
   ) %>%
   mutate_all(~as.character(.)) %>%
   mutate(
      region = gsub(".0$", "", region)
   ) %>%
   mutate(
      muncity = case_when(
         muncity == "LAPU LAPU" ~ "LAPU-LAPU",
         muncity == "CEBU CITY" ~ "CEBU",
         muncity == "QUEZON CITY" ~ "QUEZON",
         TRUE ~ muncity
      )
   ) %>%
   left_join(
      y  = epictr$ref_addr %>%
         select(
            region   = NHSSS_REG,
            province = NHSSS_PROV,
            muncity  = NHSSS_MUNC,
            PSGC_REG,
            PSGC_PROV,
            PSGC_AEM
         ),
      by = join_by(region, province, muncity)
   ) %>%
   mutate(
      kap_pop_2024 = round(as.numeric(kap_pop_2024), -2)
   )

##  Load harp data -------------------------------------------------------------

# get diagnosis data
data <- hs_data("harp_full", "reg", 2023, 3) %>%
   read_dta(
      col_select = c(
         idnum,
         region,
         province,
         muncity,
         dead,
         mort,
         outcome,
         everonart,
         onart,
         baseline_vl,
         vlp12m
      )
   ) %>%
   mutate(
      overseas_addr = case_when(
         muncity == "OUT OF COUNTRY" ~ 1,
         TRUE ~ 0
      ),

      muncity       = case_when(
         muncity == "PINAMUNGAHAN" & province == "CEBU" ~ "PINAMUNGAJAN",
         muncity == "SAN JUAN" & province == "BULACAN" ~ "MALOLOS",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ muncity
      ),
      province      = case_when(
         muncity == "UNKNOWN" & province == "NCR" ~ "UNKNOWN",
         province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ province
      ),
      region        = case_when(
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ region
      ),

      mortality     = if_else(
         (dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"),
         0,
         1,
         1
      ),
      dx            = if_else(!is.na(idnum), 1, 0, 0),
      dx_plhiv      = if_else(dx == 1 & mortality == 0, 1, 0, 0),
      vl_tested     = if_else(
         onart == 1 &
            (is.na(baseline_vl) | baseline_vl == 0) &
            !is.na(vlp12m),
         1,
         0,
         0
      ),
      vl_suppressed = if_else(
         onart == 1 &
            (is.na(baseline_vl) | baseline_vl == 0) &
            vlp12m == 1,
         1,
         0,
         0
      ),
   ) %>%
   left_join(
      y  = epictr$ref_addr %>%
         select(
            region   = NHSSS_REG,
            province = NHSSS_PROV,
            muncity  = NHSSS_MUNC,
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            PSGC_AEM
         ),
      by = join_by(region, province, muncity)
   ) %>%
   left_join(
      y = epictr$ref_addr %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            NAME_MUNC,
            NAME_REG,
         )
   ) %>%
   distinct(idnum, .keep_all = TRUE)

data_eb <- data %>%
   group_by(PSGC_REG, PSGC_PROV, PSGC_AEM) %>%
   summarise_at(
      .vars = vars(dx, dx_plhiv, everonart, onart, vl_tested, vl_suppressed),
      ~sum(., na.rm = TRUE)
   ) %>%
   ungroup() %>%
   # get estimated
   full_join(
      y  = epictr$estimates$cata_rotp %>%
         filter(report_yr == 2022) %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_AEM,
            est
         ) %>%
         mutate_at(
            .vars = vars(starts_with("PSGC_")),
            ~gsub("^PH", "", .)
         ),
      by = join_by(PSGC_REG, PSGC_PROV, PSGC_AEM)
   ) %>%
   mutate(
      dx_coverage    = (dx_plhiv / est) * 100,
      tx_coverage    = (onart / dx_plhiv) * 100,
      vl_coverage    = (vl_tested / onart) * 100,
      vl_suppression = (vl_suppressed / vl_tested) * 100,
   )

## Process map resource --------------------------------------------------------

readRenviron(".Renviron")
spdf            <- read_sf(Sys.getenv("GEOJSON_PH"))
spdf_merged_mla <- spdf %>%
   # merge manila geometry into one
   mutate(
      ADM2_EN    = case_when(
         ADM3_PCODE == "PH129804000" ~ "Cotabato",
         TRUE ~ ADM2_EN
      ),
      ADM2_PCODE = case_when(
         ADM3_PCODE == "PH129804000" ~ "PH124700000",
         TRUE ~ ADM2_PCODE
      ),
      ADM3_EN    = case_when(
         ADM2_PCODE == "PH133900000" ~ ADM2_EN,
         TRUE ~ ADM3_EN
      ),
      ADM3_PCODE = case_when(
         ADM2_PCODE == "PH133900000" ~ ADM2_PCODE,
         TRUE ~ ADM3_PCODE
      ),
   ) %>%
   group_by(ADM1_EN, ADM1_PCODE, ADM2_PCODE, ADM3_EN, ADM3_PCODE) %>%
   summarize(
      geometry = st_union(st_make_valid(geometry))
      # geometry = nngeo::st_remove_holes(st_union(st_make_valid(geometry)))
   ) %>%
   ungroup() %>%
   mutate_at(
      .vars = vars(starts_with("ADM")),
      ~gsub("^PH", "", .)
   )

spdf_aem <- spdf_merged_mla %>%
   left_join(
      y = epictr$ref_addr %>%
         select(
            ADM1_PCODE = PSGC_REG,
            ADM2_PCODE = PSGC_PROV,
            ADM3_PCODE = PSGC_MUNC,
            PSGC_AEM,
            NAME_AEM
         )
   ) %>%
   group_by(ADM1_EN, ADM1_PCODE, PSGC_AEM, NAME_AEM) %>%
   summarize(
      # geometry = st_union(geometry)
      # geometry = st_union(st_make_valid(st_set_precision(geometry, 1e6)))
      geometry = st_union(st_make_valid(geometry))
      # geometry = nngeo::st_remove_holes(st_union(st_make_valid(geometry)))
   ) %>%
   ungroup()

spdf_eb <- spdf_aem %>%
   # filter(ADM1_PCODE == "PH070000000") %>%
   left_join(
      y  = data_eb,
      by = join_by(PSGC_AEM)
   ) %>%
   left_join(
      y          = dr$sites$kap_se %>%
         group_by(PSGC_AEM) %>%
         summarise(kap_pop_2024 = sum(kap_pop_2024, na.rm = TRUE)),
      na_matches = "never",
   )

##  Process facilities information ---------------------------------------------

faci              <- list()
db_conn           <- ohasis$conn("db")
faci$service_dx   <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.facility_service WHERE SERVICE IN (?)", "101102")
faci$service_tx   <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.facility_service WHERE SERVICE IN (?)", "101201")
faci$service_cfbs <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.facility_service WHERE SERVICE IN (?)", list(c("101103", "101104")))
faci$service_prep <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.facility_service WHERE SERVICE IN (?)", "101301")
dbDisconnect(db_conn)
rm(db_conn)


##  crcl facilities ------------------------------------------------------------

faci$faci_crcl <- ohasis$ref_faci %>%
   inner_join(
      y  = faci$service_dx %>% distinct(FACI_ID),
      by = "FACI_ID"
   ) %>%
   mutate(
      IS_HUB = if_else(nchar(FACI_CODE) >= 3, 1, 0),
      drop   = case_when(
         FACI_ID == "130023" & SUB_FACI_ID == "130023_002" ~ 1,
         FACI_ID == "130023" & SUB_FACI_ID == "" ~ 1,
         FACI_ID == "130023" & SUB_FACI_ID != "" ~ 0,
         !(FACI_ID %in% c("130001", "130605")) & SUB_FACI_ID == "" ~ 0,
         !(FACI_ID %in% c("130001", "130605")) & SUB_FACI_ID != "" ~ 1,
         FACI_ID %in% c("130001", "130605") & SUB_FACI_ID != "" ~ 1,
         TRUE ~ 9999
      )
   ) %>%
   filter(drop == 0) %>%
   select(FACI_ID, SUB_FACI_ID, FACI_NAME, LONG, LAT, FACI_CODE, FACI_ADDR)

##  tx facilities --------------------------------------------------------------

faci$faci_tx <- ohasis$ref_faci %>%
   inner_join(
      y  = faci$service_tx %>% select(FACI_ID),
      by = "FACI_ID"
   ) %>%
   group_by(FACI_ID) %>%
   mutate(num_faci = n()) %>%
   ungroup() %>%
   mutate(
      IS_HUB = if_else(nchar(FACI_CODE) >= 3, 1, 0),
      drop   = case_when(
         FACI_ID == "130023" & SUB_FACI_ID != "" ~ 1,
         FACI_ID %in% c("130001", "130605") & SUB_FACI_ID == "" ~ 1,
         !(FACI_ID %in% c("130001", "130605")) & SUB_FACI_ID != "" ~ 1,
         FACI_ID %in% c("130001", "130605") & SUB_FACI_ID != "" ~ 0,
         TRUE ~ 0
      )
   ) %>%
   filter(drop == 0) %>%
   select(FACI_ID, SUB_FACI_ID, FACI_NAME, LONG, LAT, FACI_CODE, FACI_ADDR)

##  prep facilities ------------------------------------------------------------

faci$faci_prep <- ohasis$ref_faci %>%
   inner_join(
      y  = faci$service_prep %>% select(FACI_ID),
      by = "FACI_ID"
   ) %>%
   group_by(FACI_ID) %>%
   mutate(num_faci = n()) %>%
   ungroup() %>%
   mutate(
      IS_HUB = if_else(nchar(FACI_CODE) >= 3, 1, 0),
      drop   = case_when(
         FACI_ID == "130023" & SUB_FACI_ID != "" ~ 1,
         FACI_ID %in% c("130001", "130605") & SUB_FACI_ID == "" ~ 1,
         !(FACI_ID %in% c("130001", "130605")) & SUB_FACI_ID != "" ~ 1,
         FACI_ID %in% c("130001", "130605") & SUB_FACI_ID != "" ~ 0,
         TRUE ~ 0
      )
   ) %>%
   filter(drop == 0) %>%
   select(FACI_ID, SUB_FACI_ID, FACI_NAME, LONG, LAT, FACI_CODE, FACI_ADDR)

##  Plot interactive map -------------------------------------------------------

# Create a color palette for the map:
color_dist      <- colorBin("YlOrBr", domain = spdf_eb$est, na.color = "transparent", bins = c(0, 100, 500, 1000, 2000, 5000, Inf))
bin_dxcoverage  <- colorBin("RdYlGn", domain = spdf_eb$dx_coverage, na.color = "transparent", bins = c(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100))
bin_txcoverage  <- colorBin("RdYlGn", domain = spdf_eb$tx_coverage, na.color = "transparent", bins = c(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100))
bin_vlcoverage  <- colorBin("RdYlGn", domain = spdf_eb$vl_coverage, na.color = "transparent", bins = c(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100))
bin_suppression <- colorBin("RdYlGn", domain = spdf_eb$vl_suppression, na.color = "transparent", bins = c(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100))

# Prepare the text for tooltips:
chloropleth_text <- stri_c(
   "<b>", spdf_eb$NAME_AEM, "</b><br/>",
   "KP Size Estimate: <b><i>", format(spdf_eb$kap_pop_2024, big.mark = ","), "</i></b><br/>",
   "Estimated PLHIV: <b><i>", format(spdf_eb$est, big.mark = ","), "</i></b><br/>",
   "Diagnosed PLHIV: <b><i>", format(spdf_eb$dx_plhiv, big.mark = ","), "</i></b>"
) %>%
   lapply(htmltools::HTML)
text_dxcoverage  <- stri_c(
   "<b>", spdf_eb$NAME_AEM, "</b><br/>",
   "KP Size Estimate: <b><i>", format(spdf_eb$kap_pop_2024, big.mark = ","), "</i></b><br/>",
   "Estimated PLHIV: <b><i>", format(spdf_eb$est, big.mark = ","), "</i></b><br/>",
   "Diagnosed PLHIV: <b><i>", format(spdf_eb$dx_plhiv, big.mark = ","), "</i></b><br/>",
   "Diagnosis Coverage: <b><i>", format(spdf_eb$dx_coverage, digits = 2), "%</i></b>"
) %>%
   lapply(htmltools::HTML)
text_txcoverage  <- stri_c(
   "<b>", spdf_eb$NAME_AEM, "</b><br/>",
   "Diagnosed PLHIV: <b><i>", format(spdf_eb$dx_plhiv, big.mark = ","), "</i></b><br/>",
   "Alive on ARV: <b><i>", format(spdf_eb$onart, big.mark = ","), "</i></b><br/>",
   "Treatment Coverage: <b><i>", format(spdf_eb$tx_coverage, digits = 2), "%</i></b>"
) %>%
   lapply(htmltools::HTML)
text_vlcoverage  <- stri_c(
   "<b>", spdf_eb$NAME_AEM, "</b><br/>",
   "Alive on ARV: <b><i>", format(spdf_eb$onart, big.mark = ","), "</i></b><br/>",
   "VL Tested: <b><i>", format(spdf_eb$vl_tested, big.mark = ","), "</i></b><br/>",
   "VL Coverage: <b><i>", format(spdf_eb$vl_coverage, digits = 2), "%</i></b>"
) %>%
   lapply(htmltools::HTML)
text_suppression <- stri_c(
   "<b>", spdf_eb$NAME_AEM, "</b><br/>",
   "VL Tested: <b><i>", format(spdf_eb$vl_tested, big.mark = ","), "</i></b><br/>",
   "VL Suppressed (<50): <b><i>", format(spdf_eb$vl_suppressed, big.mark = ","), "</i></b><br/>",
   "VL Suppression: <b><i>", format(spdf_eb$vl_suppression, digits = 2), "%</i></b>"
) %>%
   lapply(htmltools::HTML)


crcl_text <- stri_c(
   "<b>", faci$faci_crcl$FACI_NAME, "</b><br/>",
   gsub(";", "<br/>", faci$faci_crcl$FACI_ADDR)
) %>%
   lapply(htmltools::HTML)

tx_text <- stri_c(
   "<b>", faci$faci_tx$FACI_NAME, "</b><br/>",
   gsub(";", "<br/>", faci$faci_tx$FACI_ADDR)
) %>%
   lapply(htmltools::HTML)

prep_text <- stri_c(
   "<b>", faci$faci_prep$FACI_NAME, "</b><br/>",
   gsub(";", "<br/>", faci$faci_prep$FACI_ADDR)
) %>%
   lapply(htmltools::HTML)

legend_tx   <- r"(<img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMS45OTkgNTExLjk5OSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNTExLjk5OSA1MTEuOTk5OyIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSI+DQo8cGF0aCBzdHlsZT0iZmlsbDojMjlhMDhjOyIgZD0iTTQ1NC44NDgsMTk4Ljg0OGMwLDE1OS4yMjUtMTc5Ljc1MSwzMDYuNjg5LTE3OS43NTEsMzA2LjY4OWMtMTAuNTAzLDguNjE3LTI3LjY5Miw4LjYxNy0zOC4xOTUsMA0KCWMwLDAtMTc5Ljc1MS0xNDcuNDY0LTE3OS43NTEtMzA2LjY4OUM1Ny4xNTMsODkuMDI3LDE0Ni4xOCwwLDI1NiwwUzQ1NC44NDgsODkuMDI3LDQ1NC44NDgsMTk4Ljg0OHoiLz4NCjxwYXRoIHN0eWxlPSJmaWxsOiNiNGUzY2M7IiBkPSJNMjU2LDI5OC44OWMtNTUuMTY0LDAtMTAwLjA0MS00NC44NzktMTAwLjA0MS0xMDAuMDQxUzIwMC44MzgsOTguODA2LDI1Niw5OC44MDYNCglzMTAwLjA0MSw0NC44NzksMTAwLjA0MSwxMDAuMDQxUzMxMS4xNjQsMjk4Ljg5LDI1NiwyOTguODl6Ii8+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8L3N2Zz4NCg==" height=17/>&nbsp;Treatment Hubs)" %>% lapply(htmltools::HTML)
legend_prep <- r"(<img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMS45OTkgNTExLjk5OSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNTExLjk5OSA1MTEuOTk5OyIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSI+DQo8cGF0aCBzdHlsZT0iZmlsbDojNDI4Y2M0OyIgZD0iTTQ1NC44NDgsMTk4Ljg0OGMwLDE1OS4yMjUtMTc5Ljc1MSwzMDYuNjg5LTE3OS43NTEsMzA2LjY4OWMtMTAuNTAzLDguNjE3LTI3LjY5Miw4LjYxNy0zOC4xOTUsMA0KCWMwLDAtMTc5Ljc1MS0xNDcuNDY0LTE3OS43NTEtMzA2LjY4OUM1Ny4xNTMsODkuMDI3LDE0Ni4xOCwwLDI1NiwwUzQ1NC44NDgsODkuMDI3LDQ1NC44NDgsMTk4Ljg0OHoiLz4NCjxwYXRoIHN0eWxlPSJmaWxsOiM4MGE4YzQ7IiBkPSJNMjU2LDI5OC44OWMtNTUuMTY0LDAtMTAwLjA0MS00NC44NzktMTAwLjA0MS0xMDAuMDQxUzIwMC44MzgsOTguODA2LDI1Niw5OC44MDYNCglzMTAwLjA0MSw0NC44NzksMTAwLjA0MSwxMDAuMDQxUzMxMS4xNjQsMjk4Ljg5LDI1NiwyOTguODl6Ii8+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8L3N2Zz4NCg==" height=17/>&nbsp;PrEP Sites)" %>% lapply(htmltools::HTML)
legend_crcl <- r"(<img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMS45OTkgNTExLjk5OSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNTExLjk5OSA1MTEuOTk5OyIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSI+DQo8cGF0aCBzdHlsZT0iZmlsbDojODY1ZWI3OyIgZD0iTTQ1NC44NDgsMTk4Ljg0OGMwLDE1OS4yMjUtMTc5Ljc1MSwzMDYuNjg5LTE3OS43NTEsMzA2LjY4OWMtMTAuNTAzLDguNjE3LTI3LjY5Miw4LjYxNy0zOC4xOTUsMA0KCWMwLDAtMTc5Ljc1MS0xNDcuNDY0LTE3OS43NTEtMzA2LjY4OUM1Ny4xNTMsODkuMDI3LDE0Ni4xOCwwLDI1NiwwUzQ1NC44NDgsODkuMDI3LDQ1NC44NDgsMTk4Ljg0OHoiLz4NCjxwYXRoIHN0eWxlPSJmaWxsOiNhMDhkYjc7IiBkPSJNMjU2LDI5OC44OWMtNTUuMTY0LDAtMTAwLjA0MS00NC44NzktMTAwLjA0MS0xMDAuMDQxUzIwMC44MzgsOTguODA2LDI1Niw5OC44MDYNCglzMTAwLjA0MSw0NC44NzksMTAwLjA0MSwxMDAuMDQxUzMxMS4xNjQsMjk4Ljg5LDI1NiwyOTguODl6Ii8+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8L3N2Zz4NCg==" height=17/>&nbsp;CrCLs)" %>% lapply(htmltools::HTML)

# Final Map
m <- leaflet() %>%
   addTiles() %>%
   addProviderTiles("Esri.WorldGrayCanvas", group = "Gray Overlay") %>%
   addPolygons(
      data             = spdf_eb,
      fillColor        = ~color_dist(est),
      weight           = 2,
      opacity          = 1,
      color            = "white",
      dashArray        = "3",
      fillOpacity      = 0.7,
      highlightOptions = highlightOptions(
         weight       = 5,
         color        = "#666",
         dashArray    = "",
         fillOpacity  = 0.7,
         bringToFront = TRUE),
      label            = chloropleth_text,
      labelOptions     = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group            = "EB Data (MSM & TGW and PLHIV Estimates)"
   ) %>%
   addPolygons(
      data             = spdf_eb,
      fillColor        = ~bin_dxcoverage(dx_coverage),
      weight           = 2,
      opacity          = 1,
      color            = "white",
      dashArray        = "3",
      fillOpacity      = 0.7,
      highlightOptions = highlightOptions(
         weight       = 5,
         color        = "#666",
         dashArray    = "",
         fillOpacity  = 0.7,
         bringToFront = TRUE),
      label            = text_dxcoverage,
      labelOptions     = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group            = "Dx Coverage"
   ) %>%
   addPolygons(
      data             = spdf_eb,
      fillColor        = ~bin_txcoverage(tx_coverage),
      weight           = 2,
      opacity          = 1,
      color            = "white",
      dashArray        = "3",
      fillOpacity      = 0.7,
      highlightOptions = highlightOptions(
         weight       = 5,
         color        = "#666",
         dashArray    = "",
         fillOpacity  = 0.7,
         bringToFront = TRUE),
      label            = text_dxcoverage,
      labelOptions     = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group            = "Tx Coverage"
   ) %>%
   addPolygons(
      data             = spdf_eb,
      fillColor        = ~bin_vlcoverage(vl_coverage),
      weight           = 2,
      opacity          = 1,
      color            = "white",
      dashArray        = "3",
      fillOpacity      = 0.7,
      highlightOptions = highlightOptions(
         weight       = 5,
         color        = "#666",
         dashArray    = "",
         fillOpacity  = 0.7,
         bringToFront = TRUE),
      label            = text_vlcoverage,
      labelOptions     = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group            = "VL Coverage"
   ) %>%
   addPolygons(
      data             = spdf_eb,
      fillColor        = ~bin_suppression(vl_suppression),
      weight           = 2,
      opacity          = 1,
      color            = "white",
      dashArray        = "3",
      fillOpacity      = 0.7,
      highlightOptions = highlightOptions(
         weight       = 5,
         color        = "#666",
         dashArray    = "",
         fillOpacity  = 0.7,
         bringToFront = TRUE),
      label            = text_suppression,
      labelOptions     = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group            = "VL Suppression"
   ) %>%
   addMarkers(
      ~faci$faci_tx$LONG,
      ~faci$faci_tx$LAT,
      data         = spdf_eb,
      icon         = makeIcon(
         iconUrl     = "H:/_QGIS/HIV Treatment Hubs/Marker - Tx Hubs.svg",
         iconWidth   = 20,
         iconHeight  = 20,
         iconAnchorX = 10,
         iconAnchorY = 20,
      ),
      label        = tx_text,
      labelOptions = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group        = "Treatment Hubs"
   ) %>%
   addMarkers(
      ~faci$faci_prep$LONG,
      ~faci$faci_prep$LAT,
      data         = spdf_eb,
      icon         = makeIcon(
         iconUrl     = "H:/_QGIS/HIV Treatment Hubs/Marker - Primary Care.svg",
         iconWidth   = 20,
         iconHeight  = 20,
         iconAnchorX = 10,
         iconAnchorY = 20,
      ),
      label        = prep_text,
      labelOptions = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group        = "PrEP Sites"
   ) %>%
   addMarkers(
      ~faci$faci_crcl$LONG,
      ~faci$faci_crcl$LAT,
      data         = spdf_eb,
      icon         = makeIcon(
         iconUrl     = "H:/_QGIS/HIV Treatment Hubs/Marker - CrCL.svg",
         iconWidth   = 20,
         iconHeight  = 20,
         iconAnchorX = 10,
         iconAnchorY = 20,
      ),
      label        = crcl_text,
      labelOptions = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group        = "CrCLs"
   ) %>%
   addLegend(
      data     = spdf_eb,
      pal      = color_dist,
      values   = ~est,
      opacity  = 0.9,
      position = "bottomleft",
      title    = "Estimated PLHIV",
      group    = "EB Data (MSM & TGW and PLHIV Estimates)"
   ) %>%
   addLegend(
      data      = spdf_eb,
      pal       = bin_dxcoverage,
      values    = ~dx_coverage,
      opacity   = 0.9,
      position  = "bottomleft",
      title     = "Diagnosis Coverage",
      group     = "Dx Coverage",
      labFormat = labelFormat(
         suffix = "%"
      )
   ) %>%
   addLegend(
      data      = spdf_eb,
      pal       = bin_txcoverage,
      values    = ~tx_coverage,
      opacity   = 0.9,
      position  = "bottomleft",
      title     = "Treatment Coverage",
      group     = "Tx Coverage",
      labFormat = labelFormat(
         suffix = "%"
      )
   ) %>%
   addLegend(
      data      = spdf_eb,
      pal       = bin_vlcoverage,
      values    = ~vl_coverage,
      opacity   = 0.9,
      position  = "bottomleft",
      title     = "VL Testing Coverage",
      group     = "VL Coverage",
      labFormat = labelFormat(
         suffix = "%"
      )
   ) %>%
   addLegend(
      data      = spdf_eb,
      pal       = bin_suppression,
      values    = ~vl_suppression,
      opacity   = 0.9,
      position  = "bottomleft",
      title     = "VL Suppression",
      group     = "VL Suppression",
      labFormat = labelFormat(
         suffix = "%"
      )
   ) %>%
   addControl(
      html     = legend_prep,
      position = "bottomleft"
   ) %>%
   addControl(
      html     = legend_tx,
      position = "bottomleft"
   ) %>%
   addControl(
      html     = legend_crcl,
      position = "bottomleft"
   ) %>%
   addLayersControl(
      overlayGroups = c("EB Data (MSM & TGW and PLHIV Estimates)", "Dx Coverage", "Tx Coverage", "VL Coverage", "VL Suppression", "CrCLs", "Treatment Hubs", "PrEP Sites", "Gray Overlay"),
      options       = layersControlOptions(collapsed = F)
   )

m

htmlwidgets::saveWidget(m, "H:/ph_spotmap_facis_est_dx_tx_vl_2023-03.html", selfcontained = TRUE)
htmlwidgets::saveWidget(m, "//192.168.193.228/htdocs/ph_facilities/index.html", selfcontained = FALSE)
mapshot(m, "C:/ph_facilities.html")
