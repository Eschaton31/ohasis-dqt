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
gffr   <- new.env()
epictr <- new.env()

# set params
gffr$dirs      <- list()
gffr$dirs$refs <- file.path(getwd(), "src", "official", "hivepicenter")
gffr$dirs$wd   <- file.path(getwd(), "src", "official", "harp_maps")

# get references
source(file.path(gffr$dirs$refs, "00_refs.R"))
source(file.path(gffr$dirs$refs, "02_estimates.R"))

gffr$sites         <- list()
gffr$sites$gf_2426 <- read_sheet("1Hss06mQ8YwCKZbbdY6qopYXOMyuVhGjj-wN3a3HFKoU", "FINAL GF SITES", range = "A:D") %>%
   select(
      region   = 1,
      province = 2,
      muncity  = 3
   ) %>%
   mutate_all(~as.character(.)) %>%
   mutate(
      muncity = case_when(
         muncity == "CEBU CITY" ~ "CEBU",
         muncity == "QUEZON CITY" ~ "QUEZON",
         TRUE ~ muncity
      )
   ) %>%
   filter(muncity != "PROVINCE-WIDE") %>%
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
   bind_rows(
      epictr$ref_addr %>%
         filter(
            NHSSS_REG == "7",
            NHSSS_PROV == "CEBU",
            !(NHSSS_MUNC %in% c("CEBU", "MANDAUE", "LAPU-LAPU", "TALISAY", "UNKNOWN"))
         ) %>%
         select(
            region   = NHSSS_REG,
            province = NHSSS_PROV,
            muncity  = NHSSS_MUNC,
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            PSGC_AEM
         )
   )

tmp <- tempfile(fileext = ".xlsx")
drive_download("https://docs.google.com/spreadsheets/d/1fkj8xvAKuJfpVxoo3mPNZz8AmeNZgd-f", tmp, overwrite = TRUE)
gffr$sites$gf_2123 <- read_xlsx(tmp, "All 301 Protects Sites", range = "A2:C303") %>%
   select(
      region   = 1,
      province = 2,
      muncity  = 3
   ) %>%
   mutate_all(~as.character(.)) %>%
   mutate(
      province = case_when(
         province == "METRO MANILA" ~ "NCR",
         province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         TRUE ~ province
      ),
      muncity  = case_when(
         muncity == "CEBU CITY" ~ "CEBU",
         muncity == "QUEZON CITY" ~ "QUEZON",
         muncity == "ISLAND GARDEN SAMAL" ~ "SAMAL",
         stri_detect_fixed(muncity, "(") ~ substr(muncity, 1, stri_locate_first_fixed(muncity, " (") - 1),
         TRUE ~ muncity
      )
   ) %>%
   # filter(muncity != "PROVINCE-WIDE") %>%
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
   )
unlink(tmp)
rm(tmp)

gffr$sites$kap_se <- read_dta("C:/data/20230223_projected_psebymuncity.dta") %>%
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
data <- hs_data("harp_full", "reg", 2022, 12) %>%
   read_dta(
      col_select = c(
         idnum,
         region,
         province,
         muncity,
         dead,
         mort,
         outcome
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
      y          = gffr$sites$gf_2123 %>%
         select(PSGC_REG, PSGC_PROV, PSGC_MUNC) %>%
         distinct_all() %>%
         mutate(gf_2021_2023 = 1),
      na_matches = "never"
   ) %>%
   left_join(
      y          = gffr$sites$gf_2426 %>%
         select(PSGC_REG, PSGC_PROV, PSGC_MUNC) %>%
         distinct_all() %>%
         mutate(gf_2024_2026 = 1),
      na_matches = "never"
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
   mutate(
      PSGC_GF = case_when(
         gf_2024_2026 == 1 ~ PSGC_MUNC,
         gf_2021_2023 == 1 ~ PSGC_MUNC,
         TRUE ~ PSGC_REG
      ),
      NAME_GF = case_when(
         gf_2024_2026 == 1 ~ NAME_MUNC,
         gf_2021_2023 == 1 ~ NAME_MUNC,
         TRUE ~ NAME_REG
      ),
   ) %>%
   distinct(idnum, .keep_all = TRUE)

data_eb <- data %>%
   group_by(PSGC_REG, PSGC_PROV, PSGC_AEM) %>%
   summarise(
      dx       = sum(dx, na.rm = TRUE),
      dx_plhiv = sum(dx_plhiv, na.rm = TRUE),
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
   )

data_gf <- data %>%
   group_by(PSGC_REG, PSGC_GF, NAME_GF, gf_2021_2023, gf_2024_2026) %>%
   summarise(
      dx       = sum(dx, na.rm = TRUE),
      dx_plhiv = sum(dx_plhiv, na.rm = TRUE),
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
      y          = gffr$sites$kap_se %>%
         group_by(PSGC_AEM) %>%
         summarise(kap_pop_2024 = sum(kap_pop_2024, na.rm = TRUE)),
      na_matches = "never",
   )

spdf_gf <- spdf_merged_mla %>%
   left_join(
      y = data %>%
         select(
            ADM1_PCODE = PSGC_REG,
            ADM2_PCODE = PSGC_PROV,
            ADM3_PCODE = PSGC_MUNC,
            PSGC_GF,
            NAME_GF
         ) %>%
         distinct_all()
   ) %>%
   mutate(
      PSGC_GF = if_else(!is.na(PSGC_GF), PSGC_GF, ADM1_PCODE),
      NAME_GF = if_else(!is.na(NAME_GF), NAME_GF, ADM1_EN),
   ) %>%
   group_by(ADM1_EN, ADM1_PCODE, PSGC_GF, NAME_GF) %>%
   summarize(
      # geometry = st_union(geometry)
      # geometry = st_union(st_make_valid(st_set_precision(geometry, 1e6)))
      geometry = st_union(st_make_valid(geometry))
   ) %>%
   ungroup() %>%
   left_join(
      y  = data_gf,
      by = join_by(PSGC_GF),
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
color_dist    <- colorBin("YlOrBr", domain = spdf_eb$est, na.color = "transparent", bins = c(0, 100, 500, 1000, 2000, 5000, Inf))
color_gf_2123 <- colorBin("#446879", domain = spdf_gf$gf_2021_2023, na.color = "transparent", bins = c(1, 2))
color_gf_2426 <- colorBin("#1F76B7", domain = spdf_gf$gf_2024_2026, na.color = "transparent", bins = c(1, 2))

# Prepare the text for tooltips:
chloropleth_text <- stri_c(
   "<b>", spdf_eb$NAME_AEM, "</b><br/>",
   "KP Size Estimate: <b><i>", format(spdf_eb$kap_pop_2024, big.mark = ","), "</i></b><br/>",
   "Estimated PLHIV: <b><i>", format(spdf_eb$est, big.mark = ","), "</i></b><br/>",
   "Diagnosed PLHIV: <b><i>", format(spdf_eb$dx_plhiv, big.mark = ","), "</i></b>"
) %>%
   lapply(htmltools::HTML)

gf_2123_text <- ifelse(
   !is.na(spdf_gf$gf_2021_2023),
   stri_c(
      r"(
      <img src="C:/Users/Administrator/Downloads/The_Global_Fund_logo.png" height=17/><br/>
      <b style="color: #e42313; ">GF Site (2021-2023)</b>
      <br style="display: block; margin: -20px 0 0 0"/>
      )",
      "<b>", spdf_gf$NAME_GF.x, "</b><br/>",
      "Diagnosed PLHIV: <b><i>", format(spdf_gf$dx_plhiv, big.mark = ","), "</i></b>"
   ),
   stri_c(
      "<b>", spdf_gf$NAME_GF.x, "</b><br/>",
      "Diagnosed PLHIV: <b><i>", format(spdf_gf$dx_plhiv, big.mark = ","), "</i></b>"
   )
) %>%
   lapply(htmltools::HTML)

gf_2426_text <- ifelse(
   !is.na(spdf_gf$gf_2024_2026),
   stri_c(
      r"(
      <img src="C:/Users/Administrator/Downloads/The_Global_Fund_logo.png" height=17/><br/>
      <b style="color: #e42313; ">GF Site (2024-2026)</b>
      <br style="display: block; margin: -20px 0 0 0"/>
      )",
      "<b>", spdf_gf$NAME_GF.x, "</b><br/>",
      "Diagnosed PLHIV: <b><i>", format(spdf_gf$dx_plhiv, big.mark = ","), "</i></b>"
   ),
   stri_c(
      "<b>", spdf_gf$NAME_GF.x, "</b><br/>",
      "Diagnosed PLHIV: <b><i>", format(spdf_gf$dx_plhiv, big.mark = ","), "</i></b>"
   )
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
legend_prep   <- r"(<img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMS45OTkgNTExLjk5OSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNTExLjk5OSA1MTEuOTk5OyIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSI+DQo8cGF0aCBzdHlsZT0iZmlsbDojNDI4Y2M0OyIgZD0iTTQ1NC44NDgsMTk4Ljg0OGMwLDE1OS4yMjUtMTc5Ljc1MSwzMDYuNjg5LTE3OS43NTEsMzA2LjY4OWMtMTAuNTAzLDguNjE3LTI3LjY5Miw4LjYxNy0zOC4xOTUsMA0KCWMwLDAtMTc5Ljc1MS0xNDcuNDY0LTE3OS43NTEtMzA2LjY4OUM1Ny4xNTMsODkuMDI3LDE0Ni4xOCwwLDI1NiwwUzQ1NC44NDgsODkuMDI3LDQ1NC44NDgsMTk4Ljg0OHoiLz4NCjxwYXRoIHN0eWxlPSJmaWxsOiM4MGE4YzQ7IiBkPSJNMjU2LDI5OC44OWMtNTUuMTY0LDAtMTAwLjA0MS00NC44NzktMTAwLjA0MS0xMDAuMDQxUzIwMC44MzgsOTguODA2LDI1Niw5OC44MDYNCglzMTAwLjA0MSw0NC44NzksMTAwLjA0MSwxMDAuMDQxUzMxMS4xNjQsMjk4Ljg5LDI1NiwyOTguODl6Ii8+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8L3N2Zz4NCg==" height=17/>&nbsp;PrEP Sites)" %>% lapply(htmltools::HTML)
legend_crcl   <- r"(<img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMS45OTkgNTExLjk5OSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNTExLjk5OSA1MTEuOTk5OyIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSI+DQo8cGF0aCBzdHlsZT0iZmlsbDojODY1ZWI3OyIgZD0iTTQ1NC44NDgsMTk4Ljg0OGMwLDE1OS4yMjUtMTc5Ljc1MSwzMDYuNjg5LTE3OS43NTEsMzA2LjY4OWMtMTAuNTAzLDguNjE3LTI3LjY5Miw4LjYxNy0zOC4xOTUsMA0KCWMwLDAtMTc5Ljc1MS0xNDcuNDY0LTE3OS43NTEtMzA2LjY4OUM1Ny4xNTMsODkuMDI3LDE0Ni4xOCwwLDI1NiwwUzQ1NC44NDgsODkuMDI3LDQ1NC44NDgsMTk4Ljg0OHoiLz4NCjxwYXRoIHN0eWxlPSJmaWxsOiNhMDhkYjc7IiBkPSJNMjU2LDI5OC44OWMtNTUuMTY0LDAtMTAwLjA0MS00NC44NzktMTAwLjA0MS0xMDAuMDQxUzIwMC44MzgsOTguODA2LDI1Niw5OC44MDYNCglzMTAwLjA0MSw0NC44NzksMTAwLjA0MSwxMDAuMDQxUzMxMS4xNjQsMjk4Ljg5LDI1NiwyOTguODl6Ii8+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8L3N2Zz4NCg==" height=17/>&nbsp;CrCLs)" %>% lapply(htmltools::HTML)

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
      data             = spdf_gf,
      fillColor        = color_gf_2123(spdf_gf$gf_2021_2023),
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
      label            = gf_2123_text,
      labelOptions     = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group            = "GF Sites (2021-2023)"
   ) %>%
   addPolygons(
      data             = spdf_gf,
      fillColor        = color_gf_2426(spdf_gf$gf_2024_2026),
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
      label            = gf_2426_text,
      labelOptions     = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Calibri",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
      group            = "GF Sites (2024-2026)"
   ) %>%
   addMarkers(
      ~faci$faci_tx$LONG,
      ~faci$faci_tx$LAT,
      data         = spdf_eb,
      icon         = makeIcon(
         iconUrl     = "C:/Users/Administrator/Documents/_QGIS/Geojson/marker_tx.svg",
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
         iconUrl     = "C:/Users/Administrator/Documents/_QGIS/Geojson/marker_prep.svg",
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
         iconUrl     = "C:/Users/Administrator/Documents/_QGIS/Geojson/marker_crcl.svg",
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
   addLegendFactor(
      pal      = colorFactor('#1F76B7', factor("GF Sites (2024-2026)")),
      shape    = 'polygon',
      opacity  = 0.9,
      values   = factor("GF Sites (2024-2026)"),
      position = "bottomleft",
      group    = 'GF Sites (2024-2026)'
   ) %>%
   addLegendFactor(
      pal      = colorFactor('#446879', factor("GF Sites (2021-2023)")),
      shape    = 'polygon',
      opacity  = 0.9,
      values   = factor("GF Sites (2021-2023)"),
      position = "bottomleft",
      group    = 'GF Sites (2021-2023)'
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
      overlayGroups = c("EB Data (MSM & TGW and PLHIV Estimates)", "GF Sites (2021-2023)", "GF Sites (2024-2026)", "CrCLs", "Treatment Hubs", "PrEP Sites", "Gray Overlay"),
      options       = layersControlOptions(collapsed = F)
   )

m

htmlwidgets::saveWidget(m, "C:/ph_spotmap_facis_gfsites.html", selfcontained = TRUE)
htmlwidgets::saveWidget(m, "//192.168.193.228/htdocs/ph_facilities/index.html", selfcontained = FALSE)
mapshot(m, "C:/ph_facilities.html")
