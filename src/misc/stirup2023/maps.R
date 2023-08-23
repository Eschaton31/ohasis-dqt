pacman::p_load(
   dplyr,
   sf,
   viridis,
   leaflet,
   leaflegend,
   RColorBrewer,
   mapview,
   webshot2,
   nngeo,
   htmlwidgets
)
stirup <- new.env()

# get references
stirup$refs <- psgc_aem(ohasis$ref_addr)
stirup$data <- list()
stirup$agg  <- list()
stirup$spdf <- list(
   regions    = read_sf(Sys.getenv("GEOJSON_REG")),
   merged_mla = read_sf(Sys.getenv("GEOJSON_PH")),
   aem        = read_sf(Sys.getenv("GEOJSON_AEM"))
)

##  Load harp data -------------------------------------------------------------

# get diagnosis data
stirup$data$dx <- hs_data("harp_full", "reg", 2023, 6) %>%
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
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         RES_PSGC_REG  = "region",
         RES_PSGC_PROV = "province",
         RES_PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = TRUE
   ) %>%
   mutate_at(
      .vars = vars(contains("PSGC", ignore.case = FALSE)),
      ~if_else(. != "", str_c("PH", .), .)
   ) %>%
   select(-region, -province, -muncity) %>%
   left_join(
      y  = stirup$refs$addr %>%
         select(
            RES_PSGC_REG  = PSGC_REG,
            RES_PSGC_PROV = PSGC_PROV,
            RES_PSGC_MUNC = PSGC_MUNC,
            RES_PSGC_AEM  = PSGC_AEM,
            region        = NHSSS_REG,
            province      = NHSSS_PROV,
            muncity       = NHSSS_AEM
         ),
      by = join_by(RES_PSGC_REG, RES_PSGC_PROV, RES_PSGC_MUNC)
   ) %>%
   mutate(
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
   distinct(idnum, .keep_all = TRUE) %>%
   rename_all(
      ~str_replace_all(., "RES_PSGC", "PSGC")
   )

stirup$data$pse <- read_dta("C:/Users/Administrator/Downloads/2023 pop.dta") %>%
   select(-starts_with("PSGC")) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         RES_PSGC_REG  = "region",
         RES_PSGC_PROV = "province",
         RES_PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = TRUE
   ) %>%
   mutate_at(
      .vars = vars(contains("PSGC", ignore.case = FALSE)),
      ~if_else(. != "", str_c("PH", .), .)
   ) %>%
   select(-region, -province, -muncity) %>%
   left_join(
      y  = stirup$refs$addr %>%
         select(
            RES_PSGC_REG  = PSGC_REG,
            RES_PSGC_PROV = PSGC_PROV,
            RES_PSGC_MUNC = PSGC_MUNC,
            RES_PSGC_AEM  = PSGC_AEM,
            region        = NHSSS_REG,
            province      = NHSSS_PROV,
            muncity       = NHSSS_AEM
         ),
      by = join_by(RES_PSGC_REG, RES_PSGC_PROV, RES_PSGC_MUNC)
   ) %>%
   rename(est_pop = y2023) %>%
   rename_all(
      ~str_replace_all(., "RES_PSGC", "PSGC")
   ) %>%
   mutate(
      est_pop = floor(est_pop)
   )


stirup$agg$dx <- stirup$spdf$aem %>%
   mutate(
      AEM_PCODE = if_else(AEM_PCODE == "PH099700000", "PH099701000", AEM_PCODE, AEM_PCODE)
   ) %>%
   left_join(
      y  = stirup$data$dx %>%
         group_by(PSGC_REG, PSGC_PROV, PSGC_AEM) %>%
         summarise_at(
            .vars = vars(dx, dx_plhiv, everonart, onart, vl_tested, vl_suppressed),
            ~sum(., na.rm = TRUE)
         ) %>%
         ungroup() %>%
         # get estimated
         full_join(
            y  = stirup$refs$aem %>%
               filter(report_yr == 2022) %>%
               select(
                  PSGC_REG,
                  PSGC_PROV,
                  PSGC_AEM,
                  est
               ) %>%
               mutate_at(
                  .vars = vars(contains("PSGC", ignore.case = FALSE)),
                  ~if_else(. != "", str_c("PH", .), .)
               ),
            by = join_by(PSGC_REG, PSGC_PROV, PSGC_AEM)
         ) %>%
         mutate(
            dx_coverage    = (dx_plhiv / est) * 100,
            tx_coverage    = (onart / dx_plhiv) * 100,
            vl_coverage    = (vl_tested / onart) * 100,
            vl_suppression = (vl_suppressed / vl_tested) * 100,
         ),
      by = join_by(AEM_PCODE == PSGC_AEM)
   )

stirup$agg$pse <- stirup$spdf$merged_mla %>%
   left_join(
      y  = stirup$data$dx %>%
         group_by(PSGC_REG, PSGC_PROV, PSGC_MUNC) %>%
         summarise_at(
            .vars = vars(dx, dx_plhiv, everonart, onart, vl_tested, vl_suppressed),
            ~sum(., na.rm = TRUE)
         ) %>%
         ungroup(),
      by = join_by(ADM3_PCODE == PSGC_MUNC)
   ) %>%
   left_join(
      y  = stirup$data$pse,
      by = join_by(ADM3_PCODE == PSGC_MUNC)
   ) %>%
   mutate(
      case2pop_ratio = (coalesce(dx_plhiv, 0) / coalesce(est_pop, 0)) * 10000
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

##  crcl facilities ------------------------------------------------------------

faci$faci_newcrcl <- ohasis$ref_faci %>%
   inner_join(
      y  = read_sheet("1oix0x_v2g88nOVomMfX3RMIAxIBsliOP1hZc9ZAK1kM", "Sheet1") %>%
         distinct(FACI_ID, SUB_FACI_ID) %>%
         mutate(
            SUB_FACI_ID = coalesce(SUB_FACI_ID, "")
         ),
      by = join_by(FACI_ID, SUB_FACI_ID)
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
   anti_join(
      y  = faci$faci_crcl %>%
         select(FACI_ID, SUB_FACI_ID),
      by = join_by(FACI_ID, SUB_FACI_ID)
   ) %>%
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

# layer creator
stirup$map$fns <- list(
   polygon = function(map, poly_data) {
      poly_data$data %<>%
         mutate_at(
            .vars = vars("ADM1_EN"),
            ~case_when(
               . == "Region XIII" ~ "CARAGA",
               . == "Cordillera Administrative Region" ~ "CAR",
               . == "Autonomous Region in Muslim Mindanao" ~ "BARMM",
               . == "National Capital Region" ~ "NCR",
               TRUE ~ .
            )
         )
      new_map <- map %>%
         addPolygons(
            data             = poly_data$data,
            fillColor        = poly_data$pal(poly_data$data[[poly_data$var]]),
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
               bringToFront = TRUE
            ),
            label            = poly_data$label,
            labelOptions     = labelOptions(
               style     = list(
                  "font-weight" = "normal",
                  "font-family" = "Lato",
                  padding       = "3px 8px"
               ),
               textsize  = "15px",
               direction = "auto"
            ),
            group            = poly_data$name,
            layerId          = str_c(poly_data$name, "|", poly_data$data[["ADM1_EN"]], "|", poly_data$data[[poly_data$id]])
         ) %>%
         addLegend(
            data     = poly_data$data,
            pal      = poly_data$pal,
            values   = poly_data$data[[poly_data$var]],
            opacity  = 0.9,
            position = "bottomleft",
            title    = poly_data$name,
            group    = poly_data$name,
         )

      return(new_map)
   },
   marker  = function(map, marker_data) {
      marker_legend <- lapply(glue(r"(<img src="{marker_data$img}" height=17/>&nbsp;{marker_data$name})"), htmltools::HTML)
      marker_text   <- lapply(str_c("<b>", marker_data$data$FACI_NAME, "</b><br/>", gsub(";", "<br/>", coalesce(marker_data$data$FACI_ADDR, ""))), htmltools::HTML)

      new_map <- map %>%
         addMarkers(
            ~marker_data$data$LONG,
            ~marker_data$data$LAT,
            data         = marker_data$plot,
            icon         = makeIcon(
               iconUrl     = marker_data$img,
               iconWidth   = 20,
               iconHeight  = 20,
               iconAnchorX = 10,
               iconAnchorY = 20,
            ),
            label        = marker_text,
            labelOptions = labelOptions(
               style     = list(
                  "font-weight" = "normal",
                  "font-family" = "Lato",
                  padding       = "3px 8px"
               ),
               textsize  = "15px",
               direction = "auto"
            ),
            group        = marker_data$name
         ) %>%
         addControl(
            html     = marker_legend,
            position = "bottomleft"
         )

      return(new_map)
   }
)

# Create a color palette for the map:
stirup$map$layers <- list(
   polygon = list(
      est = list(
         data  = stirup$agg$dx,
         label = lapply(
            str_c(
               "<b>", stirup$agg$dx$AEM_EN, "</b><br/>",
               "Estimated PLHIV: <b><i>", format(stirup$agg$dx$est, big.mark = ","), "</i></b><br/>",
               "Diagnosed PLHIV: <b><i>", format(stirup$agg$dx$dx_plhiv, big.mark = ","), "</i></b>"
            ),
            htmltools::HTML
         ),
         pal   = colorBin("YlOrBr", domain = stirup$agg$dx$est, na.color = "transparent", bins = quantile(stirup$agg$dx$est, seq(0, 1, 0.10), na.rm = TRUE)),
         var   = "est",
         id    = "AEM_PCODE",
         name  = "PLHIV & KP Size Estimates"
      ),
      pse = list(
         data  = stirup$agg$pse,
         label = lapply(
            str_c(
               "<b>", stirup$agg$pse$ADM3_EN, "</b><br/>",
               "Diagnosed PLHIV: <b><i>", format(stirup$agg$pse$dx_plhiv, big.mark = ","), "</i></b><br/>",
               "Total Population: <b><i>", format(stirup$agg$pse$est_pop, big.mark = ","), "</i></b><br/>",
               "Case-to-population ratio: <b><i>", format(stirup$agg$pse$case2pop_ratio, big.mark = ","), "</i></b>"
            ),
            htmltools::HTML
         ),
         pal   = colorBin("YlGnBu", domain = stirup$agg$pse, na.color = "transparent", bins = quantile(stirup$agg$pse$case2pop_ratio, seq(0, 1, 0.10), na.rm = TRUE)),
         var   = "case2pop_ratio",
         id    = "ADM3_PCODE",
         name  = "Case-to-population ratio"
      )
   ),
   marker  = list(
      tx      = list(name = "Treatment Hubs", plot = stirup$agg$dx, data = faci$faci_tx, img = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMS45OTkgNTExLjk5OSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNTExLjk5OSA1MTEuOTk5OyIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSI+DQo8cGF0aCBzdHlsZT0iZmlsbDojMjlhMDhjOyIgZD0iTTQ1NC44NDgsMTk4Ljg0OGMwLDE1OS4yMjUtMTc5Ljc1MSwzMDYuNjg5LTE3OS43NTEsMzA2LjY4OWMtMTAuNTAzLDguNjE3LTI3LjY5Miw4LjYxNy0zOC4xOTUsMA0KCWMwLDAtMTc5Ljc1MS0xNDcuNDY0LTE3OS43NTEtMzA2LjY4OUM1Ny4xNTMsODkuMDI3LDE0Ni4xOCwwLDI1NiwwUzQ1NC44NDgsODkuMDI3LDQ1NC44NDgsMTk4Ljg0OHoiLz4NCjxwYXRoIHN0eWxlPSJmaWxsOiNiNGUzY2M7IiBkPSJNMjU2LDI5OC44OWMtNTUuMTY0LDAtMTAwLjA0MS00NC44NzktMTAwLjA0MS0xMDAuMDQxUzIwMC44MzgsOTguODA2LDI1Niw5OC44MDYNCglzMTAwLjA0MSw0NC44NzksMTAwLjA0MSwxMDAuMDQxUzMxMS4xNjQsMjk4Ljg5LDI1NiwyOTguODl6Ii8+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8L3N2Zz4NCg=="),
      prep    = list(name = "PrEP Sites", plot = stirup$agg$dx, data = faci$faci_prep, img = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMS45OTkgNTExLjk5OSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNTExLjk5OSA1MTEuOTk5OyIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSI+DQo8cGF0aCBzdHlsZT0iZmlsbDojNDI4Y2M0OyIgZD0iTTQ1NC44NDgsMTk4Ljg0OGMwLDE1OS4yMjUtMTc5Ljc1MSwzMDYuNjg5LTE3OS43NTEsMzA2LjY4OWMtMTAuNTAzLDguNjE3LTI3LjY5Miw4LjYxNy0zOC4xOTUsMA0KCWMwLDAtMTc5Ljc1MS0xNDcuNDY0LTE3OS43NTEtMzA2LjY4OUM1Ny4xNTMsODkuMDI3LDE0Ni4xOCwwLDI1NiwwUzQ1NC44NDgsODkuMDI3LDQ1NC44NDgsMTk4Ljg0OHoiLz4NCjxwYXRoIHN0eWxlPSJmaWxsOiM4MGE4YzQ7IiBkPSJNMjU2LDI5OC44OWMtNTUuMTY0LDAtMTAwLjA0MS00NC44NzktMTAwLjA0MS0xMDAuMDQxUzIwMC44MzgsOTguODA2LDI1Niw5OC44MDYNCglzMTAwLjA0MSw0NC44NzksMTAwLjA0MSwxMDAuMDQxUzMxMS4xNjQsMjk4Ljg5LDI1NiwyOTguODl6Ii8+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8L3N2Zz4NCg=="),
      crcl    = list(name = "CrCLs", plot = stirup$agg$dx, data = faci$faci_crcl, img = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMS45OTkgNTExLjk5OSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNTExLjk5OSA1MTEuOTk5OyIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSI+DQo8cGF0aCBzdHlsZT0iZmlsbDojODY1ZWI3OyIgZD0iTTQ1NC44NDgsMTk4Ljg0OGMwLDE1OS4yMjUtMTc5Ljc1MSwzMDYuNjg5LTE3OS43NTEsMzA2LjY4OWMtMTAuNTAzLDguNjE3LTI3LjY5Miw4LjYxNy0zOC4xOTUsMA0KCWMwLDAtMTc5Ljc1MS0xNDcuNDY0LTE3OS43NTEtMzA2LjY4OUM1Ny4xNTMsODkuMDI3LDE0Ni4xOCwwLDI1NiwwUzQ1NC44NDgsODkuMDI3LDQ1NC44NDgsMTk4Ljg0OHoiLz4NCjxwYXRoIHN0eWxlPSJmaWxsOiNhMDhkYjc7IiBkPSJNMjU2LDI5OC44OWMtNTUuMTY0LDAtMTAwLjA0MS00NC44NzktMTAwLjA0MS0xMDAuMDQxUzIwMC44MzgsOTguODA2LDI1Niw5OC44MDYNCglzMTAwLjA0MSw0NC44NzksMTAwLjA0MSwxMDAuMDQxUzMxMS4xNjQsMjk4Ljg5LDI1NiwyOTguODl6Ii8+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8L3N2Zz4NCg=="),
      newcrcl = list(name = "rHIVda Expansion 2023", plot = stirup$agg$dx, data = faci$faci_newcrcl, img = "data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMS45OTkgNTExLjk5OSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNTExLjk5OSA1MTEuOTk5OyIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSI+DQo8cGF0aCBzdHlsZT0iZmlsbDojRTA1NjUyOyIgZD0iTTQ1NC44NDgsMTk4Ljg0OGMwLDE1OS4yMjUtMTc5Ljc1MSwzMDYuNjg5LTE3OS43NTEsMzA2LjY4OWMtMTAuNTAzLDguNjE3LTI3LjY5Miw4LjYxNy0zOC4xOTUsMA0KCWMwLDAtMTc5Ljc1MS0xNDcuNDY0LTE3OS43NTEtMzA2LjY4OUM1Ny4xNTMsODkuMDI3LDE0Ni4xOCwwLDI1NiwwUzQ1NC44NDgsODkuMDI3LDQ1NC44NDgsMTk4Ljg0OHoiLz4NCjxwYXRoIHN0eWxlPSJmaWxsOiNGNkNDQ0I7IiBkPSJNMjU2LDI5OC44OWMtNTUuMTY0LDAtMTAwLjA0MS00NC44NzktMTAwLjA0MS0xMDAuMDQxUzIwMC44MzgsOTguODA2LDI1Niw5OC44MDYNCglzMTAwLjA0MSw0NC44NzksMTAwLjA0MSwxMDAuMDQxUzMxMS4xNjQsMjk4Ljg5LDI1NiwyOTguODl6Ii8+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8L3N2Zz4NCg==")
   )
)

# Final Map
m <- leaflet() %>%
   addTiles() %>%
   addProviderTiles("Esri.WorldGrayCanvas", group = "Gray Overlay") %>%
   stirup$map$fns$polygon(stirup$map$layers$polygon$est) %>%
   stirup$map$fns$polygon(stirup$map$layers$polygon$pse) %>%
   stirup$map$fns$marker(stirup$map$layers$marker$tx) %>%
   stirup$map$fns$marker(stirup$map$layers$marker$prep) %>%
   stirup$map$fns$marker(stirup$map$layers$marker$crcl) %>%
   stirup$map$fns$marker(stirup$map$layers$marker$newcrcl) %>%
   # addPolylines(data = stirup$spdf$regions, color = "#3d3d3d", opacity = .9, weight = 2, dashArray = " ", group = "Region Borders") %>%
   addLayersControl(
      baseGroups    = c("PLHIV & KP Size Estimates", "Case-to-population ratio"),
      overlayGroups = c("CrCLs", "rHIVda Expansion 2023", "Treatment Hubs", "PrEP Sites", "Gray Overlay", "Region I", "Region II", "Region III", "Region IV-A", "Region IV-B", "Region V", "Region VI", "Region VII", "Region VIII", "Region IX", "Region X", "Region XI", "Region XII", "BARMM", "CAR", "CARAGA", "NCR"),
      options       = layersControlOptions(collapsed = FALSE)
   ) %>%
   htmlwidgets::onRender("
    function(el, x) {
      var myMap = this;
      var baseLayer = 'PLHIV & KP Size Estimates';
      console.log(myMap);
      myMap.eachLayer(function(layer){
        var id = layer.options.layerId;
        if (id){
          if ('PLHIV & KP Size Estimates' !== id.split('|')[0]){
            layer.getElement().style.display = 'none';
          }
        }
      })
      myMap.on('baselayerchange',
        function (e) {
          baseLayer=e.name;
          myMap.eachLayer(function (layer) {
              var id = layer.options.layerId;
              if (id){
                if (e.name !== id.split('|')[0]){
                  layer.getElement().style.display = 'none';
                  layer.closePopup();
                }
                if (e.name === id.split('|')[0]){
                  layer.getElement().style.display = 'block';
                }
              }

          });
        })
        myMap.on('overlayadd', function(e){
          myMap.eachLayer(function(layer){
            var id = layer.options.layerId;
            if (id){
                if (baseLayer === id.split('|')[0] && e.name === id.split('|')[1]){
                  layer.getElement().style.display = 'block';
                }
            }
          })
        })
        myMap.on('overlayremove', function(e){
          myMap.eachLayer(function(layer){
            var id = layer.options.layerId;
            if (id){
                if (baseLayer === id.split('|')[0] && e.name === id.split('|')[1]){
                  layer.getElement().style.display = 'none';
                }
            }
          })
        })
    }")

m
htmlwidgets::saveWidget(m, "H:/_R/ph_map_stirup2023.html", selfcontained = TRUE)
htmlwidgets::saveWidget(m, "//192.168.193.228/htdocs/ph_facilities/index.html", selfcontained = FALSE)
mapshot(m, "C:/ph_facilities.html")

