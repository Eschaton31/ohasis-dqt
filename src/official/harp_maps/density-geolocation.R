pacman::p_load(
   dplyr,
   sf,
   viridis,
   RColorBrewer,
   mapview,
   webshot2,
   nngeo,
   osrm,
   leaflet,
   leaflegend,
   htmlwidgets
)
options(osrm.server = "192.168.193.236:5000/")
options(osrm.profile = "car")

points_in_bbox <- function(polygon, points) {
   bbox <- st_bbox(polygon)
   new  <- points %>%
      #check if measurement is withing boundaries (yes, or not (no))
      mutate(
         passes_through_box = if_else(
            latitude >= bbox[2] &
               latitude <= bbox[4] &
               longitude >= bbox[1] &
               longitude <= bbox[3],
            "yes",
            "no",
            "no"
         )
      ) %>%
      filter(passes_through_box == "yes")
   # %>%
   #       st_as_sf(coords = c("longitude", "latitude"), crs = "+proj=longlat +datum=WGS84")

   return(new)
}

shape_ph_all   <- read_sf("H:/_QGIS/Shapefiles/Philippines 2023-07-19/ph_muncity-merged_mla.geojson")
density_ph_all <- read_csv("H:/_QGIS/Meta-Columbia University Density Maps/phl_general_2020.csv")

shape_ph_3_poly <- filter(shape_ph_all, ADM2_EN == "Zambales")
density_ph_3    <- points_in_bbox(shape_ph_3_poly, density_ph_all)
hubs_ph_3       <- ohasis$ref_faci_code %>%
   filter(FACI_NAME_PROV == "Zambales") %>%
   st_as_sf(coords = c("LONG", "LAT"), crs = "+proj=longlat +datum=WGS84")

ph3_isochrone <- list()
for (i in seq_len(nrow(hubs_ph_3))) {
   ph3_isochrone[[i]] <- osrmIsochrone(loc = hubs_ph_3[i,]$geometry, 30, res = 200)
}

leaflet() %>%
   addTiles() %>%
   # addProviderTiles("Esri.WorldGrayCanvas", group = "Gray Overlay") %>%
   # addPolygons(
   #    data             = shape_ph_3_poly,
   #    fillColor        = "white",
   #    # fillColor        = ~color_dist(est),
   #    weight           = 2,
   #    opacity          = 1,
   #    color            = "white",
   #    dashArray        = "3",
   #    fillOpacity      = 0.7,
   #    highlightOptions = highlightOptions(
   #       weight       = 5,
   #       color        = "#666",
   #       dashArray    = "",
   #       fillOpacity  = 0.7,
   #       bringToFront = TRUE),
   #    label            = ~ADM3_EN,
   #    labelOptions     = labelOptions(
   #       style     = list(
   #          "font-weight" = "normal",
   #          "font-family" = "Calibri",
   #          padding       = "3px 8px"
   #       ),
   #       textsize  = "15px",
   #       direction = "auto"
   #    ),
   #    group            = "EB Data (MSM & TGW and PLHIV Estimates)"
   # ) %>%
   addCircleMarkers(
      ~density_ph_3$longitude,
      ~density_ph_3$latitude,
      data   = shape_ph_3_poly,
      group  = "Treatment Hubs",
      radius = ~density_ph_3$phl_general_2020 / 100
   )
addPolygons(
   data             = bind_rows(ph3_isochrone),
   fillColor        = "red",
   # fillColor        = ~color_dist(est),
   weight           = 2,
   opacity          = 1,
   color            = "white",
   dashArray        = "3",
   fillOpacity      = 0.7,
   highlightOptions = highlightOptions(
      weight       = 5,
      color        = "#888",
      dashArray    = "",
      fillOpacity  = 0.7,
      bringToFront = TRUE),
   # label            = ~ADM3_EN,
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
      ~density_ph_3$geometry$LONG,
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