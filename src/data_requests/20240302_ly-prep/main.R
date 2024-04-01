prep_start <- hs_data("prep", "prepstart", 2023, 12) %>%
   read_dta() %>%
   filter(prepstart_faci == "TLY", !is.na(prepstart_date)) %>%
   mutate_at(
      .vars = vars(curr_reg, curr_prov, curr_munc),
      ~coalesce(na_if(., ""), "UNKNOWN")
   ) %>%
   mutate(
      prepstart_branch = case_when(
         prepstart_branch == "TLY-LUXECARE" ~ "TLY-LUXECARE SHAW",
         TRUE ~ prepstart_branch
      ),

      age              = calc_age(birthdate, prepstart_date),
      start_agegrp     = case_when(
         age < 15 ~ "1) <15",
         age %between% c(15, 17) ~ "2) 15-17",
         age %between% c(18, 19) ~ "3) 18-19",
         age %between% c(20, 24) ~ "4) 20-24",
         age %between% c(25, 34) ~ "5) 25-34",
         age %between% c(35, 39) ~ "6) 35-49",
         age %between% c(50, 10000) ~ "6) 50+",
         TRUE ~ "(no data)"
      ),


      # sex
      sex              = coalesce(StrLeft(sex, 1), "(no data)"),

      # KAP
      msm              = case_when(
         sex == "M" & stri_detect_fixed(prep_risk_sexwithm, "yes") ~ 1,
         sex == "M" & stri_detect_fixed(hts_risk_sexwithm, "yes") ~ 1,
         sex == "M" & kp_msm == 1 ~ 1,
         TRUE ~ 0
      ),
      tgp              = if_else(
         condition = sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero           = case_when(
         sex == "M" &
            !grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) &
            grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) ~ 1,
         sex == "F" &
            grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) &
            !grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) ~ 1,
         TRUE ~ 0
      ),
      pwid             = case_when(
         str_detect(prep_risk_injectdrug, "yes") ~ 1,
         str_detect(hts_risk_injectdrug, "yes") ~ 1,
         kp_pwid == 1 ~ 1,
         TRUE ~ 0
      ),

      # kap
      sex              = case_when(
         sex == "M" ~ "Male",
         sex == "F" ~ "Female",
         TRUE ~ "(no data)"
      ),
      kap_type         = case_when(
         msm == 1 & tgp == 0 ~ "MSM",
         msm == 1 & tgp == 1 ~ "MSM-TGP",
         hetero == 0 & pwid == 1 ~ "PWID",
         sex == "Male" ~ "Other Males",
         sex == "Female" ~ "Other Females",
         TRUE ~ "Other KP"
      ),
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PSGC_REG  = "curr_reg",
         PSGC_PROV = "curr_prov",
         PSGC_MUNC = "curr_munc"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   mutate_at(
      .vars = vars(contains("PSGC", ignore.case = FALSE)),
      ~if_else(. != "", str_c("PH", .), .)
   )

prep_screen <- hs_data("prep", "reg", 2023, 12) %>%
   read_dta() %>%
   filter(prep_first_faci == "TLY") %>%
   mutate(
      prep_first_branch = case_when(
         prep_first_branch == "TLY-LUXECARE" ~ "TLY-LUXECARE SHAW",
         TRUE ~ prep_first_branch
      )
   )

prep_curr <- hs_data("prep", "outcome", 2023, 12) %>%
   read_dta() %>%
   filter(faci == "TLY") %>%
   mutate(
      branch = case_when(
         branch == "TLY-LUXECARE" ~ "TLY-LUXECARE SHAW",
         TRUE ~ branch
      )
   )

## enrollment trend
prep_start %>%
   mutate(
      start = stri_c(year(prepstart_date), "-Q", lubridate::quarter(prepstart_date))
   ) %>%
   group_by(start) %>%
   summarise(
      enrolled = n()
   ) %>%
   write_clip()

## enrollment contribution

# 2021
prep_start %>%
   filter(year(prepstart_date) == 2023) %>%
   group_by(prepstart_branch) %>%
   summarise(
      enrolled = n()
   ) %>%
   write_clip()

# by site
prep_start %>%
   group_by(prepstart_branch) %>%
   summarise(
      `2021` = sum(year(prepstart_date) == 2021),
      `2022` = sum(year(prepstart_date) == 2022),
      `2023` = sum(year(prepstart_date) == 2023),
   ) %>%
   write_clip()

# by region
prep_start %>%
   filter(curr_reg == "4A") %>%
   group_by(curr_prov) %>%
   summarise(
      enrolled = n()
   ) %>%
   write_clip()

## site-by-site profile
prep_start %>%
   group_by(start_agegrp) %>%
   summarise(
      enrolled = n()
   ) %>%
   write_clip()
prep_start %>%
   summarise(
      msm    = sum(msm == 1),
      tgp    = sum(tgp == 1),
      pwid   = sum(pwid == 1),
      hetero = sum(hetero == 1),
   ) %>%
   write_clip()
prep_start %>%
   group_by(curr_reg, curr_munc) %>%
   summarise(
      enrolled = n()
   )

prep_start %>%
   mutate_at(
      .vars = vars(contains("PSGC", ignore.case = FALSE)),
      ~str_replace_all(., "^PH", "")
   ) %>%
   rename(
      REG_PSGC  = PSGC_REG,
      PROV_PSGC = PSGC_PROV,
      MUNC_PSGC = PSGC_MUNC,
   ) %>%
   ohasis$get_addr(
      c(
         region   = "REG_PSGC",
         province = "PROV_PSGC",
         muncity  = "MUNC_PSGC"
      ),
      "name"
   ) %>%
   group_by(region, province, muncity) %>%
   summarise(
      Enrollees = n()
   ) %>%
   write_clip()
# plan & type
prep_start %>%
   mutate(
      plan = if_else(curr_prep_plan == "free", "Free", "Paid", "Paid"),
      type = if_else(curr_prep_type == "event", "Event-driven", "Daily", "Daily")
   ) %>%
   group_by(type) %>%
   summarise(
      enrolled = n()
   ) %>%
   write_clip()


## maps
ly_sites      <- ohasis$ref_faci %>%
   filter(str_detect(FACI_NAME, "LoveYourself")) %>%
   distinct(FACI_CODE, .keep_all = TRUE) %>%
   select(
      FACI_CODE,
      LONG,
      LAT,
      FACI_NHSSS_REG,
      FACI_NHSSS_PROV
   ) %>%
   mutate(
      FACI_CODE = str_replace_all(FACI_CODE, "TLY-", ""),
      FACI_CODE = str_to_title(FACI_CODE)
   )
spdf          <- read_sf("H:/_QGIS/Shapefiles/Philippines 2023-07-19/ph_muncity-merged_mla.geojson")
spdf_reg      <- read_sf("H:/_QGIS/Shapefiles/Philippines 2023-07-19/phl_admbnda_adm2_psa_namria_20200529.shp")
prep_maps     <- spdf %>%
   left_join(
      y  = prep_start %>%
         group_by(PSGC_REG, PSGC_PROV, PSGC_MUNC) %>%
         summarise(
            enrolled = n()
         ) %>%
         ungroup(),
      by = join_by(ADM3_PCODE == PSGC_MUNC)
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         distinct(PSGC_REG, PSGC_PROV, PSGC_MUNC, NHSSS_REG, NHSSS_PROV, NHSSS_MUNC) %>%
         mutate_at(
            .vars = vars(contains("PSGC", ignore.case = FALSE)),
            ~if_else(. != "", str_c("PH", .), .)
         ),
      by = join_by(ADM1_PCODE == PSGC_REG, ADM2_PCODE == PSGC_PROV, ADM3_PCODE == PSGC_MUNC)
   )
prep_maps_reg <- spdf_reg %>%
   left_join(
      y  = prep_start %>%
         group_by(PSGC_PROV) %>%
         summarise(
            enrolled = n()
         ) %>%
         ungroup(),
      by = join_by(ADM2_PCODE == PSGC_PROV)
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         distinct(PSGC_PROV, NHSSS_PROV) %>%
         mutate_at(
            .vars = vars(contains("PSGC", ignore.case = FALSE)),
            ~if_else(. != "", str_c("PH", .), .)
         ),
      by = join_by(ADM2_PCODE == PSGC_PROV)
   )

map_ly_region <- function(region, data, sites) {
   data  <- data %>% filter(NHSSS_REG == region)
   sites <- sites %>% filter(FACI_NHSSS_REG == region)

   ggplot() +
      geom_sf(
         data   = data,
         aes(fill = data$enrolled),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_text(
         data         = data,
         aes(label = stri_c(data$enrolled, "\n", data$NHSSS_MUNC)),
         family       = "Inter",
         fontface     = "bold",
         size         = 6,
         fun.geometry = sf::st_centroid
      ) +
      geom_point(data = sites, aes(x = LONG, y = LAT), colour = "red", size = 6, font = "Inter") +
      geom_text(
         data     = sites,
         aes(x = LONG, y = LAT, label = FACI_CODE),
         family   = "Inter",
         fontface = "italic",
         size     = 6,
         vjust    = -1
      ) +
      theme_void() +
      scale_fill_viridis(
         # option    = "A",
         # trans     = "log",
         direction = -1,
         # breaks = c(1, 5, 10, 20, 50, 100),
         breaks    = scales::pretty_breaks(),
         name      = "Diagnosed Cases",
         guide     = guide_legend(
            keyheight      = unit(20, units = "mm"),
            keywidth       = unit(25, units = "mm"),
            label.position = "right",
            title.position = 'top',
            nrow           = 17,
            direction      = "horizontal",
         )
      ) +
      theme(
         axis.title       = element_blank(),
         axis.text        = element_blank(),
         axis.ticks       = element_blank(),
         panel.border     = element_blank(),
         panel.grid.major = element_blank(),
         panel.grid.minor = element_blank(),
         legend.position  = "none",
         text             = element_text(size = 20, family = "Inter"),
         plot.title       = element_blank(),
      ) +
      ggtitle(region) +
      coord_sf()
}

map_ly_province <- function(region, data, sites) {
   data  <- data %>% filter(NHSSS_PROV == region)
   sites <- sites %>% filter(FACI_NHSSS_PROV == region)

   ggplot() +
      geom_sf(
         data   = data,
         aes(fill = data$enrolled),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_text(
         data         = data,
         aes(label = stri_c(data$enrolled, "\n", data$NHSSS_MUNC)),
         family       = "Inter",
         fontface     = "bold",
         size         = 6,
         fun.geometry = sf::st_centroid
      ) +
      geom_point(data = sites, aes(x = LONG, y = LAT), colour = "red", size = 6, font = "Inter") +
      geom_text(
         data     = sites,
         aes(x = LONG, y = LAT, label = FACI_CODE),
         family   = "Inter",
         fontface = "italic",
         size     = 6,
         vjust    = -1
      ) +
      theme_void() +
      scale_fill_viridis(
         # option    = "A",
         # trans     = "log",
         direction = -1,
         # breaks = c(1, 5, 10, 20, 50, 100),
         breaks    = scales::pretty_breaks(),
         name      = "Diagnosed Cases",
         guide     = guide_legend(
            keyheight      = unit(20, units = "mm"),
            keywidth       = unit(25, units = "mm"),
            label.position = "right",
            title.position = 'top',
            nrow           = 17,
            direction      = "horizontal",
         )
      ) +
      theme(
         axis.title       = element_blank(),
         axis.text        = element_blank(),
         axis.ticks       = element_blank(),
         panel.border     = element_blank(),
         panel.grid.major = element_blank(),
         panel.grid.minor = element_blank(),
         legend.position  = "none",
         text             = element_text(size = 20, family = "Inter"),
         plot.title       = element_blank(),
      ) +
      ggtitle(region) +
      coord_sf()
}

map_ly_province <- function(region, data, sites) {
   data  <- data %>% filter(NHSSS_PROV == region)
   sites <- sites %>% filter(FACI_NHSSS_PROV == region)

   ggplot() +
      geom_sf(
         data   = data,
         aes(fill = data$enrolled),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_text(
         data         = data,
         aes(label = stri_c(data$enrolled, "\n", data$NHSSS_MUNC)),
         family       = "Inter",
         fontface     = "bold",
         size         = 6,
         fun.geometry = sf::st_centroid
      ) +
      geom_point(data = sites, aes(x = LONG, y = LAT), colour = "red", size = 6, font = "Inter") +
      geom_text(
         data     = sites,
         aes(x = LONG, y = LAT, label = FACI_CODE),
         family   = "Inter",
         fontface = "italic",
         size     = 6,
         vjust    = -1
      ) +
      theme_void() +
      scale_fill_viridis(
         # option    = "A",
         # trans     = "log",
         direction = -1,
         # breaks = c(1, 5, 10, 20, 50, 100),
         breaks    = scales::pretty_breaks(),
         name      = "Diagnosed Cases",
         guide     = guide_legend(
            keyheight      = unit(20, units = "mm"),
            keywidth       = unit(25, units = "mm"),
            label.position = "right",
            title.position = 'top',
            nrow           = 17,
            direction      = "horizontal",
         )
      ) +
      theme(
         axis.title       = element_blank(),
         axis.text        = element_blank(),
         axis.ticks       = element_blank(),
         panel.border     = element_blank(),
         panel.grid.major = element_blank(),
         panel.grid.minor = element_blank(),
         legend.position  = "none",
         text             = element_text(size = 20, family = "Inter"),
         plot.title       = element_blank(),
      ) +
      ggtitle(region) +
      coord_sf()
}

map_ly_regions <- function(data, sites) {

   ggplot() +
      geom_sf(
         data   = data,
         aes(fill = data$enrolled),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_text(
         data         = data,
         aes(label = stri_c(data$enrolled, "\n", data$NHSSS_PROV)),
         family       = "Inter",
         fontface     = "bold",
         size         = 3,
         fun.geometry = sf::st_centroid
      ) +
      geom_point(data = sites, aes(x = LONG, y = LAT), colour = "red", size = 3, font = "Inter") +
      geom_text(
         data     = sites,
         aes(x = LONG, y = LAT, label = FACI_CODE),
         family   = "Inter",
         fontface = "italic",
         size     = 3,
         vjust    = -1
      ) +
      theme_void() +
      scale_fill_viridis(
         # option    = "A",
         # trans     = "log",
         direction = -1,
         # breaks = c(1, 5, 10, 20, 50, 100),
         breaks    = scales::pretty_breaks(),
         name      = "Diagnosed Cases",
         guide     = guide_legend(
            keyheight      = unit(20, units = "mm"),
            keywidth       = unit(25, units = "mm"),
            label.position = "right",
            title.position = 'top',
            nrow           = 17,
            direction      = "horizontal",
         )
      ) +
      theme(
         axis.title       = element_blank(),
         axis.text        = element_blank(),
         axis.ticks       = element_blank(),
         panel.border     = element_blank(),
         panel.grid.major = element_blank(),
         panel.grid.minor = element_blank(),
         legend.position  = "none",
         text             = element_text(size = 10, family = "Inter"),
         plot.title       = element_blank(),
      ) +
      ggtitle(region) +
      coord_sf()
}

try         <- split(prep_maps, prep_maps$NHSSS_REG)
regions     <- unique(prep_start$curr_reg)
maps        <- lapply(regions, map_ly_region, data = prep_maps, sites = ly_sites %>% filter(FACI_CODE != "Tly"))
names(maps) <- regions
for (region in regions) {
   ggsave(file.path("C:/Users/Administrator/Downloads/ly-maps/prep", stri_c(region, ".png")), maps[[region]], bg = "transparent", dpi = 300, height = 25, width = 25)

}
ggsave("C:/Users/Administrator/Downloads/ly-maps/prep/NCR.png", maps$NCR, bg = "transparent", dpi = 300, height = 25, width = 25)

prov <- map_ly_province("BATANGAS", data = prep_maps, sites = ly_sites %>% filter(FACI_CODE != "Tly"))
ggsave("C:/Users/Administrator/Downloads/ly-maps/prep/province/BATANGAS.png", prov, bg = "transparent", dpi = 300, height = 25, width = 25)


new_map <- map_ly_regions(prep_maps_reg, ly_sites %>% filter(FACI_CODE != "Tly"))
ggsave("C:/Users/Administrator/Downloads/ly-maps/prep/PH.png", new_map, bg = "transparent", dpi = 300, height = 25, width = 25)
new_map <- leaflet() %>%
   addTiles() %>%
   addProviderTiles("Esri.WorldGrayCanvas", group = "Gray Overlay") %>%
   addPolygons(
      data             = prep_maps,
      fillColor        = colorBin("YlOrBr", domain = prep_maps$enrolled, na.color = "transparent", bins = quantile(prep_maps$enrolled, seq(0, 1, 0.05), na.rm = TRUE)),
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
      label            = prep_maps$ADM3_EN,
      labelOptions     = labelOptions(
         style     = list(
            "font-weight" = "normal",
            "font-family" = "Lato",
            padding       = "3px 8px"
         ),
         textsize  = "15px",
         direction = "auto"
      ),
   )

## -----------------------------------------------------------------------------

ggplot() +
   geom_sf(
      data   = prep_maps,
      aes(fill = prep_maps$enrolled),
      size   = 0,
      alpha  = 0.9,
      colour = NA
   ) +
   geom_sf_text(
      data         = prep_maps,
      aes(label = stri_c(prep_maps$enrolled, "\n", prep_maps$NHSSS_PROV)),
      family       = "Inter",
      fontface     = "bold",
      size         = 3,
      fun.geometry = sf::st_centroid
   ) +
   # geom_point(data = ly_sites, aes(x = LONG, y = LAT), colour = "red", size = 3, font = "Inter") +
   # geom_text(
   #    data     = sites,
   #    aes(x = LONG, y = LAT, label = FACI_CODE),
   #    family   = "Inter",
   #    fontface = "italic",
   #    size     = 3,
   #    vjust    = -1
   # ) +
   theme_void() +
   scale_fill_viridis(
      # option    = "A",
      trans     = "log",
      direction = -1,
      # breaks = c(1, 5, 10, 20, 50, 100),
      breaks    = scales::pretty_breaks(),
      name      = "Diagnosed Cases",
      guide     = guide_legend(
         keyheight      = unit(20, units = "mm"),
         keywidth       = unit(25, units = "mm"),
         label.position = "right",
         title.position = 'top',
         nrow           = 17,
         direction      = "horizontal",
      )
   ) +
   theme(
      axis.title       = element_blank(),
      axis.text        = element_blank(),
      axis.ticks       = element_blank(),
      panel.border     = element_blank(),
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      legend.position  = "none",
      text             = element_text(size = 10, family = "Inter"),
      plot.title       = element_blank(),
   ) +
   ggtitle("PH") +
   coord_sf()