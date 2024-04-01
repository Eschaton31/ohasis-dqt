pacman::p_load(
   dplyr,
   sf,
   viridis,
   RColorBrewer,
   mapview,
   webshot2,
   nngeo,
   osrm
)
lw_conn <- ohasis$conn("lw")
id_reg  <- QB$new(lw_conn)$
   from("ohasis_warehouse.id_registry")$
   select(PATIENT_ID, CENTRAL_ID)$
   get()

harp <- read_dta(hs_data("harp_full", "reg", 2023, 12))
art  <- read_dta(hs_data("harp_tx", "outcome", 2023, 12)) %>%
   left_join(
      y  = harp %>%
         select(idnum, region, province, muncity, transmit, sexhow, reg_sex = sex, self_identity),
      by = join_by(idnum)
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "region",
         PERM_PSGC_PROV = "province",
         PERM_PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   get_latest_pii(
      "CENTRAL_ID",
      c(
         "PERM_PSGC_REG",
         "PERM_PSGC_PROV",
         "PERM_PSGC_MUNC"
      )
   ) %>%
   mutate_at(
      .vars = vars(contains("PSGC", ignore.case = FALSE)),
      ~if_else(. != "", str_c("PH", .), .)
   ) %>%
   mutate_at(
      .vars = vars(contains("PSGC", ignore.case = FALSE)),
      ~str_replace_all(., "01$", "00")
   )

onart <- art %>%
   filter(realhub == "TLY") %>%
   mutate(
      realhub_branch = case_when(
         realhub_branch == "TLY-LUXECARE" ~ "TLY-LUXECARE SHAW",
         TRUE ~ realhub_branch
      ),

      start_agegrp   = case_when(
         curr_age < 15 ~ "1) <15",
         curr_age %between% c(15, 17) ~ "2) 15-17",
         curr_age %between% c(18, 19) ~ "3) 18-19",
         curr_age %between% c(20, 24) ~ "4) 20-24",
         curr_age %between% c(25, 34) ~ "5) 25-34",
         curr_age %between% c(35, 39) ~ "6) 35-49",
         curr_age %between% c(50, 10000) ~ "6) 50+",
         TRUE ~ "(no data)"
      ),


      # sex
      sex            = coalesce(StrLeft(sex, 1), "(no data)"),

      # KAP
      msm            = case_when(
         reg_sex == "MALE" & sexhow %in% c("BISEXUAL", "HOMOSEXUAL") ~ 1,
         reg_sex == "MALE" & sexhow == "HETEROSEXUAL" ~ 0,
         TRUE ~ 0
      ),
      tgp            = if_else(
         condition = reg_sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      pwid           = if_else(
         condition = sexhow == "IVDU",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero         = if_else(
         condition = sexhow == "HETEROSEXUAL",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      unknown        = case_when(
         transmit == "UNKNOWN" ~ 1,
         is.na(transmit) ~ 1,
         TRUE ~ 0
      ),
      sex            = case_when(
         sex == "M" ~ "Male",
         sex == "F" ~ "Female",
         TRUE ~ "(no data)"
      ),

      # kap
      kap_type       = case_when(
         msm == 1 & tgp == 0 ~ "MSM",
         msm == 1 & tgp == 1 ~ "MSM-TGP",
         hetero == 0 & pwid == 1 ~ "PWID",
         hetero == 1 & sex == "Male" ~ "Hetero Males",
         hetero == 1 & sex == "Female" ~ "Hetero Fenales",
         TRUE ~ "Other KP"
      ),
   )

## enrollment trend
onart %>%
   mutate(
      start = stri_c(year(prepstart_date), "-Q", lubridate::quarter(prepstart_date))
   ) %>%
   group_by(start) %>%
   summarise(
      onart = n()
   ) %>%
   write_clip()

## enrollment contribution

# 2021
onart %>%
   filter(year(prepstart_date) == 2023) %>%
   group_by(realhub_branch) %>%
   summarise(
      onart = n()
   ) %>%
   write_clip()

# by site
onart %>%
   group_by(realhub_branch) %>%
   summarise(
      `2021` = sum(year(prepstart_date) == 2021),
      `2022` = sum(year(prepstart_date) == 2022),
      `2023` = sum(year(prepstart_date) == 2023),
   ) %>%
   write_clip()

# by region
onart %>%
   filter(curr_reg == "4A") %>%
   group_by(curr_prov) %>%
   summarise(
      onart = n()
   ) %>%
   write_clip()

## site-by-site profile
onart %>%
   group_by(start_curr_agegrp) %>%
   summarise(
      onart = n()
   ) %>%
   write_clip()
onart %>%
   summarise(
      msm    = sum(msm == 1),
      tgp    = sum(tgp == 1),
      pwid   = sum(pwid == 1),
      hetero = sum(hetero == 1),
   ) %>%
   write_clip()
onart %>%
   group_by(curr_reg, curr_munc) %>%
   summarise(
      onart = n()
   )

onart %>%
   mutate_at(
      .vars = vars(contains("PSGC", ignore.case = FALSE)),
      ~str_replace_all(., "^PH", "")
   ) %>%
   rename(
      REG_PSGC  = PERM_PSGC_REG,
      PROV_PSGC = PERM_PSGC_PROV,
      MUNC_PSGC = PERM_PSGC_MUNC,
   ) %>%
   ohasis$get_addr(
      c(
         reg  = "REG_PSGC",
         prov = "PROV_PSGC",
         munc = "MUNC_PSGC"
      ),
      "name"
   ) %>%
   group_by(reg, prov, munc) %>%
   summarise(
      Clients = n()
   ) %>%
   ungroup() %>%
   bind_rows(ohasis$ref_addr %>% distinct(reg = NAME_REG)) %>%
   mutate(
      sort = if_else(is.na(prov), 1, 2, 2)
   ) %>%
   arrange(reg, sort, prov, munc) %>%
   mutate(
      reg = if_else(sort == 2, NA_character_, reg)
   ) %>%
   select(-sort) %>%
   write_clip()
# plan & type
onart %>%
   mutate(
      plan = if_else(curr_prep_plan == "free", "Free", "Paid", "Paid"),
      type = if_else(curr_prep_type == "event", "Event-driven", "Daily", "Daily")
   ) %>%
   group_by(type) %>%
   summarise(
      onart = n()
   ) %>%
   write_clip()


## maps
ly_sites <- ohasis$ref_faci %>%
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
   ) %>%
   filter(FACI_CODE != "Tly")

spdf        <- read_sf("H:/_QGIS/Shapefiles/Philippines 2023-07-19/ph_muncity-merged_mla.geojson")
spdf_reg    <- read_sf("H:/_QGIS/Shapefiles/Philippines 2023-07-19/phl_admbnda_adm2_psa_namria_20200529.shp")
tx_maps     <- spdf %>%
   left_join(
      y  = onart %>%
         rename_all(~str_replace_all(., "^PERM_", "")) %>%
         group_by(PSGC_REG, PSGC_PROV, PSGC_MUNC) %>%
         summarise(
            onart = n()
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
tx_maps_reg <- spdf_reg %>%
   left_join(
      y  = onart %>%
         rename_all(~str_replace_all(., "^PERM_", "")) %>%
         group_by(PSGC_PROV) %>%
         summarise(
            onart = n()
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
         aes(fill = onart),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_text(
         data         = data,
         aes(label = stri_c(onart, "\n", NHSSS_MUNC)),
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
         aes(fill = onart),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_text(
         data         = data,
         aes(label = stri_c(onart, "\n", NHSSS_MUNC)),
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
         aes(fill = onart),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_text(
         data         = data,
         aes(label = stri_c(onart, "\n", NHSSS_MUNC)),
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
         aes(fill = onart),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_text(
         data         = data,
         aes(label = stri_c(onart, "\n", NHSSS_PROV)),
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
      coord_sf()
}

try         <- split(tx_maps, tx_maps$NHSSS_REG)
regions     <- unique(onart$region)
maps        <- lapply(regions, map_ly_region, data = tx_maps, sites = ly_sites)
names(maps) <- regions
for (region in regions) {
   ggsave(file.path("C:/Users/Administrator/Downloads/ly-maps/art", stri_c(region, ".png")), maps[[region]], bg = "transparent", dpi = 300, height = 25, width = 25)

}
ggsave("C:/Users/Administrator/Downloads/ly-maps/art/ARMM.png", maps$ARMM, bg = "transparent", dpi = 300, height = 25, width = 25)

prov <- map_ly_province("BATANGAS", data = tx_maps, sites = ly_sites)
ggsave("C:/Users/Administrator/Downloads/ly-maps/art/province/BATANGAS.png", prov, bg = "transparent", dpi = 300, height = 25, width = 25)


new_map <- map_ly_regions(tx_maps_reg, ly_sites)
ggsave("C:/Users/Administrator/Downloads/ly-maps/art/PH.png", new_map, bg = "transparent", dpi = 300, height = 25, width = 25)
new_map <- leaflet() %>%
   addTiles() %>%
   addProviderTiles("Esri.WorldGrayCanvas", group = "Gray Overlay") %>%
   addPolygons(
      data             = tx_maps,
      fillColor        = colorBin("YlOrBr", domain = tx_maps$onart, na.color = "transparent", bins = quantile(tx_maps$onart, seq(0, 1, 0.05), na.rm = TRUE)),
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
      label            = tx_maps$ADM3_EN,
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
