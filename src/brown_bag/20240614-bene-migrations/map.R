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

dx_map <- dx_migrate %>%
   filter(migrate == 1, year == 2022) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "region",
         PERM_PSGC_PROV = "province",
         PERM_PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = FALSE
   )

spdf        <- read_sf("H:/_QGIS/Shapefiles/Philippines 2023-07-19/ph_muncity-merged_mla.geojson")
map         <- spdf %>%
   left_join(
      y  = dx_map %>%
         rename_all(~str_replace_all(., "^PERM_", "")) %>%
         group_by(PSGC_REG, PSGC_PROV, PSGC_MUNC) %>%
         mutate_at(
            .vars = vars(contains("PSGC", ignore.case = FALSE)),
            ~if_else(. != "", str_c("PH", .), .)
         ) %>%
         summarise(
            migrated = n()
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

plot <- ggplot() +
      geom_sf(
         data   = map,
         aes(fill = migrated),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_text(
         data         = map,
         aes(label = stri_c(migrated, "\n", NHSSS_MUNC)),
         family       = "Inter",
         fontface     = "bold",
         size         = 6,
         fun.geometry = sf::st_centroid
      ) +
      # geom_point(data = sites, aes(x = LONG, y = LAT), colour = "red", size = 6, font = "Inter") +
      # geom_text(
      #    data     = sites,
      #    aes(x = LONG, y = LAT, label = FACI_CODE),
      #    family   = "Inter",
      #    fontface = "italic",
      #    size     = 6,
      #    vjust    = -1
      # ) +
      theme_void() +
      scale_fill_viridis(
         # option    = "A",
         trans     = "log",
         direction = -1,
         # breaks = c(1, 5, 10, 20, 50, 100),
         breaks    = scales::pretty_breaks(),
         name      = "Migrating Cases",
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
      ggtitle("Migrations") +
      coord_sf()

ggsave("H:/migration-initial.png", plot, bg = "transparent", dpi = 300, height = 25, width = 25)

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
