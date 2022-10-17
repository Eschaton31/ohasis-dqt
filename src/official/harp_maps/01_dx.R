dir  <- "H:/Events/STIR-UP/2022/Maps"
dx   <- read_dta(hs_data("harp_dx", "reg", 2022, 8))
addr <- ohasis$ref_addr %>%
   left_join(
      y = read_sheet("16_ytFIRiAgmo6cqtoy0JkZoI62S5IuWxKyNKHO_R1fs", "v2022") %>%
         mutate(aem_class = "a"),
   ) %>%
   select(
      region   = NHSSS_REG,
      province = NHSSS_PROV,
      muncity  = NHSSS_MUNC,
      PSGC_REG,
      PSGC_PROV,
      PSGC_MUNC,
      aem_class
   )

data <- dx %>%
   select(
      idnum,
      region,
      province,
      muncity
   ) %>%
   left_join(
      y = addr
   ) %>%
   distinct(idnum, .keep_all = TRUE) %>%
   mutate(
      id = paste0("PH", PSGC_MUNC),
   ) %>%
   group_by(id) %>%
   summarise(
      dx = n()
   ) %>%
   left_join(
      y = addr %>%
         mutate(id = paste0("PH", PSGC_MUNC)) %>%
         select(id, aem_class)
   )

spdf <- read_sf("H:/_QGIS/Shapefiles/Philippines 2020-05-29/phl_admbnda_adm3_psa_namria_20200529.shp")

spdf_addr <- spdf %>%
   as.data.frame() %>%
   distinct(ADM1_EN, ADM1_PCODE)

for (i in seq_len(nrow(spdf_addr))) {
   psgc   <- spdf_addr[i,]$ADM1_PCODE
   name   <- spdf_addr[i,]$ADM1_EN
   subdir <- file.path(dir, name)
   check_dir(subdir)

   spdf_filtered <- spdf %>%
      filter(ADM1_PCODE == psgc) %>%
      left_join(
         y  = data,
         by = c("ADM3_PCODE" = "id")
      ) %>%
      mutate(
         muncity = if_else(aem_class == "a", ADM3_EN, NA_character_)
      )

   p <- ggplot() +
      geom_sf(
         data   = spdf_filtered,
         aes(fill = dx),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_label(
         data = spdf_filtered,
         aes(label = muncity),
         size = 2
      ) +
      theme_void() +
      scale_fill_viridis(
         # option = "F",
         # trans  = "log",
         # breaks = c(1, 5, 10, 20, 50, 100),
         breaks = scales::pretty_breaks(),
         name   = "Number of cases",
         guide  = guide_legend(keyheight      = unit(3, units = "mm"),
                               keywidth       = unit(12, units = "mm"),
                               label.position = "bottom",
                               title.position = 'top',
                               nrow           = 1)) +
      labs(
         title    = glue("Diagnosis among {name} Residents"),
         subtitle = "Number of diagnosed HIV cases per city/municipality",
         caption  = "Data: HIV/AIDS & ART Registry of the Philippines, August 2022"
      ) +
      theme(
         text              = element_text(color = "#22211d", family = "Infra"),
         plot.background   = element_rect(fill = "#f5f5f2", color = NA),
         panel.background  = element_rect(fill = "#f5f5f2", color = NA),
         legend.background = element_rect(fill = "#f5f5f2", color = NA),

         plot.title        = element_text(
            size   = 22,
            hjust  = 0.01,
            color  = "#4e4d47",
            # margin = margin(b = -0.1, t = 0.4, l = 2, unit = "cm"),
            family = "Infra",
            face   = "bold"
         ),
         plot.subtitle     = element_text(
            size   = 17,
            hjust  = 0.01,
            color  = "#4e4d47",
            # margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm"),
            family = "Infra",
            face   = "plain"
         ),
         plot.caption      = element_text(
            size   = 12, color = "#4e4d47",
            # margin = margin(b = 0.3, r = -99, unit = "cm"),
            family = "Infra",
            face   = "italic"
         ),
         plot.margin       = grid::unit(c(10, 10, 10, 10), "mm"),

         legend.position   = c(0.7, 0.09)
      )
   .log_info("Saving {name}.")
   ggsave(glue("{subdir}/dx.png"), p, "png", width = 11.7, height = 8.4)
   # png(glue("{subdir}/dx.png"))
   # print(p)
   # dev.off()
}
