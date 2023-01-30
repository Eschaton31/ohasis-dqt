dir  <- "H:/Events/STIR-UP/2022/Maps"
dx   <- read_dta(hs_data("harp_dx", "reg", 2022, 12))
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
   ) %>%
   mutate_at(
      .vars = vars(starts_with("PSGC")),
      ~paste0("PH", .)
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
      ISO = case_when(
         muncity == "MANILA" ~ "PH133900000",
         aem_class == "a" ~ PSGC_MUNC,
         TRUE ~ PSGC_PROV
      )
   ) %>%
   group_by(ISO) %>%
   summarise(
      dx = n()
   ) %>%
   left_join(
      y = addr %>%
         mutate(ISO = paste0("", PSGC_MUNC)) %>%
         select(ISO, aem_class)
   )

spdf <- read_sf("H:/_QGIS/Shapefiles/Philippines 2020-05-29/phl_admbnda_adm3_psa_namria_20200529.shp")

spdf            <- read_sf("H:/_QGIS/geojson_bene/ph_muncity_rewind.geojson")
spdf_simplified <- spdf %>%
   left_join(
      y = addr %>%
         select(
            ADM1_PCODE = PSGC_REG,
            ADM2_PCODE = PSGC_PROV,
            ADM3_PCODE = PSGC_MUNC,
            aem_class
         )
   ) %>%
   mutate(
      NAME_1 = case_when(
         ADM2_EN == "NCR, City of Manila, First District" ~ "Manila City",
         aem_class == "a" ~ ADM3_EN,
         TRUE ~ ADM2_EN
      ),
      ISO    = case_when(
         ADM2_EN == "NCR, City of Manila, First District" ~ "PH133900000",
         aem_class == "a" ~ ADM3_PCODE,
         TRUE ~ ADM2_PCODE
      )
   ) %>%
   group_by(ADM1_EN, ADM1_PCODE, ISO, NAME_1) %>%
   summarize(
      # geometry = st_union(geometry)
      geometry = st_union(st_make_valid(st_set_precision(geometry, 1e6)))
   )
spdf_filtered   <- spdf_simplified %>%
   filter(ADM1_PCODE == "PH070000000") %>%
   left_join(
      y = data,
      by = c("ISO" = "ISO")
   )
est_regions     <- read_xlsx("C:/Users/johnb/Downloads/est_plhiv_regions.xlsx")
spdf_filtered   <- spdf_simplified %>%
   select(PSGC_REG = ADM1_PCODE, ADM1_EN) %>%
   right_join(
      y = est_regions %>%
         left_join(
            y = addr %>%
               distinct(region, PSGC_REG)
         )
   )

ggplot() +
   geom_sf(
      data   = spdf_filtered,
      aes(fill = dx),
      size   = 0,
      alpha  = 0.9,
      colour = NA
   )
est_regions <- data.frame()
p           <- ggplot() +
   geom_sf(
      data   = spdf_filtered,
      aes(fill = spdf_filtered$dx),
      size   = 0,
      alpha  = 0.9,
      colour = NA
   ) +
   geom_sf_label(
      data   = spdf_filtered,
      aes(label = spdf_filtered$NAME_1),
      size   = 10,
      family = "Infra"
   ) +
   theme_void() +
   scale_fill_viridis(
      # option = "F",
      trans  = "log",
      # breaks = c(1, 5, 10, 20, 50, 100),
      breaks = scales::pretty_breaks(),
      name   = "Diagnosed Cases",
      guide  = guide_legend(
         keyheight      = unit(20, units = "mm"),
         keywidth       = unit(25, units = "mm"),
         label.position = "right",
         title.position = 'top',
         nrow           = 17,
         direction      = "horizontal",
      )
   ) +
   # labs(
   #    title    = glue("Diagnosis among {name} Residents"),
   #    subtitle = "Number of diagnosed HIV cases per city/municipality",
   #    caption  = "Data: HIV/AIDS & ART Registry of the Philippines, August 2022"
   # ) +
   theme(
      text             = element_text(color = "#22211d", family = "Infra"),
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      plot.background  = element_rect(fill = "transparent", color = NA),
      panel.background = element_rect(fill = "transparent", color = NA),
      # legend.background    = element_rect(fill = "#f5f5f2", color = NA),

      # plot.title           = element_text(
      #    size   = 22,
      #    hjust  = 0.01,
      #    color  = "#4e4d47",
      #    # margin = margin(b = -0.1, t = 0.4, l = 2, unit = "cm"),
      #    family = "Infra",
      #    face   = "bold"
      # ),
      # plot.subtitle        = element_text(
      #    size   = 17,
      #    hjust  = 0.01,
      #    color  = "#4e4d47",
      #    # margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm"),
      #    family = "Infra",
      #    face   = "plain"
      # ),
      # plot.caption         = element_text(
      #    size   = 12, color = "#4e4d47",
      #    # margin = margin(b = 0.3, r = -99, unit = "cm"),
      #    family = "Infra",
      #    face   = "italic"
      # ),
      # plot.margin          = grid::unit(c(3, 2, 3, 2), "mm"),

      # legend.position      = "bottom",
      # legend.justification = "left",
      legend.title     = element_text(
         size   = 20,
         family = "Infra",
         face   = "bold"
      ),
      legend.text      = element_text(
         size   = 20,
         family = "Infra"
      ),
   )

ggsave("C:/Users/johnb/Downloads/dx7_2022.png", p, bg = "transparent", dpi = 300, height = 25, width = 25)
spdf_addr <- spdf %>%
   as.data.frame() %>%
   distinct(ADM1_EN, ADM1_PCODE)

for (i in seq_len(nrow(spdf_addr))) {
   psgc   <- spdf_addr[i,]$ADM1_PCODE
   name   <- spdf_addr[i,]$ADM1_EN
   subdir <- file.path(dir, name)
   check_dir(subdir)

   # spdf_filtered <- spdf %>%
   #    filter(ADM1_PCODE == psgc) %>%
   #    left_join(
   #       y  = data,
   #       by = c("ADM3_PCODE" = "id")
   #    ) %>%
   #    mutate(
   #       muncity  = if_else(aem_class == "a", ADM3_EN, ADM2_EN, ADM2_EN),
   #       PSGC_AEM = if_else(aem_class == "a", ADM3_PCODE, ADM2_PCODE, ADM2_PCODE)
   #    ) %>%
   #    group_by(PSGC_AEM, muncity) %>%
   #    summarize(
   #       geometry = st_union(geometry),
   #       dx       = sum(dx, na.rm = TRUE)
   #    )
   spdf_region <- spdf_filtered %>%
      filter(ADM1_PCODE == psgc)

   p <- ggplot() +
      geom_sf(
         data   = spdf_region,
         aes(fill = dx),
         size   = 0,
         alpha  = 0.9,
         colour = NA
      ) +
      geom_sf_label(
         data = spdf_region,
         aes(label = NAME_1),
         size = 10
      ) +
      theme_void() +
      scale_fill_viridis(
         # option = "F",
         trans  = "log",
         # breaks = c(1, 5, 10, 20, 50, 100),
         breaks = scales::pretty_breaks(),
         name   = "Number of cases",
         guide  = guide_legend(keyheight      = unit(3, units = "mm"),
                               keywidth       = unit(12, units = "mm"),
                               label.position = "bottom",
                               title.position = 'top',
                               nrow           = 1)
      ) +
      # labs(
      #    title    = glue("Diagnosis among {name} Residents"),
      #    subtitle = "Number of diagnosed HIV cases per city/municipality",
      #    caption  = "Data: HIV/AIDS & ART Registry of the Philippines, August 2022"
      # ) +
      theme(
         text                 = element_text(color = "#22211d", family = "Infra"),
         plot.background      = element_rect(fill = "#f5f5f2", color = NA),
         panel.background     = element_rect(fill = "#f5f5f2", color = NA),
         legend.background    = element_rect(fill = "#f5f5f2", color = NA),

         plot.title           = element_text(
            size   = 22,
            hjust  = 0.01,
            color  = "#4e4d47",
            # margin = margin(b = -0.1, t = 0.4, l = 2, unit = "cm"),
            family = "Infra",
            face   = "bold"
         ),
         plot.subtitle        = element_text(
            size   = 17,
            hjust  = 0.01,
            color  = "#4e4d47",
            # margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm"),
            family = "Infra",
            face   = "plain"
         ),
         plot.caption         = element_text(
            size   = 12, color = "#4e4d47",
            # margin = margin(b = 0.3, r = -99, unit = "cm"),
            family = "Infra",
            face   = "italic"
         ),
         plot.margin          = grid::unit(c(3, 2, 3, 2), "mm"),

         legend.position      = "bottom",
         legend.justification = "left",
         legend.title         = element_text(
            size   = 30,
            family = "Infra",
            face   = "bold"
         ),
         legend.text          = element_text(
            size   = 30,
            family = "Infra",
            face   = "bold"
         ),
      )
   .log_info("Saving {green(name)}.")

   plot_ratio <- get_asp_ratio(spdf_region)
   len_y      <- 12
   len_x      <- plot_ratio * len_y
   # len_x      <- ifelse(len_x <= 8, 8, len_x)

   ggsave(glue("{subdir}/dx.png"), p, "png", width = len_x, height = len_y)
   # agg_png(glue("{subdir}/dx.png"), width = len_x, height = len_y, units = 'in')
   # print(p)
   # dev.off()
}

ggsave("H:/trialdx_map.pdf", p, "pdf")
st_write(spdf_simplified, "H:/ph_aem.geojson", driver = "GeoJSON")
st_write(spdf_simplified, "H:/ph_aem_cata_rotp_hd.shp")

jsonlite::write_json(spdf_simplified, "H:/trial.geojson")

spdf_geojson <- data.frame()
for (i in seq_len(nrow(spdf_filtered))) {
   spdf_geojson <- spdf_geojson %>%
      bind_rows(
         data.frame(
            ISO        = spdf_filtered[i,]$ISO,
            ADM1_EN    = spdf_filtered[i,]$ADM1_EN,
            ADM1_PCODE = spdf_filtered[i,]$ADM1_PCODE,
            NAME_1     = spdf_filtered[i,]$NAME_1,
            dx         = spdf_filtered[i,]$dx,
            # geometry   = as.character(jsonlite::toJSON(jsonlite::fromJSON(geojson_json(spdf_filtered[i,]))$
            #                                               features$
            #                                               geometry$
            #                                               coordinates)),
            geometry   = as.character(jsonlite::toJSON(jsonlite::fromJSON(geojson_json(spdf_filtered[i,]))))
         )
      )
}


lw_conn <- ohasis$conn("lw")
schema  <- Id(schema = "harp", table = "aem_dx")
if (dbExistsTable(lw_conn, schema))
   dbRemoveTable(lw_conn, schema)

dbCreateTable(lw_conn, schema, spdf_geojson)
# dbxUpsert(lw_conn, schema, full_faci, c("art_id", "faci"), batch_size = 1000)
ohasis$upsert(lw_conn, "harp", "aem_dx", spdf_geojson, "ISO")
dbDisconnect(lw_conn)

write_csv(data, "H:/_QGIS/stirup2022/diagnosis")

json <- jsonlite::read_json("H:/ph_aem.geojson",)

json_dta <- data.frame()
tag      <- 1
for (i in seq_len(length(json$features))) {
   l1 <- max(lengths(lapply(json$features[[i]]$geometry$coordinates, unlist)))
   for (j in seq_len(length(json$features[[i]]$geometry$coordinates))) {
      for (k in seq_len(length(json$features[[i]]$geometry$coordinates[[j]]))) {
         if (length(json$features[[i]]$geometry$coordinates[[j]][[k]]) == 2) {
            json$features[[i]]$geometry$coordinates[[j]][[k]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]][[k]])), "]")
            tag                                               <- 0
         } else {
            for (l in seq_len(length(json$features[[i]]$geometry$coordinates[[j]][[k]]))) {
               json$features[[i]]$geometry$coordinates[[j]][[k]][[l]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]][[k]][[l]])), "]")
            }
            tag <- 1

            json$features[[i]]$geometry$coordinates[[j]][[k]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]][[k]])), "]")
            orig_len                                          <- length(json$features[[i]]$geometry$coordinates[[j]][[k]])
            # json$features[[i]]$geometry$coordinates[[j]][[k]] <- unlist(jsonlite::toJSON(json$features[[i]]$geometry$coordinates[[j]][[k]]))
            #
            # if (orig_len == (l1 / 2)) {
            #    json$features[[i]]$geometry$coordinates[[j]][[k]] <- substr(
            #       json$features[[i]]$geometry$coordinates[[j]][[k]],
            #       2,
            #       nchar(json$features[[i]]$geometry$coordinates[[j]][[k]]) - 1
            #    )
            # }

            # if (orig_len == (l1 / 2)) {
            #    json$features[[i]]$geometry$coordinates <- json$features[[i]]$geometry$coordinates[[j]][[k]]
            # }
         }
      }
      json$features[[i]]$geometry$coordinates[[j]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]])), "]")
   }
   if (length(json$features[[i]]$geometry$coordinates) == 1) {
      df <- tibble(
         ELEM_NUM = i,
         ISO      = json$features[[i]]$properties$ISO,
         NAME_1   = json$features[[i]]$properties$NAME_1,
         dx       = json$features[[i]]$properties$dx,
         geojson  = paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates))
      )
   } else {
      df <- tibble(
         ELEM_NUM = i,
         ISO      = json$features[[i]]$properties$ISO,
         NAME_1   = json$features[[i]]$properties$NAME_1,
         dx       = json$features[[i]]$properties$dx,
         geojson  = paste0(r"([)", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates)), r"(])")
      )
   }

   if (nrow(json_dta) == 0)
      json_dta <- df
   else
      json_dta <- bind_rows(json_dta, df)
}
json_dta %<>% distinct_all()
spdf_geojson <- json_dta