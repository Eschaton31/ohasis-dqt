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
   textclean
)
pse    <- new.env()
epictr <- new.env()

# set params
pse$dirs      <- list()
pse$dirs$refs <- file.path(getwd(), "src", "official", "hivepicenter")
pse$dirs$wd   <- file.path(getwd(), "src", "official", "harp_maps")

# get references
source(file.path(pse$dirs$refs, "00_refs.R"))
source(file.path(pse$dirs$refs, "02_estimates.R"))

pse$ref_addr <- epictr$ref_addr %>%
   select(
      region   = NHSSS_REG,
      province = NHSSS_PROV,
      muncity  = NHSSS_MUNC,
      PSGC_REG,
      PSGC_PROV,
      PSGC_MUNC
   )
pse$files    <- list(
   pse_gf = "H:/Software/OHASIS/DQT/src/official/pse2023/2022-Pop-Size-estimates-with-GF.dta",
   harp   = "H:/Software/OHASIS/DQT/src/official/pse2023/harp-cases-with-psgc.dta",
   labbs  = "H:/Software/OHASIS/DQT/src/official/pse2023/labbs.dta",
   hepc   = "H:/Software/OHASIS/DQT/src/official/pse2023/Hep-C-merged.dta",
   dx     = "C:/Users/Administrator/Downloads/harpdx.dta"
)

pse$data <- lapply(pse$files, read_dta)

pse$psgc$pse_gf <- pse$data$pse_gf %>%
   select(-REGION_NAME, -mergeid) %>%
   rename(region = REGION) %>%
   mutate(
      region   = case_when(
         region == "BARMM" ~ "ARMM",
         TRUE ~ region
      ),
      province = case_when(
         province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         province == "COTABATO (NORTH COTABATO)" ~ "COTABATO",
         province == "SAMAR (WESTERN SAMAR)" ~ "SAMAR",
         province == "BASILAN" & muncity == "ISABELA" ~ "BASILAN-RO9",
         TRUE ~ province
      ),
      muncity  = case_when(
         str_detect(muncity, "SCIENCE MU.OZ") ~ str_extract(muncity, "MU.OZ"),
         muncity == "ISLAND GARDEN SAMAL" ~ "SAMAL",
         muncity == "STO. TOMAS" ~ "SANTO TOMAS",
         TRUE ~ muncity
      )
   ) %>%
   left_join(pse$ref_addr)

pse$psgc$dx <- pse$data$dx %>%
   mutate(
      overseas_addr = case_when(
         muncity == "OUT OF COUNTRY" ~ 1,
         TRUE ~ 0
      ),

      # address
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
   ) %>%
   left_join(pse$ref_addr)

pse$psgc$harp <- pse$data$harp %>%
   select(-starts_with("RES_PSGC")) %>%
   mutate(
      overseas_addr = case_when(
         muncity == "OUT OF COUNTRY" ~ 1,
         TRUE ~ 0
      ),

      # address
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
   ) %>%
   left_join(pse$ref_addr) %>%
   group_by(region, province, muncity, PSGC_REG, PSGC_PROV, PSGC_MUNC) %>%
   summarise_at(
      vars(-group_cols()),
      ~sum(.)
   )

pse$psgc$labbs <- pse$data$labbs %>%
   mutate(
      region   = case_when(
         province == "BATANES" ~ "2",
         muncity == "MARAWI" ~ "ARMM",
         TRUE ~ region
      ),
      province = case_when(
         muncity == "COTABATO" ~ "COTABATO",
         province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         province == "COTABATO (NORTH COTABATO)" ~ "COTABATO",
         province == "SAMAR (WESTERN SAMAR)" ~ "SAMAR",
         province == "BASILAN" & muncity == "ISABELA" ~ "BASILAN-RO9",
         TRUE ~ province
      ),
      muncity  = case_when(
         str_detect(muncity, "SCIENCE MU.OZ") ~ str_extract(muncity, "MU.OZ"),
         muncity == "ISLAND GARDEN SAMAL" ~ "SAMAL",
         muncity == "STO. TOMAS" ~ "SANTO TOMAS",
         muncity == "CLARK" ~ "ANGELES",
         province == "BATAAN" & muncity == "SABANGAN" ~ "BALANGA",
         TRUE ~ muncity
      )
   ) %>%
   left_join(pse$ref_addr) %>%
   group_by(region, province, muncity, PSGC_REG, PSGC_PROV, PSGC_MUNC) %>%
   summarise_at(
      vars(-group_cols()),
      ~sum(.)
   ) %>%
   ungroup()

pse$psgc$hepc <- pse$data$hepc %>%
   mutate(
      region   = str_replace(region, "^0", ""),
      muncity  = str_replace(muncity, " CITY$", ""),

      province = case_when(
         muncity == "BAGUIO" ~ "BENGUET",
         muncity == "DAVAO" ~ "DAVAO DEL SUR",
         muncity == "GENERAL SANTOS" ~ "SOUTH COTABATO",
         province == "METRO MANILA" ~ "NCR",
         province == "BASILAN9" ~ "BASILAN-RO9",
         TRUE ~ province
      ),
      muncity  = case_when(
         muncity == "JETAFE" ~ "GETAFE",
         muncity == "CORDOBA" ~ "CORDOVA",
         muncity == "TONDO" ~ "MANILA",
         muncity == "SAMPALOC" ~ "MANILA",
         TRUE ~ muncity
      ),
   ) %>%
   left_join(pse$ref_addr)

final <- pse$psgc$pse_gf %>%
   select(-region, -province, -muncity) %>%
   full_join(
      pse$psgc$harp %>%
         select(-region, -province, -muncity),
      join_by(PSGC_REG, PSGC_PROV, PSGC_MUNC)
   ) %>%
   full_join(
      pse$psgc$dx %>%
         select(-region, -province, -muncity),
      join_by(PSGC_REG, PSGC_PROV, PSGC_MUNC)
   ) %>%
   full_join(
      pse$psgc$labbs %>%
         select(-region, -province, -muncity),
      join_by(PSGC_REG, PSGC_PROV, PSGC_MUNC)
   ) %>%
   select(-`_merge`) %>%
   select(-starts_with("overseas_addr")) %>%
   relocate(PSGC_REG, PSGC_PROV, PSGC_MUNC, .before = 1) %>%
   left_join(
      epictr$ref_addr %>%
         select(
            region      = NHSSS_REG,
            province    = NHSSS_PROV,
            muncity     = NHSSS_MUNC,
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            prov_income = INCOME_PROV,
            munc_income = INCOME_MUNC,
            popcen2015  = POPCEN_MUNC_2015,
            popcen2020  = POPCEN_MUNC_2020,
         )
   ) %>%
   relocate(region, province, muncity, .before = 1)

file <- "H:/20230724_pse_harp-dx-labbs-gf_2023-05.dta"
final %>%
   write_dta(file)
compress_stata(file)


##  PSE Plots
city <- list(
   angeles  = c("A3:F7", "Angeles City"),
   baguio   = c("A17:F21", "Baguio City"),
   bacolod  = c("A27:F31", "Bacolod City"),
   bacoor   = c("A35:F39", "Bacoor City"),
   batangas = c("A43:F47", "Batangas City"),
   cdo      = c("A53:F57", "Cagayan De Oro City"),
   cebu     = c("A61:F65", "Cebu City"),
   davao    = c("A69:F73", "Davao City"),
   gensan   = c("A77:F81", "General Santos City"),
   iloilo   = c("A85:F89", "Iloilo City"),
   naga     = c("A93:F97", "Naga City"),
   ppc      = c("A101:F105", "Puerto Princesa City"),
   tugue    = c("A109:F113", "Tuguegarao City"),
   zambo    = c("A119:F123", "Zamboanga City"),
   ncr      = c("A128:F132", "National Capital Region")
)

data <- lapply(city, function(ref) {
   cells <- ref[1]
   city  <- ref[2]
   df    <- read_xlsx("C:/Users/Administrator/Downloads/graph-1.xlsx", range = cells, .name_repair = "unique_quiet") %>%
      rename(
         est                = 1,
         prior              = 2,
         uo                 = 3,
         service_multiplier = 4,
         sspse              = 5,
         mean               = 6
      ) %>%
      mutate(
         est = toupper(est),
         est = case_when(
            est == "LOW ESTIMATE" ~ "low",
            est == "POINT ESTIMATE" ~ "point",
            est == "HIGH ESTIMATE" ~ "high",
         )
      ) %>%
      pivot_longer(
         cols = c(prior, uo, service_multiplier, sspse, mean)
      ) %>%
      filter(!is.na(est)) %>%
      pivot_wider(
         values_from = value,
         id_cols     = name,
         names_from  = est,
      ) %>%
      mutate(
         .before = 1,
         muncity = city
      )

   return(df)
})

p_load(plotly, quantmod)

fig <- data$angeles %>%
   plot_ly(
      x    = ~name,
      type = "candlestick",
      open = ~point, close = ~point,
      high = ~high, low = ~low
   )
fig <- fig %>% layout(title = "Basic Candlestick Chart")

fig