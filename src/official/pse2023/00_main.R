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
   hepc   = "H:/Software/OHASIS/DQT/src/official/pse2023/Hep-C-merged.dta"
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

pse$psgc$harp <- pse$data$harp %>%
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
      pse$psgc$labbs %>%
         select(-region, -province, -muncity),
      join_by(PSGC_REG, PSGC_PROV, PSGC_MUNC)
   ) %>%
   select(-`_merge`) %>%
   relocate(PSGC_REG, PSGC_PROV, PSGC_MUNC, .before = 1) %>%
   left_join(pse$ref_addr) %>%
   relocate(region, province, muncity, .before = 1)

file <- "H:/20230720_pse_harp-labbs-gf_2023-05.dta"
final %>%
   format_stata() %>%
   write_dta(file)
compress_stata(file)
