dx       <- read_dta(hs_data("harp_full", "reg", 2024, 5))
analysis <- dx %>%
   mutate(
      confirm_branch = "",
      confirm_type   = case_when(
         rhivda_done == 1 ~ "CrCL",
         confirmlab == "SACCL" ~ "SACCL",
         TRUE ~ "Others"
      ),
      artstart_date  = case_when(
         art_id == 106685 ~ as.Date("2020-09-01"),
         TRUE ~ artstart_date
      )
   ) %>%
   dxlab_to_id(
      c("HARP_FACI", "HARP_SUB_FACI"),
      c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
      ohasis$ref_faci
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(CONFIRM_FACI = "confirmlab", CONFIRM_SUB_FACI = "confirm_branch")
   ) %>%
   mutate_at(
      .vars = vars(HARP_FACI, HARP_SUB_FACI, CONFIRM_FACI, CONFIRM_SUB_FACI),
      ~replace_na(., "")
   ) %>%
   ohasis$get_faci(
      list(DX_LAB = c("HARP_FACI", "HARP_SUB_FACI")),
      "name",
      c("DX_REG", "DX_PROV", "DX_MUNC")
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
   ohasis$get_addr(
      c(
         PERM_REG  = "PERM_PSGC_REG",
         PERM_PROV = "PERM_PSGC_PROV",
         PERM_MUNC = "PERM_PSGC_MUNC"
      ),
      "name"
   )

# NOTE ALL PER FACI
lala_dxahd <- function(data, ...) {
   return(
      data %>%
         group_by(..., year) %>%
         summarise(
            dx  = n(),
            ahd = sum(class2022 == "AIDS")
         ) %>%
         mutate(
            `%ahd` = coalesce(ahd, 0) / dx,
            dx     = cumsum(dx),
            ahd    = cumsum(ahd)
         ) %>%
         ungroup() %>%
         arrange(year) %>%
         pivot_longer(
            cols = c(dx, ahd),
         ) %>%
         pivot_wider(
            id_cols     = c(..., name),
            names_from  = year,
            values_from = value
         ) %>%
         arrange(...)
   )
}

disagg_tat <- function(tat) {
   tat <- floor(tat)
   return(case_when(
      tat < 0 ~ "0) Before",
      tat == 0 ~ "1) Same day",
      tat %in% seq(1, 7) ~ "2) Within 7 days",
      tat %in% seq(8, 14) ~ "3) 8-14 days",
      tat >= 15 ~ "4) 15 days and above",
   ))
}

# per faci dx cases + ahd, yearly
dxahd_dxfaci <- lala_dxahd(analysis, DX_REG, DX_PROV, DX_MUNC, DX_LAB)
dxahd_dxreg  <- lala_dxahd(analysis, DX_REG)
dxahd_resreg <- lala_dxahd(analysis, PERM_REG)

# mgiration, resides in x, diagnosed not in x, vice versa
# include initiation rate (freq + perc)
migration <- analysis %>%
   group_by(PERM_REG, DX_REG) %>%
   summarise(
      Migration = n(),
      Enrolled  = sum(everonart == 1, na.rm = TRUE)
   ) %>%
   ungroup()

migration_res    <- split(migration, ~PERM_REG)
migration_res    <- lapply(migration_res, mutate, `Initiation Rate` = coalesce(Enrolled, 0) / Migration)
migration_res    <- lapply(migration_res, adorn_totals)
migration_dxfaci <- split(migration, ~DX_REG)
migration_dxfaci <- lapply(migration_dxfaci, adorn_totals)
migration_dxfaci <- lapply(migration_dxfaci, mutate, `Initiation Rate` = coalesce(Enrolled, 0) / Migration)

# trend of newly dx cases per month, Jan 2022 - May 2024
trend_dxfaci <- analysis %>%
   filter(year >= 2022) %>%
   mutate(
      ym = end_ym(year, month),
      ym = as.Date(ym),
   ) %>%
   group_by(DX_REG, DX_PROV, DX_MUNC, DX_LAB, ym) %>%
   summarise(
      dx = n()
   ) %>%
   ungroup() %>%
   arrange(ym) %>%
   mutate(
      ym = format(ym, "%b %Y")
   ) %>%
   pivot_wider(
      id_cols     = c(DX_REG, DX_PROV, DX_MUNC, DX_LAB),
      names_from  = ym,
      values_from = dx
   ) %>%
   arrange(DX_REG, DX_PROV, DX_MUNC, DX_LAB)
trend_dxfaci <- split(trend_dxfaci, ~DX_REG)
trend_dxfaci <- lapply(trend_dxfaci, adorn_totals)

# saccl vs rhivda
nrlcrcl_dxfaci <- analysis %>%
   filter(year >= 2021) %>%
   group_by(DX_REG, DX_PROV, DX_MUNC, DX_LAB, confirm_type) %>%
   summarise(
      dx = n()
   ) %>%
   ungroup() %>%
   pivot_wider(
      id_cols     = c(DX_REG, DX_PROV, DX_MUNC, DX_LAB),
      names_from  = confirm_type,
      values_from = dx
   ) %>%
   arrange(DX_REG, DX_PROV, DX_MUNC, DX_LAB)
nrlcrcl_dxfaci <- split(nrlcrcl_dxfaci, ~DX_REG)
nrlcrcl_dxfaci <- lapply(nrlcrcl_dxfaci, mutate, `%CrCL` = CrCL / (coalesce(CrCL, 0) +
   coalesce(SACCL, 0)))
nrlcrcl_dxfaci <- lapply(nrlcrcl_dxfaci, mutate, `%SACCL` = SACCL / (coalesce(CrCL, 0) +
   coalesce(SACCL, 0)))
nrlcrcl_dxfaci <- lapply(nrlcrcl_dxfaci, adorn_totals)

# tat confirmatory results from reactive_date
tatconfirm_dxreg <- analysis %>%
   mutate(
      tat = interval(reactive_date, confirm_date) / days(1),
      tat = disagg_tat(tat)
   ) %>%
   group_by(DX_REG, tat, confirm_type) %>%
   summarise(
      Enrolled = n()
   ) %>%
   ungroup() %>%
   pivot_wider(
      id_cols     = c(DX_REG, tat),
      names_from  = confirm_type,
      values_from = Enrolled
   )
tatconfirm_dxreg <- split(tatconfirm_dxreg, ~DX_REG)
tatconfirm_dxreg <- lapply(tatconfirm_dxreg, mutate, `%CrCL` = CrCL / sum(CrCL, na.rm = TRUE))
tatconfirm_dxreg <- lapply(tatconfirm_dxreg, mutate, `%SACCL` = SACCL / sum(SACCL, na.rm = TRUE))
tatconfirm_dxreg <- lapply(tatconfirm_dxreg, adorn_totals)

# tat confirmation to artstart, saccl vs rhivda
tatenroll_dxreg  <- analysis %>%
   filter(everonart == 1) %>%
   mutate(
      tat = interval(confirm_date, artstart_date) / days(1),
      tat = disagg_tat(tat)
   ) %>%
   group_by(DX_REG, tat, confirm_type) %>%
   summarise(
      Enrolled = n()
   ) %>%
   ungroup() %>%
   pivot_wider(
      id_cols     = c(DX_REG, tat),
      names_from  = confirm_type,
      values_from = Enrolled
   )
tatenroll_dxreg  <- split(tatenroll_dxreg, ~DX_REG)
tatenroll_dxreg  <- lapply(tatenroll_dxreg, mutate, `%CrCL` = CrCL / sum(CrCL, na.rm = TRUE))
tatenroll_dxreg  <- lapply(tatenroll_dxreg, mutate, `%SACCL` = SACCL / sum(SACCL, na.rm = TRUE))
tatenroll_dxreg  <- lapply(tatenroll_dxreg, adorn_totals)
tatenroll_dxfaci <- analysis %>%
   filter(everonart == 1) %>%
   mutate(
      tat = interval(confirm_date, artstart_date) / days(1),
      tat = disagg_tat(tat)
   ) %>%
   group_by(DX_REG, DX_PROV, DX_MUNC, DX_LAB, tat, confirm_type) %>%
   summarise(
      Enrolled = n()
   ) %>%
   ungroup() %>%
   pivot_wider(
      id_cols     = c(DX_REG, DX_PROV, DX_MUNC, DX_LAB, tat),
      names_from  = confirm_type,
      values_from = Enrolled
   ) %>%
   pivot_wider(
      id_cols     = c(DX_REG, DX_PROV, DX_MUNC, DX_LAB),
      names_from  = tat,
      values_from = c(CrCL, SACCL, Others)
   ) %>%
   arrange(DX_REG, DX_PROV, DX_MUNC, DX_LAB)
tatenroll_dxfaci <- split(tatenroll_dxfaci, ~DX_REG)

# by dxreg, unique dxlab, dx
nlab_dxreg <- analysis %>%
   group_by(DX_REG, year) %>%
   summarise(
      dx    = n(),
      facis = n_distinct(DX_LAB)
   ) %>%
   ungroup() %>%
   arrange(year) %>%
   pivot_wider(
      id_cols     = DX_REG,
      names_from  = year,
      values_from = c(dx, facis)
   )

dir <- "C:/Users/johnb/Downloads/stirup2024_lala"
write_xlsx(dxahd_dxfaci, file.path(dir, "dxahd_dxfaci.xlsx"))
write_xlsx(dxahd_dxreg, file.path(dir, "dxahd_dxreg.xlsx"))
write_xlsx(dxahd_resreg, file.path(dir, "dxahd_resreg.xlsx"))
write_xlsx(migration_res, file.path(dir, "migration_res.xlsx"))
write_xlsx(migration_dxfaci, file.path(dir, "migration_dxfaci.xlsx"))
write_xlsx(trend_dxfaci, file.path(dir, "trend_dxfaci.xlsx"))
write_xlsx(nrlcrcl_dxfaci, file.path(dir, "nrlcrcl_dxfaci.xlsx"))
write_xlsx(tatconfirm_dxreg, file.path(dir, "tatconfirm_dxreg.xlsx"))
write_xlsx(tatenroll_dxreg, file.path(dir, "tatenroll_dxreg.xlsx"))
write_xlsx(tatenroll_dxfaci, file.path(dir, "tatenroll_dxfaci.xlsx"))
write_xlsx(nlab_dxreg, file.path(dir, "nlab_dxreg.xlsx"))
write_xlsx(plhiv, file.path(dir, "plhiv_2019-2024.xlsx"))

##  plhiv
years       <- seq(2019, 2024)
files       <- lapply(years, hs_data, sys = "harp_full", type = "reg", mo = "12")
full        <- lapply(files, read_dta, col_select = c(region, dead, outcome, mort))
full        <- lapply(full, mutate, plhiv = if_else((dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"), 1, 0, 0))
full        <- lapply(full, group_by, region)
full        <- lapply(full, summarise, plhiv = sum(plhiv == 1, na.rm = TRUE))
names(full) <- years

plhiv <- bind_rows(full, .id = "year") %>%
   pivot_wider(
      id_cols     = region,
      names_from  = year,
      values_from = plhiv
   )

## ahd
lala_ahd <- function(data, ...) {
   return(
      data %>%
         filter(year >= 2011) %>%
         group_by(..., year) %>%
         summarise(
            dx  = n(),
            ahd = sum(class2022 == "AIDS")
         ) %>%
         ungroup() %>%
         mutate(
            `%ahd` = coalesce(ahd, 0) / dx
         )
   )
}

ahd_res    <- lala_ahd(analysis, PERM_REG)
ahd_dxreg  <- lala_ahd(analysis, DX_REG)
ahd_dxfaci <- lala_ahd(analysis, DX_REG, DX_PROV, DX_MUNC, DX_LAB)
write_xlsx(ahd_res, file.path(dir, "ahd_res.xlsx"))
write_xlsx(ahd_dxreg, file.path(dir, "ahd_dxreg.xlsx"))
write_xlsx(ahd_dxfaci, file.path(dir, "ahd_dxfaci.xlsx"))

# tat reactive
tatreactive_saccl <- analysis %>%
   filter(year >= 2021, confirmlab == "SACCL") %>%
   mutate(
      tat = interval(reactive_date, confirm_date) / days(1),
      tat = disagg_tat(tat)
   ) %>%
   group_by(DX_REG, tat) %>%
   summarise(
      Dx = n()
   ) %>%
   ungroup() %>%
   pivot_wider(
      id_cols     = DX_REG,
      names_from  = tat,
      values_from = Dx
   )
write_xlsx(tatreactive_saccl, file.path(dir, "tatreactive_saccl.xlsx"))


## -----------------------------------------------------------------------------
excel_sheets("C:/Users/johnb/Downloads/stirup2024_lala/migration_res.xlsx")
reg <- "Region XIII (Caraga)"
lala_migration(reg)


lala_migration <- function(reg) {
   res     <- read_excel("C:/Users/johnb/Downloads/stirup2024_lala/migration_res.xlsx", reg)
   clients <- read_excel("C:/Users/johnb/Downloads/stirup2024_lala/migration_dxfaci.xlsx", reg)

   res %>%
      relocate(DX_REG, .before = 1) %>%
      filter(PERM_REG != "Total") %>%
      mutate(
         `%`  = Migration / sum(Migration),
         `% ` = Enrolled / sum(Enrolled),
      ) %>%
      adorn_totals() %>%
      select(
         Region                                              = DX_REG,
         `Resides in the Region, Diagnosed in other Regions` = Migration,
         `%`,
         `Initiated on ART among diagnosed`                  = Enrolled,
         `% `
      ) %>%
      full_join(
         y  = clients %>%
            filter(PERM_REG != "Total") %>%
            mutate(
               `%  `  = Migration / sum(Migration),
               `%   ` = Enrolled / sum(Enrolled),
            ) %>%
            adorn_totals() %>%
            select(
               Region                                         = PERM_REG,
               `Resides in other Region, Diagnosed in Region` = Migration,
               `%  `,
               `Initiated on ART among diagnosed `            = Enrolled,
               `%   `
            ),
         by = join_by(Region)
      ) %>%
      mutate_at(
         .vars = vars(contains("%")),
         ~if_else(!is.na(.), stri_c(format(. * 100, digits = 2), "%"), as.character(.))
      ) %>%
      mutate(
         sort = case_when(
            Region == "Total" ~ 999,
            TRUE ~ 1
         )
      ) %>%
      arrange(sort, Region) %>%
      select(-sort) %>%
      write_clip()
}