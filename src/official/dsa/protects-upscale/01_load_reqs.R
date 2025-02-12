cities    <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", "gf-city")
supported <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", "Sheet1")
supported %<>%
   filter(site_gf_2024 == 1, !is.na(FACI_ID))

sites <- ohasis$ref_faci %>%
   inner_join(
      y  = cities %>%
         select(
            FACI_PSGC_REG,
            FACI_PSGC_PROV,
            FACI_PSGC_MUNC
         ),
      by = join_by(
         FACI_PSGC_REG,
         FACI_PSGC_PROV,
         FACI_PSGC_MUNC
      )
   ) %>%
   left_join(
      y = supported %>%
         select(FACI_ID, site_gf_2024),
      by = join_by(FACI_ID)
   )

min <- "2024-01-01"
max <- "2024-12-31"
yr  <- "2024"
mo  <- "12"

faci_type <- read_sheet("1aOqYjx5wbc403xy-64YHJU6NzhEBRUu6Ldg59yDEUMw", "Sheet1", range = "A:D", col_types = "c")
ohasis$ref_faci %>%
   left_join(
      y = sites %>%
         filter(site_gf_2024 == 1) %>%
         distinct(FACI_ID, site_gf_2024)
   ) %>%
   left_join(
      y = faci_type %>%
         distinct(FACI_ID = HARP_FACI, FINAL_PUBPRIV)
   ) %>%
   select(
      'FACI_ID', 'SUB_FACI_ID', 'FACI_NAME', 'FINAL_PUBPRIV', 'FACI_NAME_REG', 'FACI_NAME_PROV', 'FACI_NAME_MUNC', 'FACI_ADDR', 'MOBILE', 'EMAIL', 'LONG', 'LAT', 'site_gf_2024'
   ) %>%
   write_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "ref_faci")

staff <- QB$new(`oh-live`)$from("ohasis_interim.users")$get() %>%
   inner_join(
      y = sites %>%
         filter(site_gf_2024 == 1) %>%
         distinct(FACI_ID, site_gf_2024)
   ) %>%
   unite(
      col   = "FMS",
      sep   = " ",
      FIRST,
      MIDDLE,
      SUFFIX,
      na.rm = TRUE
   ) %>%
   unite(
      col   = "FULLNAME",
      sep   = ", ",
      LAST,
      FMS,
      na.rm = TRUE
   ) %>%
   mutate(
      IS_USER = USER_NAME != '' & PASSWORD != ''
   ) %>%
   select(
      FACI_ID,
      STAFF_ID     = USER_ID,
      STAFF_NAME   = FULLNAME,
      STAFF_DESIG  = DESIGNATION,
      STAFF_EMAIL  = EMAIL,
      STAFF_MOBILE = MOBILE,
      IS_USER
   )

ohasis$ref_staff %>%
   inner_join(
      y = sites %>%
         filter(site_gf_2024 == 1) %>%
         distinct(FACI_ID, site_gf_2024)
   ) %>%
   select(
      FACI_ID,
      STAFF_ID,
      STAFF_NAME,
      STAFF_DESIG,
      STAFF_EMAIL  = EMAIL,
      STAFF_MOBILE = MOBILE,
   ) %>%
   write_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "ref_staff")

staff %>%
   write_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "ref_staff")
