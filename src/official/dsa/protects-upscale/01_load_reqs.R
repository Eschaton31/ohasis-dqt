cities    <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", "gf-city")
supported <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", "Sheet1")
supported %<>%
   rename_all(tolower) %>%
   filter(site_gf_2024 == 1, !is.na(faci_id))

cities %<>%
   left_join(
      y          = ohasis$ref_addr %>%
         select(
            FACI_PSGC_MUNC = psgc_old,
            psgc_reg       = reg,
            psgc_prov      = prov,
            psgc_munc      = munc
         ),
      by         = join_by(FACI_PSGC_MUNC),
      na_matches = "never"
   )

sites <- ohasis$ref_faci %>%
   rename(
      faci_psgc_reg  = addr_psgc_reg,
      faci_psgc_prov = addr_psgc_prov,
      faci_psgc_munc = addr_psgc_munc,
   ) %>%
   inner_join(
      y  = cities %>%
         select(
            faci_psgc_reg  = psgc_reg,
            faci_psgc_prov = psgc_prov,
            faci_psgc_munc = psgc_munc
         ),
      by = join_by(
         faci_psgc_reg,
         faci_psgc_prov,
         faci_psgc_munc
      )
   ) %>%
   left_join(
      y  = supported %>%
         select(faci_id, site_gf_2024),
      by = join_by(faci_id)
   )

min <- "2025-01-01"
max <- "2025-09-30"
yr  <- "2025"
mo  <- "09"

faci_type <- read_sheet("1aOqYjx5wbc403xy-64YHJU6NzhEBRUu6Ldg59yDEUMw", "Sheet1", range = "A:D", col_types = "c")
read_csv("C:/Users/Bene-G16/Downloads/facilities-20251017.csv") %>%
   left_join(
      y = sites %>%
         filter(site_gf_2024 == 1) %>%
         distinct(faci_id = faci_id, site_gf_2024)
   ) %>%
   select(
      'faci_id', 'sub_faci_id', 'faci_name', 'ownership', 'addr_name_reg', 'addr_name_prov', 'addr_name_munc', 'physical_address', 'mobile', 'email', 'longitude', 'latitude', 'site_gf_2024'
   ) %>%
   write_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "ref_faci")

conn  <- connect('ohasis-live')
staff <- QB$new(conn)$from("ohasis.users")$get() %>%
   inner_join(
      y = sites %>%
         filter(site_gf_2024 == 1) %>%
         distinct(faci_id, site_gf_2024)
   ) %>%
   unite(
      col   = "fms",
      sep   = " ",
      first,
      middle,
      suffix,
      na.rm = TRUE
   ) %>%
   unite(
      col   = "fullname",
      sep   = ", ",
      last,
      fms,
      na.rm = TRUE
   ) %>%
   mutate(
      is_user = user_name != '' & password != ''
   ) %>%
   select(
      faci_id,
      staff_id     = user_id,
      staff_name   = fullname,
      staff_desig  = designation,
      staff_email  = email,
      staff_mobile = mobile,
      is_user
   )
dbDisconnect(conn)

# ohasis$ref_staff %>%
#    inner_join(
#       y = sites %>%
#          filter(site_gf_2024 == 1) %>%
#          distinct(faci_id, site_gf_2024)
#    ) %>%
#    select(
#       faci_id,
#       staff_id,
#       staff_name,
#       staff_desig,
#       staff_email  = email,
#       staff_mobile = mobile,
#    ) %>%
#    write_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "ref_staff")

staff %>%
   write_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "ref_staff")
