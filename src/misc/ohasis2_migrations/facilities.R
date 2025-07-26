facilities <- OhasisMigration$new('facilities', 'local', 'oh2')
facilities$download()

psgc_main <- 'C:/Users/Bene-G16/Downloads/Documents/PSGC-4Q-2024-Publication-Datafile.xlsx' %>%
   read_xlsx(sheet = 'PSGC', col_types = 'text') %>%
   select(
      PSGC             = `10-digit PSGC`,
      PSGC_OLD         = `Correspondence Code`,
      NAME             = `Name`,
      PSGL             = `Geographic Level`,
      NAME_OLD         = `Old names`,
      CLASS_CITY       = `City Class`,
      CLASS_INCOME     = `Income\r\nClassification`,
      URBAN_RURAL_2020 = `Urban / Rural\r\n(based on 2020 CPH)`,
      POPCEN_2020      = `2020 Population`,
   ) %>%
   mutate(
      PSGL     = if_else(is.na(PSGL), 'Special', PSGL),
      PSGC_PUB = '2024-12-31',
      ALIAS    = str_extract(NAME, "\\((.+)\\)", 1),
      ALIAS    = if_else(PSGL == "Reg", ALIAS, NA_character_)
   )

facility <- QB$new(`oh-live`)$from("ohasis_interim.facility")$get()
match    <- facility %>%
   arrange(desc(EDIT_NUM)) %>%
   distinct(FACI_ID, .keep_all = TRUE) %>%
   left_join(
      y  = psgc_main %>%
         filter(!is.na(PSGC_OLD)) %>%
         select(REG = PSGC_OLD, ADDR_REG = PSGC),
      by = join_by(REG)
   ) %>%
   left_join(
      y  = psgc_main %>%
         filter(!is.na(PSGC_OLD)) %>%
         select(PROV = PSGC_OLD, ADDR_PROV = PSGC),
      by = join_by(PROV)
   ) %>%
   left_join(
      y  = psgc_main %>%
         filter(!is.na(PSGC_OLD)) %>%
         select(MUNC = PSGC_OLD, ADDR_MUNC = PSGC),
      by = join_by(MUNC)
   ) %>%
   mutate(
      ADDR_PROV = if_else(!is.na(ADDR_MUNC), stri_pad_right(str_left(ADDR_MUNC, 5), 10, '0'), ADDR_PROV, ADDR_PROV),
      ADDR_REG  = if_else(!is.na(ADDR_PROV), stri_pad_right(str_left(ADDR_PROV, 2), 10, '0'), ADDR_REG, ADDR_REG),

      ADDR_REG  = if_else(str_left(REG, 2) == '99', '9900000000', ADDR_REG, ADDR_REG),
      ADDR_PROV = if_else(str_left(REG, 2) == '99', '9999900000', ADDR_PROV, ADDR_PROV),
      ADDR_MUNC = if_else(str_left(REG, 2) == '99', '9999999000', ADDR_MUNC, ADDR_MUNC),
   ) %>%
   mutate_at(vars(ADDR_REG, ADDR_PROV, ADDR_MUNC), ~coalesce(., "")) %>%
   rename(
      FACI_NAME_ALT   = ALT_FACI_NAME,
      FACI_NAME_HARP  = FACI_NAME_CLEAN,
      LONGITUDE       = LONG,
      LATITUDE        = LAT,
      OWNERSHIP       = PUBPRIV,
      PHYSICAL_ADRESS = ADDRESS,
   ) %>%
   rename_all(tolower) %>%
   add_missing_columns(facilities$data$to) %>%
   select(any_of(names(facilities$data$to)))

pb <- progress_bar$new(
   format = ":current of :total rows | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed",
   total  = nrow(match),
   width  = 100,
   clear  = FALSE
)
for (i in seq_len(nrow(match))) {

   logo <- na_if(match[i,]$logo, '')
   logo <- file.path("Z:/xampp/htdocs/interim/img/logo", basename(logo))
   if (!is.na(logo) && file.exists(logo)) {
      match[i,]$logo <- knitr::image_uri(logo)
   }

   header <- na_if(as.character(match[i,]$header), '')
   header <- file.path("Z:/xampp/htdocs/interim/img/headers", basename(header))
   if (!is.na(header) && file.exists(header)) {
      match[i,]$header <- knitr::image_uri(header)
   }

   footer <- na_if(as.character(match[i,]$footer), '')
   footer <- file.path("Z:/xampp/htdocs/interim/img/footers", basename(footer))
   if (!is.na(footer) && file.exists(footer)) {
      match[i,]$footer <- knitr::image_uri(footer)
   }

   pb$tick(1)
}

facilities$upload(match, "faci_id")


## facility_unit
facility_unit <- OhasisMigration$new('facility_unit', 'ohasis-live', 'local')
facility_unit$download()

match <- facility_unit$data$from %>%
   left_join(
      y = data %>%
         select(FACI_ID = faci_id, addr_reg, addr_prov, addr_munc),
      by = join_by(FACI_ID)
   ) %>%
   left_join(
      y  = psgc_main %>%
         filter(!is.na(PSGC_OLD)) %>%
         select(REG = PSGC_OLD, ADDR_REG = PSGC),
      by = join_by(REG)
   ) %>%
   left_join(
      y  = psgc_main %>%
         filter(!is.na(PSGC_OLD)) %>%
         select(PROV = PSGC_OLD, ADDR_PROV = PSGC),
      by = join_by(PROV)
   ) %>%
   left_join(
      y  = psgc_main %>%
         filter(!is.na(PSGC_OLD)) %>%
         select(MUNC = PSGC_OLD, ADDR_MUNC = PSGC),
      by = join_by(MUNC)
   ) %>%
   mutate(
      ADDR_MUNC = coalesce(ADDR_MUNC, addr_munc),
      ADDR_PROV = coalesce(ADDR_PROV, addr_prov),
      ADDR_REG = coalesce(ADDR_REG, addr_reg),

      ADDR_PROV = if_else(!is.na(ADDR_MUNC), stri_pad_right(str_left(ADDR_MUNC, 5), 10, '0'), ADDR_PROV, ADDR_PROV),
      ADDR_REG  = if_else(!is.na(ADDR_PROV), stri_pad_right(str_left(ADDR_PROV, 2), 10, '0'), ADDR_REG, ADDR_REG),

      ADDR_REG  = if_else(str_left(REG, 2) == '99', '9900000000', ADDR_REG, ADDR_REG),
      ADDR_PROV = if_else(str_left(REG, 2) == '99', '9999900000', ADDR_PROV, ADDR_PROV),
      ADDR_MUNC = if_else(str_left(REG, 2) == '99', '9999999000', ADDR_MUNC, ADDR_MUNC),
   ) %>%
   select(-addr_reg, -addr_prov, -addr_munc) %>%
   mutate_at(vars(ADDR_REG, ADDR_PROV, ADDR_MUNC), ~coalesce(., "")) %>%
   rename(
      SUB_FACI_NAME_ALT = ALT_FACI_NAME,
      FACI_NAME_HARP    = SUB_FACI_NAME_CLEAN,
      LONGITUDE         = LONG,
      LATITUDE          = LAT,
      PHYSICAL_ADRESS   = ADDRESS,
   ) %>%
   rename_all(tolower) %>%
   add_missing_columns(facility_unit$data$to) %>%
   select(any_of(names(facility_unit$data$to))) %>%
   mutate(
      created_at = coalesce(created_at, as.POSIXct('2024-12-31 11:59:59'))
   )

facility_unit$upload(distinct(match, sub_faci_id, .keep_all = TRUE), "sub_faci_id")

conn           <- connect('local')
facilities     <- QB$new(conn)$from("ohasis.facilities")$get()
facility_units <- QB$new(conn)$from("ohasis.facility_units")$get()
users          <- QB$new(conn)$from("ohasis.users")$get()
dbDisconnect(conn)

write_rds(facilities, "H:/ohasis_dev-facilities.rds")
write_rds(facility_units, "H:/ohasis_dev-facility_units.rds")
write_rds(users, "H:/ohasis_dev-users.rds")

data <- read_rds("H:/ohasis_dev-facilities.rds")
conn <- connect('local')
dbAppendTable(conn, "facilities", data)
dbDisconnect(conn)