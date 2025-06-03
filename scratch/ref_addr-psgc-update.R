## -----------------------------------------------------------------------------
##  GMail                                                                     --
## -----------------------------------------------------------------------------

# Curr.addr_reg  <- readTable('addr_reg')
# Curr.ohasis[['db']][['addr_prov']] <- readTable('addr_prov')
# Curr.ohasis[['db']][['addr_munc']] <- readTable('addr_munc')


psgc_main <- 'E:/Bene-MSI/D/Downloads/Documents/PSGC-4Q-2024-Publication-Datafile.xlsx' %>%
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


psgc_reg <- psgc_main %>%
   rename(REG = PSGC) %>%
   filter(PSGL == 'Reg') %>%
   add_row(
      REG      = '9900000000',
      NAME     = 'Overseas',
      PSGC_OLD = '990000000',
      PSGC_PUB = '2024-12-31'
   )

psgc_prov <- psgc_main %>%
   rename(PROV = PSGC) %>%
   filter(PSGL == 'Prov' |
             PSGL == 'Dist' |
             PSGL == 'Special' |
             CLASS_CITY == 'HUC' |
             PROV == '1381701000') %>%
   add_row(
      PROV     = '9999900000',
      NAME     = 'Overseas',
      PSGC_OLD = '999900000',
      PSGC_PUB = '2024-12-31'
   ) %>%
   mutate(
      REG = stri_pad_right(str_left(PROV, 2), 10, '0'),
      PROV = stri_pad_right(str_left(PROV, 5), 10, '0'),
   )

psgc_munc <- psgc_main %>%
   rename(MUNC = PSGC) %>%
   filter(PSGL == 'City' |
             PSGL == 'Mun' |
             PSGL == 'SubMun' |
             MUNC == "0990100000") %>%
   add_row(
      MUNC     = '9999999000',
      NAME     = 'Overseas',
      PSGC_OLD = '999900000',
      PSGC_PUB = '2024-12-31'
   ) %>%
   mutate(
      PROV = stri_pad_right(str_left(MUNC, 5), 10, '0'),
      REG  = stri_pad_right(str_left(PROV, 2), 10, '0'),
   )

psgc_brgy <- psgc_main %>%
   rename(BRGY = PSGC) %>%
   filter(PSGL == 'Bgy') %>%
   add_row(
      BRGY     = '9999999000',
      NAME     = 'Overseas',
      PSGC_OLD = '999900000',
      PSGC_PUB = '2024-12-31'
   ) %>%
   mutate(
      MUNC = stri_pad_right(str_left(BRGY, 7), 10, '0'),
      PROV = stri_pad_right(str_left(MUNC, 5), 10, '0'),
      REG  = stri_pad_right(str_left(PROV, 2), 10, '0'),
   )


psgc <- psgc_main %>%
   add_row(
      PSGC     = '9999999000',
      NAME     = 'Overseas',
      PSGC_OLD = '999900000',
      PSGC_PUB = '2024-12-31'
   ) %>%
   mutate(
      BRGY = PSGC,
      MUNC = stri_pad_right(str_left(BRGY, 7), 10, '0'),
      PROV = stri_pad_right(str_left(MUNC, 5), 10, '0'),
      REG  = stri_pad_right(str_left(PROV, 2), 10, '0'),
   ) %>%
   left_join(
      y = psgc_brgy %>%
         select(PSGC_BRGY = BRGY, NAME_BRGY = NAME),
      by = join_by(BRGY == PSGC_BRGY)
   ) %>%
   left_join(
      y = psgc_munc %>%
         select(PSGC_MUNC = MUNC, NAME_MUNC = NAME),
      by = join_by(MUNC == PSGC_MUNC)
   ) %>%
   left_join(
      y = psgc_prov %>%
         select(PSGC_PROV = PROV, NAME_PROV = NAME),
      by = join_by(PROV == PSGC_PROV)
   ) %>%
   left_join(
      y = psgc_reg %>%
         select(PSGC_REG = REG, NAME_REG = NAME, ALIAS_REG = ALIAS),
      by = join_by(REG == PSGC_REG)
   ) %>%
   distinct(PSGC, .keep_all = TRUE)

conn <- dbConnect(
   RMariaDB::MariaDB(),
   user     = "root",
   password = "Jbrp1234",
   host     = "127.0.0.1",
   port     = 3306,
   timeout  = -1,
   "iscerv",
)

dbxUpsert(
   conn,
   "addr_reg",
   psgc_reg %>%
      select(
         region           = REG,
         region_old       = PSGC_OLD,
         name             = NAME,
         geographic_level = PSGL,
         old_names        = NAME_OLD,
         city_class       = CLASS_CITY,
         income_class     = CLASS_INCOME,
         urban_rural      = URBAN_RURAL_2020,
         popcen_2020      = POPCEN_2020,
         publication_date = PSGC_PUB,
      ) %>%
      mutate(
         created_by = 1,
         created_at = '2024-03-07 00:00:00'
      ),
   "region"
)

dbxUpsert(
   conn,
   "addr_prov",
   psgc_prov %>%
      select(
         province         = PROV,
         province_old     = PSGC_OLD,
         region           = REG,
         name             = NAME,
         geographic_level = PSGL,
         old_names        = NAME_OLD,
         city_class       = CLASS_CITY,
         income_class     = CLASS_INCOME,
         urban_rural      = URBAN_RURAL_2020,
         popcen_2020      = POPCEN_2020,
         publication_date = PSGC_PUB,
      ) %>%
      mutate(
         created_by = 1,
         created_at = '2024-03-07 00:00:00'
      ),
   "province"
)

dbxUpsert(
   conn,
   "addr_munc",
   psgc_munc %>%
      mutate(POPCEN_2020 = parse_number(POPCEN_2020)) %>%
      select(
         muncity          = MUNC,
         muncity_old      = PSGC_OLD,
         region           = REG,
         province         = PROV,
         name             = NAME,
         geographic_level = PSGL,
         old_names        = NAME_OLD,
         city_class       = CLASS_CITY,
         income_class     = CLASS_INCOME,
         urban_rural      = URBAN_RURAL_2020,
         popcen_2020      = POPCEN_2020,
         publication_date = PSGC_PUB,
      ) %>%
      mutate(
         created_by = 1,
         created_at = '2024-03-07 00:00:00'
      ),
   "muncity"
)

dbxUpsert(
   conn,
   "addr_brgy",
   psgc_brgy %>%
      mutate(POPCEN_2020 = parse_number(POPCEN_2020)) %>%
      select(
         barangay         = BRGY,
         barangay_old     = PSGC_OLD,
         region           = REG,
         province         = PROV,
         muncity          = MUNC,
         name             = NAME,
         geographic_level = PSGL,
         old_names        = NAME_OLD,
         city_class       = CLASS_CITY,
         income_class     = CLASS_INCOME,
         urban_rural      = URBAN_RURAL_2020,
         popcen_2020      = POPCEN_2020,
         publication_date = PSGC_PUB,
      ) %>%
      mutate(
         created_by = 1,
         created_at = '2024-03-07 00:00:00'
      ),
   "barangay"
)

psgc_reg <- psgc_main %>%
   rename(REG = PSGC) %>%
   filter(PSGL == 'Reg') %>%
   add_row(
      REG      = '9900000000',
      NAME     = 'Overseas',
      PSGC_OLD = '990000000',
      PSGC_PUB = '2024-12-31'
   )
mutate(NEW_REG = 1) %>%
   full_join(
      y  = Curr.ohasis[['db']][['addr_reg']] %>%
         select(REG, NHSSS) %>%
         mutate(OLD_REG = 1),
      by = 'REG'
   ) %>%
   arrange(
      NEW_REG,
      OLD_REG
   )

psgc_prov <- psgc_main %>%
   rename(PROV = PSGC) %>%
   filter(PSGL == 'Prov' |
             PSGL == 'Dist' |
             PSGL == 'Special' |
             CLASS_CITY == 'HUC' |
             PROV == '1381701000') %>%
   add_row(
      PROV     = '9999900000',
      NAME     = 'Overseas',
      PSGC_OLD = '999900000',
      PSGC_PUB = '2024-12-31'
   ) %>%
   mutate(
      REG  = stri_pad_right(str_left(PROV, 2), 10, '0'),
      PROV = stri_pad_right(str_left(PROV, 5), 10, "0")
   )
mutate(NEW_PROV = 1) %>%
   full_join(
      y  = Curr.ohasis[['db']][['addr_prov']] %>%
         select(PROV, NHSSS) %>%
         mutate(OLD_PROV = 1),
      by = 'PROV'
   ) %>%
   arrange(
      NEW_PROV,
      OLD_PROV
   )

psgc_munc <- psgc_main %>%
   rename(MUNC = PSGC) %>%
   filter(PSGL == 'City' |
             PSGL == 'Mun' |
             PSGL == 'SubMun' |
             MUNC == "0990100000") %>%
   add_row(
      MUNC     = '9999999000',
      NAME     = 'Overseas',
      PSGC_OLD = '999900000',
      PSGC_PUB = '2024-12-31'
   ) %>%
   mutate(
      PROV = stri_pad_right(str_left(MUNC, 5), 10, '0'),
      REG  = stri_pad_right(str_left(PROV, 2), 10, '0'),
   )
mutate(NEW_MUNC = 1) %>%
   full_join(
      y  = Curr.ohasis[['db']][['addr_munc']] %>%
         select(MUNC, NHSSS) %>%
         mutate(OLD_MUNC = 1),
      by = 'MUNC'
   ) %>%
   arrange(
      NEW_MUNC,
      OLD_MUNC
   )


psgc_brgy <- psgc_main %>%
   rename(BRGY = PSGC) %>%
   filter(PSGL == 'Bgy') %>%
   add_row(
      BRGY     = '9999999000',
      NAME     = 'Overseas',
      PSGC_OLD = '999900000',
      PSGC_PUB = '2024-12-31'
   ) %>%
   mutate(
      MUNC = stri_pad_right(str_left(BRGY, 7), 10, '0'),
      PROV = stri_pad_right(str_left(MUNC, 5), 10, '0'),
      REG  = stri_pad_right(str_left(PROV, 2), 10, '0'),
   )

##  check if all are matching
# ! region
psgc_prov %>%
   left_join(psgc_reg %>% select(REG) %>% mutate(exists = 1), join_by(REG)) %>%
   tab(exists)
psgc_munc %>%
   left_join(psgc_reg %>% select(REG) %>% mutate(exists = 1), join_by(REG)) %>%
   tab(exists)
psgc_brgy %>%
   left_join(psgc_reg %>% select(REG) %>% mutate(exists = 1), join_by(REG)) %>%
   tab(exists)

# ! province
psgc_munc %>%
   left_join(psgc_prov %>% select(PROV) %>% mutate(exists = 1), join_by(PROV)) %>%
   tab(exists)
psgc_brgy %>%
   left_join(psgc_prov %>% select(PROV) %>% mutate(exists = 1), join_by(PROV)) %>%
   tab(exists)

# ! muncity
psgc_brgy %>%
   left_join(psgc_munc %>% select(MUNC) %>% mutate(exists = 1), join_by(MUNC)) %>%
   tab(exists)

psgc_brgy %>%
   inner_join(psgc_munc %>% select(MUNC)) %>%
   inner_join(psgc_prov %>% select(PROV)) %>%
   nrow()

psgc_brgy <- psgc_main %>%
   rename(BRGY = PSGC) %>%
   filter(PSGL == 'Bgy')

New.ohasis[['db']][['addr_reg']] <- psgc_reg %>%
   mutate(
      NAME       = if_else(.left(REG, 2) == '99', 'Overseas', NAME),
      CREATED_BY = '1300000001',
      CREATED_AT = '2021-09-11 17:30:00',
      UPDATED_BY = NA_character_,
      UPDATED_AT = NA_character_,
      DELETED_BY = NA_character_,
      DELETED_AT = NA_character_,
   ) %>%
   select(
      -NEW_REG,
      -OLD_REG
   )

New.ohasis[['db']][['addr_prov']] <- psgc_prov %>%
   mutate(
      NAME       = if_else(.left(PROV, 2) == '99', 'Overseas', NAME),
      REG        = stri_pad_right(.left(PROV, 2), 9, '0'),
      CREATED_BY = '1300000001',
      CREATED_AT = '2021-09-11 17:30:00',
      UPDATED_BY = NA_character_,
      UPDATED_AT = NA_character_,
      DELETED_BY = NA_character_,
      DELETED_AT = NA_character_,
   ) %>%
   relocate(
      REG,
      .before = PROV
   ) %>%
   select(
      -NEW_PROV,
      -OLD_PROV
   )

New.ohasis[['db']][['addr_munc']] <- psgc_munc %>%
   mutate(
      NAME       = if_else(.left(MUNC, 2) == '99', 'Overseas', NAME),
      PROV       = stri_pad_right(.left(MUNC, 4), 9, '0'),
      CREATED_BY = '1300000001',
      CREATED_AT = '2021-09-11 17:30:00',
      UPDATED_BY = NA_character_,
      UPDATED_AT = NA_character_,
      DELETED_BY = NA_character_,
      DELETED_AT = NA_character_,
   ) %>%
   relocate(
      PROV,
      .before = MUNC
   ) %>%
   select(
      -NEW_MUNC,
      -OLD_MUNC
   )

New.addr_brgy <- psgc_brgy %>%
   mutate(
      MUNC       = stri_pad_right(.left(BRGY, 6), 9, '0'),
      NHSSS      = NA_character_,
      CREATED_BY = '1300000001',
      CREATED_AT = '2021-09-11 17:30:00',
      UPDATED_BY = NA_character_,
      UPDATED_AT = NA_character_,
      DELETED_BY = NA_character_,
      DELETED_AT = NA_character_,
   ) %>%
   relocate(
      MUNC,
      .before = BRGY
   )


# write tables
dbWriteTable(
   conn        = dbConnect(
      RMariaDB::MariaDB(),
      user     = 'ohasis',
      password = 't1rh0uGCyN2sz6zk',
      host     = '122.53.181.142',
      port     = '3307',
      'ohasis_interim'
   ),
   name        = 'update_addr_reg',
   value       = New.addr_reg,
   field.types = c(
      REG              = 'CHAR(9)',
      NAME             = 'VARCHAR(255)',
      PSGL             = 'VARCHAR(255)',
      NAME_OLD         = 'VARCHAR(255)',
      CLASS_CITY       = 'VARCHAR(255)',
      CLASS_INCOME     = 'VARCHAR(255)',
      URBAN_RURAL_2015 = 'CHAR(1)',
      POPCEN_2015      = 'VARCHAR(255)',
      POPCEN_2020      = 'VARCHAR(255)',
      PSGC_PUB         = 'DATE',
      NHSSS            = 'VARCHAR(255)',
      CREATED_BY       = 'CHAR(10)',
      CREATED_AT       = 'DATETIME',
      UPDATED_BY       = 'CHAR(10)',
      UPDATED_AT       = 'DATETIME',
      DELETED_BY       = 'CHAR(10)',
      DELETED_AT       = 'DATETIME'
   ),
   overwrite   = T
)

dbWriteTable(
   conn        = dbConnect(
      RMariaDB::MariaDB(),
      user     = 'ohasis',
      password = 't1rh0uGCyN2sz6zk',
      host     = '122.53.181.142',
      port     = '3307',
      'ohasis_interim'
   ),
   name        = 'update_addr_prov',
   value       = New.addr_prov,
   field.types = c(
      REG              = 'CHAR(9)',
      PROV             = 'CHAR(9)',
      NAME             = 'VARCHAR(255)',
      PSGL             = 'VARCHAR(255)',
      NAME_OLD         = 'VARCHAR(255)',
      CLASS_CITY       = 'VARCHAR(255)',
      CLASS_INCOME     = 'VARCHAR(255)',
      URBAN_RURAL_2015 = 'CHAR(1)',
      POPCEN_2015      = 'VARCHAR(255)',
      POPCEN_2020      = 'VARCHAR(255)',
      PSGC_PUB         = 'DATE',
      NHSSS            = 'VARCHAR(255)',
      CREATED_BY       = 'CHAR(10)',
      CREATED_AT       = 'DATETIME',
      UPDATED_BY       = 'CHAR(10)',
      UPDATED_AT       = 'DATETIME',
      DELETED_BY       = 'CHAR(10)',
      DELETED_AT       = 'DATETIME'
   ),
   overwrite   = T
)

dbWriteTable(
   conn        = dbConnect(
      RMariaDB::MariaDB(),
      user     = 'ohasis',
      password = 't1rh0uGCyN2sz6zk',
      host     = '122.53.181.142',
      port     = '3307',
      'ohasis_interim'
   ),
   name        = 'update_addr_munc',
   value       = New.addr_munc,
   field.types = c(
      PROV             = 'CHAR(9)',
      MUNC             = 'CHAR(9)',
      NAME             = 'VARCHAR(255)',
      PSGL             = 'VARCHAR(255)',
      NAME_OLD         = 'VARCHAR(255)',
      CLASS_CITY       = 'VARCHAR(255)',
      CLASS_INCOME     = 'VARCHAR(255)',
      URBAN_RURAL_2015 = 'CHAR(1)',
      POPCEN_2015      = 'VARCHAR(255)',
      POPCEN_2020      = 'VARCHAR(255)',
      PSGC_PUB         = 'DATE',
      NHSSS            = 'VARCHAR(255)',
      CREATED_BY       = 'CHAR(10)',
      CREATED_AT       = 'DATETIME',
      UPDATED_BY       = 'CHAR(10)',
      UPDATED_AT       = 'DATETIME',
      DELETED_BY       = 'CHAR(10)',
      DELETED_AT       = 'DATETIME'
   ),
   overwrite   = T
)

dbWriteTable(
   conn        = dbConnect(
      RMariaDB::MariaDB(),
      user     = 'ohasis',
      password = 't1rh0uGCyN2sz6zk',
      host     = '122.53.181.142',
      port     = '3307',
      'ohasis_interim'
   ),
   name        = 'update_addr_brgy',
   value       = New.addr_brgy,
   field.types = c(
      MUNC             = 'CHAR(9)',
      BRGY             = 'CHAR(9)',
      NAME             = 'VARCHAR(255)',
      PSGL             = 'VARCHAR(255)',
      NAME_OLD         = 'VARCHAR(255)',
      CLASS_CITY       = 'VARCHAR(255)',
      CLASS_INCOME     = 'VARCHAR(255)',
      URBAN_RURAL_2015 = 'CHAR(1)',
      POPCEN_2015      = 'VARCHAR(255)',
      POPCEN_2020      = 'VARCHAR(255)',
      PSGC_PUB         = 'DATE',
      NHSSS            = 'VARCHAR(255)',
      CREATED_BY       = 'CHAR(10)',
      CREATED_AT       = 'DATETIME',
      UPDATED_BY       = 'CHAR(10)',
      UPDATED_AT       = 'DATETIME',
      DELETED_BY       = 'CHAR(10)',
      DELETED_AT       = 'DATETIME'
   ),
   overwrite   = T
)