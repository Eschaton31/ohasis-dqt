DataFactory <- R6Class(
   'DataFactory',
   public  = list(
      wd             = NA_character_,
      schema         = NA_character_,
      table          = NA_character_,
      procedure      = NA_character_,

      initialize     = function(schema, table, procedure) {
         self$wd        <- here("src", stri_c("data_", schema))
         self$schema    <- ifelse(schema %in% c("lake", "warehouse"), stri_c("ohasis_", schema), schema)
         self$table     <- table
         self$procedure <- procesdure
      },

      upsert = function (table) {

      }
   ),
   active  = list(
      schemaId = function() Id(schema = self$schema, table = self$table)
   ),
   private = list(
   )
)

try <- DataFactory$new()

dx        <- read_dta(hs_data("harp_dx", "reg", 2024, 9))
dir       <- here("src", "data_lake")
factories <- fs::dir_info(dir, recurse = TRUE, type = "file") %>%
   select(path) %>%
   mutate(path = substr(str_replace_all(path, dir, ""), 2, 1000)) %>%
   separate_wider_delim(path, "/", names = c("procedure", "file")) %>%
   separate_wider_delim(file, ".", names = c("table", "type")) %>%
   mutate(exists = 1) %>%
   pivot_wider(names_from = type, values_from = exists)
factories <- substr(str_replace_all(factories, dir, ""), 2, 1000)
factories <- lapply(factories, str_split, "/")
factories <- lapply(factories, "[[", 1)

names(factories) <- sapply(factories, "[[", 2)

upsert  <- factories[lapply(factories, "[[", 1) == "upsert"]
refresh <- factories[lapply(factories, "[[", 1) == "refresh"]

classes <- sapply(dx, class)