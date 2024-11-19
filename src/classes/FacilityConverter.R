FacilityConverter <- R6Class(
   "FacilityConverter",
   public  = list(
      type       = character(),

      refs       = list(
         faci      = lake_ref_table("ref_faci"),
         faci_code = tibble()
      ),

      initialize = function(type) self$type <- type,

      fromId     = function(data, faciId, subFaciId, facility = site, region, province, muncity) {
         nameFacility <- "FACI_NAME"
         nameRegion   <- "FACI_NAME_REG"
         nameProvince <- "FACI_NAME_PROV"
         nameMuncity  <- "FACI_NAME_MUNC"

         if (self$type == "nhsss") {
            nameFacility <- "FACI_NAME_CLEAN"
         }

         if (self$type == "code") {
            nameFacility <- "FACI_CODE"
         }

         if (self$type %in% c("name", "code")) {
            nameRegion   <- "FACI_NHSSS_REG"
            nameProvince <- "FACI_NHSSS_PROV"
            nameMuncity  <- "FACI_NHSSS_MUNC"
         }

         ref <- self$refs$faci %>%
            select(
               {{faciId}}    := FACI_ID,
               {{subFaciId}} := SUB_FACI_ID,
               {{facility}}  := {{nameFacility}},
               {{nameRegion}},
               {{nameProvince}},
               {{nameMuncity}},
            )

         if (missing(region)) ref <- select(ref, -{{nameRegion}})
         else ref <- rename(ref, {{region}} := {{nameRegion}})

         if (missing(province)) ref <- select(ref, -{{nameProvince}})
         else ref <- rename(ref, {{province}} := {{nameProvince}})

         if (missing(muncity)) ref <- select(ref, -{{nameMuncity}})
         else ref <- rename(ref, {{muncity}} := {{nameMuncity}})

         # perform cleaning on ids
         data %<>%
            mutate(
               {{faciId}}    := coalesce({{faciId}}, ""),
               {{subFaciId}} := if_else(str_left({{subFaciId}}, 6) != {{faciId}}, "", {{subFaciId}}, "")
            ) %>%
            # get referenced data
            left_join(
               y  = ref,
               by = join_by({{faciId}}, {{subFaciId}})
            ) %>%
            # move then rename to old version
            relocate(any_of(names(ref)), .after = {{subFaciId}}) %>%
            # remove id data
            select(-{{faciId}}, -{{subFaciId}})

         return(data)
      }
   )
)
