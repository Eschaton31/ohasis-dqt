cids <- c('20231119990005774R', '20231119990005774R', '2023112899000584Q5')

prep_changed_names <- function(cids, first, middle, last, suffix, uic, birthdate = NA_character_) {
   conn <- connect("ohasis-lw")
   prep <- QB$new(conn)$select(REC_ID, CENTRAL_ID, prep_id, first, middle, last, suffix, uic, birthdate)$from("prep.reg_202410")$whereIn("CENTRAL_ID", cids)$get()
   dbDisconnect(conn)


   if (is.null(first)) {
      conn <- connect("ohasis-live")
      live <- QB$new(conn)$
         select(px_name.REC_ID, FIRST, MIDDLE, LAST, SUFFIX, UIC, BIRTHDATE)$
         from("ohasis_interim.px_name")$whereIn("REC_ID", prep$REC_ID)$
         leftJoin("ohasis_interim.px_info", "px_name.REC_ID", "=", "px_info.REC_ID")$
         get()
      dbDisconnect(conn)
   } else {
      live <- prep %>%
         select(REC_ID)

      if (!missing(first))
         live <- mutate(live, FIRST = first)
      else
         live <- mutate(live, FIRST = NA_character_)

      if (!missing(middle))
         live <- mutate(live, MIDDLE = middle)
      else
         live <- mutate(live, MIDDLE = NA_character_)

      if (!missing(last))
         live <- mutate(live, LAST = last)
      else
         live <- mutate(live, LAST = NA_character_)

      if (!missing(suffix))
         live <- mutate(live, SUFFIX = suffix)
      else
         live <- mutate(live, SUFFIX = NA_character_)

      if (!missing(uic))
         live <- mutate(live, UIC = uic)
      else
         live <- mutate(live, UIC = NA_character_)

      if (!missing(birthdate))
         live <- mutate(live, BIRTHDATE = birthdate)
      else
         live <- mutate(live, BIRTHDATE = NA_character_)
   }


   corrections <- prep %>%
      mutate(birthdate = as.character(birthdate)) %>%
      left_join(
         y  = live %>% mutate(BIRTHDATE = as.character(BIRTHDATE)),
         by = join_by(REC_ID)
      ) %>%
      select(
         prep_id,
         new_value.first     = FIRST,
         old_value.first     = first,
         new_value.middle    = MIDDLE,
         old_value.middle    = middle,
         new_value.last      = LAST,
         old_value.last      = last,
         new_value.suffix    = SUFFIX,
         old_value.suffix    = suffix,
         new_value.uic       = UIC,
         old_value.uic       = uic,
         new_value.birthdate = BIRTHDATE,
         old_value.birthdate = birthdate,
      ) %>%
      pivot_longer(
         cols      = c(contains("_value.")),
         names_to  = c(".value", "variable"),
         names_sep = "\\."
      ) %>%
      mutate_at(
         .vars = vars(old_value, new_value),
         ~toupper(coalesce(., "NULL"))
      ) %>%
      mutate(
         period = "2024.10",
         format = "character"
      ) %>%
      select(
         period,
         prep_id,
         variable,
         old_value,
         new_value,
         format
      ) %>%
      filter(old_value != new_value)

   conn <- connect('ohasis-lw')
   dbxUpsert(conn, Id(schema = "prep", table = "corr_reg"), corrections, c("period", "prep_id", "variable"))
   dbDisconnect(conn)

   return(corrections)
}

art_changed_names <- function(cids) {
   conn <- connect("ohasis-lw")
   prep <- QB$new(conn)$select(REC_ID, CENTRAL_ID, art_id, first, middle, last, suffix, uic, birthdate)$from("harp_tx.reg_202410")$whereIn("CENTRAL_ID", cids)$get()
   dbDisconnect(conn)


   conn <- connect("ohasis-live")
   live <- QB$new(conn)$
      select(px_name.REC_ID, FIRST, MIDDLE, LAST, SUFFIX, UIC, BIRTHDATE)$
      from("ohasis_interim.px_name")$whereIn("REC_ID", prep$REC_ID)$
      leftJoin("ohasis_interim.px_info", "px_name.REC_ID", "=", "px_info.REC_ID")$
      get()
   dbDisconnect(conn)

   corrections <- prep %>%
      mutate(birthdate = as.character(birthdate)) %>%
      left_join(
         y  = live %>% mutate(BIRTHDATE = as.character(BIRTHDATE)),
         by = join_by(REC_ID)
      ) %>%
      select(
         art_id,
         new_value.first     = FIRST,
         old_value.first     = first,
         new_value.middle    = MIDDLE,
         old_value.middle    = middle,
         new_value.last      = LAST,
         old_value.last      = last,
         new_value.suffix    = SUFFIX,
         old_value.suffix    = suffix,
         new_value.uic       = UIC,
         old_value.uic       = uic,
         new_value.birthdate = BIRTHDATE,
         old_value.birthdate = birthdate,
      ) %>%
      pivot_longer(
         cols      = c(contains("_value.")),
         names_to  = c(".value", "variable"),
         names_sep = "\\."
      ) %>%
      mutate_at(
         .vars = vars(old_value, new_value),
         ~toupper(coalesce(., "NULL"))
      ) %>%
      mutate(
         period = "2024.10",
         format = "character"
      ) %>%
      select(
         period,
         art_id,
         variable,
         old_value,
         new_value,
         format
      ) %>%
      filter(old_value != new_value)

   conn <- connect('ohasis-lw')
   dbxUpsert(conn, Id(schema = "harp_tx", table = "corr_reg"), corrections, c("period", "art_id", "variable"))
   dbDisconnect(conn)

   return(corrections)
}

# prep_changed_names(c('20220504030081356T','2022081803008139H3','20230815030081B017','202309050300813C16','2023101203008157K8','2024012403008108F9','20240509030081F037','20240527030081084R','202405270300811L14','2024052703008142B2','20240527030081542Q','20240527030081M803','20240528030081693K','20240528030081L793','20240528030081N843','20240528030081V809','20240528030081Z759','20240605030081U299','20240618030081066J','202406180300811S67','20240619030081358G','2024062003008138D0','20240620030081456M','202406200300815P71','2024062403008194W7'))
write_clip(unique(c('2021111313000110O1', '20231226030193N571', '20230113130814F169', '202409201306051L76', '20230113130814F169', '20231226030193N571', '20231226030193N571', '202409201306051L76', '2021111313000110O1', '202409201306051L76')))

prep_changed_names('20210327130605967Z', 'Michael', 'Pascua', 'Dimatira', NA_character_, 'MADO0106251992', '1992-06-25')


