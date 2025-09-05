remove_table_alias <- function(data) {
   return(
      data %>%
         rename_all(
            ~case_when(
               str_detect(., "\\.") ~ str_extract(., ".+\\.(.+)", 1),
               TRUE ~ .
            )
         )
   )
}

get_addr <- function(data, addr_set = NULL, type = "nhsss") {
   # addr_set format:
   # reg, prov, munc

   type <- tolower(type)
   if (type %in% c("nhsss", "code")) {
      get_reg  <- as.symbol("nhsss_reg")
      get_prov <- as.symbol("nhsss_prov")
      get_munc <- as.symbol("nhsss_munc")
   } else if (type == "label") {
      get_reg  <- as.symbol("label_reg")
      get_prov <- as.symbol("label_prov")
      get_munc <- as.symbol("label_munc")
   } else if (type == "name") {
      get_reg  <- as.symbol("name_reg")
      get_prov <- as.symbol("name_prov")
      get_munc <- as.symbol("name_munc")
   }

   coded_reg  <- addr_set[1] %>% as.symbol()
   coded_prov <- addr_set[2] %>% as.symbol()
   coded_munc <- addr_set[3] %>% as.symbol()

   named_reg  <- names(addr_set)[1]
   named_prov <- names(addr_set)[2]
   named_munc <- names(addr_set)[3]

   # rename columns
   data %<>%
      select(
         -any_of(
            c(
               "psgc_reg",
               "psgc_prov",
               "psgc_munc",
               "psgc"
            )
         )
      ) %>%
      mutate(
         psgc = coalesce(!!coded_munc, !!coded_prov, !!coded_reg)
      ) %>%
      select(
         -!!coded_reg,
         -!!coded_prov,
         -!!coded_munc
      ) %>%
      left_join(
         y  = ohasis$ref_addr %>%
            select(
               psgc,
               !!named_reg  := !!get_reg,
               !!named_prov := !!get_prov,
               !!named_munc := !!get_munc,
            ),
         by = join_by(psgc)
      ) %>%
      relocate(!!named_reg, !!named_prov, !!named_munc, .before = psgc)

   return(data)
}

flow_dta <- function(data, surv_name, type, yr, mo) {
   yr <- stri_pad_left(yr, 4, "0")
   mo <- stri_pad_left(mo, 2, "0")

   db     <- surv_name
   table  <- stri_c(type, "_", yr, mo)
   schema <- Id(schema = db, table = table)
   id_col <- switch(
      surv_name,
      harp_dx   = "idnum",
      harp_tx   = "art_id",
      harp_dead = "mort_id",
      harp_full = "idnum",
      prep      = "prep_id",
      tbhiv     = "art_id"
   )


   log_info("Uploading {green(table)}.")
   lw_conn <- connect(surv_name)
   if (dbExistsTable(lw_conn, table)) {
      dbExecute(lw_conn, glue(r"(TRUNCATE `{db}`.`{table}`;)"))
   }
   ohasis$upsert(
      lw_conn,
      db,
      table,
      data %>%
         mutate_if(
            .predicate = is.labelled,
            ~to_character(.)
         ),
      id_col
   )

   version <- tibble(
      type        = type,
      period      = stri_c(yr, ".", mo),
      last_update = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   )
   ohasis$upsert(lw_conn, surv_name, "version", version, c("type", "period"))

   dbDisconnect(lw_conn)
}
