ss <- "1-zLeS4GHNZC9FvpNPyQgje2ZwdsTy81LrGPUzIFIDSk"

# get all info from the corrections sheet
corrections <- read_sheet(ss, "names-20241112", col_types = "c")

corrections %>%
   filter(Done == "FALSE", `No Issue` == "FALSE") %>%
   tab(firstname) %>%
   arrange(desc(Freq.))

# fetch existing data from live OHASIS
rec_ids   <- corrections$REC_ID
conn      <- ohasis$conn("db")
live_name <- QB$new(conn)$
   from("ohasis_interim.px_name")$
   whereIn("REC_ID", rec_ids)$
   get()
dbDisconnect(conn)

# build the corrections
px_name <- corrections %>%
   filter(Done == "TRUE") %>%
   select(
      REC_ID,
      MIDDLE     = 2,
      MIDDLE_OLD = 9
   ) %>%
   # cleaning stage
   mutate_at(.vars = vars(MIDDLE_OLD), ~na_if(., "-")) %>%
   mutate_at(.vars = vars(MIDDLE_OLD), ~na_if(., "--")) %>%
   mutate_at(.vars = vars(MIDDLE_OLD), ~na_if(., "XX")) %>%
   mutate_at(.vars = vars(MIDDLE_OLD), ~na_if(., "NA")) %>%
   mutate_at(.vars = vars(MIDDLE, MIDDLE_OLD), ~str_replace_all(., "\\.", "")) %>%
   mutate_at(.vars = vars(MIDDLE, MIDDLE_OLD), ~coalesce(., "")) %>%
   filter(MIDDLE != MIDDLE_OLD) %>%
   left_join(
      y  = live_name %>%
         select(
            REC_ID,
            PATIENT_ID
         ),
      by = join_by(REC_ID)
   ) %>%
   select(
      REC_ID,
      PATIENT_ID,
      MIDDLE
   )

px_record <- corrections %>%
   filter(Done == "TRUE") %>%
   select(
      REC_ID,
      UPDATED_BY = 3
   ) %>%
   filter(!is.na(UPDATED_BY)) %>%
   left_join(
      y  = live_name %>%
         select(
            REC_ID,
            PATIENT_ID
         ),
      by = join_by(REC_ID)
   ) %>%
   mutate(
      UPDATED_AT = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   ) %>%
   select(
      REC_ID,
      PATIENT_ID,
      UPDATED_BY,
      UPDATED_AT
   )

harp_dx_corr <- corrections %>%
   filter(Done == "TRUE") %>%
   select(
      idnum,
      new_value = 2,
      old_value = 9
   ) %>%
   mutate_at(.vars = vars(new_value), ~str_replace_all(., "\\.", "")) %>%
   mutate_at(.vars = vars(new_value), ~toupper(.)) %>%
   mutate_at(.vars = vars(new_value), ~coalesce(., "NULL")) %>%
   mutate(
      period   = "2024.10",
      variable = "middle",
      format   = "character"
   ) %>%
   select(
      period,
      idnum,
      variable,
      old_value,
      new_value,
      format
   )

# upsert the data to live
conn <- ohasis$conn("db")
dbxUpsert(
   conn,
   Id(schema = "ohasis_interim", table = "px_name"),
   px_name,
   c("REC_ID", "PATIENT_ID")
)
dbxUpsert(
   conn,
   Id(schema = "ohasis_interim", table = "px_record"),
   px_record,
   c("REC_ID", "PATIENT_ID")
)
dbDisconnect(conn)

# upsert the data to harp_dx
conn <- ohasis$conn("lw")
dbxUpsert(
   conn,
   Id(schema = "harp_dx", table = "corr_reg"),
   harp_dx_corr,
   c("idnum", "period", "variable")
)
dbDisconnect(conn)


try <- corrections %>%
   select(
      idnum,
      new_value.last   = 4,
      old_value.last   = 13,
      new_value.middle = 2,
      old_value.middle = 12,
   ) %>%
   pivot_longer(
      cols      = c(new_value.last, old_value.last, new_value.middle, old_value.middle),
      names_to  = c(".value", "variable"),
      names_sep = "\\."
   )