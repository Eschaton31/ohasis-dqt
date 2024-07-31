file         <- "C:/Users/johnb/Downloads/EGASP_Working File_2024_Q1.xlsx"
sheets       <- excel_sheets(file)
egasp        <- lapply(sheets, read_excel, path = file, col_types = "text", .name_repair = "unique_quiet")
names(egasp) <- sheets

who <- egasp$`EGASP Dataset`
who <- bind_rows(egasp$EGASP_CSV_Q1, egasp$EGASP_CSV_Q2, egasp$EGASP_CSV_Q3, egasp$EGASP_CSV_Q4) %>%
   mutate_at(
      .vars = vars(date_c, date_cult),
      ~excel_numeric_to_date(parse_number(.))
   ) %>%
   mutate(
      year = year(date_c)
   )

who %>%
   # filter(year(date_c) == 2023, uret == 1) %>%
   mutate(
      episode = if_else(year(date_c) == 2023 & uret == 1, 1, 0, 0)
   ) %>%
   group_by(egasp_id) %>%
   summarise(
      episodes = max(episode)
   ) %>%
   ungroup() %>%
   tab(episodes)

who %>% tab(year)

try %>%
   mutate(
      episode = if_else(year(date_c) == 2023 & disur == 1, 1, 0, 0)
   ) %>%
   group_by(egasp_id) %>%
   summarise(
      episode23 = max(episode),
      episode   = max(disur),
      visit23   = year(date_c) == 2023
   ) %>%
   tab(visit23, episode23)

who %>%
   filter(year(date_cult) == 2023) %>%
   tab(fuvis)
try2 %>%
   filter(year(date_cult) == 2023) %>%
   distinct(`gasp_individual-pt_id`) %>%
   nrow()

try <- read_delim("C:/Users/johnb/Downloads/export.csv", delim = ";")
try2 <- read_delim("C:/Users/johnb/Downloads/export (1).csv", delim = ";")
try3 <- read_delim("C:/Users/johnb/Downloads/export (2).csv", delim = ";")

vars <- names(try2)
names(vars) <- names(try)