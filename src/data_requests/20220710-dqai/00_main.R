dqai         <- list()
dqai$faci    <- read_sheet("1papdQbR_3f_WzX-UNdj7E674dsTewqZFgZ8Dl7k73vs", "Region_Ref") %>%
   select(
	  site_reg  = 1,
	  faci_code = 2,
	  site_name = 3
   )
dqai$tx$data <- read_sheet("1papdQbR_3f_WzX-UNdj7E674dsTewqZFgZ8Dl7k73vs", "Tx-Faci", skip = 1, n_max = 182) %>%
   mutate_all(~as.character(.)) %>%
   select(-3, -4) %>%
   select(
	  site_reg         = 1,
	  site_name        = 2,
	  total_faci       = 4,
	  total_eb         = 5,
	  varTotal_num     = 6,
	  varTotal_perc    = 7,
	  alive_faci       = 8,
	  alive_eb         = 9,
	  varAlive_num     = 10,
	  varAlive_perc    = 11,
	  new_faci         = 12,
	  new_eb           = 13,
	  varNew_num       = 14,
	  varNew_perc      = 15,
	  ltfu_faci        = 16,
	  ltfu_eb          = 17,
	  varLtfu_num      = 18,
	  varLtfu_perc     = 19,
	  dead_faci        = 20,
	  dead_eb          = 21,
	  varDead_num      = 22,
	  varDead_perc     = 23,
	  transout_faci    = 24,
	  transout_eb      = 25,
	  varTransout_num  = 26,
	  varTransout_perc = 27,
	  transin_faci     = 28,
	  transin_eb       = 29,
	  varTransin_num   = 30,
	  varTransin_perc  = 31,
	  stop_faci        = 32,
	  stop_eb          = 33,
	  varStop_num      = 34,
	  varStop_perc     = 35
   )

dqai$prep$data <- read_sheet("1papdQbR_3f_WzX-UNdj7E674dsTewqZFgZ8Dl7k73vs", "PrEP-Faci", skip = 1, n_max = 178) %>%
   mutate_all(~as.character(.)) %>%
   mutate_all(~if_else(. == "NULL" | . == "N/A", NA_character_, ., .)) %>%
   select(
	  site_reg    = 1,
	  site_name   = 2,
	  new_faci    = 3,
	  new_eb      = 4,
	  varNew_num  = 5,
	  varNew_perc = 6
   )

dqai$tx$clean <- dqai$tx$data %>%
   select(-ends_with("perc")) %>%
   mutate_at(
	  .vars = vars(ends_with("eb"), ends_with("faci"), ends_with("num")),
	  ~as.integer(.)
   ) %>%
   left_join(
	  y  = dqai$faci,
	  by = "site_name"
   ) %>%
   mutate(
	  exclude  = case_when(
		 faci_code == "TLY" ~ 1,
		 faci_code == "RIT" ~ 1,
		 TRUE ~ 0
	  ),
	  category = case_when(
		 exclude == 1 ~ "special",
		 total_eb <= 51 ~ "cat1",
		 total_eb >= 52 & total_eb <= 132 ~ "cat2",
		 total_eb >= 133 & total_eb <= 442 ~ "cat3",
		 total_eb >= 443 & total_eb <= 999 ~ "cat4",
		 total_eb >= 1000 ~ "cat5",
	  )
   )

dqai$prep$clean <- dqai$prep$data %>%
   select(-ends_with("perc")) %>%
   mutate_at(
	  .vars = vars(ends_with("eb"), ends_with("faci"), ends_with("num")),
	  ~as.integer(.)
   ) %>%
   left_join(
	  y  = dqai$faci,
	  by = "site_name"
   ) %>%
   mutate(
	  exclude = case_when(
		 faci_code == "TLY" ~ 1,
		 faci_code == "RIT" ~ 1,
		 TRUE ~ 0
	  )
   )

##  everonart ------------------------------------------------------------------

dqai$everonart <- dqai$tx$clean %>%
   filter(!is.na(total_eb), !is.na(total_faci)) %>%
   select(
	  category,
	  faci_code,
	  eb   = total_eb,
	  faci = total_faci
   )

dqai$onart <- dqai$tx$clean %>%
   filter(!is.na(alive_eb), !is.na(alive_faci)) %>%
   select(
	  category,
	  faci_code,
	  eb   = alive_eb,
	  faci = alive_faci
   )

dqai$newonart <- dqai$tx$clean %>%
   filter(!is.na(new_eb), !is.na(new_faci)) %>%
   select(
	  category,
	  faci_code,
	  eb   = new_eb,
	  faci = new_faci
   )

dqai$ltfu <- dqai$tx$clean %>%
   filter(!is.na(ltfu_eb), !is.na(ltfu_faci)) %>%
   select(
	  category,
	  faci_code,
	  eb   = ltfu_eb,
	  faci = ltfu_faci
   )

for_run <- c(
   "everonart" = "ART Ever Clients",
   "onart"     = "On ART Clients",
   "newonart"  = "ART Enrollment",
   "ltfu"      = "LTFU"
)
for (i in seq_len(length(for_run))) {
   ind   <- names(for_run[i])
   title <- for_run[i]

   for (cat in unique(dqai[[ind]]$category)) {
	  data <- dqai[[ind]] %>%
		 filter(category == cat)

	  max             <- max(data$eb, data$faci, na.rm = TRUE)
	  `EB Data`       <- data$eb
	  `Facility Data` <- data$faci
	  Label           <- data$faci_code
	  plot            <- ggplot(data, aes(x = `EB Data`, y = `Facility Data`, label = Label)) +
		 geom_text(vjust = -1) +
		 geom_point(colour = "#2e5f4f") +
		 geom_abline(colour = "#ce2c3d", intercept = 0) +
		 xlim(0, max) +
		 ylim(0, max) +
		 ggtitle(glue("EB vs Facility Data Variance: {title} among {stri_trans_totitle(cat)} Sites"))

	  # dqai$plots[[glue("{ind}_{cat}")]] <- plot
	  ggsave(glue("C:/Users/Administrator/Downloads/slides/{ind}_{cat}.png"), plot, "png")
   }
}

for (cat in c("cat1", "cat2", "cat3", "cat4", "cat4", "cat5", "special")) {
   onart <- dqai$onart %>%
	  filter(category == cat)
   ltfu  <- dqai$ltfu %>%
	  filter(category == cat)

   max             <- max(onart$eb, onart$faci, na.rm = TRUE)
   `EB Data`       <- onart$eb
   `Facility Data` <- onart$faci
   Label           <- onart$faci_code
   plot1           <- ggplot(onart, aes(x = `EB Data`, y = `Facility Data`, label = Label)) +
	  geom_text(vjust = -1) +
	  geom_point(colour = "#2e5f4f") +
	  geom_abline(colour = "#ce2c3d", intercept = 0) +
	  xlim(0, max) +
	  ylim(0, max) +
	  ggtitle(glue("EB vs Facility Data Variance: On ART among {stri_trans_totitle(cat)} Sites"))

   max             <- max(ltfu$eb, ltfu$faci, na.rm = TRUE)
   `EB Data`       <- ltfu$eb
   `Facility Data` <- ltfu$faci
   Label           <- ltfu$faci_code
   plot2           <- ggplot(ltfu, aes(x = `EB Data`, y = `Facility Data`, label = Label)) +
	  geom_text(vjust = -1) +
	  geom_point(colour = "#2e5f4f") +
	  geom_abline(colour = "#ce2c3d", intercept = 0) +
	  xlim(0, max) +
	  ylim(0, max) +
	  ggtitle(glue("EB vs Facility Data Variance: LTFU among {stri_trans_totitle(cat)} Sites"))

   plot <- cowplot::plot_grid(plot1, plot2)

   # dqai$plots[[glue("{ind}_{cat}")]] <- plot
   ggsave(glue("C:/Users/Administrator/Downloads/slides/onart-ltfu_{cat}.png"), plot, "png", width = 29.4)
}

##  prep_new -------------------------------------------------------------------

dqai$prep_new <- dqai$prep$clean %>%
   filter(exclude == 0) %>%
   filter(!is.na(new_eb), !is.na(new_faci))

`EB Data`       <- dqai$prep_new$new_eb
`Facility Data` <- dqai$prep_new$new_faci
Label           <- dqai$prep_new$faci_code
plot            <- ggplot(dqai$prep_new, aes(x = `EB Data`, y = `Facility Data`, label = Label)) +
   geom_text(vjust = -1) +
   geom_point(colour = "#2e5f4f") +
   # geom_smooth(method = lm, colour = "#ce2c3d") +
   geom_abline(colour = "#ce2c3d") +
   ggtitle("EB vs Facility Data Variance: PrEP Enrollment")

ggsave("C:/Users/Administrator/Downloads/slides/newonprep.png", plot, "png")

rm(cat, data, `EB Data`, `Facility Data`, for_run, i, ind, Label, max, plot, title, onart, ltfu, plot1, plot2)