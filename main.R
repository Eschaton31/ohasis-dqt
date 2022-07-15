shell("cls")
################################################################################
# Project name: > OHASIS Data Wrangling
# Author(s):    > Palo, John Benedict
# Date:         > 2022-01-27
# Description:  > This program serves as the primary controller for the various
#                 data pipelines of the NHSSS Unit.
################################################################################

rm(list = ls())

##  Load credentials and authentications ---------------------------------------

# dependencies
source("src/dependencies/options.R")
source("src/dependencies/libraries.R")
source("src/dependencies/functions.R")
source("src/dependencies/full_tables.R")
source("src/dependencies/cloud.R")
source("src/dependencies/dedup.R")

# accounts
source("src/dependencies/auth_acct.R")
source("src/dependencies/gmail.R")

# classes
source("src/classes/Project.R")
source("src/classes/DB.R")

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB()

########
# review$mismatch_last <- data$dx %>%
#    inner_join(
# 	  y  = data$tx %>%
# 		 select(
# 			art_id,
# 			idnum,
# 			art_first  = first,
# 			art_middle = middle,
# 			art_last   = last,
# 			art_suffix = suffix,
# 			art_sex    = sex,
# 			art_bdate  = birthdate
# 		 ),
# 	  by = "idnum"
#    ) %>%
#    filter(last != art_last) %>%
#    mutate(
# 	  # name
# 	  name     = paste0(
# 		 if_else(
# 			condition = is.na(last),
# 			true      = "",
# 			false     = last
# 		 ), ", ",
# 		 if_else(
# 			condition = is.na(firstname),
# 			true      = "",
# 			false     = firstname
# 		 ), " ",
# 		 if_else(
# 			condition = is.na(middle),
# 			true      = "",
# 			false     = middle
# 		 ), " ",
# 		 if_else(
# 			condition = is.na(name_suffix),
# 			true      = "",
# 			false     = name_suffix
# 		 )
# 	  ),
# 	  art_name = paste0(
# 		 if_else(
# 			condition = is.na(art_last),
# 			true      = "",
# 			false     = art_last
# 		 ), ", ",
# 		 if_else(
# 			condition = is.na(art_first),
# 			true      = "",
# 			false     = art_first
# 		 ), " ",
# 		 if_else(
# 			condition = is.na(art_middle),
# 			true      = "",
# 			false     = art_middle
# 		 ), " ",
# 		 if_else(
# 			condition = is.na(art_suffix),
# 			true      = "",
# 			false     = art_suffix
# 		 )
# 	  ),
# 	  Bene     = NA_character_,
# 	  Kath     = NA_character_,
# 	  Meg      = NA_character_,
#    ) %>%
#    select(
# 	  idnum,
# 	  art_id,
# 	  name,
# 	  art_name,
# 	  bdate,
# 	  art_bdate,
# 	  sex,
# 	  art_sex,
# 	  last,
# 	  art_last,
# 	  Bene,
# 	  Kath,
# 	  Meg
#    )

###  quartiles ng length of time na naka-art mga tao ---------------------------

onart <- read_dta(ohasis$get_data("harp_tx-outcome", "2022", "05")) %>%
   mutate(
	  `Months on ART` = interval(artstart_date, latest_nextpickup) / months(1),
	  `Months on ART` = floor(`Months on ART`)
   )

onart_summary <- onart %>%
   group_by(outcome) %>%
   summarise(
	  min    = min(`Months on ART`, na.rm = TRUE),
	  q25    = quantile(`Months on ART`, probs = .25, na.rm = TRUE),
	  median = median(`Months on ART`, na.rm = TRUE),
	  q75    = quantile(`Months on ART`, probs = .75, na.rm = TRUE),
	  q99    = quantile(`Months on ART`, probs = .99, na.rm = TRUE),
	  max    = max(`Months on ART`, na.rm = TRUE),
   )

box <- ggplot(data = onart, aes(y = `Months on ART`, x = outcome)) +
   geom_boxplot() +
   # theme(text            = element_text(size = 8, family = "chronica"),
   # 	 legend.position = 'top',
   # 	 axis.title.y    = element_blank()) +
   coord_flip()

ggsave("H:/time_onart.png", box, "png")