##  PH Elections 2022 Controller -----------------------------------------------

# define datasets
if (!exists('ph22'))
   ph22 <- list()

##  Begin linkage of datasets --------------------------------------------------

# source("src/official/gf_logsheet/01_load_corrections.R")
source("src/misc/elections_ph_2022/01_process_contests.R")