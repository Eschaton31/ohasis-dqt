##  Load credentials and authentications ---------------------------------------

# dependencies
source("src/dependencies/options.R")
source("src/dependencies/libraries.R")
source("src/dependencies/functions.R")
source("src/dependencies/full_tables.R")
source("src/dependencies/cloud.R")
source("src/dependencies/dedup.R")
source("src/dependencies/googlesheets.R")
source("src/dependencies/excel.R")
source("src/dependencies/latex.R")
source("src/dependencies/reports.R")

# accounts
source("src/dependencies/auth_acct.R")

# classes
source("src/classes/QB.R")
source("src/classes/DB.R")
source("src/classes/Dedup.R")

# register pipelines
source("src/pipeline/pipeline.R", chdir = TRUE)
flow_register()

# report classes
source("src/classes/HarpVl.R")
