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
source("src/dependencies/googlesheets.R")

# accounts
source("src/dependencies/auth_acct.R")
source("src/dependencies/gmail.R")

# classes
source("src/classes/Project.R")
source("src/classes/DB.R")

# register pipelines
source("src/pipeline/pipeline.R", chdir = TRUE)
flow_register()

##  Load primary classes -------------------------------------------------------

# initiate the project & database
ohasis <- DB("2022", "11", "flows")

##  example flow pipeline
flow_register()

# diagnosis
nhsss$harp_dx$steps$`01_load_reqs`$.init()
nhsss$harp_dx$steps$`02_data_initial`$.init()
nhsss$harp_dx$steps$`03_data_convert`$.init()
nhsss$harp_dx$steps$`04_data_final`$.init()
nhsss$harp_dx$steps$x1_dedup_new$.init()
nhsss$harp_dx$steps$x2_dedup_old$.init()

# treatment
nhsss$harp_tx$steps$`01_load_reqs`$.init()
nhsss$harp_tx$steps$`02_data_reg.initial`$.init()
nhsss$harp_tx$steps$`03_data_reg.convert`$.init()
nhsss$harp_tx$steps$`04_data_reg.final`$.init()
nhsss$harp_tx$steps$`05_data_outcome.initial`$.init()
nhsss$harp_tx$steps$`06_data_outcome.convert`$.init()
nhsss$harp_tx$steps$`07_data_outcome.final`$.init()
nhsss$harp_tx$steps$x1_dedup_new$.init()
nhsss$harp_tx$steps$x2_dedup_old$.init()
nhsss$harp_tx$steps$x3_dedup_dx$.init()
