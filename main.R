shell("cls")
################################################################################
# Project name: > OHASIS Data Wrangling
# Author(s):    > Palo, John Benedict
# Date:         > 2022-01-27
# Description:  > This program creates the various OHASIS extracted data sets.
#
# Updates:      > See Changelog
#
# Input:        > OHASIS SQL Tables
#               > HARP Datasets
#
# Notes:        >
################################################################################

rm(list = ls())

##------------------------------------------------------------------------------
##  Load Environment Variables                                                --
##------------------------------------------------------------------------------

source("src/dependencies/libraries.R")
source("src/dependencies/classes.R")
source("src/dependencies/project.R")