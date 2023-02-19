# install/load packages based on a vector
# pkgs <- c("dplyr", "writexl", "readxl", "haven")
# install.packages(pkgs) # run if not installed

# load libraries
library(dplyr)
library(writexl)
library(readxl)
library(haven)

# basic parmas
data_dir  <- "D:/Downloads/Documents" # set dir
data_path <- file.path(data_dir, "play_data.xlsx") # set path
data_orig <- read_xlsx(data_path) # read excel file
data_new  <- data_orig # make a copy

# setwd(data_dir)
# convert to camel-case
names(data_new) <- gsub("_([a-z])",
                        toupper("\\U\\1\\E"),
                        names(data_orig),
                        perl = TRUE)

# replace sex into standard formats
data_new$sex <- ifelse(!is.na(play_data$sex),
                       substr(toupper(data_new), 1, 1),
                       "(no data)")

# laat name to uppercase
data_new$lastName <- toupper(data_new$lastName)

# concatenate name parts into fullname, first letter only for middle name
data_new$fullName <- paste0(
   ifelse(!is.na(data_new$lastName), data_new$lastName, ""), ", ",
   ifelse(!is.na(data_new$lastName), data_new$lastName, ""), " ",
   substr(ifelse(!is.na(data_new$middleName),
                 data_new$middleName,
                 ""),
          1,
          1)
)

# extract birth year
data_new$birthYear <- ifelse(!is.na(data_new$bdate),
                             substr(data_new$bdate, 1, 4),
                             "")

# save as dta
write_dta(data_new, file.path(data_dir, "output_bene.dta"))
