########################## ALL CLEAR! ##########################################
rm(list = ls())

########################## LIBRARIES ###########################################
library(tidyverse)
library(here)

########################## READ IN DATA ########################################
# Define the folder path
folder_path <- "C:/Users/TomHun/OneDrive - City & Guilds/Documents/Code/Python/monitor/output"

# List the csv files (theres a couple we don't want to bundled in)
csv_files <- list.files(folder_path, pattern = "\\.csv$", full.names = TRUE)

# Isolate the csv files
csv_files <- csv_files[!grepl("urls", csv_files)]

# Read the chosen csvs into a list based on their file names
df_list <- map(csv_files, ~ read_csv(.x, show_col_types = FALSE))

# name each dataframe based on file name
names(df_list) <- basename(csv_files)

# read in urls separately in case I build a cool link features
urls_file <- file.path(folder_path, "urls.csv")
urls <- read_csv(file = urls_file, show_col_types = FALSE)
