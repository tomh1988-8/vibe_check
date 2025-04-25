# Running a separate update script as the url httr section is computationally demanding.
######################### ALL CLEAR! ##########################################
rm(list = ls())

########################## LIBRARIES ###########################################
library(tidyverse)
library(lubridate)
library(httr)
library(here)
# Add furrr for parallel processing
library(furrr)

combined_posts <- readRDS(file = here("data", "combined_posts.rds"))

########################## READ IN DATA ########################################
# Define the folder path
folder_path <- "C:/Users/TomHun/OneDrive - City & Guilds/Documents/Code/R/vibe_check/backend_x_scraper/output"

# List the csv files (theres a couple we don't want to bundled in)
csv_files <- list.files(folder_path, pattern = "\\.csv$", full.names = TRUE)

# Isolate the csv files
csv_files <- csv_files[!grepl("urls", csv_files)]

# Read the chosen csvs into a list based on their file names with parsing fix
df_list <- map(
  csv_files,
  ~ {
    df <- read_csv(.x, show_col_types = FALSE)
    # Ensure Created At is always a datetime
    if (
      "Created At" %in% colnames(df) && !inherits(df$`Created At`, "POSIXct")
    ) {
      df$`Created At` <- parse_datetime(df$`Created At`)
    }
    return(df)
  }
)

# name each dataframe based on file name
names(df_list) <- basename(csv_files)

# read in urls separately in case I build a cool link features
url_path <- "C:/Users/TomHun/OneDrive - City & Guilds/Documents/Code/R/vibe_check/backend_x_scraper/output"
urls_file <- file.path(url_path, "urls.csv")
urls <- read_csv(file = urls_file, show_col_types = FALSE)

# Join together all dataframes in list using bind_rows
updated_posts <- bind_rows(df_list)

########################## CLEAN ###############################################
# Remove unsuccessful extraction columns
updated_posts <- updated_posts |>
  select(-c(`Tweet ID`, Likes, Retweets, Replies)) |>
  select(
    tag,
    created_at = `Created At`,
    Text,
    post_url = `Tweet URL`,
    everything()
  ) |>
  rename_with(tolower) |>
  mutate(unique_id = str_extract(post_url, "(?<=/status/)[0-9]+"))
# mutate added to allow for easy id of new rows

########################## POST LENGTH CALCULATION ############################
# Add post length calculations (with and without spaces)
updated_posts <- updated_posts %>%
  mutate(
    # Calculate post length with spaces (standard readable length)
    post_length = nchar(text),

    # Calculate post length without spaces (sometimes useful for analysis)
    post_length_no_spaces = nchar(gsub("\\s+", "", text)),

    # Calculate post length category for potential grouping
    post_length_category = case_when(
      post_length < 50 ~ "very short",
      post_length < 100 ~ "short",
      post_length < 200 ~ "medium",
      post_length < 280 ~ "long",
      TRUE ~ "very long"
    )
  )

########################## FIND NEW DATA #######################################
# Extract the tweet id: digits following "/status/"
new_unique_ids <- updated_posts |>
  select(unique_id) |>
  as.vector()


old_unique_ids <- combined_posts |>
  select(unique_id) |>
  as.vector()

updated_posts <- updated_posts |>
  anti_join(combined_posts |> select(unique_id), by = "unique_id")

########################## PROCESS NEW DATA ####################################
########################## ENGINEER FEATURES -----------------------------------
#------------------------- DATE INFO -------------------------------------------
updated_posts <- updated_posts %>%
  mutate(
    calendar_year = year(created_at),
    financial_year = if_else(
      month(created_at) >= 4,
      year(created_at) + 1,
      year(created_at)
    ),
    calendar_quarter = quarter(created_at),
    financial_quarter = (((month(created_at) - 4) %% 12) %/% 3) + 1,
    month = month(created_at),
    day_of_week = wday(created_at, label = TRUE, abbr = FALSE),
    weekend_weekday = if_else(
      wday(created_at) %in% c(1, 7),
      "weekend",
      "weekday"
    ),
    hour_of_day = hour(created_at),
    season = case_when(
      month(created_at) %in% c(12, 1, 2) ~ "winter",
      month(created_at) %in% c(3, 4, 5) ~ "spring",
      month(created_at) %in% c(6, 7, 8) ~ "summer",
      month(created_at) %in% c(9, 10, 11) ~ "autumn",
      TRUE ~ NA_character_
    ),
    # Calendar year-quarter variable
    year_quarter = paste0(calendar_year, "-Q", calendar_quarter),
    # For financial year-quarter, first compute the lower bound of the financial year:
    financial_year_lower = if_else(
      month(created_at) >= 4,
      year(created_at),
      year(created_at) - 1
    ),
    # Create a financial year label like "2015-16":
    financial_year_label = paste0(
      financial_year_lower,
      "-",
      str_sub(as.character(financial_year_lower + 1), 3, 4)
    ),
    # Combine with the financial quarter
    financial_year_quarter = paste0(
      financial_year_label,
      "_Q",
      financial_quarter
    )
  )

#------------------------- POST URL EXTRACTIONS -----------------------------------
updated_posts <- updated_posts %>%
  mutate(
    # Extract the account name: characters after "https://x.com/" and before the next "/"
    poster_account = str_extract(post_url, "(?<=https://x\\.com/)[^/]+"),
    # Extract the tweet id: digits following "/status/"
    unique_id = str_extract(post_url, "(?<=/status/)[0-9]+"),
    # Create from_us: "y" if poster_account is cityandguilds, otherwise "n"
    from_us = if_else(tolower(poster_account) == "cityandguilds", "y", "n")
  )

#------------------------- hashtag EXTRACTIONS ------------------------------------
updated_posts <- updated_posts %>%
  # Overwrite hashtags with a list of individual, trimmed, and lowercased hashtag strings
  mutate(
    hashtags = map(
      hashtags,
      ~ if (is.na(.x)) {
        character(0)
      } else {
        map_chr(str_split(.x, pattern = ",")[[1]], ~ str_to_lower(str_trim(.x)))
      }
    )
  ) %>%
  # Compute counts and categorical variables for hashtags
  mutate(
    num_hashtags = map_int(hashtags, length),
    num_hashtags_excl = map_int(hashtags, ~ sum(.x != "#cityandguilds")),
    hashtag_category = case_when(
      num_hashtags_excl == 0 ~ "none",
      num_hashtags_excl == 1 ~ "one",
      num_hashtags_excl > 1 ~ "more than one"
    ),
    extended_hashtag_category = case_when(
      num_hashtags_excl == 0 ~ "none",
      num_hashtags_excl == 1 ~ "one",
      num_hashtags_excl > 10 ~ "more than ten",
      num_hashtags_excl > 5 ~ "more than five",
      num_hashtags_excl > 1 ~ "more than one"
    )
  )
#------------------------- mentions EXTRACTIONS -----------------------------------
updated_posts <- updated_posts %>%
  # Overwrite mentions with a list of individual, trimmed, and lowercased mention strings
  mutate(
    mentions = map(
      mentions,
      ~ if (is.na(.x)) {
        character(0)
      } else {
        map_chr(str_split(.x, pattern = ",")[[1]], ~ str_to_lower(str_trim(.x)))
      }
    )
  ) %>%
  # Compute counts and categorical variables for mentions
  mutate(
    num_mentions = map_int(mentions, length),
    num_mentions_excl = map_int(mentions, ~ sum(.x != "@cityandguilds")),
    mention_category = case_when(
      num_mentions_excl == 0 ~ "none",
      num_mentions_excl == 1 ~ "one",
      num_mentions_excl > 1 ~ "more than one"
    ),
    extended_mention_category = case_when(
      num_mentions_excl == 0 ~ "none",
      num_mentions_excl == 1 ~ "one",
      num_mentions_excl > 10 ~ "more than ten",
      num_mentions_excl > 5 ~ "more than five",
      num_mentions_excl > 1 ~ "more than one"
    )
  )

#------------------------- attached url EXTRACTIONS -------------------------------
updated_posts <- updated_posts %>%
  # Process URLs: split by comma, trim whitespace
  mutate(
    urls = map(
      urls,
      ~ if (is.na(.x)) {
        character(0)
      } else {
        map_chr(str_split(.x, pattern = ",")[[1]], ~ str_trim(.x))
      }
    )
  ) %>%
  # Compute counts and categorical variables for URLs
  mutate(
    num_urls = map_int(urls, length),
    url_category = case_when(
      num_urls == 0 ~ "none",
      num_urls == 1 ~ "one",
      num_urls > 1 ~ "more than one"
    ),
    extended_url_category = case_when(
      num_urls == 0 ~ "none",
      num_urls == 1 ~ "one",
      num_urls > 10 ~ "more than ten",
      num_urls > 5 ~ "more than five",
      num_urls > 1 ~ "more than one"
    )
  )

#------------------------- EXTRA POST CONTENT ##################################
updated_posts <- updated_posts %>%
  mutate(
    # Total add-ons: sum of hashtags, mentions, and urls counts
    total_add_ons = num_hashtags + num_mentions + num_urls,

    # Basic category: "none", "one", or "more than one"
    add_on_category = case_when(
      total_add_ons == 0 ~ "none",
      total_add_ons == 1 ~ "one",
      total_add_ons > 1 ~ "more than one"
    ),

    # Extended category with finer thresholds
    extended_add_on_category = case_when(
      total_add_ons == 0 ~ "none",
      total_add_ons == 1 ~ "one",
      total_add_ons > 10 ~ "more than ten",
      total_add_ons > 5 ~ "more than five",
      total_add_ons > 1 ~ "more than one"
    )
  )

########################## ATTATCHED URL CONVERSION ############################
# Enhanced URL expansion function with retries
expand_url <- function(url, max_attempts = 3) {
  for (i in 1:max_attempts) {
    res <- try(HEAD(url, timeout(5)), silent = TRUE)
    if (!inherits(res, "try-error")) return(res$url)
    Sys.sleep(1) # Wait before retry
  }
  return(url) # Return original if all attempts fail
}

# Set up parallel processing with appropriate number of workers
available_cores <- parallel::detectCores() - 1
available_cores <- max(1, available_cores) # Ensure at least 1 core
plan(multisession, workers = min(available_cores, 3))

# Apply the expansion function to each element of the urls list-column in parallel
updated_posts <- updated_posts %>%
  mutate(
    expanded_urls = future_map(
      urls,
      ~ if (length(.x) == 0) {
        character(0)
      } else {
        future_map_chr(.x, expand_url)
      },
      .options = furrr_options(seed = TRUE)
    )
  )

# Clean up parallel workers
plan(sequential)

# Free up memory
gc()

########################## ADD NEW DATA ########################################
combined_posts <- bind_rows(combined_posts, updated_posts)
########################## SAVE DATA ###########################################
# Save combined_posts as an RDS file
saveRDS(combined_posts, file = here("data", "combined_posts.rds"))
# .... and as a package file
usethis::use_data(combined_posts, overwrite = TRUE)
