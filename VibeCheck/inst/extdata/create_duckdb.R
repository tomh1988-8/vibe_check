######################### SETUP DUCKDB DATABASE #############################
# This script only needs to be run once to set up the database
# Last updated: 2025-05-02 14:29:32
# Author: tomh1988-8

library(tidyverse)
library(lubridate)
library(duckdb)
library(here)

# Define database path - store in project data directory
db_path <- here("data", "vibe_check.duckdb")

# Create a connection to the DuckDB database file
con <- dbConnect(duckdb::duckdb(), dbdir = db_path)

# Import existing data and create row ID primary key
if (file.exists(here("data", "combined_posts.rds"))) {
  combined_posts <- readRDS(file = here("data", "combined_posts.rds"))

  # First filter out rows with NULL text values
  cat("Filtering out rows with NULL text values...\n")
  combined_posts <- combined_posts %>%
    filter(!is.null(text) & !is.na(text))

  # Add a new sequential ID column based on row numbers
  cat("Creating new sequential ID as primary key...\n")
  combined_posts <- combined_posts %>%
    mutate(post_id = row_number()) %>% # Changed to post_id to avoid potential keyword issues
    select(post_id, everything()) # Move post_id to first column for clarity

  # Drop existing table if it exists and create a fresh one
  cat("Creating table schema...\n")
  dbExecute(con, "DROP TABLE IF EXISTS posts")

  # Create the main table schema with the new ID as primary key
  dbExecute(
    con,
    "
    CREATE TABLE posts (
      post_id INTEGER PRIMARY KEY,  -- New primary key based on row numbers
      unique_id VARCHAR,       -- Original ID now just a regular column
      tag VARCHAR,
      created_at TIMESTAMP,
      text VARCHAR,
      post_url VARCHAR,
      hashtags VARCHAR,
      mentions VARCHAR,
      urls VARCHAR,
      expanded_urls VARCHAR,
      sentiment VARCHAR,
      post_length INTEGER,
      post_length_no_spaces INTEGER,
      post_length_category VARCHAR,
      calendar_year INTEGER,
      financial_year INTEGER,
      calendar_quarter INTEGER,
      financial_quarter INTEGER,
      month INTEGER,
      day_of_week VARCHAR,
      weekend_weekday VARCHAR,
      hour_of_day INTEGER,
      season VARCHAR,
      year_quarter VARCHAR,
      financial_year_lower INTEGER,
      financial_year_label VARCHAR,
      financial_year_quarter VARCHAR,
      poster_account VARCHAR,
      from_us VARCHAR,
      num_hashtags INTEGER,
      num_hashtags_excl INTEGER,
      hashtag_category VARCHAR,
      extended_hashtag_category VARCHAR,
      num_mentions INTEGER,
      num_mentions_excl INTEGER,
      mention_category VARCHAR,
      extended_mention_category VARCHAR,
      num_urls INTEGER,
      url_category VARCHAR,
      extended_url_category VARCHAR,
      total_add_ons INTEGER,
      add_on_category VARCHAR,
      extended_add_on_category VARCHAR,
      sentiment_extended VARCHAR,
      sentiment_combined VARCHAR
    )
    "
  )

  # Convert list columns to JSON strings for storage
  combined_posts_for_db <- combined_posts %>%
    mutate(
      hashtags = map_chr(hashtags, ~ jsonlite::toJSON(.x)),
      mentions = map_chr(mentions, ~ jsonlite::toJSON(.x)),
      urls = map_chr(urls, ~ jsonlite::toJSON(.x)),
      expanded_urls = map_chr(expanded_urls, ~ jsonlite::toJSON(.x))
    )

  # Insert initial data
  cat("Inserting", nrow(combined_posts_for_db), "rows into database...\n")
  dbAppendTable(con, "posts", combined_posts_for_db)

  # Create indexes for faster queries on common fields
  cat("Creating indexes for faster queries...\n")
  dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_created_at ON posts(created_at)"
  )
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_from_us ON posts(from_us)")
  dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_poster ON posts(poster_account)"
  )
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_sentiment ON posts(sentiment)")
  dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_sentiment_extended ON posts(sentiment_extended)"
  )

  # Create index on the original unique_id for lookups
  dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_original_id ON posts(unique_id)"
  )

  # Create composite index for common filtering combinations
  dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_date_source ON posts(created_at, from_us)"
  )
}

# Create auxiliary tables with the appropriate ID type
dbExecute(con, "DROP TABLE IF EXISTS tags")
dbExecute(
  con,
  "
  CREATE TABLE tags (
    tag_id INTEGER PRIMARY KEY,  -- DuckDB automatically makes this auto-incrementing
    tag_name VARCHAR UNIQUE
  )
"
)

# Save the database path to a config file
config <- list(db_path = db_path)
saveRDS(config, file = here("data", "duckdb_config.rds"))

# Explicitly close the connection with shutdown=TRUE
cat("Closing database connection...\n")
dbDisconnect(con, shutdown = TRUE)

cat("DuckDB setup complete!\n")
cat("Database location:", db_path, "\n")
cat("Total posts imported:", nrow(combined_posts), "\n")
cat("Setup completed:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
