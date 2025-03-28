import sys
import pandas as pd
from pathlib import Path
from pyprojroot import here

# Add the 'notebooks' folder to your module search path so we can import our modules.
sys.path.append(str(here("notebooks")))

# Import the helper function for scraping
from def_url_scraper import url_scraper


def process_year(year):
    """
    Scrapes tweets for each URL in the lookup CSV that match the specified year and writes each scraped
    DataFrame to the output folder with the filename based on the 'tag' column (e.g., hashtags_2015.csv).

    If the output file already exists, new tweets are appended and deduplicated using the 'Tweet URL' column.

    Args:
        year (str or int): The year (or env_suffix) to filter the lookup CSV.
    """
    # -------------------------------------------------------------------------------
    # Load the lookup CSV file that contains the URLs
    # -------------------------------------------------------------------------------
    lookup_csv_path = Path(str(here("output"))) / "urls.csv"
    print("Lookup CSV path:", lookup_csv_path)

    if not lookup_csv_path.is_file():
        raise FileNotFoundError(f"Lookup CSV not found at {lookup_csv_path}")

    # Read the CSV into a DataFrame
    urls = pd.read_csv(str(lookup_csv_path))
    print("Lookup DataFrame head:")
    print(urls.head())

    # -------------------------------------------------------------------------------
    # Filter for rows with env_suffix equal to the provided year
    # -------------------------------------------------------------------------------
    urls_year = urls[urls["env_suffix"].astype(str) == str(year)]
    print(f"Found {len(urls_year)} URLs for year {year}.")

    if urls_year.empty:
        print(f"No URLs found for env_suffix '{year}'.")
        return

    # Shuffle the rows in urls_year
    urls_year = urls_year.sample(frac=1).reset_index(drop=True)
    print("Shuffled URLs DataFrame head:")
    print(urls_year.head())

    # Ensure the output directory exists
    output_dir = Path(str(here("output")))
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    # -------------------------------------------------------------------------------
    # Loop through each URL for the specified year, scrape tweets, and write each result separately.
    # -------------------------------------------------------------------------------
    for idx, row in urls_year.iterrows():
        tag = row["tag"]
        target_url = row["url"]
        print(f"Scraping URL for {tag}: {target_url}")

        try:
            tweets_df = url_scraper(target_url)
            # Diagnose if no tweets were scraped from this URL:
            if tweets_df.empty:
                print(f"Warning: No tweets scraped from URL for tag {tag}.")
            else:
                print(f"Scraped {len(tweets_df)} tweets from URL for tag {tag}.")

            # Add the tag column to the tweets DataFrame (if not already present)
            tweets_df["tag"] = tag

            # Build the output file path using the tag. Example: hashtags_2015.csv
            output_file = output_dir / f"{tag}.csv"

            if output_file.exists():
                # If the file exists, load the existing tweets and append new ones.
                existing_df = pd.read_csv(output_file)
                existing_count = len(existing_df)
                print(
                    f"Existing tweets loaded from {output_file} (total: {existing_count})."
                )
                # Concatenate and drop duplicates based on 'Tweet URL'
                combined_df = pd.concat([existing_df, tweets_df], ignore_index=True)
                combined_df.drop_duplicates(subset="Tweet URL", inplace=True)
                new_total = len(combined_df)
                extra_added = new_total - existing_count
                print(f"After deduplication, total tweets for {tag}: {new_total}.")
                print(f"Extra tweets added for {tag}: {extra_added}.")
                tweets_df = combined_df
            else:
                print(f"No existing file for {tag}. Creating new file.")
                print(f"Tweets for {tag}: {len(tweets_df)} added.")

            # Write the (updated) DataFrame to the CSV file.
            tweets_df.to_csv(output_file, index=False)
            print(f"Tweets for tag {tag} written to {output_file}")
        except Exception as e:
            print(f"Error scraping {target_url} ({tag}): {e}")
            continue
