import os
import re
import pandas as pd
import torch
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from datetime import datetime


def parse_date_window_from_urls(url_csv_path):
    """
    Reads the constructed Twitter search URLs from 'url_csv_path'
    and extracts the min 'since' date and max 'until' date to define a date window.
    Returns (start_date, end_date) as strings in 'YYYY-MM-DD' format, or (None, None)
    if something goes wrong.
    """
    print(f"[DEBUG] Attempting to parse date window from {url_csv_path}")
    if not os.path.exists(url_csv_path):
        print(f"[WARNING] {url_csv_path} not found. No date window extracted.")
        return None, None

    df_urls = pd.read_csv(url_csv_path)
    if "url" not in df_urls.columns:
        print("[WARNING] 'url' column not found in urls.csv.")
        return None, None

    re_until = re.compile(r"until%3A(\d{4}-\d{2}-\d{2})")
    re_since = re.compile(r"since%3A(\d{4}-\d{2}-\d{2})")

    all_since = []
    all_until = []

    for url in df_urls["url"]:
        match_until = re_until.search(url)
        match_since = re_since.search(url)
        if match_until:
            all_until.append(match_until.group(1))
        if match_since:
            all_since.append(match_since.group(1))

    if not all_since or not all_until:
        print("[WARNING] Could not parse any valid date from urls.csv.")
        return None, None

    start_date = min(all_since)
    end_date = max(all_until)
    print(f"[DEBUG] Parsed date window: {start_date} to {end_date}")
    return start_date, end_date


def get_offline_pipeline(model_folder: Path):
    """
    Loads a local sentiment-analysis pipeline (tokenizer + PyTorch model)
    strictly from local files in 'model_folder'. Falls back to CPU if CUDA is not available.
    """
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[INFO] Using device: {device}")

    local_dir = str(model_folder)
    print(f"[DEBUG] Loading tokenizer and model from {local_dir}")
    tokenizer = AutoTokenizer.from_pretrained(local_dir, local_files_only=True)
    model = AutoModelForSequenceClassification.from_pretrained(
        local_dir, local_files_only=True, from_tf=False
    )

    sentiment_pipe = pipeline(
        "text-classification",
        model=model,
        tokenizer=tokenizer,
        device=0 if device == "cuda" else -1,
    )
    print("[DEBUG] Sentiment pipeline created successfully.")
    return sentiment_pipe


def safe_sentiment_analysis(pipe, text_list):
    """
    Runs sentiment analysis with the pipeline, handling NaN/NULL.
    Returns 'unknown' for invalid/empty text entries.
    """
    print(f"[DEBUG] Running sentiment analysis on {len(text_list)} texts.")
    valid_texts = [
        str(t) if pd.notna(t) and t not in {"unknown", "NULL", "NA"} else ""
        for t in text_list
    ]
    results = pipe(valid_texts)
    labels = [r["label"] if txt else "unknown" for txt, r in zip(valid_texts, results)]
    print(f"[DEBUG] Sentiment analysis complete. Processed {len(labels)} results.")
    return labels


def partial_update_csv(
    csv_path: str,
    sentiment_pipeline,
    start_date: str,
    end_date: str,
    date_col="Created At",
):
    """
    Updates sentiment values for rows in the CSV file that:
      1) Are within the [start_date, end_date] window (if valid dates provided)
      2) Have no 'sentiment' value

    Always saves back to the original CSV file, never creates a new one.
    """
    print(f"[INFO] Processing file: {os.path.basename(csv_path)}")

    if not os.path.exists(csv_path):
        print(f"[WARNING] File not found: {csv_path}")
        return

    try:
        df = pd.read_csv(csv_path)
        print(f"[DEBUG] Successfully read file with {len(df)} rows")
    except Exception as e:
        print(f"[ERROR] Could not read file {csv_path}: {e}")
        return

    # Check for required columns
    if "Text" not in df.columns:
        print(f"[WARNING] Skipping file: No 'Text' column found")
        return

    # If date filtering is requested, ensure date column exists
    if (start_date and end_date) and date_col not in df.columns:
        print(f"[WARNING] Cannot filter by date: No '{date_col}' column")
        # Continue processing but without date filtering

    # Add sentiment column if it doesn't exist
    if "sentiment" not in df.columns:
        df["sentiment"] = pd.NA
        print(f"[DEBUG] Added new 'sentiment' column")

    # Identify rows missing sentiment
    mask_new = df["sentiment"].isna()
    rows_with_missing_sentiment = mask_new.sum()

    if rows_with_missing_sentiment == 0:
        print(f"[INFO] No rows need sentiment analysis - all rows already have values")
        return

    # Apply date filtering if possible
    if start_date and end_date and date_col in df.columns:
        try:
            # Convert the date column to datetime
            df[date_col] = pd.to_datetime(
                df[date_col], errors="coerce", utc=True
            ).dt.tz_convert(None)

            # Create date filter mask
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            mask_date = df[date_col].between(start_dt, end_dt, inclusive="both")

            # Combine with missing sentiment mask
            mask_update = mask_new & mask_date
            date_filtered_count = mask_update.sum()

            print(
                f"[DEBUG] Date filter applied: {rows_with_missing_sentiment} rows missing sentiment, "
                f"{date_filtered_count} within date window {start_date} to {end_date}"
            )
        except Exception as e:
            print(
                f"[WARNING] Date filtering failed: {e} - processing all rows with missing sentiment"
            )
            mask_update = mask_new
    else:
        mask_update = mask_new
        print(
            f"[DEBUG] No date filtering applied - processing all {rows_with_missing_sentiment} rows with missing sentiment"
        )

    # Final count of rows to update
    rows_to_update = mask_update.sum()

    if rows_to_update == 0:
        print(f"[INFO] No rows to update after filtering")
        return

    print(f"[INFO] Analyzing sentiment for {rows_to_update} rows...")

    # Get texts to analyze
    texts_to_analyze = df.loc[mask_update, "Text"].tolist()

    # Run sentiment analysis
    sentiment_labels = safe_sentiment_analysis(sentiment_pipeline, texts_to_analyze)

    # Update the dataframe in-place
    df.loc[mask_update, "sentiment"] = sentiment_labels

    # Save back to THE SAME FILE - CRITICAL
    try:
        df.to_csv(csv_path, index=False)
        print(
            f"[INFO] Successfully updated {rows_to_update} rows with sentiment values in {os.path.basename(csv_path)}"
        )
    except Exception as e:
        print(f"[ERROR] Failed to save updates to {csv_path}: {e}")


if __name__ == "__main__":
    # Print start timestamp
    start_time = datetime.now()
    print(
        f"[INFO] Starting sentiment_cron.py at {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    # Determine output directory
    output_dir = "../output/"
    print(f"[INFO] Using output directory: {output_dir}")

    # Parse date window from urls.csv if available
    urls_csv = os.path.join(output_dir, "urls.csv")
    start_date, end_date = parse_date_window_from_urls(urls_csv)

    if start_date and end_date:
        print(f"[INFO] Using date window: {start_date} to {end_date}")
    else:
        print(
            "[INFO] No valid date window found - will process all rows with missing sentiment"
        )

    # Load sentiment model
    model_folder = Path(
        r"C:\Users\TomHun\OneDrive - City & Guilds\Documents\Code\R\vibe_check\backend_x_scraper\twitter-roberta-base-sentiment-latest"
    )
    sentiment_pipeline = get_offline_pipeline(model_folder)

    # Find CSV files to process - respect original files
    ignore_files = {"urls.csv", "log.txt"}
    csv_files = [
        f
        for f in os.listdir(output_dir)
        if f.endswith(".csv") and f not in ignore_files
    ]

    file_count = len(csv_files)
    print(f"[INFO] Found {file_count} CSV files to process")

    # Process each file individually
    for idx, filename in enumerate(sorted(csv_files), start=1):
        print(f"\n[INFO] Processing file {idx}/{file_count}: {filename}")
        csv_path = os.path.join(output_dir, filename)

        partial_update_csv(
            csv_path, sentiment_pipeline, start_date, end_date, date_col="Created At"
        )

    # Print end timestamp and duration
    end_time = datetime.now()
    duration = end_time - start_time
    print(
        f"\n[INFO] Finished sentiment_cron.py at {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print(f"[INFO] Total execution time: {duration}")
    print("[INFO] Sentiment analysis complete - all files processed in-place")
