import os
import re
import pandas as pd
import torch
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from datetime import datetime
import gc  # Garbage Collector for potentially large dataframes


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

    try:
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
    except Exception as e:
        print(f"[ERROR] Failed to parse date window from {url_csv_path}: {e}")
        return None, None


def get_offline_pipeline(model_folder: Path):
    """
    Loads a local sentiment-analysis pipeline (tokenizer + PyTorch model)
    strictly from local files in 'model_folder'. Falls back to CPU if CUDA is not available.
    """
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[INFO] Using device: {device}")

    try:
        local_dir = str(model_folder)
        print(f"[DEBUG] Loading tokenizer and model from {local_dir}")
        # Ensure cache is not used if files might change, though local_files_only should handle this
        # cache_dir = os.path.join(local_dir, "hf_cache")
        tokenizer = AutoTokenizer.from_pretrained(
            local_dir, local_files_only=True
        )  # , cache_dir=cache_dir)
        model = AutoModelForSequenceClassification.from_pretrained(
            local_dir,
            local_files_only=True,
            from_tf=False,  # , cache_dir=cache_dir
        )

        sentiment_pipe = pipeline(
            "text-classification",
            model=model,
            tokenizer=tokenizer,
            device=0
            if device == "cuda"
            else -1,  # Use device index 0 for CUDA, -1 for CPU
        )
        print("[DEBUG] Sentiment pipeline created successfully.")
        return sentiment_pipe
    except Exception as e:
        print(f"[ERROR] Failed to load model from {model_folder}: {e}")
        return None


def safe_sentiment_analysis(pipe, text_list):
    """
    Runs sentiment analysis with the pipeline, handling NaN/NULL/empty strings.
    Returns 'unknown' for invalid/empty text entries or pipeline errors.
    """
    if not text_list:  # Handle empty input list
        return []

    print(f"[DEBUG] Running sentiment analysis on {len(text_list)} texts.")
    # Prepare texts: use empty string for anything invalid/null/whitespace-only
    valid_texts = [
        str(t).strip()
        if pd.notna(t) and str(t).strip() not in {"", "unknown", "NULL", "NA"}
        else ""
        for t in text_list
    ]

    # Filter out empty strings before sending to the pipeline
    texts_to_process = [t for t in valid_texts if t]
    indices_to_process = [i for i, t in enumerate(valid_texts) if t]

    # Initialize results with 'unknown'
    labels = ["unknown"] * len(text_list)

    if texts_to_process:
        try:
            # Process in batches if list is very large (optional, adjust batch_size as needed)
            # batch_size = 64
            # results_raw = []
            # for i in range(0, len(texts_to_process), batch_size):
            #     batch = texts_to_process[i:i + batch_size]
            #     results_raw.extend(pipe(batch))

            results_raw = pipe(texts_to_process)  # Process all at once if memory allows

            # Map results back to their original positions
            for original_index, result in zip(indices_to_process, results_raw):
                labels[original_index] = result["label"]

        except Exception as e:
            print(f"[ERROR] Sentiment analysis pipeline failed during processing: {e}")
            # Keep 'unknown' for all in case of error

    print(f"[DEBUG] Sentiment analysis complete. Returning {len(labels)} results.")
    return labels


def partial_update_csv(
    csv_path: str,
    sentiment_pipeline,
    start_date: str,
    end_date: str,
    date_col="Created At",
):
    """
    (First Pass) Updates sentiment values for rows in the CSV file that:
      1) Are within the [start_date, end_date] window (if valid dates provided)
      2) Have missing ('NA', 'unknown') sentiment AND valid text.

    Always saves back to the original CSV file. Returns rows updated count or None on failure.
    """
    print(
        f"[INFO] First Pass Processing: {os.path.basename(csv_path)} (Date Window: {start_date}-{end_date})"
    )
    rows_updated_count = 0

    if not os.path.exists(csv_path):
        print(f"[WARNING] File not found: {csv_path}")
        return None

    try:
        df = pd.read_csv(csv_path)
        print(f"[DEBUG] Read {len(df)} rows for first pass.")
    except Exception as e:
        print(f"[ERROR] Could not read file {csv_path}: {e}")
        return None

    if "Text" not in df.columns:
        print(f"[WARNING] Skipping first pass: No 'Text' column found.")
        return 0  # Return 0 updates, not None, as file exists but wasn't processed

    if "sentiment" not in df.columns:
        df["sentiment"] = pd.NA
        print(f"[DEBUG] Added new 'sentiment' column.")

    # Consistent check for missing sentiment
    df["sentiment"] = (
        df["sentiment"].astype(object).where(df["sentiment"].notna(), pd.NA)
    )
    mask_missing_initial = df["sentiment"].isna() | (df["sentiment"] == "unknown")

    if not mask_missing_initial.any():
        print(f"[INFO] First Pass: No rows initially missing sentiment.")
        del df
        gc.collect()  # Clean up memory
        return 0

    # --- Date Filtering ---
    mask_date_filter = pd.Series([True] * len(df), index=df.index)
    if start_date and end_date:
        if date_col not in df.columns:
            print(
                f"[WARNING] First Pass: Cannot filter by date: No '{date_col}' column."
            )
        else:
            try:
                date_series_dt = pd.to_datetime(
                    df[date_col], errors="coerce", utc=True
                ).dt.tz_convert(None)
                valid_dates_mask = date_series_dt.notna()
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                # Only consider rows with valid dates within the window
                mask_date_filter = (
                    date_series_dt.between(start_dt, end_dt, inclusive="both")
                    & valid_dates_mask
                )
                print(
                    f"[DEBUG] First Pass: Date filter applied. {(~mask_date_filter).sum()} rows outside window or invalid date."
                )
            except Exception as e:
                print(
                    f"[WARNING] First Pass: Date filtering failed: {e}. Applying to all rows."
                )
                mask_date_filter = pd.Series([True] * len(df), index=df.index)
    else:
        print("[DEBUG] First Pass: No date filtering applied.")
    # --- End Date Filtering ---

    # Combine masks: Missing sentiment AND within date window (if applicable)
    mask_update_candidates = mask_missing_initial & mask_date_filter

    # Further filter: only update if 'Text' is valid
    mask_valid_text = df["Text"].notna() & df["Text"].astype(str).str.strip().ne("")
    mask_update_final = mask_update_candidates & mask_valid_text

    rows_to_update = mask_update_final.sum()

    if rows_to_update > 0:
        print(
            f"[INFO] First Pass: Analyzing sentiment for {rows_to_update} rows within date window..."
        )
        texts_to_analyze = df.loc[mask_update_final, "Text"].tolist()
        sentiment_labels = safe_sentiment_analysis(sentiment_pipeline, texts_to_analyze)
        df.loc[mask_update_final, "sentiment"] = sentiment_labels
        rows_updated_count = rows_to_update

        try:
            df.to_csv(csv_path, index=False)
            print(
                f"[INFO] First Pass: Successfully updated {rows_updated_count} rows in {os.path.basename(csv_path)}"
            )
        except Exception as e:
            print(f"[ERROR] First Pass: Failed to save updates to {csv_path}: {e}")
            rows_updated_count = 0  # Reset count if save failed
    else:
        print(f"[INFO] First Pass: No rows needed updating within the date window.")
        # No need to save if no changes were made

    del df
    gc.collect()  # Explicitly clean up memory
    return rows_updated_count


def sweeper_sentiment_analysis(csv_path: str, sentiment_pipeline):
    """
    (Sweeper Pass) Updates sentiment for *any* remaining rows with missing
    sentiment ('NA' or 'unknown') AND valid text, regardless of date.
    Saves back to the original CSV. Returns count of rows updated in this pass.
    """
    print(f"[INFO] Sweeper Pass Processing: {os.path.basename(csv_path)}")
    rows_updated_count = 0

    if not os.path.exists(csv_path):
        print(f"[WARNING] Sweeper Pass: File not found: {csv_path}")
        return None  # Indicate failure to process

    try:
        # Read the potentially updated file from the first pass
        df = pd.read_csv(csv_path)
        print(f"[DEBUG] Read {len(df)} rows for sweeper pass.")
    except Exception as e:
        print(f"[ERROR] Sweeper Pass: Could not read file {csv_path}: {e}")
        return None

    # Ensure required columns exist
    if "Text" not in df.columns or "sentiment" not in df.columns:
        print(
            f"[WARNING] Sweeper Pass: Skipping - Missing 'Text' or 'sentiment' column."
        )
        del df
        gc.collect()
        return 0  # File exists but cannot be processed

    # Consistent check for missing sentiment
    df["sentiment"] = (
        df["sentiment"].astype(object).where(df["sentiment"].notna(), pd.NA)
    )
    mask_missing = df["sentiment"].isna() | (df["sentiment"] == "unknown")

    if not mask_missing.any():
        print(f"[INFO] Sweeper Pass: No rows missing sentiment.")
        del df
        gc.collect()
        return 0

    # Filter for valid text among those missing sentiment
    mask_valid_text = df["Text"].notna() & df["Text"].astype(str).str.strip().ne("")
    mask_update_final = mask_missing & mask_valid_text

    rows_to_update = mask_update_final.sum()

    if rows_to_update > 0:
        print(
            f"[INFO] Sweeper Pass: Analyzing sentiment for {rows_to_update} remaining rows..."
        )
        texts_to_analyze = df.loc[mask_update_final, "Text"].tolist()
        sentiment_labels = safe_sentiment_analysis(sentiment_pipeline, texts_to_analyze)
        df.loc[mask_update_final, "sentiment"] = sentiment_labels
        rows_updated_count = rows_to_update

        try:
            df.to_csv(csv_path, index=False)
            print(
                f"[INFO] Sweeper Pass: Successfully updated {rows_updated_count} rows in {os.path.basename(csv_path)}"
            )
        except Exception as e:
            print(f"[ERROR] Sweeper Pass: Failed to save updates to {csv_path}: {e}")
            rows_updated_count = 0  # Reset count if save failed
    else:
        print(f"[INFO] Sweeper Pass: No rows required updating.")
        # No need to save if no changes were made

    del df
    gc.collect()  # Explicitly clean up memory
    return rows_updated_count


def calculate_final_stats(csv_path: str):
    """Reads a CSV and calculates final sentiment statistics."""
    stats = {
        "total_rows": 0,
        "rows_with_sentiment": 0,
        "rows_without_sentiment": 0,
        "rows_without_sentiment_na_text": 0,
    }
    try:
        df = pd.read_csv(csv_path)
        stats["total_rows"] = len(df)
        if stats["total_rows"] == 0:
            return stats  # Empty file

        if "sentiment" not in df.columns:
            # If sentiment column doesn't exist after all passes, none have sentiment
            stats["rows_without_sentiment"] = stats["total_rows"]
            if "Text" not in df.columns:
                # If Text also doesn't exist, all without sentiment are due to missing Text
                stats["rows_without_sentiment_na_text"] = stats["total_rows"]
            else:
                # Calculate how many had NA/empty text
                mask_na_text = df["Text"].isna() | df["Text"].astype(
                    str
                ).str.strip().eq("")
                stats["rows_without_sentiment_na_text"] = mask_na_text.sum()
            return stats

        # Calculate final state
        df["sentiment"] = (
            df["sentiment"].astype(object).where(df["sentiment"].notna(), pd.NA)
        )
        mask_has_sentiment = df["sentiment"].notna() & (df["sentiment"] != "unknown")
        stats["rows_with_sentiment"] = mask_has_sentiment.sum()
        stats["rows_without_sentiment"] = (
            stats["total_rows"] - stats["rows_with_sentiment"]
        )

        if stats["rows_without_sentiment"] > 0:
            mask_no_sentiment = ~mask_has_sentiment
            # Check for NA/empty text only among those *still* without sentiment
            if "Text" in df.columns:
                mask_na_text = df["Text"].isna() | df["Text"].astype(
                    str
                ).str.strip().eq("")
                stats["rows_without_sentiment_na_text"] = (
                    mask_no_sentiment & mask_na_text
                ).sum()
            else:
                # If Text column missing, all without sentiment count as NA text reason
                stats["rows_without_sentiment_na_text"] = stats[
                    "rows_without_sentiment"
                ]

        del df
        gc.collect()
        return stats

    except Exception as e:
        print(
            f"[ERROR] Failed to calculate final stats for {os.path.basename(csv_path)}: {e}"
        )
        return None  # Indicate failure


# ====================
#        MAIN
# ====================
if __name__ == "__main__":
    start_time = datetime.now()
    print(f"[INFO] Starting script at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    output_dir = "../output/"
    print(f"[INFO] Using output directory: {output_dir}")

    urls_csv = os.path.join(output_dir, "urls.csv")
    start_date, end_date = parse_date_window_from_urls(urls_csv)

    if start_date and end_date:
        print(f"[INFO] Using date window for first pass: {start_date} to {end_date}")
    else:
        print(
            "[INFO] No valid date window found - first pass will process all rows with missing sentiment"
        )

    model_path_str = r"C:\Users\TomHun\OneDrive - City & Guilds\Documents\Code\R\vibe_check\backend_x_scraper\twitter-roberta-base-sentiment-latest"
    model_folder = Path(model_path_str)

    if not model_folder.exists():
        print(f"[ERROR] Model folder not found: {model_path_str}. Exiting.")
        exit(1)

    sentiment_pipeline = get_offline_pipeline(model_folder)
    if sentiment_pipeline is None:
        print("[ERROR] Failed to load sentiment pipeline. Exiting.")
        exit(1)

    # --- File Discovery ---
    ignore_files = {"urls.csv", "log.txt"}
    try:
        all_files = os.listdir(output_dir)
    except FileNotFoundError:
        print(f"[ERROR] Output directory not found: {output_dir}. Exiting.")
        exit(1)

    csv_files = sorted(
        [f for f in all_files if f.endswith(".csv") and f not in ignore_files]
    )
    file_count = len(csv_files)
    print(f"[INFO] Found {file_count} CSV files to process")

    processed_files_pass1 = []
    failed_files_pass1 = []
    total_updated_pass1 = 0

    # --- First Pass (Date Windowed) ---
    print("\n" + "=" * 30 + " Starting First Pass (Date Windowed) " + "=" * 30)
    if file_count > 0:
        for idx, filename in enumerate(csv_files, start=1):
            print(f"\n[INFO] First Pass - File {idx}/{file_count}: {filename}")
            csv_path = os.path.join(output_dir, filename)
            updated_count = partial_update_csv(
                csv_path,
                sentiment_pipeline,
                start_date,
                end_date,
                date_col="Created At",
            )
            if updated_count is not None:
                processed_files_pass1.append(filename)
                total_updated_pass1 += updated_count
            else:
                failed_files_pass1.append(filename)
    else:
        print("[INFO] No CSV files found for first pass.")
    print("=" * 75)

    # --- Sweeper Pass ---
    print("\n" + "=" * 30 + " Starting Sweeper Pass (Remaining NAs) " + "=" * 30)
    processed_files_sweeper = []
    failed_files_sweeper = []
    total_updated_sweeper = 0
    files_to_sweep = (
        processed_files_pass1  # Only sweep files successfully handled in pass 1
    )

    if files_to_sweep:
        print(f"[INFO] Running sweeper pass on {len(files_to_sweep)} files.")
        for idx, filename in enumerate(files_to_sweep, start=1):
            print(
                f"\n[INFO] Sweeper Pass - File {idx}/{len(files_to_sweep)}: {filename}"
            )
            csv_path = os.path.join(output_dir, filename)
            updated_count = sweeper_sentiment_analysis(csv_path, sentiment_pipeline)

            if updated_count is not None:
                processed_files_sweeper.append(filename)
                total_updated_sweeper += updated_count
            else:
                failed_files_sweeper.append(filename)
                # If sweeper failed, remove from list used for final stats calculation?
                # For now, we'll still try to calculate stats for it.
    else:
        print("[INFO] No files successfully processed in first pass to run sweeper on.")
    print("=" * 75)

    # --- Final Summary Calculation ---
    print("\n" + "=" * 30 + " Calculating Final Statistics " + "=" * 30)
    final_agg_stats = {
        "total_rows": 0,
        "total_with_sentiment": 0,
        "total_without_sentiment": 0,
        "total_without_sentiment_na_text": 0,
        "files_analyzed_final": 0,
        "files_failed_final_stat_read": 0,
    }
    # Use the list of files intended for sweeping for final stats
    files_for_final_stats = files_to_sweep

    if files_for_final_stats:
        print(
            f"[INFO] Calculating final stats across {len(files_for_final_stats)} files..."
        )
        for filename in files_for_final_stats:
            csv_path = os.path.join(output_dir, filename)
            file_stats = calculate_final_stats(csv_path)
            if file_stats:
                final_agg_stats["files_analyzed_final"] += 1
                final_agg_stats["total_rows"] += file_stats["total_rows"]
                final_agg_stats["total_with_sentiment"] += file_stats[
                    "rows_with_sentiment"
                ]
                final_agg_stats["total_without_sentiment"] += file_stats[
                    "rows_without_sentiment"
                ]
                final_agg_stats["total_without_sentiment_na_text"] += file_stats[
                    "rows_without_sentiment_na_text"
                ]
            else:
                final_agg_stats["files_failed_final_stat_read"] += 1
        print("[INFO] Final statistics calculation complete.")
    else:
        print(
            "[INFO] No files were successfully processed in the first pass; skipping final summary calculation."
        )

    # --- Timestamps and Final Printout ---
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n[INFO] Script finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total execution time: {duration}")

    print("\n" + "=" * 30 + " Final Summary " + "=" * 30)
    print(f"Files found initially: {file_count}")
    print(f"Files processed in Pass 1 (Date Windowed): {len(processed_files_pass1)}")
    if failed_files_pass1:
        print(f"Files failed/skipped in Pass 1: {len(failed_files_pass1)}")
    print(f"Rows updated in Pass 1: {total_updated_pass1}")
    print("-" * 30)
    print(f"Files processed in Sweeper Pass: {len(processed_files_sweeper)}")
    if failed_files_sweeper:
        print(f"Files failed/skipped in Sweeper Pass: {len(failed_files_sweeper)}")
    print(f"Rows updated in Sweeper Pass: {total_updated_sweeper}")
    print("-" * 75)

    if final_agg_stats["files_analyzed_final"] > 0:
        total_rows = final_agg_stats["total_rows"]
        total_with = final_agg_stats["total_with_sentiment"]
        total_without = final_agg_stats["total_without_sentiment"]
        total_without_na = final_agg_stats["total_without_sentiment_na_text"]

        perc_with_sentiment = (total_with / total_rows) * 100 if total_rows > 0 else 0
        perc_without_sentiment = (
            (total_without / total_rows) * 100 if total_rows > 0 else 0
        )
        perc_without_na_text = (
            (total_without_na / total_without) * 100 if total_without > 0 else 0
        )

        print(
            f"Final State Across {final_agg_stats['files_analyzed_final']} Analyzed Files:"
        )
        if final_agg_stats["files_failed_final_stat_read"] > 0:
            print(
                f"(Note: Failed to read final stats for {final_agg_stats['files_failed_final_stat_read']} files)"
            )
        print(f"Total rows: {total_rows}")
        print(f"Rows with sentiment: {total_with} ({perc_with_sentiment:.2f}%)")
        print(
            f"Rows without sentiment: {total_without} ({perc_without_sentiment:.2f}%)"
        )
        if total_without > 0:
            print(
                f"  - Of which had NA/empty 'Text': {total_without_na} ({perc_without_na_text:.2f}% of rows without sentiment)"
            )
    elif file_count > 0:
        print(
            "No files were successfully processed through both passes to generate a final summary."
        )
    else:
        print("No CSV files were found to process.")

    print("=" * 75)
    print("[INFO] Script complete.")
    # Clean up pipeline and release GPU memory if applicable
    del sentiment_pipeline
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    gc.collect()
