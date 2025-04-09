# -------------------------------------------
# Offline Hugging Face Sentiment Script (Final)
# -------------------------------------------
# 1) Uses an absolute Path to avoid HFValidationError.
# 2) Loads local files ONLY (no SSL/download issues).
# 3) Processes CSVs and adds sentiment column directly in the output folder.
# 4) Prints iteration progress.

import os
import pandas as pd
import torch
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

# ---------------------------------
# Step 1: Build Offline Pipeline
# ---------------------------------


def get_offline_pipeline(model_folder: Path):
    """
    Loads a local PyTorch model & tokenizer from 'model_folder', strictly offline.
    Requires merges.txt, vocab.json, config.json, pytorch_model.bin, etc.
    """
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[INFO] Using device: {device}\n")

    # Convert Path to string for 'from_pretrained'
    model_str = str(model_folder)

    print("[INFO] Loading local tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(model_str, local_files_only=True)

    print("[INFO] Loading local PyTorch model...")
    model = AutoModelForSequenceClassification.from_pretrained(
        model_str, local_files_only=True, from_tf=False
    )

    print("[INFO] Building local sentiment pipeline...\n")
    sentiment_pipe = pipeline(
        "text-classification",
        model=model,
        tokenizer=tokenizer,
        device=0 if device == "cuda" else -1,
    )

    return sentiment_pipe


# -------------------------------------------
# Step 2: Safe Sentiment Analysis
# -------------------------------------------


def safe_sentiment_analysis(pipe, text_list):
    """
    Run sentiment analysis with the pipeline, handling NaN/NULL.

    Returns 'unknown' if the text entry is invalid or empty.
    """
    valid_texts = [
        str(t) if pd.notna(t) and t not in {"unknown", "NULL", "NA"} else ""
        for t in text_list
    ]

    results = pipe(valid_texts)
    labels = [r["label"] if txt else "unknown" for txt, r in zip(valid_texts, results)]
    return labels


# -------------------------------------------
# Step 3: Main Script
# -------------------------------------------
if __name__ == "__main__":
    # 1) Update 'model_folder' with your real absolute path
    model_folder = Path(
        r"C:\Users\TomHun\OneDrive - City & Guilds\Documents\Code\R\vibe_check\backend_x_scraper\twitter-roberta-base-sentiment-latest"
    )

    # 2) Build offline pipeline
    sentiment_pipeline = get_offline_pipeline(model_folder)

    # 3) Configure input/output directory - now only using /output
    output_dir = "../output/"  # CSV files path

    # Optional ignore set
    ignore_files = {"urls.csv", "log.txt"}

    # 4) Gather CSV files
    csv_files = [
        f
        for f in os.listdir(output_dir)
        if f.endswith(".csv") and f not in ignore_files
    ]

    # 5) Process each CSV
    for idx, filename in enumerate(csv_files, start=1):
        print(f"Processing file {idx}/{len(csv_files)}: {filename}")

        filepath = os.path.join(output_dir, filename)
        df = pd.read_csv(filepath)

        # Ensure 'Text' column is present
        if "Text" not in df.columns:
            print(f"  -> Skipped {filename}: No 'Text' column")
            continue

        # Check if sentiment column already exists, if not add it
        if "sentiment" not in df.columns:
            # Perform sentiment analysis
            df["sentiment"] = safe_sentiment_analysis(
                sentiment_pipeline, df["Text"].tolist()
            )

            # Save results back to the original file
            df.to_csv(filepath, index=False)
            print(f"  -> Added sentiment analysis to {filename}\n")
        else:
            # Update only rows without sentiment values
            mask_new = df["sentiment"].isna()
            num_new = mask_new.sum()

            if num_new > 0:
                print(f"  -> Found {num_new} rows in {filename} without sentiment")
                df.loc[mask_new, "sentiment"] = safe_sentiment_analysis(
                    sentiment_pipeline, df.loc[mask_new, "Text"].tolist()
                )
                df.to_csv(filepath, index=False)
                print(f"  -> Updated sentiment for {num_new} rows in {filename}\n")
            else:
                print(f"  -> All rows in {filename} already have sentiment values\n")

    print("All CSV files processed successfully.")
