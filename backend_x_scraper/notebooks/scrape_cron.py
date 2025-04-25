import os
import time
from datetime import datetime, timedelta
import pandas as pd
from def_process_year import process_year  # Your existing function

# log the scheduler
file = open(
    r"C:\Users\TomHun\OneDrive - City & Guilds\Documents\Code\Python\monitor\output\log.txt",
    "a",
)
file.write(f"{datetime.now()} - Scheduler started\n")


def get_previous_week_range():
    """
    Computes the date window for the previous week.
    Returns:
        tuple: (start_date, end_date) formatted as 'YYYY-MM-DD'
    """
    today = datetime.now().date()
    end_date = today - timedelta(days=1)  # yesterday
    start_date = end_date - timedelta(days=14)  # 6 days before yesterday
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def construct_url(search_type, start_date, end_date):
    """
    Constructs a Twitter search URL for the given search type and date window.
    The URL uses the pre-defined base (cityandguilds).

    Args:
        search_type (str): One of "hashtag", "from", "mention", "to",
                           "phrase", "phrase_amp", or "phrase_plus".
        start_date (str): Date in 'YYYY-MM-DD' format.
        end_date (str): Date in 'YYYY-MM-DD' format.

    Returns:
        str: The constructed URL.
    """
    base = "cityandguilds"
    if search_type == "hashtag":
        url = f"https://x.com/search?q=(%23{base})%20until%3A{end_date}%20since%3A{start_date}&src=typed_query&f=top"
    elif search_type == "from":
        url = f"https://x.com/search?q=(from%3A{base})%20until%3A{end_date}%20since%3A{start_date}&src=typed_query&f=top"
    elif search_type == "mention":
        url = f"https://x.com/search?q=(%40{base})%20until%3A{end_date}%20since%3A{start_date}&src=typed_query&f=top"
    elif search_type == "to":
        url = f"https://x.com/search?q=(to%3A{base})%20until%3A{end_date}%20since%3A{start_date}&src=typed_query&f=top"
    elif search_type == "phrase":
        url = f"https://x.com/search?q=%22city%20and%20guilds%22%20until%3A{end_date}%20since%3A{start_date}&src=typed_query&f=top"
    elif search_type == "phrase_amp":
        url = f"https://x.com/search?q=%22city%20%26%20guilds%22%20until%3A{end_date}%20since%3A{start_date}&src=typed_query&f=top"
    elif search_type == "phrase_plus":
        url = f"https://x.com/search?q=%22city%20%2B%20guilds%22%20until%3A{end_date}%20since%3A{start_date}&src=typed_query&f=top"
    else:
        url = ""
    return url


def update_lookup_csv():
    """
    Builds a dynamic lookup of search tags and URLs using the previous week's date range,
    and saves it to the lookup CSV file used by process_year.
    """
    start_date, end_date = get_previous_week_range()
    current_year = str(datetime.now().year)
    search_types = [
        "hashtag",
        "from",
        "mention",
        "to",
        "phrase",
        "phrase_amp",
        "phrase_plus",
    ]

    search_dict = {}
    for st in search_types:
        url = construct_url(st, start_date, end_date)
        # The tag will be like: to_2025, phrase_amp_2025, etc.
        tag = f"{st}_{current_year}"
        search_dict[tag] = url

    # Create a DataFrame with the lookup information
    df = pd.DataFrame(list(search_dict.items()), columns=["tag", "url"])
    df["env_suffix"] = current_year  # This column is used by process_year

    # Determine the output directory (assumed to be at the workspace root)
    try:
        workspace_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    except NameError:
        workspace_root = os.getcwd()
    output_dir = os.path.join(workspace_root, "output")
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "urls.csv")
    df.to_csv(output_path, index=False)

    print("Lookup CSV updated:")
    print(df)
    return output_path


if __name__ == "__main__":
    # Testing functions
    print("Testing get_previous_week_range()")
    start_date, end_date = get_previous_week_range()
    print("Previous week range:", start_date, "to", end_date)

    print("\nTesting construct_url()")
    test_search_types = [
        "hashtag",
        "from",
        "mention",
        "to",
        "phrase",
        "phrase_amp",
        "phrase_plus",
        "invalid",  # test for an unsupported search type
    ]
    for st in test_search_types:
        url = construct_url(st, start_date, end_date)
        print(f"Search type: {st} -> URL: {url}")

    print("\nTesting update_lookup_csv()")
    output_csv = update_lookup_csv()

    # Read back the CSV file and display its contents
    if os.path.exists(output_csv):
        df_test = pd.read_csv(output_csv)
        print("\nContent of urls.csv:")
        print(df_test)
    else:
        print("urls.csv not found.")

    # Automatically proceed to main loop
    runtime_seconds = 2 * 3600  # Run for 3 hours (in seconds)
    script_start = time.time()
    current_year = str(datetime.now().year)

    while time.time() - script_start < runtime_seconds:
        print(f"\nRunning process_year for env_suffix {current_year}...")
        try:
            process_year(current_year)
        except Exception as e:
            print(f"Error in process_year: {e}")
        print("Sleeping for 10 minutes before next iteration...")
        time.sleep(600)
