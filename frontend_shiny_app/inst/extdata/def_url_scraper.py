from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import time
import os
import pandas as pd


def url_scraper(target_url):
    """
    Scrapes tweets from a specified Twitter URL.

    Args:
        target_url (str): The URL of the Twitter profile or search results page.

    Returns:
        pd.DataFrame: A DataFrame containing extracted tweet data.
    """
    # Load environment variables from your .env file
    load_dotenv()
    EMAIL = os.getenv("EMAIL_MAIN")
    USERNAME = os.getenv("USERNAME_MAIN")
    PASSWORD = os.getenv("PASSWORD")

    if not EMAIL or not USERNAME or not PASSWORD:
        raise ValueError("Twitter credentials are not set. Check your .env file!")

    # Setup Chrome WebDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)

    try:
        # Open Twitter Login Page
        driver.get("https://x.com/login")

        # Wait for the email/username field
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "text"))
        )

        # Enter Email (first login step)
        username_input = driver.find_element(By.NAME, "text")
        username_input.send_keys(EMAIL)
        username_input.send_keys(Keys.RETURN)

        # Handle an extra login prompt (if present)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.NAME, "text"))
            )
            second_input = driver.find_element(By.NAME, "text")
            second_input.send_keys(USERNAME)
            second_input.send_keys(Keys.RETURN)
        except Exception:
            print("No second login prompt detected, proceeding...")

        # Wait for Password field
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "password"))
        )

        # Enter Password
        password_input = driver.find_element(By.NAME, "password")
        password_input.send_keys(PASSWORD)
        password_input.send_keys(Keys.RETURN)

        # Wait for homepage to load
        time.sleep(5)

        # Navigate to the target Twitter URL
        driver.get(target_url)

        # Wait until tweets are loaded (by waiting for article elements)
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "article"))
        )

        # Initialize an empty DataFrame to store tweet data
        tweet_df = pd.DataFrame(columns=["Tweet URL", "Created At", "Text"])

        # Setup scrolling parameters
        SCROLL_PAUSE_TIME = 10
        prev_tweets_count = 0
        max_wait_time = 120  # Adjust as needed
        start_time = time.time()

        # Scroll until no new tweets load within the allotted time
        while True:
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(SCROLL_PAUSE_TIME)

            # DEBUG: Print current scroll action
            print("DEBUG: Scrolled 500 pixels.")

            # Extract tweets using BeautifulSoup
            soup = BeautifulSoup(driver.page_source, "html.parser")
            tweets = soup.find_all("article")

            # DEBUG: Print number of tweet articles found on the page
            print(f"DEBUG: Found {len(tweets)} tweet articles.")

            new_tweet_data = []
            for tweet in tweets:
                try:
                    # Extract the first <a> tag containing '/status/'
                    tweet_link_tag = tweet.find(
                        "a", href=lambda href: href and "/status/" in href
                    )
                    tweet_url = (
                        f"https://x.com{tweet_link_tag['href']}"
                        if tweet_link_tag
                        else None
                    )

                    # Extract tweet creation time
                    date_tag = tweet.find("time")
                    created_at = date_tag["datetime"] if date_tag else "Unknown"

                    # Extract Tweet ID (if available)
                    tweet_id = tweet.get("data-tweet-id", "Unknown")

                    # Extract engagement metrics
                    likes_tag = tweet.find("div", {"data-testid": "like"})
                    likes = likes_tag.get_text(strip=True) if likes_tag else "Unknown"

                    retweets_tag = tweet.find("div", {"data-testid": "retweet"})
                    retweets = (
                        retweets_tag.get_text(strip=True) if retweets_tag else "Unknown"
                    )

                    replies_tag = tweet.find("div", {"data-testid": "reply"})
                    replies = (
                        replies_tag.get_text(strip=True) if replies_tag else "Unknown"
                    )

                    # Extract hashtags and mentions
                    hashtags = [
                        tag.get_text(strip=True)
                        for tag in tweet.find_all("a")
                        if "#" in tag.get_text()
                    ]
                    mentions = [
                        tag.get_text(strip=True)
                        for tag in tweet.find_all("a")
                        if "@" in tag.get_text()
                    ]

                    # Extract URLs in tweet
                    urls = [
                        tag.get("href", "Unknown")
                        for tag in tweet.find_all("a")
                        if "http" in tag.get("href", "")
                    ]

                    # Extract tweet text
                    tweet_text = (
                        tweet.find("div", {"lang": True}).get_text(strip=True)
                        if tweet.find("div", {"lang": True})
                        else "Unknown"
                    )

                    # Add tweet if it hasn't already been added
                    if tweet_url and tweet_url not in tweet_df["Tweet URL"].values:
                        new_tweet_data.append(
                            {
                                "Tweet URL": tweet_url,
                                "Created At": created_at,
                                "Text": tweet_text,
                                "Tweet ID": tweet_id,
                                "Likes": likes,
                                "Retweets": retweets,
                                "Replies": replies,
                                "Hashtags": ", ".join(hashtags),
                                "Mentions": ", ".join(mentions),
                                "URLs": ", ".join(urls),
                            }
                        )
                except Exception as e:
                    print(f"Error extracting tweet: {e}")
                    continue

            new_tweet_df = pd.DataFrame(new_tweet_data)

            # DEBUG: Print number of new tweets added in this iteration
            print(f"DEBUG: New tweets found this iteration: {len(new_tweet_df)}")

            if not new_tweet_df.empty:
                tweet_df = pd.concat([tweet_df, new_tweet_df], ignore_index=True)

            # DEBUG: Print total tweets collected so far
            print(f"DEBUG: Total tweets collected: {len(tweet_df)}")

            if len(tweet_df) == prev_tweets_count:
                elapsed_time = time.time() - start_time
                if elapsed_time >= max_wait_time:
                    print("No new tweets for the allotted time. Stopping scrolling.")
                    break
            else:
                start_time = time.time()

            prev_tweets_count = len(tweet_df)

        return tweet_df

    finally:
        driver.quit()


# -----------------------------
# Example usage:
# Uncomment the following lines to test the function on a specific Twitter URL:

# test_url = "https://x.com/search?q=(%23cityandguilds)%20until%3A2015-12-31%20since%3A2015-01-01&src=typed_query&f=top"
# tweets = url_scraper(test_url)
