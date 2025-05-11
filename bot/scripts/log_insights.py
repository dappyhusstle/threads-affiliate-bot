# /bot/scripts/log_insights.py

import os
import requests
from dotenv import load_dotenv
import sys
import json
import gspread
from google.oauth2.service_account import Credentials
import datetime

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Base URL for the Threads API as per documentation
THREADS_API_BASE_URL = "https://graph.threads.net/v1.0/"

# Google Sheets Configuration
# Path to your Google Sheets service account JSON key file
GOOGLE_SHEETS_CREDENTIALS_PATH = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
if not GOOGLE_SHEETS_CREDENTIALS_PATH or not os.path.exists(GOOGLE_SHEETS_CREDENTIALS_PATH):
    print("Error: Google Sheets credentials path not set or file not found.")
    print("Please set the 'GOOGLE_SHEETS_CREDENTIALS_PATH' environment variable to the path of your service account JSON key file.")
    sys.exit(1)

# The URL or ID of your Google Sheet
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
if not GOOGLE_SHEET_URL:
    print("Error: Google Sheet URL not set.")
    print("Please set the 'GOOGLE_SHEET_URL' environment variable.")
    sys.exit(1)

# The name of the worksheet where logs will be stored
LOGS_WORKSHEET_NAME = os.getenv("LOGS_WORKSHEET_NAME", "Post_Logs") # Default worksheet name

# Get account-specific access token from environment variables
# Environment variable format: THREADS_ACCESS_TOKEN_<ACCOUNT_NAME>
# The script expects the account name, post ID, and original post content as command-line arguments.
if len(sys.argv) < 4:
    print("Usage: python log_insights.py <account_name> <post_id> <post_content>")
    sys.exit(1)

ACCOUNT_NAME = sys.argv[1]
POST_ID = sys.argv[2] # This is the Threads Media ID from the publish step
POST_CONTENT = sys.argv[3] # Original content for logging

# Construct the environment variable name for the access token
ACCESS_TOKEN_ENV_VAR = f"THREADS_ACCESS_TOKEN_{ACCOUNT_NAME.upper()}"
THREADS_ACCESS_TOKEN = os.getenv(ACCESS_TOKEN_ENV_VAR)

if not THREADS_ACCESS_TOKEN:
    print(f"Error: Threads access token not found for account '{ACCOUNT_NAME}'. "
          f"Please set the '{ACCESS_TOKEN_ENV_VAR}' environment variable.")
    sys.exit(1)

# --- Google Sheets Interaction ---

def get_google_sheet_client():
    """Authenticates and returns a gspread client."""
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = Credentials.from_service_account_file(
            GOOGLE_SHEETS_CREDENTIALS_PATH, scopes=scopes
        )
        client = gspread.authorize(credentials)
        print("Google Sheets client authenticated.")
        return client
    except Exception as e:
        print(f"Error authenticating with Google Sheets: {e}")
        return None

def open_google_sheet(client, sheet_url):
    """Opens the specified Google Sheet."""
    try:
        sheet = client.open_by_url(sheet_url)
        print(f"Opened Google Sheet: {sheet_url}")
        return sheet
    except Exception as e:
        print(f"Error opening Google Sheet '{sheet_url}': {e}")
        return None

def get_or_create_worksheet(sheet, worksheet_name):
    """Gets a worksheet by name, creating it if it doesn't exist."""
    try:
        worksheet = sheet.worksheet(worksheet_name)
        print(f"Found worksheet: {worksheet_name}")
        return worksheet
    except gspread.WorksheetNotFound:
        print(f"Worksheet '{worksheet_name}' not found. Creating it...")
        worksheet = sheet.add_worksheet(title=worksheet_name, rows="100", cols="20")
        # Add headers if it's a new sheet
        headers = ["Post_ID", "Account", "Post_Content", "Views", "Likes", "Replies", "Date_Posted", "Time_Posted"]
        worksheet.append_row(headers)
        print(f"Created worksheet '{worksheet_name}' with headers.")
        return worksheet
    except Exception as e:
        print(f"Error getting or creating worksheet '{worksheet_name}': {e}")
        return None


def log_to_google_sheet(worksheet, data_row):
    """Appends a row of data to the Google Sheet worksheet."""
    try:
        worksheet.append_row(data_row)
        print("Data successfully logged to Google Sheet.")
    except Exception as e:
        print(f"Error logging data to Google Sheet: {e}")
        # Implement error handling (e.g., retry, notification)

# --- Threads API Interaction (for Insights) ---

def get_post_insights(media_id: str, access_token: str):
    """
    Fetches insights (views, likes, replies) for a specific post from the Threads API.

    Args:
        media_id: The Threads Media ID of the post to get insights for.
        access_token: The access token for the account that posted.

    Returns:
        dict: A dictionary containing insights (e.g., {'views': 100, 'likes': 10, 'replies': 2}),
              or None if insights could not be fetched.
    """
    print(f"Attempting to fetch insights for Media ID: {media_id}")

    endpoint = f"{THREADS_API_BASE_URL}{media_id}/insights"
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    # Specify the metrics we want
    params = {
        "metric": "views,likes,replies"
    }

    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        insights_data = response.json()

        # Parse the response to extract the metric values
        parsed_insights = {}
        if "data" in insights_data:
            for metric_data in insights_data["data"]:
                name = metric_data.get("name")
                # For 'views' which is time series, get the last value
                if name == "views" and "values" in metric_data and metric_data["values"]:
                     # The documentation shows 'values' is a list for time series, take the last value
                     parsed_insights[name] = metric_data["values"][-1].get("value", 0)
                # For 'likes' and 'replies' which are total value, get the total_value
                elif name in ["likes", "replies"] and "total_value" in metric_data:
                     parsed_insights[name] = metric_data["total_value"].get("value", 0)
                # Fallback if total_value is not present but values is (check documentation if needed)
                elif name in ["likes", "replies"] and "values" in metric_data and metric_data["values"]:
                     # If total_value is not available, sum up values if it's a list (less likely for lifetime)
                     parsed_insights[name] = sum(item.get("value", 0) for item in metric_data["values"])


        print(f"Fetched Insights: {parsed_insights}")
        return parsed_insights

    except requests.exceptions.RequestException as e:
        print(f"Error fetching insights for Media ID {media_id}: {e}")
        # Implement error handling
        return None
    except Exception as e:
        print(f"An unexpected error occurred while fetching insights: {e}")
        return None


# --- Main execution ---
if __name__ == "__main__":
    # The account name, post ID, and original post content are passed as command-line arguments.
    # In an n8n workflow, the output from post_to_threads.py (containing post_id) and the original content
    # would be passed to this script.

    if len(sys.argv) < 4:
        print("Error: Account name, post ID (Threads Media ID), and post content must be provided as command-line arguments.")
        sys.exit(1)

    account_name_arg = sys.argv[1]
    post_id_arg = sys.argv[2] # This is the Threads Media ID
    post_content_arg = sys.argv[3]

    # Reload access token based on the provided account name argument
    ACCESS_TOKEN_ENV_VAR_ARG = f"THREADS_ACCESS_TOKEN_{account_name_arg.upper()}"
    threads_access_token_arg = os.getenv(ACCESS_TOKEN_ENV_VAR_ARG)

    if not threads_access_token_arg:
        print(f"Error: Threads access token not found for account '{account_name_arg}'. "
              f"Please set the '{ACCESS_TOKEN_ENV_VAR_ARG}' environment variable.")
        sys.exit(1)

    # 1. Fetch Insights
    # The insights endpoint uses the Threads Media ID directly, not the user ID in the path.
    insights = get_post_insights(post_id_arg, threads_access_token_arg)

    if insights:
        # 2. Prepare data for Google Sheets
        now = datetime.datetime.now()
        date_posted = now.strftime("%Y-%m-%d")
        time_posted = now.strftime("%H:%M:%S")

        data_row = [
            post_id_arg,
            account_name_arg,
            post_content_arg,
            insights.get("views", 0), # Default to 0 if key not found
            insights.get("likes", 0),
            insights.get("replies", 0),
            date_posted,
            time_posted
        ]

        # 3. Log to Google Sheets
        gs_client = get_google_sheet_client()
        if gs_client:
            sheet = open_google_sheet(gs_client, GOOGLE_SHEET_URL)
            if sheet:
                worksheet = get_or_create_worksheet(sheet, LOGS_WORKSHEET_NAME)
                if worksheet:
                    log_to_google_sheet(worksheet, data_row)
                    sys.exit(0) # Indicate success to n8n
                else:
                     print("Failed to get or create Google Sheet worksheet.")
                     sys.exit(1) # Indicate failure to n8n
            else:
                print("Failed to open Google Sheet.")
                sys.exit(1) # Indicate failure to n8n
        else:
            print("Failed to authenticate with Google Sheets.")
            sys.exit(1) # Indicate failure to n8n
    else:
        print(f"Failed to fetch insights for Post ID {post_id_arg}. Skipping logging.")
        sys.exit(1) # Indicate failure to n8n

