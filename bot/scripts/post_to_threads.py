# /bot/scripts/post_to_threads.py

import os
import requests
from dotenv import load_dotenv
import sys
import time # Import time for delay
import json # Import json for output
import gspread
from google.oauth2.service_account import Credentials

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

# The name of the worksheet containing posts ready to be posted
READY_TO_POST_WORKSHEET_NAME = os.getenv("READY_TO_POST_WORKSHEET_NAME", "Ready_To_Post") # Default worksheet name

# Get account-specific access token and user ID from environment variables
# Environment variable format: THREADS_ACCESS_TOKEN_<ACCOUNT_NAME>
# Environment variable format: THREADS_USER_ID_<ACCOUNT_NAME>
# The script expects the account name and the Post_ID from the Google Sheet row as command-line arguments.
if len(sys.argv) < 3:
    print("Usage: python post_to_threads.py <account_name> <sheet_post_id>")
    sys.exit(1)

ACCOUNT_NAME = sys.argv[1]
SHEET_POST_ID = sys.argv[2] # The unique ID from the Google Sheet row

# Construct the environment variable names for the access token and user ID
ACCESS_TOKEN_ENV_VAR = f"THREADS_ACCESS_TOKEN_{ACCOUNT_NAME.upper()}"
USER_ID_ENV_VAR = f"THREADS_USER_ID_{ACCOUNT_NAME.upper()}"

THREADS_ACCESS_TOKEN = os.getenv(ACCESS_TOKEN_ENV_VAR)
THREADS_USER_ID = os.getenv(USER_ID_ENV_VAR)

if not THREADS_ACCESS_TOKEN:
    print(f"Error: Threads access token not found for account '{ACCOUNT_NAME}'. "
          f"Please set the '{ACCESS_TOKEN_ENV_VAR}' environment variable.")
    sys.exit(1)

if not THREADS_USER_ID:
    print(f"Error: Threads user ID not found for account '{ACCOUNT_NAME}'. "
          f"Please set the '{USER_ID_ENV_VAR}' environment variable.")
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

def get_worksheet(sheet, worksheet_name):
    """Gets a worksheet by name."""
    try:
        worksheet = sheet.worksheet(worksheet_name)
        print(f"Found worksheet: {worksheet_name}")
        return worksheet
    except gspread.WorksheetNotFound:
        print(f"Worksheet '{worksheet_name}' not found.")
        return None
    except Exception as e:
        print(f"Error getting worksheet '{worksheet_name}': {e}")
        return None

def get_post_by_id(worksheet, post_id):
    """Finds and returns a post row from the worksheet by its Post_ID."""
    try:
        # Get all records from the worksheet
        records = worksheet.get_all_records()
        # Find the row with the matching Post_ID
        for row in records:
            if row.get("Post_ID") == post_id:
                print(f"Found post with ID: {post_id}")
                return row
        print(f"Post with ID '{post_id}' not found in the sheet.")
        return None
    except Exception as e:
        print(f"Error reading from Google Sheet: {e}")
        return None

def update_post_status(worksheet, post_id, status, threads_post_id=None):
    """Updates the status of a post in the Google Sheet."""
    try:
        # Find the cell with the matching Post_ID in the first column
        cell = worksheet.find(post_id, in_column=1) # Assuming Post_ID is in the first column (A)
        if cell:
            row_index = cell.row
            # Find the column index for 'Status' and 'Threads_Post_ID' dynamically
            headers = worksheet.row_values(1) # Get header row
            try:
                status_col_index = headers.index("Status") + 1 # +1 because gspread is 1-indexed
                # Check if Threads_Post_ID column exists before trying to find its index
                threads_post_id_col_index = headers.index("Threads_Post_ID") + 1 if "Threads_Post_ID" in headers else None
            except ValueError:
                print("Error: 'Status' column not found in the sheet headers.")
                return False # Indicate failure if Status column is missing

            updates = [(row_index, status_col_index, status)]
            if threads_post_id and threads_post_id_col_index:
                updates.append((row_index, threads_post_id_col_index, threads_post_id))

            worksheet.batch_update(updates)
            print(f"Updated status for Post ID {post_id} to '{status}'.")
            return True
        else:
            print(f"Could not find Post ID '{post_id}' in the sheet to update status.")
            return False
    except Exception as e:
        print(f"Error updating Google Sheet status for Post ID {post_id}: {e}")
        return False


# --- Threads API Interaction ---

def create_media_container(user_id: str, text_content: str, access_token: str, reply_to_id: str = None):
    """
    Creates a Threads media container for a text-only post or reply.

    Args:
        user_id: The Threads user ID.
        text_content: The text content of the post/reply.
        access_token: The access token for the Threads account.
        reply_to_id: The ID of the post/reply this is a reply to (for chained posts).

    Returns:
        str: The media container ID if successful, None otherwise.
    """
    print(f"Creating media container for user ID: {user_id}")
    if reply_to_id:
        print(f"  Replying to ID: {reply_to_id}")

    endpoint = f"{THREADS_API_BASE_URL}{user_id}/threads"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json" # Use application/json for POST body
    }
    # Use data parameter for form-urlencoded as shown in documentation examples
    payload = {
        "media_type": "TEXT",
        "text": text_content,
        # Removed link_attachment parameter
    }
    if reply_to_id:
        payload["reply_to_id"] = reply_to_id


    try:
        response = requests.post(endpoint, headers=headers, data=payload)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        response_data = response.json()
        container_id = response_data.get("id")
        if container_id:
            print(f"Media container created successfully. Container ID: {container_id}")
            return container_id
        else:
            print(f"Error: 'id' not found in container creation response: {response_data}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error creating media container: {e}")
        # Implement more sophisticated error handling (e.g., retry logic)
        return None
    except Exception as e:
        print(f"An unexpected error occurred during container creation: {e}")
        return None

def publish_media_container(user_id: str, creation_id: str, access_token: str):
    """
    Publishes a Threads media container.

    Args:
        user_id: The Threads user ID.
        creation_id: The ID of the media container to publish.
        access_token: The access token for the Threads account.

    Returns:
        str: The published post ID (Threads Media ID) if successful, None otherwise.
    """
    print(f"Publishing media container with creation ID: {creation_id}")

    endpoint = f"{THREADS_API_BASE_URL}{user_id}/threads_publish"
    headers = {
        "Authorization": f"Bearer {access_token}",
        # Content-Type is not strictly needed for this POST with data payload
    }
    # Use data parameter for form-urlencoded as shown in documentation examples
    payload = {
        "creation_id": creation_id
    }

    try:
        response = requests.post(endpoint, headers=headers, data=payload)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        response_data = response.json()
        post_id = response_data.get("id")
        if post_id:
            print(f"Post published successfully. Post ID (Threads Media ID): {post_id}")
            return post_id
        else:
            print(f"Error: 'id' not found in publish response: {response_data}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error publishing media container: {e}")
        # Implement more sophisticated error handling
        return None
    except Exception as e:
        print(f"An unexpected error occurred during publishing: {e}")
        return None

# --- Main execution ---
if __name__ == "__main__":
    # The account name and the Sheet_Post_ID are passed as command-line arguments by n8n.

    if len(sys.argv) < 3:
        print("Error: Account name and Sheet_Post_ID must be provided as command-line arguments.")
        sys.exit(1)

    account_name_arg = sys.argv[1]
    sheet_post_id_arg = sys.argv[2]

    # Reload access token and user ID based on the provided account name argument
    ACCESS_TOKEN_ENV_VAR_ARG = f"THREADS_ACCESS_TOKEN_{account_name_arg.upper()}"
    USER_ID_ENV_VAR_ARG = f"THREADS_USER_ID_{account_name_arg.upper()}"

    threads_access_token_arg = os.getenv(ACCESS_TOKEN_ENV_VAR_ARG)
    threads_user_id_arg = os.getenv(USER_ID_ENV_VAR_ARG)

    if not threads_access_token_arg:
        print(f"Error: Threads access token not found for account '{account_name_arg}'. "
              f"Please set the '{ACCESS_TOKEN_ENV_VAR_ARG}' environment variable.")
        sys.exit(1)

    if not threads_user_id_arg:
        print(f"Error: Threads user ID not found for account '{account_name_arg}'. "
              f"Please set the '{USER_ID_ENV_VAR_ARG}' environment variable.")
        sys.exit(1)

    # --- Read post content from Google Sheet ---
    gs_client = get_google_sheet_client()
    if not gs_client:
        sys.exit(1)

    sheet = open_google_sheet(gs_client, GOOGLE_SHEET_URL)
    if not sheet:
        sys.exit(1)

    ready_to_post_worksheet = get_worksheet(sheet, READY_TO_POST_WORKSHEET_NAME)
    if not ready_to_post_worksheet:
        print(f"Error: Ready to post worksheet '{READY_TO_POST_WORKSHEET_NAME}' not found.")
        sys.exit(1)

    post_data = get_post_by_id(ready_to_post_worksheet, sheet_post_id_arg)
    if not post_data:
        print(f"Error: Post with Sheet_Post_ID '{sheet_post_id_arg}' not found in the '{READY_TO_POST_WORKSHEET_NAME}' worksheet.")
        sys.exit(1)

    # Extract block content from the sheet data
    # Use a list comprehension to get blocks and filter out empty ones
    post_blocks_arg = [
        post_data.get(f"Block_{i}_Content", "").strip()
        for i in range(1, 5) # Assuming Block_1_Content to Block_4_Content
    ]
    post_blocks_arg = [block for block in post_blocks_arg if block]


    if not post_blocks_arg:
        print(f"Error: No valid block content found for Post ID '{sheet_post_id_arg}'.")
        # Optionally update status in sheet to 'Error'
        update_post_status(ready_to_post_worksheet, sheet_post_id_arg, "Error")
        sys.exit(1)

    print(f"Read {len(post_blocks_arg)} blocks from sheet for Post ID: {sheet_post_id_arg}")

    # --- Posting the chain ---
    last_post_id = None # To keep track of the previous post's ID for replies
    root_post_id = None # To store the ID of the very first post
    full_post_content_for_logging = "\n\n".join(post_blocks_arg) # Combine for the log sheet

    # Update status in sheet to 'Posting'
    update_post_status(ready_to_post_worksheet, sheet_post_id_arg, "Posting")


    for i, block_content in enumerate(post_blocks_arg):
        print(f"\nProcessing Block {i + 1}/{len(post_blocks_arg)}")

        current_reply_to_id = last_post_id

        # Optional: Add a character count check (though GPT is instructed to stay under 500)
        if len(block_content) > 500:
            print(f"Warning: Block {i + 1} exceeds 500 characters ({len(block_content)}). It might be truncated by the API.")
            # You might want to handle this more robustly, e.g., log as error, skip post.


        # Step 1: Create Media Container for the current block
        container_id = create_media_container(
            user_id=threads_user_id_arg,
            text_content=block_content, # Use the block content directly
            access_token=threads_access_token_arg,
            reply_to_id=current_reply_to_id # This will be None for the first block
            # Removed link_attachment parameter
        )

        if container_id:
            # Step 2: Publish Media Container
            # Recommended delay before publishing subsequent posts/replies
            publish_delay = 30 if i == 0 else 10 # Shorter delay for replies
            print(f"Waiting {publish_delay} seconds before publishing...")
            time.sleep(publish_delay)

            published_post_id = publish_media_container(
                user_id=threads_user_id_arg,
                creation_id=container_id,
                access_token=threads_access_token_arg
            )

            if published_post_id:
                last_post_id = published_post_id # Update last_post_id for the next iteration
                if i == 0:
                    root_post_id = published_post_id # Store the root post ID

                print(f"Block {i + 1} published successfully. Published ID: {published_post_id}")
            else:
                print(f"Block {i + 1} publishing failed.")
                # Update status in sheet to 'Error'
                update_post_status(ready_to_post_worksheet, sheet_post_id_arg, "Error")
                sys.exit(1) # Indicate failure to n8n
        else:
            print(f"Block {i + 1} media container creation failed.")
            # Update status in sheet to 'Error'
            update_post_status(ready_to_post_worksheet, sheet_post_id_arg, "Error")
            sys.exit(1) # Indicate failure to n8n

    # --- Chain posting complete ---
    if root_post_id:
        print("\nFull thread chain posted successfully!")
        # Update status in sheet to 'Posted' and add the Threads_Post_ID
        update_post_status(ready_to_post_worksheet, sheet_post_id_arg, "Posted", root_post_id)

        # Output the root_post_id, account_name, and original_full_content as JSON for n8n to parse
        # The log_insights script will use this root_post_id to fetch insights for the entire thread.
        # Pass the combined full content for easier logging later.
        output_data = {
            "threads_post_id": root_post_id, # Use threads_post_id key to match sheet
            "account_name": account_name_arg,
            "full_post_content": full_post_content_for_logging # Pass the combined content
        }
        print(json.dumps(output_data))
        sys.exit(0) # Indicate success to n8n
    else:
        print("\nFailed to post the full thread chain.")
        # Status should already be 'Error' from failures within the loop, but double-check
        # update_post_status(ready_to_post_worksheet, sheet_post_id_arg, "Error") # Redundant if handled in loop
        sys.exit(1) # Indicate failure to n8n







