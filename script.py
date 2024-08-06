import os
import sys
import dotenv
import requests
import json
from time import sleep
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from enum import Enum
import argparse

dotenv.load_dotenv()

KEY = os.getenv("EXPORT_COMMENTS_KEY")

HEADERS = {
    "X-AUTH-TOKEN": KEY,
    "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
}

BASE_URL = "https://exportcomments.com"
API_URL = f"{BASE_URL}/api/v2"
MAX_RESULTS = 5000


class SocialNetwork(Enum):
    """The social networks for which the URLs are extracted."""

    # Name of the columns that contain the post URLs
    FACEBOOK = "Unnamed: 6"
    INSTAGRAM = "Unnamed: 20"
    TWITTER = "Unnamed: 12"


def read_urls_from_extraction(file_path:str, social_network: SocialNetwork) -> list[str]:
    """Reads the URLs from the extraction file."""
    
    df = pd.read_excel(file_path)

    #Facebook - Post URL -> Unnamed: 6
    #Twitter - Status URL -> Unnamed: 20
    #Instagram - URL -> Unnamed: 12

    if social_network == SocialNetwork.FACEBOOK:
        return df[social_network.value].dropna().tolist()
    elif social_network == SocialNetwork.TWITTER:
        return df[social_network.value].dropna().tolist()
    elif social_network == SocialNetwork.INSTAGRAM:
        return df[social_network.value].dropna().tolist()


def job_response(guid: str) -> dict:
    """The exported data for the job in a dictionary."""
    
    response = requests.get(
        f"{API_URL}/export", 
        headers=HEADERS, 
        params={"guid": guid}, 
        timeout=60
        )
    return response.json()


def job_status(guid: str) -> str:
    """Checks status of the job with the given guid"""

    response = job_response(guid)
    data = response["data"]
    
    try:
        return data[0]["status"]
    except KeyError:
        raise ValueError(f"'status' or index 0 not found in data: {data}")


def get_response(guid: str):
    """Gets the response for the job with the given guid."""
    
    while True:
        status = job_status(guid)
    
        if status == 'done':
            break
        elif status == 'error':
            print(f"Error processing job {guid}. Status: {status}")
            error = job_response(guid)["data"][0]["error"]
            print(f"Error: {error}")
            break
            #sys.exit()

        print(f"Status: {status}. Waiting 20 seconds to check again.")
        sleep(20)

def raw_url(guid: str) -> str:
    """Gets the url for the download of the raw exported data.
    This reponse is relative to a base URL"""

    response = job_response(guid)
    data = response["data"]
    
    try:
        return data[0]["rawUrl"]
    except KeyError:
        raise ValueError(f"'rawUrl' or index 0 not found in data: {data}")


def xlsl_url(guid: str) -> str:
    """Gets the url for the download of the raw exported data.
    This reponse is relative to a base URL"""

    response = job_response(guid)
    data = response["data"]
    
    try:
        return data[0]["downloadUrl"]
    except KeyError:
        raise ValueError(f"'downloadUrl' or index 0 not found in data: {data}")


def download_raw(raw_url: str) -> list[dict]:
    """Downloads the raw results as JSON from the given URL"""

    response = requests.get(f"{BASE_URL}{raw_url}", headers=HEADERS, timeout=60)

    if response.status_code == 200:
        return response.json()
    raise ValueError(f"[FAILED TO DOWNLOAD] Status Code: {response.status_code}")


def download_xlsl(download_url: str) -> bytes:
    """Downloads the raw results as XLSX from the given URL"""
    response = requests.get(f"{BASE_URL}{download_url}", headers=HEADERS, timeout=60)

    if response.status_code == 200:
        return response.content
    raise ValueError(f"[FAILED TO DOWNLOAD] Status Code: {response.status_code}")


def to_csv(data: list[dict], file_path: str):
    """Writes the data to a CSV file."""
    
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print(f"Data written to {file_path}")


def start_job(url: str) -> Optional[str]:
    """Starts the job and returns the guid. Retries if the rate limit is exceeded."""

    MAX_RETRIES = 3
    PARAMS = {
        "url": url,
        "options": json.dumps({
            "limit": MAX_RESULTS,
        })
    } 
    
    for _ in range(MAX_RETRIES):
        try:
            response = requests.put(
                f"{API_URL}/export",
                headers=HEADERS,
                timeout=30,
                params=PARAMS
            )
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            raise e

        if response.status_code != 200:
            raise ValueError(f"Failed to start job: {response.text}")

        response_data = response.json()
        status_code = response_data["data"].get("status_code")
        
        if status_code == 429:
            seconds_to_wait = response_data["data"]["seconds_to_wait"]
            print(f"Rate limit exceeded. Waiting for {seconds_to_wait} seconds before retrying...")
            sleep(seconds_to_wait)
            continue

        guid = response_data["data"].get("guid")
        job_status = response_data["data"].get("status")
        
        if job_status is None:
            print(f"Job started but no status returned. Response: {response_data}")
            print("Retrying...")
            continue

        print(f"Job {guid} started. Status: {job_status}, URL: {url}")
        

        return guid

    print(f"Failed to start job after {MAX_RETRIES} attempts. URL: {url}")
    return None


def process_job(url: str) -> pd.DataFrame:
    """Processes the job and returns the data as a DataFrame."""
    
    guid = start_job(url)
    if guid:
        get_response(guid)
        response = download_xlsl(download_url=xlsl_url(guid))
        return pd.read_excel(response)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Extracts comments from post URLs extracted by ExportComments",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    
    parser.add_argument(
        "file_path",
        type=str,
        help="Extraction file path",
    )
    
    args = parser.parse_args()
    urls = read_urls_from_extraction(args.file_path, SocialNetwork.FACEBOOK)
    
    all_data = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_job, url) for url in urls]
        for future in as_completed(futures):
            data = future.result()
            if data is not None:
                all_data.append(data)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_excel(f"coleta_comentarios.xlsx", index=False)
        print(f"Feedback collection completed. Data saved in colega_comentarios.xlsx")