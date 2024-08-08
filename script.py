import os
import sys
import requests
import json
from time import sleep
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from py_mini_racer import py_mini_racer
from typing import Optional
from enum import Enum
import argparse


try:
    ctx = py_mini_racer.MiniRacer()
    with open('config.js', 'r') as f:
        config_js = f.read()
    config = ctx.execute(config_js)
    KEY = config['EXPORT_COMMENTS_KEY']
except (FileNotFoundError, KeyError, py_mini_racer.JSEvalException) as e:
    print(f"Error loading configuration: {e}")
    sys.exit(1)

BASE_URL = "https://exportcomments.com"
API_URL = f"{BASE_URL}/api/v2"
MAX_RESULTS = 5000
HEADERS = {
    "X-AUTH-TOKEN": KEY,
    "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
}


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
        # mensagem em português
        raise ValueError(f"'status' ou índice 0 não encontrado nos dados: {data}")


def get_response(guid: str):
    """Gets the response for the job with the given guid."""
    time_sleep = 30
    while True:
        status = job_status(guid)
    
        if status == 'done':
            break
        elif status == 'error':
            print(f"Erro ao processar a coleta {guid}. Status: {status}")
            error = job_response(guid)["data"][0]["error"]
            print(f"Error: {error}")
            break
            #sys.exit()

        print(f"Status da coleta {guid}: {status}. Aguardando {time_sleep} segundos para verificar novamente...")
        sleep(time_sleep)

def raw_url(guid: str) -> str:
    """Gets the url for the download of the raw exported data.
    This reponse is relative to a base URL"""

    response = job_response(guid)
    data = response["data"]
    
    try:
        return data[0]["rawUrl"]
    except KeyError:
        raise ValueError(f"'rawUrl' ou índice 0 não encontrado nos dados: {data}")


def xlsl_url(guid: str) -> str:
    """Gets the url for the download of the raw exported data.
    This reponse is relative to a base URL"""

    response = job_response(guid)
    data = response["data"]
    
    try:
        return data[0]["downloadUrl"]
    except KeyError:
        raise ValueError(f"'downloadUrl' ou índice 0 não encontrado nos dados: {data}")


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
    
    try:
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
    except Exception as e:
        print(f"Erro ao salvar arquivo: {e}")


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
            raise ValueError(f"Falha ao iniciar a coleta: {response.text}")

        response_data = response.json()
        status_code = response_data["data"].get("status_code")
        
        if status_code == 429:
            seconds_to_wait = response_data["data"]["seconds_to_wait"]
            print(f"Limite de taxa excedido. Aguardando {seconds_to_wait} segundos antes de tentar novamente...")
            sleep(seconds_to_wait)
            continue

        guid = response_data["data"].get("guid")
        job_status = response_data["data"].get("status")
        
        if job_status is None:
            print(f"O trabalho foi iniciado, mas nenhum status foi retornado. Resposta: {response_data}")
            print("Tentando novamente...")
            continue

        print(f"Iniciando coleta do post {url} . Status: {job_status}. ID: {guid}")

        return guid

    print(f"Falha ao iniciar o trabalho após {MAX_RETRIES} tentativas. URL: {url}")
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
        description="Extrai comentários de posts coletados pelo ExportComments",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    
    parser.add_argument(
        "file_path",
        type=str,
        help="Camino para o arquivo de extração com as URLs dos posts",
    )
    
    args = parser.parse_args()
    rede = int(input("Escolha a rede social para coletar os comentários: \n1 - Facebook\n2 - Instagram\n3 - Twitter\n"))
    if rede == 1:
        social_network = SocialNetwork.FACEBOOK
    elif rede == 2:
        social_network = SocialNetwork.INSTAGRAM
    elif rede == 3:
        social_network = SocialNetwork.TWITTER
    else:
        print("Opção inválida")
        sys.exit(1)
        
    urls = read_urls_from_extraction(args.file_path, social_network)
    
    all_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_job, url) for url in urls]
        for future in as_completed(futures):
            data = future.result()
            if data is not None:
                all_data.append(data)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        file_name = f"coleta_comentarios_{social_network.name.lower()}.xlsx"
        combined_df.to_excel(file_name, index=False)
        print(f"Arquivo salvo em: {file_name}")