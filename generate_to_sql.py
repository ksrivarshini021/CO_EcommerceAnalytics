import pandas as pd
import requests
from sqlalchemy import create_engine
import random
import time

db_url = "mysql+mysqlconnector://root:Password%40123@127.0.0.1:3306/ecommerce_schema"
engine = create_engine(db_url)

def fetch_breweries():
    all_breweries = []
    page = 1
    per_page = 200
    state = 'Colorado'

    print(f"fetching all breweries from {state}...")

    while True:
        url = f"https://api.openbrewerydb.org/v1/breweries?by_state={state}&page={page}&per_page={per_page}"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"{response.status_code}; error...")
            break

        data = response.json()
        if not data:
            break

        all_breweries.extend(data)
        print(f"Page {page} of {per_page}: found {len(data)} breweries....")
        page += 1
        time.sleep(0.1)

    return pd.DataFrame(all_breweries)

def run_pipeline():
    df = fetch_breweries()
    if df.empty:
        print("no breweries found")
        return
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    df['brewery_type'] = df['brewery_type'].apply(lambda x: x.lower())

    try:
        df.to_sql('breweries', engine, index=False, if_exists='replace')
        print(f"Successfully saved {len(df)} breweries...")
    except Exception as e:
        print(f"Failed to save {len(df)} breweries...{e}")

if __name__ == '__main__':
    run_pipeline()

