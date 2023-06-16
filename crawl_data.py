import requests
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path


def fetch_Data_RIOT_API(request_urls, queue, headers):
    """Fetch data"""
    total_users = []

    for x in request_urls:
        temp_a = x.replace('//', '.')
        temp_b = temp_a.split(".")
        region = temp_b[1]

        response = requests.get(x, headers=headers)
            
        if response.status_code == 200:
            print(f"Region: {region}, Tier: {response.json()['tier']}, Queue: {response.json()['queue']}, Total Players: {len(response.json()['entries'])}")

            for y in response.json()['entries']:
                y['tier'] = response.json()['tier']
                y['request_region'] = region
                y['queue'] = queue
                total_users.append(y)
        else:
            print(f"'Request error, response code: {response.status_code}")
        
    return total_users

def transform_data(total_users):
    """Drop na value"""
    df = pd.DataFrame(total_users)
    print(df.head())
    print(df.shape)
    df2 = df.dropna()
    print(df2.shape)

    return df2

def insert_mysql(df2):
    engine = create_engine('mysql+mysqlconnector://root:@localhost:3306/world')
    engine.connect()
    # create table in schema
    print(pd.io.sql.get_schema(df2, name='league-v4-2.0', con=engine))
    df2.to_sql(name='league-v4-2.0', con=engine, if_exists='replace')
    print("Load data to MySQL finish")

def export_csv(df2):
    filepath = Path('dataset/out.csv')  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    df2.to_csv(filepath) 
    print("Export data to CSV finish")

def main_task():
    """Main ETL"""
    regions = ["BR1", "VN2", "KR"]
    queue = "RANKED_SOLO_5x5"
    total_users = []
    request_urls = []
    api_key = "RGAPI-51d9ac2f-fbaa-4d6e-a378-b645b77d20fa"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": api_key
    }

    for region in regions:
        challenger_url = 'https://{}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{}'.format(region,queue)
        grandmaster_url = 'https://{}.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/{}'.format(region,queue)
    
        request_urls.append(challenger_url)
        request_urls.append(grandmaster_url)
    
    total_users = fetch_Data_RIOT_API(request_urls, queue, headers)
    df2 = transform_data(total_users)
    insert_mysql(df2)
    export_csv(df2)


if __name__=='__main__':
    main_task()
