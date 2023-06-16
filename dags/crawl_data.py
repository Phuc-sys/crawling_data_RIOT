import requests
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


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
engine = create_engine('mysql+mysqlconnector://root:@localhost:3306/world')

def generate_url(regions, queue, request_urls):
    for region in regions:
        challenger_url = 'https://{}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{}'.format(region,queue)
        grandmaster_url = 'https://{}.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/{}'.format(region,queue)  
        request_urls.append(challenger_url)
        request_urls.append(grandmaster_url)
    
    return request_urls

def fetch_Data_RIOT_API(ti, total_users, queue, headers):
    """Fetch data"""
    request_urls = ti.xcom_pull(task_ids='generateURL')

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
        

def transform_data(ti):
    """Drop na value"""
    total_users = ti.xcom_pull(task_ids='fetchData')

    df = pd.DataFrame(total_users)
    print(df.head())
    print(df.shape)
    df2 = df.dropna()
    print(df2.shape)

    return df2




default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="crawling_data_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['nthph-project'],
) as dag:
    
    generateURL = PythonOperator(
        task_id="generateURL",
        python_callable=generate_url,
        op_kwargs={
            "regions": regions,
            "queue": queue,
            "request_urls": request_urls,
        },
    )
    
    fetchData = PythonOperator(
        task_id="fetchData",
        python_callable=fetch_Data_RIOT_API,
        op_kwargs={
            "total_users": total_users,
            "queue": queue,
            "headers": headers,
        },
    )

    transformData = PythonOperator(
        task_id="transformData",
        python_callable=transform_data
    )


    generateURL >> fetchData >> transformData 
