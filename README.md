# Objective
Building a small ETL project for crawling data from a game names LOL.
# Overview
## Step 1: Get API Key
In order to retrieve the data you need RIOT API, go to riot website to get your Development API Key (https://developer.riotgames.com/) 
**Attention: API Key deactivated every 24 hours. You'll need to regularly reset yours to keep it alive.**

![Screenshot (36)](https://github.com/Phuc-sys/crawling_data_RIOT/assets/81355271/110bf2a4-478c-4ddb-ad36-521ddd7e0190)

## Step 2: Fetch data using RIOT API 
Here i chose LEAGUE-V4 API to retrieve players data which are high rank as "Challenger", "Grand Master" from regions: Brazil (BR1), Vietnam (VN2) and Korean (KR) in queue: RANK_SOLO_5x5

![image](https://github.com/Phuc-sys/crawling_data_RIOT/assets/81355271/b45cb596-19a3-4411-82cd-5a86bd5a3120)

**Basically, there is a transformation step, but hence this is a clean data therefore, we don't take time on doing it.**

## Step 3: Uploading data to On-Premise
I already have a MySQL database, next I will upload the data to the db

![Screenshot (34)](https://github.com/Phuc-sys/crawling_data_RIOT/assets/81355271/fb116e2c-a752-4dbf-9c3d-7c31f85a0b0b)

### Below the script, there is a code to upload to BigQuery, however my billing expired so I can't upload data to it.

### Next, I transfer the data to a CSV file and convert the ipynb file to python file

## Step 5: Set up Airflow for workflow orchestration
First of all, I set up Airflow according to airflow-instruction.txt using Docker. The script code will be placed in /dags folder. I have cut off the step uploading data to DB since I dont have any Cloud repository available.

![Screenshot (35)](https://github.com/Phuc-sys/crawling_data_RIOT/assets/81355271/97b0ba0c-e61f-4547-b704-8c157712ed04)

**You can easily setup Airflow following this video (https://youtu.be/aTaytcxy2Ck).**

