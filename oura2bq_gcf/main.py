import os
from datetime import timedelta

import pandas as pd
import oura

import bigquery


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./gcp-credentials.json"
PROJECT_NUMBER = "723647330842"
SECRET_ID = "oura-personal-access-token"


def fetch_oura_token():
    return bigquery.fetch_secret(PROJECT_NUMBER, SECRET_ID)


def fetch_and_update_oura_bedtime(access_token):
    oura_client = oura.client_pandas.OuraClient(personal_access_token=access_token)
    latest_date = bigquery.fetch_latest_date('bedtime', column_date='date')
    res = oura_client.bedtime_summary(start=str(latest_date + timedelta(days=1)))
    jsonl = res['ideal_bedtimes']
    print(f"bedtime: data size to insert BQ: {len(jsonl)}")
    if len(jsonl):
        bigquery.insert(jsonl, 'bedtime')


def fetch_and_update_oura(access_token):
    oura_client = oura.client_pandas.OuraClient(personal_access_token=access_token)
    for summary_kind in ['activity', 'sleep', 'readiness']:
        latest_date = bigquery.fetch_latest_date(summary_kind)
        res = getattr(oura_client, f'{summary_kind}_summary')(start=str(latest_date + timedelta(days=1)))
        jsonl = res[summary_kind]
        print(f"{summary_kind}: data size to insert BQ: {len(jsonl)}")
        if len(jsonl):
            bigquery.insert(jsonl, summary_kind)
    fetch_and_update_oura_bedtime(access_token)


# Cloud Function用
def main(_):
    fetch_and_update_oura(fetch_oura_token())


# ローカル実行用
if __name__ == '__main__':
    fetch_and_update_oura(fetch_oura_token())
