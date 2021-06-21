import requests
from pandas import json_normalize, to_numeric, set_option, concat, DataFrame, read_csv
import pandas as pd
import json as j
import os.path

ACCESS_KEY = "13b9f964-e8ae-410a-9447-7742275b517f"

def mergeTransactions(transactions):
    try:
        df_transactions = DataFrame()
        for transaction in transactions:
            json_str = str(transaction).replace('\'', '"')
            df_transactions = df_transactions.append(pd.DataFrame.from_dict(j.loads(json_str)))
        df_transactions = df_transactions.drop_duplicates()
        df_transactions = df_transactions.fillna(0)
        dict = list(df_transactions.T.to_dict().values())
    except Exception as e:
        print(e)
    return str(dict)

if __name__ == "__main__":
    response = requests.get(
            'https://www.ura.gov.sg/uraDataService/insertNewToken.action',
            headers={'AccessKey': ACCESS_KEY, 'User-Agent': None})
    token = response.json()['Result']
    print("Got token successful " + token)
    #token = "V8p73n-7a6WyK9147p9bP2V24319fq7ZJpF-4+11R1Z-7c9-4e4brnCZ4uynb-1Uv8DBhR6-12+-t9f795fjz6TeGJ7E3f91-8C@"
    batches = [1]
    if os.path.isfile('raw.csv'):
        df_existing = read_csv('raw.csv')
        df_existing = df_existing.fillna(0)
    else:
        df_existing = DataFrame()
    df_new = DataFrame()
    for batch in batches:
        print("Fetching batch " + str(batch))
        response = requests.get(
            'https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Transaction&batch=' + str(batch),
            headers={'AccessKey': ACCESS_KEY, 'Token': token, 'User-Agent': None})
        properties = response.json()['Result']
        df_properties = json_normalize(properties)
        df_new = concat([df_new, df_properties])

    df_combined = df_existing.append(df_new).groupby(['project', 'street'], as_index=False).agg({'transaction': lambda x:mergeTransactions(x),
                                                                                                'street':'first', 'project':'first', 'marketSegment':'first',
                                                                                                'x': 'first', 'y': 'first'})
    diff = pd.concat([df_combined, df_existing]).drop_duplicates(keep=False)
    df_combined.to_csv("raw.csv", mode='w+')