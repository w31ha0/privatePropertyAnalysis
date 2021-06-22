import requests
from pandas import json_normalize, concat, DataFrame, read_csv
import pandas as pd
import json as j
import os.path

ACCESS_KEY = "13b9f964-e8ae-410a-9447-7742275b517f"

def mergeTransactions(transactions):
    try:
        df_transactions = DataFrame()
        for transaction in transactions:
            json_str = str(transaction).replace('\'', '"')
            df_transaction = pd.DataFrame.from_dict(j.loads(json_str))
            df_transaction = df_transaction.fillna(0)
            df_transactions = df_transactions.append(df_transaction)
        df_transactions = df_transactions.drop_duplicates()
        df_transactions.sort_values(df_transactions.columns.to_list())
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
    batches = [1, 2, 3, 4]
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

    diff = pd.concat([df_combined, df_existing]).drop_duplicates(subset=['street', 'project'], keep=False)
    if not diff.empty:
        print("New Residences:")
        print(diff[['street', 'project']])
    else:
        print("No new Residences")

    diff = pd.concat([df_combined, df_existing]).drop_duplicates(subset=['street', 'project', 'transaction'], keep=False)
    diff = diff.sort_values('project')
    if not diff.empty:
        print("")
        print("New Transactions:")
        for index, row in diff.iterrows():
            transactions = row['transaction']
            print(row['street'] + " : " + row['project'])
            for transaction in j.loads(transactions.replace('\'', '"')):
                print(transaction)
    else:
        print("No new Transactions")

    df_combined.to_csv("raw.csv", mode='w+')