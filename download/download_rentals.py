import requests
from pandas import json_normalize, concat, DataFrame, read_csv, set_option
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
        dict = list(df_transactions.reset_index(drop=True).T.to_dict().values())
    except Exception as e:
        print(e)
    return str(dict)

def diffTransactions(transactions):
    df_transactions = DataFrame()
    for transaction in transactions:
        json_str = str(transaction).replace('\'', '"')
        df_transaction = pd.DataFrame.from_dict(j.loads(json_str))
        df_transaction = df_transaction.fillna(0)
        df_transactions = df_transactions.append(df_transaction)
    df_transactions = df_transactions.drop_duplicates(keep=False)
    return str(list(df_transactions.reset_index(drop=True).T.to_dict().values()))

if __name__ == "__main__":
    response = requests.get(
            'https://www.ura.gov.sg/uraDataService/insertNewToken.action',
            headers={'AccessKey': ACCESS_KEY, 'User-Agent': None})
    token = response.json()['Result']
    print("Got token successful " + token)
    #token = "7RA5fRp7pwH1C74-5F2dbF2f4SZQ79bY+z27sfm4c-71a4gtkeJ@Q5P39b74-7P5ef9--JuWjs7ze4cvA42EaJ9tRaFvn74FF7Ta"
    batches = [1, 2, 3, 4]
    if os.path.isfile('../rentals.csv'):
        df_existing = read_csv('../rentals.csv')
        df_existing = df_existing.fillna(0)
    else:
        df_existing = DataFrame()
    df_new = DataFrame()
    for year in range(16, 22):
        for q in range(1, 5):
            batch = str(year) + "q" + str(q)
            print("Fetching batch " + str(batch))
            response = requests.get(
                'https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Rental&refPeriod=' + batch,
                headers={'AccessKey': ACCESS_KEY, 'Token': token, 'User-Agent': None})
            properties = response.json()['Result']
            df_properties = json_normalize(properties)
            df_new = concat([df_new, df_properties])

    df_combined = df_existing.append(df_new).groupby(['project', 'street'], as_index=False).agg({'rental': lambda x:mergeTransactions(x),
                                                                                                'street':'first', 'project':'first',
                                                                                                 'x': 'first', 'y': 'first'})

    set_option('display.max_columns', 10)
    print("")
    diff = pd.concat([df_combined, df_existing]).drop_duplicates(subset=['street', 'project'], keep=False)
    if not diff.empty:
        print("New Residences:")
        print(diff[['street', 'project']])
    else:
        print("No new Residences")

    print("")
    diff = pd.concat([df_combined, df_existing]).drop_duplicates(subset=['street', 'project', 'rental'], keep=False)

    df_combined.to_csv("rentals.csv", mode='w+')