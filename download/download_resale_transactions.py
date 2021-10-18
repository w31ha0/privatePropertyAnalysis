import requests
from pandas import json_normalize, concat, DataFrame, read_csv, set_option
import pandas as pd
import json as j
import os.path

ACCESS_KEY = "13b9f964-e8ae-410a-9447-7742275b517f"

def mergeTransactions(transactions):
    df_transactions = DataFrame()
    for transaction in transactions:
        json_str = str(transaction).replace('\'', '"').replace('"nan"', 'nan').replace('nan', '"nan"')
        df_transaction = pd.DataFrame.from_dict(j.loads(json_str))
        df_transaction = df_transaction.fillna(0)
        df_transactions = df_transactions.append(df_transaction)
    df_transactions = df_transactions.drop_duplicates()
    df_transactions.sort_values(df_transactions.columns.to_list())
    if 'nettPrice' in df_transactions:
        del df_transactions['nettPrice']
    dict = list(df_transactions.reset_index(drop=True).T.to_dict().values())
    return str(dict)

def diffTransactions(transactions):
    df_transactions = DataFrame()
    for transaction in transactions:
        try:
            json_str = str(transaction).replace('\'', '"').replace('"nan"', 'nan').replace('nan', '"nan"')
            df_transaction = pd.DataFrame.from_dict(j.loads(json_str))
            df_transaction = df_transaction.fillna(0)
            df_transactions = df_transactions.append(df_transaction)
        except Exception as e:
            print(e)
        df_transactions = df_transactions.drop_duplicates(keep=False)
    return str(list(df_transactions.reset_index(drop=True).T.to_dict().values()))

if __name__ == "__main__":
    response = requests.get(
        'https://www.ura.gov.sg/uraDataService/insertNewToken.action',
        headers={'AccessKey': ACCESS_KEY, 'User-Agent': None})
    token = response.json()['Result']
    print("Got token successful " + token)
    #token = "4eq8fhP@-hNX6+Ab@f-Mqe47P4nAmGG74tf8173r27aq8f6f-934Q-H5f991Yb6zxNee-j7-5-G26ad14r85f266d6V2-t-J7P-Z"
    batches = [1, 2, 3, 4]
    if os.path.isfile('resale_transactions.csv'):
        df_existing = read_csv('resale_transactions.csv')
        df_existing = df_existing.fillna(0)
    else:
        df_existing = DataFrame()
    df_new = DataFrame()
    for batch in batches:
            df_properties = None

            while df_properties is None:
                try:
                    print("Fetching batch " + str(batch))
                    response = requests.get(
                        'https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Transaction&batch=' + str(batch),
                        headers={'AccessKey': ACCESS_KEY, 'Token': token, 'User-Agent': 'PostmanRuntime/7.28.4'})
                    properties = response.json()['Result']
                    df_properties = json_normalize(properties)
                    df_properties = df_properties.fillna(0)
                    df_new = concat([df_new, df_properties])
                except Exception as e:
                    print(e)
                    print("Retrying batch " + str(batch))

    df_combined = df_existing.append(df_new).groupby(['project', 'street'], as_index=False).agg({'transaction': lambda x:mergeTransactions(x),
                                                                                                'street':'first', 'project':'first', 'marketSegment':'first',
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
    diff = pd.concat([df_combined, df_existing]).drop_duplicates(subset=['street', 'project', 'transaction'], keep=False)
    if not diff.empty:
        print("New Transactions:")
        diff = diff.groupby(['street', 'project']).agg({'transaction': lambda x: diffTransactions(x), 'street': 'first', 'project': 'first'})
        for index, row in diff.iterrows():
            print(row["street"] + " : " + row["project"])
            print(row["transaction"])
    else:
        print("No new Transactions")

    df_combined.to_csv("resale_transactions.csv", mode='w+')