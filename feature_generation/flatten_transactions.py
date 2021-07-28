import pandas as pd
import json as j
import logging as log
from pandas import concat
import math
import openpyxl
import csv

ACCESS_KEY = "13b9f964-e8ae-410a-9447-7742275b517f"

all_transactions = pd.DataFrame()

def distanceToCentral(x, y):
    if x == 0.0 or y == 0.0:
        return math.inf

    central_coordinates = [29365.079, 31281.379]
    return math.sqrt((central_coordinates[0] - x)**2 + (central_coordinates[1] - y)**2)

def distanceToNearestMrt(x, y, df_mrt):
    if x == 0.0 or y == 0.0:
        return math.inf

    df_distances = pd.DataFrame()
    df_distances['distances'] = (df_mrt['X'] - x)**2 + (df_mrt['Y'] - y)**2
    df_distances['distances'] = df_distances['distances'].apply(lambda x: math.sqrt(x))
    df_max = df_distances[df_distances['distances'] == df_distances['distances'].min()]
    return df_max['distances'].iloc[0]

def calculate_initial_rental(json, identifier, distanceToNearestMrt, distanceToNearestMall, distanceToCentral, leaseCommencement):
    global all_transactions

    json = json.replace('\'', '"')
    df_transactions = pd.DataFrame.from_dict(j.loads(json.replace('\'', '"').replace('"nan"', 'nan').replace('nan', '"nan"')))
    df_transactions['psm'] = df_transactions['price'].astype('float') / df_transactions['area'].astype('float')
    df_transactions['year'] = df_transactions['contractDate'].apply(lambda date: date[2:])
    df_transactions['YYDD'] = df_transactions['contractDate'].apply(lambda date: date[2:] + date[:2])
    df_transactions['identifier'] = identifier
    df_transactions['distanceToNearestMrt'] = distanceToNearestMrt
    df_transactions['distanceToNearestMall'] = distanceToNearestMall
    df_transactions['distanceToCentral'] = distanceToCentral
    df_transactions['leaseCommencement'] = leaseCommencement
    all_transactions = concat([all_transactions, df_transactions])

def leaseCommencement(tenure):
    if "commencing from" in tenure:
        return int(tenure[tenure.index("commencing from") + 16:])
    else:
        return 0

def flatten(json, field):
    json = json.replace('\'', '"')
    df_transactions = pd.DataFrame.from_dict(j.loads(json.replace('\'', '"').replace('"nan"', 'nan').replace('nan', '"nan"')))
    return str(df_transactions[field].iloc[0])

if __name__ == "__main__":
    log.basicConfig(level=log.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    current_year = 2021
    df_properties = pd.read_csv('../download/resale_transactions.csv')
    df_mrt = pd.read_csv('../download/mrt.csv')
    df_malls = pd.read_csv('../download/malls.csv')
    log.info("Loaded into csv")
    df_properties = df_properties.fillna(0.0)

    df_properties['distanceToCentral'] = df_properties.apply(lambda df: distanceToCentral(df['x'], df['y']), axis=1)
    log.info("Done calculating distance to central")
    df_properties['distanceToNearestMall'] = df_properties.apply(lambda df: distanceToNearestMrt(df['x'], df['y'], df_malls), axis=1)
    log.info("Done calculating distance to nearest mall")
    df_properties['distanceToNearestMrt'] = df_properties.apply(lambda df: distanceToNearestMrt(df['x'], df['y'], df_mrt), axis=1)
    log.info("Done calculating distance to nearest mrt")
    df_properties['leaseCommencement'] = df_properties['transaction'].apply(lambda x: leaseCommencement(flatten(x, "tenure")))
    log.info("Done calculating lease commencement")
    df_properties['identifier'] = df_properties['street'] + ":" + df_properties['project']
    df_properties.apply(lambda df: calculate_initial_rental(df['transaction'],
                                                            df['identifier'],
                                                            df['distanceToNearestMrt'],
                                                            df['distanceToNearestMall'],
                                                            df['distanceToCentral'],
                                                            df['leaseCommencement']), axis=1)
    log.info("Done exploding transactions")
    all_transactions['sqft'] = all_transactions['area'].astype('float') * 10.764
    del all_transactions['noOfUnits']
    del all_transactions['typeOfSale']
    del all_transactions['contractDate']
    all_transactions.to_csv("flatted_transactions.csv")

    '''
    wb = openpyxl.Workbook()
    ws = wb.active

    with open('flatted_transactions.csv') as f:
        reader = csv.reader(f, delimiter=':')
        for row in reader:
            ws.append(row)

    ws.auto_filter.ref = "A1:P" + str(len(all_transactions))
    ws.auto_filter.add_filter_column(len(all_transactions) - 1, [0 ])
    ws.auto_filter.add_sort_condition("B1:B" + str(len(all_transactions)))
    wb.save('flatted_transactions2.xlsx')
    '''