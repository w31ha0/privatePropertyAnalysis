from pandas import set_option
import pandas as pd
import json as j
import math
import logging as log

ACCESS_KEY = "13b9f964-e8ae-410a-9447-7742275b517f"

def calculate_annual_appreciation(json):
    json = json.replace('\'', '"')
    df_transactions = pd.DataFrame.from_dict(j.loads(json))
    df_transactions['psm'] = df_transactions['price'].astype('float') / df_transactions['area'].astype('float')
    df_transactions['year'] = df_transactions['contractDate'].apply(lambda date: date[2:])
    df_transactions['YYDD'] = df_transactions['contractDate'].apply(lambda date: date[2:] + date[:2])
    grouped_date = df_transactions.groupby('year')['psm'].mean().reset_index().rename(columns={'mean': 'psm', 'year': 'year'})
    grouped_date.sort_values('year', inplace=True)
    n_years = int(grouped_date['year'].iloc[-1]) - int(grouped_date['year'].iloc[0])
    annual_appreciation = ((grouped_date['psm'].iloc[-1] - grouped_date['psm'].iloc[0]) / grouped_date['psm'].iloc[0]) / n_years
    return annual_appreciation

def calculate_initial_price(json_transactions):
    json = json_transactions.replace('\'', '"')
    df_transactions = pd.DataFrame.from_dict(j.loads(json))
    df_transactions['psm'] = df_transactions['price'].astype('float') / df_transactions['area'].astype('float')
    df_transactions['year'] = df_transactions['contractDate'].apply(lambda date: date[2:])
    df_transactions['YYDD'] = df_transactions['contractDate'].apply(lambda date: date[2:] + date[:2])
    grouped_date_transactions = df_transactions.groupby('year')['psm'].mean().reset_index().rename(columns={'mean': 'psm', 'year': 'year'})
    grouped_date_transactions.sort_values('year', inplace=True)
    return int(grouped_date_transactions['psm'].iloc[0])

def parseRentalRange(range):
    if "-" in range:
        return (int(range.split('-')[0]) + int(range.split('-')[1])) / 2
    else:
        return int(range)

def calculate_initial_rental(json_rentals):
    if isinstance(json_rentals, float):
        return 0 if math.isnan(json_rentals) else json_rentals
    df_rentals = pd.DataFrame.from_dict(j.loads(json_rentals.replace('\'', '"').replace('nan', '"nan"')))
    df_rentals['areaSqm'] = df_rentals['areaSqm'].apply(lambda range: range.replace('>', '').replace('<', '').replace('=', ''))
    df_rentals['areaSqm'] = df_rentals['areaSqm'].apply(lambda range: parseRentalRange(range))
    df_rentals['psm'] = df_rentals['rent'].astype('float') / df_rentals['areaSqm'].astype('float')
    df_rentals['year'] = df_rentals['leaseDate'].apply(lambda date: date[2:])
    df_rentals['YYDD'] = df_rentals['leaseDate'].apply(lambda date: date[2:] + date[:2])
    grouped_date_rentals = df_rentals.groupby('year')['psm'].mean().reset_index().rename(columns={'mean': 'psm', 'year': 'year'})
    grouped_date_rentals.sort_values('year', inplace=True)
    return int(grouped_date_rentals['psm'].iloc[0])

def en_block(json):
    json = json.replace('\'', '"')
    df_transactions = pd.DataFrame.from_dict(j.loads(json))
    df_transactions['psm'] = df_transactions['price'].astype('float') / df_transactions['area'].astype('float')
    df_transactions['year'] = df_transactions['contractDate'].apply(lambda date: date[2:])
    df_transactions['YYDD'] = df_transactions['contractDate'].apply(lambda date: date[2:] + date[:2])
    return int(df_transactions.sort_values('YYDD').iloc[-1]["noOfUnits"]) > 1

def flatten(json, field):
    json = json.replace('\'', '"')
    df_transactions = pd.DataFrame.from_dict(j.loads(json))
    return str(df_transactions[field].iloc[0])

def compressTenure(tenure):
    if 'yrs lease' in tenure:
        years = int(tenure[:tenure.index(' yrs lease')])
        if years < 200:
            return "Leasehold"
        else:
            return "Freehold"
    else:
        return tenure

def totalLeaseYears(tenure):
    if 'yrs lease' in tenure:
        return int(tenure[:tenure.index(' yrs lease')])
    else:
        return math.inf

def yearBegan(json):
    json = json.replace('\'', '"')
    df_transactions = pd.DataFrame.from_dict(j.loads(json))
    df_transactions['year'] = df_transactions['contractDate'].apply(lambda date: date[2:])
    return int(df_transactions.sort_values('year').iloc[0]['year']) + 2000

def leaseCommencement(tenure):
    if "commencing from" in tenure:
        return int(tenure[tenure.index("commencing from") + 16:])
    else:
        return 0

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

if __name__ == "__main__":
    log.basicConfig(level=log.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    df_properties = pd.read_csv('../download/resale_transactions.csv')
    df_mrt = pd.read_csv('../download/mrt.csv')
    df_malls = pd.read_csv('../download/malls.csv')
    df_rentals = pd.read_csv('../download/rentals.csv')
    log.info("Loaded into csv")

    df_properties = df_properties.merge(df_rentals, on=['street', 'project'], how='left')
    df_properties['initialPrice'] = df_properties['transaction'].apply(lambda df: calculate_initial_price(df))
    log.info("Done calculating initial sales price")
    df_properties['initialRental'] = df_properties['rental'].apply(lambda df: calculate_initial_rental(df))
    log.info("Done calculating initial rental")
    df_properties['rentalYield'] = df_properties['initialRental'] / df_properties['initialPrice']
    log.info("Done calculating rental yield")
    df_properties['annualAppreciation'] = df_properties['transaction'].apply(lambda x: calculate_annual_appreciation(x))
    log.info("Done calculating annual appreciation")
    df_properties['enBlock'] = df_properties['transaction'].apply(lambda x: en_block(x))
    log.info("Done calculating enBlock")
    df_properties['propertyType'] = df_properties['transaction'].apply(lambda x: flatten(x, 'propertyType'))
    log.info("Done calculating propertyType")
    df_properties['initialYear'] = df_properties['transaction'].apply(lambda x: yearBegan(x))
    log.info("Done calculating initial Year")
    df_properties['leaseCommencement'] = df_properties['transaction'].apply(lambda x: leaseCommencement(flatten(x, "tenure")))
    log.info("Done calculating lease commencement")
    df_properties['totalLease'] = df_properties['transaction'].apply(lambda x: totalLeaseYears(flatten(x, 'tenure')))
    log.info("Done calculating total lease")
    df_properties['leaseRemaining'] = df_properties['totalLease'] - (df_properties['initialYear'] - df_properties['leaseCommencement'])
    log.info("Done calculating lease remaining")
    df_properties['tenure'] = df_properties['transaction'].apply(lambda x: compressTenure(flatten(x, 'tenure')))
    log.info("Done calculating tenure")
    df_properties['noOfTransactions'] = df_properties['transaction'].apply(lambda x: len(x))
    log.info("Done calculating number of transactions")
    df_properties['distanceToCentral'] = df_properties.apply(lambda df: distanceToCentral(df['x_x'], df['y_x']), axis=1)
    log.info("Done calculating distance to central")
    df_properties['distanceToNearestMall'] = df_properties.apply(lambda df: distanceToNearestMrt(df['x_x'], df['y_x'], df_malls), axis=1)
    log.info("Done calculating distance to nearest mall")
    df_properties['distanceToNearestMrt'] = df_properties.apply(lambda df: distanceToNearestMrt(df['x_x'], df['y_x'], df_mrt), axis=1)
    log.info("Done calculating distance to nearest mrt")

    del df_properties['x_y']
    del df_properties['y_y']
    df_properties[['rental']] = df_properties[['rental']].fillna('[]')
    df_properties = df_properties.dropna(subset=['annualAppreciation'])

    appreciation_threshold = 0.03

    total = len(df_properties[(df_properties['annualAppreciation'] > appreciation_threshold)]) / len(df_properties)
    freehold = len(df_properties[(df_properties['annualAppreciation'] > appreciation_threshold) & (df_properties['tenure'] == "Freehold")]) / len(df_properties[df_properties['tenure'] == "Freehold"])
    leasehold = len(df_properties[(df_properties['annualAppreciation'] > appreciation_threshold) & (df_properties['tenure'] == "Leasehold")]) / len(df_properties[df_properties['tenure'] == "Leasehold"])

    print(str(total * 100) + "% of all properties appreciate.")
    print(str(freehold * 100) + "% of freehold properties appreciate.")
    print(str(leasehold * 100) + "% of leasehold properties appreciate.")

    set_option("display.max_rows", None)
    set_option('display.max_columns', 5)
    df_properties.sort_values('annualAppreciation', ascending=False, inplace=True)
    del df_properties['transaction']
    del df_properties['rental']
    del df_properties['Unnamed: 0_x']
    df_properties.to_csv('Analysis.csv')