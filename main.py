from pandas import set_option
import pandas as pd
import json as j
import math

ACCESS_KEY = "13b9f964-e8ae-410a-9447-7742275b517f"

def calculate_annual_appreciation(json):
    json = json.replace('\'', '"')
    df_transactions = pd.DataFrame.from_dict(j.loads(json))
    df_transactions['psm'] = df_transactions['price'].astype('float') / df_transactions['area'].astype('float')
    df_transactions['year'] = df_transactions['contractDate'].apply(lambda date: date[2:])
    df_transactions['YYDD'] = df_transactions['contractDate'].apply(lambda date: date[2:] + date[:2])
    grouped_date = df_transactions.groupby('year')['psm'].mean().reset_index().rename(columns={'mean': 'psm', 'year': 'year'})
    grouped_date.sort_values('year')
    n_years = int(grouped_date['year'].iloc[-1]) - int(grouped_date['year'].iloc[0])
    annual_appreciation = ((grouped_date['psm'].iloc[-1] - grouped_date['psm'].iloc[0]) / grouped_date['psm'].iloc[0]) / n_years
    return annual_appreciation

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

if __name__ == "__main__":
    df_properties = pd.read_csv('raw.csv')
    df_properties['annual_appreciation'] = df_properties['transaction'].apply(lambda x: calculate_annual_appreciation(x))
    df_properties['enBlock'] = df_properties['transaction'].apply(lambda x: en_block(x))
    df_properties['propertyType'] = df_properties['transaction'].apply(lambda x: flatten(x, 'propertyType'))
    df_properties['initialYear'] = df_properties['transaction'].apply(lambda x: yearBegan(x))
    df_properties['leaseCommencement'] = df_properties['transaction'].apply(lambda x: leaseCommencement(flatten(x, "tenure")))
    df_properties['totalLease'] = df_properties['transaction'].apply(lambda x: totalLeaseYears(flatten(x, 'tenure')))
    df_properties['leaseRemaining'] = df_properties['totalLease'] - (df_properties['initialYear'] - df_properties['leaseCommencement'])
    df_properties['tenure'] = df_properties['transaction'].apply(lambda x: compressTenure(flatten(x, 'tenure')))
    df_properties['noOfTransactions'] = df_properties['transaction'].apply(lambda x: len(x))
    df_properties = df_properties.dropna()

    appreciation_threshold = 0.03

    total = len(df_properties[(df_properties['annual_appreciation'] > appreciation_threshold)]) / len(df_properties)
    freehold = len(df_properties[(df_properties['annual_appreciation'] > appreciation_threshold) & (df_properties['tenure'] == "Freehold")]) / len(df_properties[df_properties['tenure'] == "Freehold"])
    leasehold = len(df_properties[(df_properties['annual_appreciation'] > appreciation_threshold) & (df_properties['tenure'] == "Leasehold")]) / len(df_properties[df_properties['tenure'] == "Leasehold"])

    print(str(total * 100) + "% of all properties appreciate.")
    print(str(freehold * 100) + "% of freehold properties appreciate.")
    print(str(leasehold * 100) + "% of leasehold properties appreciate.")

    set_option("display.max_rows", None)
    set_option('display.max_columns', 5)
    df_properties.sort_values('annual_appreciation', ascending=False, inplace=True)
    df_properties[['street', 'project', 'marketSegment', 'propertyType', 'enBlock', 'tenure', 'annual_appreciation', 'noOfTransactions', 'initialYear', 'leaseCommencement', 'leaseRemaining']].to_csv('Analysis.csv')