import pandas as pd
import requests

if __name__ == "__main__":
    df_properties = pd.read_csv('../download/resale_transactions.csv')
    df_properties['identifier'] = df_properties['street'] + ":" + df_properties['project']
    df_properties = df_properties.set_index('identifier')
    for index, row in df_properties.iterrows():
        if row['x'] == 0.0:
            print("Retrieving coordinates for " + row['street'] + " : " + row['project'])
            response = requests.get(
                'https://developers.onemap.sg/commonapi/search?searchVal=' + row['street'] + '&returnGeom=Y&getAddrDetails=Y',
                headers={'User-Agent': None})
            coordinate_json = response.json()['results']
            try:
                row['x'] = coordinate_json[0]['X']
                row['y'] = coordinate_json[0]['Y']
                print("Found coordinates " + row['x'] + "," + row['y'] + " for " + coordinate_json[0]['ADDRESS'])
                df_properties._set_value(row['street'] + ":" + row['project'], 'x', row['x'])
                df_properties._set_value(row['street'] + ":" + row['project'], 'y', row['y'])
            except Exception as e:
                print("No coordinates found for " + row['street'] + " : " + row['project'])
    df_properties.to_csv('../download/resale_transactions.csv')


