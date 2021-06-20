import requests
from pandas import json_normalize, to_numeric, set_option, concat, DataFrame

ACCESS_KEY = "13b9f964-e8ae-410a-9447-7742275b517f"

if __name__ == "__main__":
    '''response = requests.get(
            'https://www.ura.gov.sg/uraDataService/insertNewToken.action',
            headers={'AccessKey': ACCESS_KEY, 'User-Agent': None})
        token = response.json()['Result']
        print("Got token successful " + token)'''
    token = "YvvqsJ4UbU4P1k7h5AA3vW-b7-CY4D@R4911ytEck74VvvK86gv2VBhp1PTedj4tD7Rr24x242p4W1Ng2R2u4953h@g-9XMrVb5M"
    batches = [1, 2, 3, 4]
    df_overall = DataFrame()
    for batch in batches:
        print("Fetching batch " + str(batch))
        response = requests.get(
            'https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Transaction&batch=' + str(batch),
            headers={'AccessKey': ACCESS_KEY, 'Token': token, 'User-Agent': None})
        properties = response.json()['Result']
        df_properties = json_normalize(properties)
        df_overall = concat([df_overall, df_properties])

    df_overall.to_csv("raw.csv")