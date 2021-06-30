import requests
from pandas import json_normalize, concat, DataFrame

if __name__ == "__main__":
    response = requests.get(
            'https://developers.onemap.sg/commonapi/search?searchVal=MRT+STATION+EXIT+A&returnGeom=Y&getAddrDetails=Y',
            headers={'User-Agent': None})
    numOfPages = response.json()['totalNumPages']
    df = DataFrame()
    for i in range(1, numOfPages + 1):
        response = requests.get(
            'https://developers.onemap.sg/commonapi/search?searchVal=MRT+STATION+EXIT+A&returnGeom=Y&getAddrDetails=Y&pageNum=' + str(i),
            headers={'User-Agent': None})
        mrt_list = response.json()['results']
        print("Retreived " + str(mrt_list))
        df = concat([df, json_normalize(mrt_list)])

    df.to_csv("mrt.csv")

