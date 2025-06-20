import requests
import pandas as pd

def removeServiceItems(locationId,token):
    baseUrl = f'https://mybusinessbusinessinformation.googleapis.com/v1/locations/{locationId}?updateMask=serviceItems'
    headers = {'Authorization': f'Bearer {token}'}

    response = requests.patch(url=baseUrl,headers=headers,json = {'serviceItems':[]})

    print(response.status_code,response.text)

def main(filepath):
    locs = pd.read_csv(filepath)
    locations = locs['googleId']
    token = ''
    for location in locations:
        removeServiceItems(location,token)

if __name__ == "__main__":
    main('escalateGoogle.csv')