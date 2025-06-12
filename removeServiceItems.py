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
    token = 'ya29.a0AW4Xtxh3oKisXThVBg2zW0KK8XlIxshBSmqvpjuRu3tn9wZYDzCmxkZk-UBuKsZQpuPGgMCFcyT1zIts9o1TEpoIMwaBxmJqnO59HCYUdK6WdMd_SD8uHcYpzhmE91LSAGzYVzsfFVhdBvWvqNuA5l4uQ6daSr2u0FARgG1ffkEaCgYKATISARASFQHGX2Mi1VPDsfpkDxHwO8NVbdB4ZQ0178'
    for location in locations:
        removeServiceItems(location,token)

if __name__ == "__main__":
    main('escalateGoogle.csv')