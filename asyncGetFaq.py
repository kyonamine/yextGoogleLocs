import aiohttp
import asyncio
import pandas as pd
import os

# def getQuestions(id, heads):
#     call = 'https://mybusinessqanda.googleapis.com/v1/locations/'
#     additional = '/questions?pageSize=10&answersPerQuestion=10'
#     url = f'{call}{str(id)}{additional}'
#     all_data = []
#     while url:
#         response = requests.get(url, headers = heads)
#         response_json = response.json()
#         rStatusCode = response.status_code
#         if rStatusCode == 401:
#             exitApp(1)
            
#         data = response_json.get('questions', [])
#         nextPageToken = response_json.get('nextPageToken')
        
#         all_data.extend(data)      
#         if nextPageToken:
#             url = f'{call}{str(id)}/questions?pageSize=10&pageToken={nextPageToken}&answersPerQuestion=10'
#         else:
#             url = None
    
#     df = pd.DataFrame(all_data)
#     return df

async def getQuestions(id, heads):
    os.write(1,  f"Getting questions inside asyncGetFaq\n".encode())
    call = 'https://mybusinessqanda.googleapis.com/v1/locations/'
    additional = '/questions?pageSize=10&answersPerQuestion=10'
    url = f'{call}{str(id)}{additional}'
    all_data = []

    async with aiohttp.ClientSession() as session:
        while url:
            async with session.get(url, headers=heads) as response:
                response_json = await response.json()
                # rStatusCode = response.status

                data = response_json.get('questions', [])
                nextPageToken = response_json.get('nextPageToken')

                all_data.extend(data)
                if nextPageToken:
                    url = f'{call}{str(id)}/questions?pageSize=10&pageToken={nextPageToken}&answersPerQuestion=10'
                else:
                    url = None
    os.write(1,  f"all_data is: {all_data}".encode())
    return all_data