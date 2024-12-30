import aiohttp
import asyncio
import pandas as pd
import os

async def getQuestions(id, heads):
    os.write(1,  f"Getting for Location ID: {id}".encode())
    call = 'https://mybusinessqanda.googleapis.com/v1/locations/'
    additional = '/questions?pageSize=10&answersPerQuestion=10'
    url = f'{call}{str(id)}{additional}'
    all_data = []

    async with aiohttp.ClientSession() as session:
        while url:
            async with session.get(url, headers=heads) as response:
                response_json = await response.json()
                data = response_json.get('questions', [])
                nextPageToken = response_json.get('nextPageToken')

                all_data.extend(data)
                if nextPageToken:
                    url = f'{call}{str(id)}/questions?pageSize=10&pageToken={nextPageToken}&answersPerQuestion=10'
                else:
                    url = None
    df = pd.DataFrame(all_data)
    return df