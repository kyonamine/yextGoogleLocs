import aiohttp
import asyncio
import pandas as pd

async def localPostGet(googleAccountNum, externalId, headers):
    baseApi = f'https://mybusiness.googleapis.com/v4/accounts/{googleAccountNum}/locations/{externalId}/localPosts?pageSize=100'
    async with aiohttp.ClientSession() as session:
        async with session.get(baseApi, headers=headers) as r_info:
            responseCode = r_info.status
            if responseCode != 200:
                if responseCode == 404:
                    return 'Could not find location ' + str(externalId)
                elif responseCode == 401:
                    return 'Need authorization token for ' + str(externalId) + '!'
                return 'Failed for ' + str(externalId)
            
            response = await r_info.json()
            try:
                temp = response['localPosts']
            except KeyError: 
                return 'No localPosts for ' + str(externalId)
            
            df = pd.DataFrame(temp)
            return df
