import aiohttp
import asyncio
import pandas as pd

async def getVOptions(externalId, headers):
    baseApi = f'https://mybusinessverifications.googleapis.com/v1/locations/{externalId}:fetchVerificationOptions'
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
                temp = response['options']
                df = pd.DataFrame(temp)
            except KeyError: 
                df = pd.DataFrame([{'error': 'No verification options for Google ID: ' + str(externalId)}])
            return df