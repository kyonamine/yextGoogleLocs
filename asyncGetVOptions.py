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
                if not temp or any(not option for option in temp):
                    df = pd.DataFrame([{'verificationMethod': 0, 'phoneNumber': 0, 'error': 'No valid verification options for ' + str(externalId)}])
                else:
                    # Ensure each option has the required fields, with placeholders if necessary
                    for option in temp:
                        option.setdefault('verificationMethod', 0)
                        option.setdefault('phoneNumber', 0)
                    df = pd.DataFrame(temp)
            except KeyError: 
                df = pd.DataFrame([{'verificationMethod': 0, 'phoneNumber': 0, 'error': 'No verification options for ' + str(externalId)}])
            return df