import aiohttp
import asyncio
import pandas as pd

async def getVOption(externalId, headers):
    baseApi = f'https://mybusinessverifications.googleapis.com/v1/locations/{externalId}:fetchVerificationOptions'
    async with aiohttp.ClientSession() as session:
        async with session.post(baseApi, headers=headers, json={"languageCode": "en"}) as r_info:
            responseCode = r_info.status
            if responseCode != 200:
                if responseCode == 404:
                    return pd.DataFrame([{'externalId': externalId, 'verificationMethod': 0, 'phoneNumber': 0, 'error': 'Could not find location ' + str(externalId)}])
                elif responseCode == 401:
                    return pd.DataFrame([{'externalId': externalId, 'verificationMethod': 0, 'phoneNumber': 0, 'error': 'Need authorization token for ' + str(externalId) + '!'}])
                return pd.DataFrame([{'externalId': externalId, 'verificationMethod': 0, 'phoneNumber': 0, 'error': 'Failed for ' + str(externalId)}])
            
            response = await r_info.json()
            try:
                temp = response['options']
                if not response or response == {}:
                    df = pd.DataFrame([{'externalId': externalId, 'verificationMethod': 'No options found', 'phoneNumber': 0}])
                elif temp == [{}]:
                    df = pd.DataFrame([{'externalId': externalId, 'verificationMethod': 'Video', 'phoneNumber': 0}])
                elif not temp or any(not option for option in temp):
                    df = pd.DataFrame([{'externalId': externalId, 'verificationMethod': 0, 'phoneNumber': 0, 'error': 'No valid verification options for ' + str(externalId)}])
                else:
                    # Ensure each option has the required fields, with placeholders if necessary
                    for option in temp:
                        option.setdefault('verificationMethod', 0)
                        option.setdefault('phoneNumber', 0)
                        option['externalId'] = externalId
                    df = pd.DataFrame(temp)
            except KeyError: 
                df = pd.DataFrame([{'externalId': externalId, 'verificationMethod': 'No options found', 'phoneNumber': 0}])
            return df

async def getVOptions(externalIds, headers):
    tasks = [getVOption(externalId, headers) for externalId in externalIds]
    results = await asyncio.gather(*tasks)
    return pd.concat(results, ignore_index=True)