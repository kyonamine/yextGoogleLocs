import pandas as pd
import asyncio
import aiohttp

async def asyncDeleteFaqs(locationId, questionIdList, heads):
    base = 'https://mybusinessqanda.googleapis.com/v1/locations/'
    additional = '/questions/'
    df = await loopAndDelete(locationId, questionIdList, heads, base, additional)
    return df

async def loopAndDelete(externalId, targetIdList, heads, base, additional):
    df = pd.DataFrame(columns=['Google Location ID', 'Target ID', 'API Response Code'])
    if not targetIdList:
        df.loc[len(df)] = [externalId, f'No duplicates for {externalId}', -1]
        return df

    async with aiohttp.ClientSession() as session:
        tasks = []
        for targetId in targetIdList:
            call = f"{base}{externalId}{additional}{targetId}"
            tasks.append(delete_single_faq(session, call, heads, externalId, targetId, df))
        await asyncio.gather(*tasks)
    return df

async def delete_single_faq(session, call, heads, externalId, targetId, df):
    async with session.delete(call, headers=heads) as r_info:
        response = r_info.status
        df.loc[len(df)] = [externalId, targetId, response]