import aiohttp
import asyncio
import pandas as pd
import os

async def deletePost(accountId, postIdList, externalId, heads):
    baseApi = f'https://mybusiness.googleapis.com/v4/accounts/{str(accountId)}/locations/'
    df = pd.DataFrame(columns = ['Google Location ID', 'localPostId', 'API Response Code'])
    os.write(1,  f"{len(postIdList)} posts to delete on location ID: {externalId}, account ID {accountId}\n".encode())

    async with aiohttp.ClientSession() as session:
        tasks = []
        for postId in postIdList:
            call = f"{baseApi}{externalId}/localPosts/{postId}"
            tasks.append(delete_single_post(session, call, heads, externalId, postId, df))
        await asyncio.gather(*tasks)
    return df

async def delete_single_post(session, call, heads, externalId, postId, df):
    async with session.delete(call, headers=heads) as r_info:
        response = r_info.status
        df.loc[len(df)] = [externalId, str(postId), response]
