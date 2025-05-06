import aiohttp
import asyncio
import pandas as pd
import os

async def getQuestions(id, heads):
    os.write(1,  f"Getting for Location ID: {id}\n".encode())
    call = 'https://mybusinessqanda.googleapis.com/v1/locations/'
    additional = '/questions?pageSize=10&answersPerQuestion=10'
    url = f'{call}{str(id)}{additional}'
    all_data = []
    # os.write(1,  f"URL is: {url}\n".encode())
    async with aiohttp.ClientSession() as session:
        while url:
            async with session.get(url, headers=heads) as response:
                error_message = None
                if response.status == 401:
                    os.write(1,  f"401 Unauthorized for Location ID: {id}\n".encode())
                    error_message = {'error_message': f'Failed starting with location ID: {id}', 'status_code': response.status}
                    # all_data.append(authCode)
                elif response.status == 404:
                    os.write(1,  f"404 Not found for Location ID: {id}\n".encode())
                    error_message = {'error_message': f'Failed starting with location ID: {id}', 'status_code': response.status}
                    # all_data.append(authCode)
                elif response.status != 200:
                    os.write(1,  f"Error for Location ID: {id}\n".encode())
                    authCode = [f'Failed starting with location ID: {id}']
                    error_message = {'error_message': f'Failed starting with location ID: {id}', 'status_code': response.status}
                    # all_data.append(authCode)
                if error_message:
                    all_data.append(error_message)
                    break
                else:
                    response_json = await response.json()
                    data = response_json.get('questions', [])
                    os.write(1,data)
                    nextPageToken = response_json.get('nextPageToken')
                    all_data.extend(data)
                    if nextPageToken:
                        os.write(1,f'Getting next page'.encode())
                        url = f'{call}{str(id)}/questions?pageSize=10&pageToken={nextPageToken}&answersPerQuestion=10'
                    else:
                        url = None
    df = pd.DataFrame(all_data)
    
    return df