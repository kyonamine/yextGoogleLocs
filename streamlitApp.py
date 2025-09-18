import streamlit as st
import pandas as pd
import numpy as np
import json
import requests
from io import StringIO
import sys
from datetime import date
import os
import time
from datetime import datetime
# import streamlit_analytics2 as streamlit_analytics
# import streamlit_analytics
from google.cloud import firestore
from google.oauth2 import service_account
from asyncGetPosts import localPostGet
from asyncDeletePosts import asyncDeletePost
from asyncGetVOptions import getVOptions
from asyncGetFaq import getQuestions
from asyncDeleteFaq import asyncDeleteFaqs
import asyncio
import aiohttp

# Initialize 'password_correct' in session state.  Crucial!
if "password_correct" not in st.session_state:
    st.session_state["password_correct"] = False
# st.write(st.session_state.password_correct)
if "session_data" not in st.session_state:
    st.session_state["session_data"] = {}

# key_dict = json.loads(st.secrets["textkey"])
# creds = service_account.Credentials.from_service_account_info(key_dict)
# db = firestore.Client(credentials=creds, project="tpm-streamlit-analytics")

def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["pw"] == st.secrets["pw"]:
            st.session_state["password_correct"] = True
            del st.session_state["pw"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change = password_entered, key = "pw"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change = password_entered, key = "pw"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

def uploadFile(field):
    exampleSheet = 'https://docs.google.com/spreadsheets/d/18tJfjrlZFd3qQT5ZTnz3eIw5v6KSILTBN-Ol9G7sFLo/edit#gid=0'
    # st.info('It\'s recommended to use a Google Sheet [in this format](%s) and download as a CSV.' % exampleSheet, icon = "ℹ️")
    uploaded_file = st.file_uploader("Provide a file with IDs. Make sure the CSV file has headers of \"Yext ID\" (first) and \"Google ID\" (second).", 
                                    type = ['csv', 'txt'],
                                    help = 'It\'s recommended to use a Google Sheet [in this format](%s) and download as a CSV.' % exampleSheet)
    
    if uploaded_file is not None:
        if field == 'Update Primary Category':
            df = pd.read_csv(uploaded_file, usecols=['Google ID'])
        dataframe = pd.read_csv(uploaded_file, dtype = {'Yext ID': str, 'Google ID': str})
        if len(dataframe.columns) != 2:
            st.error("Error: CSV file should contain exactly 2 columns.")
            exitApp(2)
        else:
            expected_columns = ['Yext ID', 'Google ID']
            if not all(col in dataframe.columns for col in expected_columns):
                st.error("Error: Columns should be titled \"Yext ID\" and \"Google ID\".")
                exitApp(2)
            else:
                for col in dataframe.columns:
                    if col == 'Yext ID' or col == 'Google ID':
                        if not dataframe[col].apply(lambda x: str(x).isdigit() if pd.notna(x) else True).all():
                            st.error(f"Error: Values in column '{col}' should be numbers and should all be greater than 3 digits in length.")
                            exitApp(2)

                df = dataframe.dropna()
                df = df.astype(str)
                st.write(df)
        # os.write(1, df.to_string().encode() + b"\n")
        return df
    
def parseFile(df, field):
    if field != 'Update Primary Category':
        listGoogleIds = df['Google ID'].tolist()
        listYextIds = df['Yext ID'].tolist()
    else:
        listGoogleIds = df['Google ID'].tolist()
        listYextIds = []
    return listYextIds, listGoogleIds

def exitApp(inp):
    if inp == 1:
        st.error("Need authorization token!")
        sys.exit(1)    
    elif inp == 2:
        sys.exit(1)
    elif inp == 3:
        st.error("Need a Google account number!")
        sys.exit(1)
    return

def authErrors(response):
    try:
        if 'invalid authentication credentials' in response['error']['message']:
            exitApp(1)
        elif 'Failed for ' in response:
            st.error('Need authorization token!')
            os.write(1,  f"{response}\n".encode()) # This should print the external ID that failed
            exitApp(1)
    except:
        return 0
    
def makeDf(firstKey, apiResponse):
    resp = apiResponse[firstKey]
    return pd.DataFrame(resp)
    
def dfCols(df, *columns):
    column_data = {col: df[col].values for col in columns}
    return pd.DataFrame(column_data)

async def loopThroughIds(accountId, endpoint, id, headers):
    response = 0
    if endpoint == 'placeActionLinks':
        response  = placeActionGetCall(id, headers)
    elif endpoint == 'Social Posts':
        response = await localPostGet(accountId, id, headers)
    elif endpoint == 'All FAQs' or endpoint == 'Dupe FAQs':
        response = await getQuestions(id, headers)
    elif endpoint == 'Photos':
        response = getPhotosCall(accountId, id, headers)
    authStatus = authErrors(response)
    if authStatus == 0:
        return response
    return 0

def deleteHours(externalId, headers):
    fullApi = f'https://mybusinessbusinessinformation.googleapis.com/v1/locations/{str(externalId)}?updateMask=moreHours'
    patchData = {'moreHours': []}
    r_info = requests.patch(fullApi, data = patchData, headers = headers)
    df = pd.DataFrame(columns = ['Google Location ID', 'API Response Code'])
    df.loc[0] = [externalId, r_info.status_code]
    return df

def localPostGetCall(accountId, externalId, headers):
    base = 'https://mybusiness.googleapis.com/v4/accounts/' + str(accountId) + '/locations/'
    end = str(externalId) + '/localPosts?pageSize=100'
    fullApi = base + end
    r_info = requests.get(fullApi, headers = headers)
    responseCode = r_info.status_code
    if responseCode != 200:
        if responseCode == 404:
            return 'Could not find location ' + str(externalId)
        elif responseCode == 401:
            return 'Need authorization token for ' + str(externalId) + '!'
        return 'Failed for ' + str(externalId)
    response = r_info.json()
    try:
        temp = response['localPosts']
    except: 
        return 'No localPosts for ' + str(externalId)
    
    df = pd.DataFrame(temp)
    return df

def parseLocalPostsResponse(accountNum, df, externalId, filterType, filterData, myRange):
    accountStr = 'accounts/' + str(accountNum) + '/locations/' + str(externalId) + '/localPosts/'
    df['name'] = df['name'].str.replace(str(accountStr), '')

    df = dfCols(df, 'name', 'summary', 'createTime', 'topicType', 'state', 'languageCode')

    # Search for posts that meet the criteria
    if filterType == 'createTime':
        try:
            df['createTime'] = pd.to_datetime(df['createTime']).dt.tz_localize(None)
            filterData = pd.to_datetime(filterData)
            filtered_df = filterByDate(df, myRange, 'createTime', filterData)
        except ValueError as e:
            # print("Error:", e)
            return []

    elif filterType == 'Key Text Search':
        filtered_df = filterByKeyText(df, filterData, 'summary')

    postList = filtered_df['name'].tolist()
    return postList

def deletePost(accountId, postIdList, externalId, heads):
    baseApi = 'https://mybusiness.googleapis.com/v4/accounts/' + str(accountId) + '/locations/'
    df = pd.DataFrame(columns = ['Google Location ID', 'localPostId', 'API Response Code'])
    os.write(1,  f"{len(postIdList)} posts to delete on location ID: {externalId}, account ID {accountId}\n".encode())

    with requests.Session() as session:
        for postId in postIdList:
            call = f"{baseApi}{externalId}/localPosts/{postId}"
            r_info = session.delete(call, headers = heads)
            response = r_info.status_code
            df.loc[len(df)] = [externalId, str(postId), response]
    return df

def filterByKeyText(df, filterData, apiFieldKey):
    filtered_df = df[df[apiFieldKey].str.contains(filterData, na = False)]
    return filtered_df

def placeActionGetCall(id, heads):
    call = 'https://mybusinessplaceactions.googleapis.com/v1/locations/'
    additional = '/placeActionLinks/?pageSize=100'
    r_info = requests.get(call + str(id) + additional, headers = heads).json()
    return r_info

def parsePlaceActionResponse(apiResponse, id, filterOption, typeFilter, filterData, myRange):
    try:
        df = makeDf('placeActionLinks', apiResponse)
        
        df = dfCols(df, 'name', 'placeActionType', 'uri', 'createTime', 'updateTime', 'providerType')

        df['createTime'] = df['createTime'].astype(str)
        df['createTime'] = pd.to_datetime(df['createTime'])
        if filterOption == 'placeActionType':
            if typeFilter == 'All':
                filtered_df = df[df['providerType'] == 'MERCHANT']
            else:
                filtered_df = df[df[filterOption] == typeFilter]
        elif filterOption == 'uri':
            filtered_df = filterByKeyText(df, filterData, 'uri')
        elif filterOption == 'createTime':
            filterData = pd.to_datetime(filterData).date()
            filtered_df = filterByDate(df, myRange, 'createTime', filterData)

        retList = []
        for i in range(len(filtered_df)):
            locName = 'locations/' + str(id) + '/placeActionLinks/'
            result_string = filtered_df.iloc[i].iloc[0].split(locName)[1]
            retList.append(result_string)
    except: 
        return 0
    return retList

def deleteLink(locationId, placeActionIdList, heads):
    base = 'https://mybusinessplaceactions.googleapis.com/v1/locations/'
    additional = '/placeActionLinks/'
    df = loopAndDelete(locationId, placeActionIdList, heads, base, additional)
    return df

def filterByDate(df, option, columnName, filterData):
    filterData = pd.to_datetime(filterData).date()
    df[columnName] = pd.to_datetime(df[columnName])
    df[columnName] = df[columnName].dt.floor('s')

    if option == 'Before':
        filtered_df = df[df[columnName].dt.date < filterData]
    elif option == 'On or Before':
        filtered_df = df[df[columnName].dt.date <= filterData]
    elif option == 'After':
        filtered_df = df[df[columnName].dt.date > filterData]
    else:
        filtered_df = df[df[columnName].dt.date >= filterData]
    return filtered_df

def writeLogs(name, dfLog):
    logCsv = dfLog.to_csv(index = False)
    return logCsv

# def progress(numRows):
#     progress_text = "Operation in progress. Please wait."
#     my_bar = st.progress(0, text = progress_text)
#     for i in range(numRows):
#         time.sleep(0.01)
#         my_bar.progress((i + 1) / numRows)
#     st.write('Task completed!')
#     my_bar.empty()
#     return

def fieldSpecificInfo(field):
    if field == 'Dupe FAQs':
        myStr = f'This only deletes questions posted by the merchant.\nIt will ensure that there is not more than 1 of the same FAQ on each listing. It will NOT delete all FAQs.'
    elif field == 'Logo':
        myStr = f'This will update logos for the provided IDs.'
    elif field == 'Update Primary Category':
        myStr = f'This will update the primary category to "Key duplication service" for the provided IDs. Let TPM know if another category is needed. Would not be difficult to update to other categories.'
    elif field == 'Get VOM':
        myStr = f'This will get the verification options for the provided IDs.'
    else:
        myStr = f'This will delete {field} that match the filter from each listing.'
    return myStr

def useWarnings():
    st.warning('Please be careful, the actions by this tool cannot be undone! This app is for internal use only and should not be shared with customers. In most cases, this tool only checks the first 100 results that Google returns.', icon = "⚠️")
    st.info('If you have a problem uploading a file, check the error messages, refresh the page, and try again. If you have an authorization token issue, contact Pubops to get a token.', icon = "ℹ️")
    return

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

def deleteAllQuestions(df, locationId, heads):
    logDf = pd.DataFrame(columns = ['Google Location ID', 'Question ID', 'API Response Code'])
    if df.empty:
        logDf = pd.concat([logDf, pd.DataFrame([{'Google Location ID': locationId, 'Question ID': 'No questions to delete', 'API Response Code': 200}])], ignore_index=True)
        return logDf
    base = f'https://mybusinessqanda.googleapis.com/v1/locations/{locationId}/questions/'
    
    if len(df[df.columns[0]].values.tolist()) > 0:
        for i in df[df.columns[0]].values.tolist():
            questionId = i.replace(f'locations/{locationId}/questions/', '')
            r_info = requests.delete(f'{base}{questionId}', headers = heads)
            logDf.loc[len(logDf)] = [locationId, f'Deleting question {questionId}', r_info.status_code]
    else:
        logDf = logDf.append({'Google Location ID': locationId, 'Question ID': 'No questions to delete', 'API Response Code': 200}, ignore_index=True)
    return logDf

def parseQuestions(df, id, filterOption, filterData, myRange):
    try:
        df = dfCols(df, 'name', 'text', 'createTime', 'updateTime')
        df['createTime'] = df['createTime'].astype(str)
        df['updateTime'] = df['updateTime'].astype(str)
        df['createTime'] = pd.to_datetime(df['createTime'])
        df['updateTime'] = pd.to_datetime(df['updateTime'])

        if filterOption == 'createTime':
            filterData = pd.to_datetime(filterData).date()
            filtered_df = filterByDate(df, myRange, 'createTime', filterData)

        duplicates = filtered_df[filtered_df.duplicated(subset = ['text'], keep = 'first')]
        dupeVals = duplicates['name'].tolist()
        dupeVals = [value.replace('locations/' + id + '/questions/', '') for value in dupeVals]
    except Exception as e: 
        os.write(1,  f"Exception! {e}\n".encode())
        return 0
    return dupeVals

def deleteDupeQuestions(locationId, questionIdList, heads):
    base = 'https://mybusinessqanda.googleapis.com/v1/locations/'
    additional = '/questions/'
    df = loopAndDelete(locationId, questionIdList, heads, base, additional)
    return df

def loopAndDelete(externalId, targetIdList, heads, base, additional):
    df = pd.DataFrame(columns = ['Google Location ID', 'Target ID', 'API Response Code'])
    if targetIdList == 0:
        df.loc[len(df)] = [externalId, f'No duplicates for {externalId}', -1]
    else:
        for i in range(len(targetIdList)):
            call = base + str(externalId) + additional + targetIdList[i]
            r_info = requests.delete(call, headers = heads)
            response = r_info.status_code
            if response == 401:
                exitApp(1)
            df.loc[len(df)] = [externalId, targetIdList[i], response]
    return df

def getPhotosCall(accountId, externalId, headers):
    url = f'https://mybusiness.googleapis.com/v4/accounts/{accountId}/locations/{externalId}/media?pageSize=500'
    r_info = requests.get(url, headers = headers)
    responseCode = r_info.status_code
    if responseCode != 200:
        if responseCode == 404:
            return 'Could not find location ' + str(externalId)
        elif responseCode == 401:
            return 'Need authorization token for ' + str(externalId) + '!'
        return 'Failed for ' + str(externalId)
    response = r_info.json()
    try:
        temp = response['mediaItems']
    except: 
        return 'No mediaItems for ' + str(externalId)
    
    df = pd.DataFrame(temp)
    return df

def parseMedia(accountNum, df, externalId, filterType, filterData, myRange):
    accountStr = 'accounts/' + str(accountNum) + '/locations/' + str(externalId) + '/media/'
    
    if 'name' not in df.columns:
        os.write(1, f"Error: 'name' column does not exist in the DataFrame".encode())
        return []

    df['name'] = df['name'].str.replace(str(accountStr), '')    
    df = dfCols(df, 'name', 'sourceUrl', 'mediaFormat', 'googleUrl', 'thumbnailUrl', 'createTime')

    # Search for posts that meet the criteria
    if filterType == 'createTime':
        try:
            df['createTime'] = pd.to_datetime(df['createTime']).dt.tz_localize(None)
            filterData = pd.to_datetime(filterData)
            filtered_df = filterByDate(df, myRange, 'createTime', filterData)
        except ValueError as e:
            return []
    elif filterType == 'sourceUrl':
        try:
            filtered_df = filterByKeyText(df, filterData, 'sourceUrl')
        except ValueError as e:
            os.write(1, f"ValueError: {str(e)}\n".encode())
            return []

    mediaList = filtered_df['name'].tolist()
    return mediaList

def deleteMedia(accountId, mediaIdList, externalId, heads):
    baseApi = f'https://mybusiness.googleapis.com/v4/accounts/{accountId}/locations/{externalId}/media/'
    df = pd.DataFrame(columns = ['Google Location ID', 'Media ID', 'API Response Code'])
    for mediaId in mediaIdList:
        call = f"{baseApi}{mediaId}"
        os.write(1, f"Deleted {mediaId}\n".encode())
        r_info = requests.delete(call, headers = heads)
        response = r_info.status_code
        df.loc[len(df)] = [externalId, str(mediaId), response]
    return df

def deleteLogo(accountId, externalId, heads):
    baseApi = f'https://mybusiness.googleapis.com/v4/accounts/{accountId}/locations/{externalId}/media/profile'
    df = pd.DataFrame(columns = ['Google Location ID', 'Media ID', 'API Response Code'])
    r_info = requests.delete(baseApi, headers = heads)
    response = r_info.status_code
    df.loc[len(df)] = [externalId, f'Delete to /profile', response]
    return df

def postLogo(accountId, externalId, heads, logoSource):
    baseApi = f'https://mybusiness.googleapis.com/v4/accounts/{accountId}/locations/{externalId}/media'
    df = pd.DataFrame(columns = ['Google Location ID', 'Media ID', 'API Response Code'])
    body = f'''{{
        "locationAssociation": {{"category": "PROFILE"}},
        "mediaFormat": "PHOTO",
        "sourceUrl": "{logoSource}"
    }}'''
    r_info = requests.post(baseApi, headers = heads, json = json.loads(body))
    response = r_info.status_code
    response_json = r_info.json()
    if response == 200:
        responseInfo = response_json.get('name', 'Unknown')
    else:
        responseInfo = r_info.text
    df.loc[len(df)] = [externalId, f'Post logo', response]
    return df

def deleteMenu(accountId, externalId, heads):
    baseApi = f'https://mybusiness.googleapis.com/v4/accounts/{accountId}/locations/{externalId}/foodMenus'
    df = pd.DataFrame(columns = ['Google Location ID', 'Menu', 'API Response Code'])
    body = f'''{{
        "name": "accounts/{accountId}/locations/{externalId}/foodMenus",
        "menus": []
    }}'''
    r_info = requests.patch(baseApi, headers = heads, json = json.loads(body))
    response = r_info.status_code
    df.loc[len(df)] = [externalId, f'PATCH empty menu', response]
    return df

def deleteServiceItems(externalId, heads):
    baseApi = f'https://mybusinessbusinessinformation.googleapis.com/v1/locations/{externalId}?updateMask=serviceItems'
    df = pd.DataFrame(columns = ['Google Location ID', 'Menu', 'API Response Code'])
    body = f'''{{
        "name": "locations/{externalId}",
        "serviceItems": []
    }}'''
    r_info = requests.patch(baseApi, headers = heads, json = json.loads(body))
    response = r_info.status_code
    df.loc[len(df)] = [externalId, f'PATCH empty service items', response]
    return df

def updatePrimaryCategory(externalId, heads):
    baseApi = f'https://mybusinessbusinessinformation.googleapis.com/v1/locations/{externalId}?updateMask=categories,categories.additionalCategories,categories.primaryCategory,categories.primaryCategory.name'
    df = pd.DataFrame(columns = ['Google Location ID', 'API Response Code'])
    body = {
        "categories": {
            "primaryCategory": {
                "name": "categories/gcid:key_duplication_service",
                "displayName": "Key duplication service" 
                },
        "additionalCategories":[]
        }
}
    r_info = requests.patch(baseApi, headers = heads, json = body)
    response = r_info.status_code
    os.write(1,  f"{externalId} got {response}\n".encode())
    response_json = r_info.json()
    if response == 200:
        responseInfo = response_json.get('name', 'Unknown')
    else:
        responseInfo = r_info.text
    df.loc[len(df)] = [externalId, response]
    return df

def getVom(externalId, heads):
    baseApi = f'https://mybusinessverifications.googleapis.com/v1/locations/{externalId}/VoiceOfMerchantState'
    df = pd.DataFrame(columns = ['Google Location ID', 'responseBody', 'API Response Code'])
    r_info = requests.get(baseApi, headers = heads)
    response = r_info.status_code
    response_text = r_info.text
    if response == 200:
        # responseInfo = response_json.get('verificationOptions', 'Unknown')
        responseInfo = response_text
    else:
        responseInfo = r_info.text
    df.loc[len(df)] = [externalId, str(responseInfo), response]
    return df

def varElseNone(var):
    if var:
        return var
    return None

async def main():
    # st.session_state.state_dict = {}
    if 'count' not in st.session_state:
        st.session_state.count = 0


    st.set_page_config(
        page_title = "Google Locations"
    )
    useWarnings()

    locationLog = pd.DataFrame()
    if check_password():
        # streamlit_analytics.start_tracking()
        st.title("Google Locations")
        
        my_dict = {
                "Place Action Links": ["placeActionType", "uri", "createTime"], 
                "Social Posts": ["createTime", "Key Text Search"], 
                "Dupe FAQs": ["createTime"],
                # "All FAQs": ["All"],
                "Photos": ["createTime", "sourceUrl"],
                "moreHours": ["All"], 
                "Logo": ["Logo"],
                "Menu": ["All"],
                "Get Verification Options": ["All"],
                "Update Primary Category": ["All"],
                "Service Items": ["All"],
                "Get VOM": ["All"]
            }
        col1, col2 = st.columns([2, 1])

        with col1:
            field = st.selectbox("Choose field", options = my_dict.keys(), key = 1)
        with col2:
            filterOption = st.selectbox("Choose filter option", options = my_dict[field], key = 2)
        st.write(fieldSpecificInfo(field))

        with st.form("Form"):
            frame = uploadFile(field)
            filterData = ''
            daterange = ''
            placeActionTypeFilter = ''
            if field == 'Social Posts' or field == 'Photos' or field == 'Logo' or field == 'Menu':
                googleAccountNum = st.text_input("Enter the Google account number (all locations must be in the same account):")
            else:
                googleAccountNum = 0
            if filterOption == 'createTime':
                daterange = st.radio(
                    "Select time filter",
                    ('Before', 'On or Before', 'After', 'On or After'))
                filterData = st.date_input("What date should we use? (You can use a date in the future):", value = None)
            elif filterOption == 'placeActionType':
                placeActionTypeFilter = st.radio(
                    "Select place action type",
                    ('All', 'APPOINTMENT', 'DINING_RESERVATION', 'FOOD_TAKEOUT', 'ONLINE_APPOINTMENT', 'SHOP_ONLINE', 'FOOD_ORDERING', 'FOOD_DELIVERY'))
            elif filterOption == 'Logo':
                logoSourceUrl = st.text_input("Enter the URL of the logo you want to upload:")
            else: 
                if field != 'All FAQs' and field != 'moreHours' and field != 'Menu' and field != 'Get Verification Options' and field != 'Update Primary Category' and field != 'Service Items' and field != 'Get VOM':
                    filterData = st.text_input("Enter filter (this is case sensitive):") # This would be for key text search

            token = st.text_input("Enter Google API Authorization token (No 'Bearer' included. Should start with 'ya29.'):")
            if field == 'Logo':
                form_submitted = st.form_submit_button("Update Logos")
            elif field == 'Get Verification Options':
                form_submitted = st.form_submit_button("Get Verification Options")
            elif field == 'Update Primary Category':
                form_submitted = st.form_submit_button("Update Primary Category")
            elif field == 'Get VOM':
                form_submitted = st.form_submit_button("Get VOM")
            else:
                form_submitted = st.form_submit_button("Delete " +  field)
 
        if form_submitted:
            os.write(1,  f"{field}\n".encode())
            listYextIds, listGoogleIds = parseFile(frame, field)
            dfLog = pd.DataFrame()

            headers = {"Authorization": "Bearer " + token}
            # progress(len(frame.index))
            if field == 'Place Action Links':
                for i in listGoogleIds:
                    response = await loopThroughIds(googleAccountNum, 'placeActionLinks', i, headers)
                    placeActionsToDel = parsePlaceActionResponse(response, i, filterOption, placeActionTypeFilter, filterData, daterange)
                    locationLog = deleteLink(i, placeActionsToDel, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)
                    
            elif field == 'Social Posts':
                for i in listGoogleIds:
                    if googleAccountNum == '':
                        exitApp(3)
                    else:
                        response = await loopThroughIds(googleAccountNum, 'Social Posts', i, headers)
                        if isinstance(response, str):
                            if 'No localPosts for' in response:
                                locationLog = pd.DataFrame({'Google Location ID': [i], 'localPostId': [response], 'API Response Code': ['-1']})
                            elif 'Could not find location' in response:
                                locationLog = pd.DataFrame({'Google Location ID': [i], 'localPostId': [response], 'API Response Code': ['404']})
                            dfLog = pd.concat([dfLog, locationLog], ignore_index = True)
                            continue
                        elif not isinstance(response, pd.DataFrame):
                            st.error(response + '! Stopping.')
                            locationLog = pd.DataFrame({'Google Location ID': [i], 'localPostId': [response], 'API Response Code': ['Failed']})
                            dfLog = pd.concat([dfLog, locationLog], ignore_index = True)
                            break
                        postsToDel = parseLocalPostsResponse(googleAccountNum, response, i, filterOption, filterData, daterange)
                        locationLog = await asyncDeletePost(googleAccountNum, postsToDel, i, headers)
                        dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'Dupe FAQs':
                for i in listGoogleIds:
                    response = await loopThroughIds(googleAccountNum, field, i, headers)
                    error_detected = False
                    first_col_str = response[response.columns[0]].astype(str)
                    os.write(1,f'first_col_str is: {first_col_str}\n'.encode())
                    mask = first_col_str.str.contains("Failed starting with location", na=False)
                    if 'error_message' in response.columns:
                        mask = response['error_message'].astype(str).str.contains("Failed starting with location", na=False)
                        if mask.any():
                            error_detected = True

                    if error_detected:
                        if mask.any():
                            error_info = response.loc[mask, 'error_message'].iloc[0]
                        else:
                            error_info = f'Error for {i}. Check the logs and restart Streamlit.'
                        locationLog = pd.DataFrame(columns=['ID','Info','Code'])
                        locationLog.loc[len(locationLog)] = [i, error_info, -1]
                        dfLog = pd.concat([dfLog, locationLog], ignore_index=True)
                            # st.write(f'{error_info}. Remove earlier rows and restart Streamlit')
                            # break 
                    else:
                        dupeQuestions = parseQuestions(response, i, filterOption, filterData, daterange)
                        if dupeQuestions is not None: 
                            os.write(1,  f"{dupeQuestions}\n".encode())
                            locationLog = await asyncDeleteFaqs(i, dupeQuestions, headers)
                            dfLog = pd.concat([dfLog, locationLog], ignore_index = True)
                        else:
                            locationLog = pd.DataFrame([{'ID': i, 'Info': 'No duplicates found or parsing error', 'Code': 0}])
                            dfLog = pd.concat([dfLog, locationLog], ignore_index = True)       

            # elif field == 'All FAQs':
            #     for i in listGoogleIds:
            #         response = await loopThroughIds(googleAccountNum, field, i, headers)
            #         locationLog = deleteAllQuestions(response, i, headers)
            #         dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'Photos':
                for i in listGoogleIds:
                    response = await loopThroughIds(googleAccountNum, field, i, headers)
                    photosToDel = parseMedia(googleAccountNum, response, i, filterOption, filterData, daterange)
                    locationLog = deleteMedia(googleAccountNum, photosToDel, i, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'moreHours':
                for i in listGoogleIds:
                    locationLog = deleteHours(i, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)
                    
            elif field == 'Logo':
                for i in listGoogleIds:
                    locationLog = deleteLogo(googleAccountNum, i, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)
                    locationLog = postLogo(googleAccountNum, i, headers, logoSourceUrl)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'Menu':   
                for i in listGoogleIds:
                    locationLog = deleteMenu(googleAccountNum, i, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'Get Verification Options':
                dfLog = await getVOptions(listGoogleIds, headers)

            elif field == 'Update Primary Category':
                for i in listGoogleIds:
                    locationLog = updatePrimaryCategory(i, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'Service Items':
                for i in listGoogleIds:
                    locationLog = deleteServiceItems(i, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'Get VOM':
                for i in listGoogleIds:
                    locationLog = getVom(i, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            os.write(1,  f"Done!\n".encode())
            fileName = 'Streamlit_' + str(date.today()) + '_LogOutput.csv'
            logCsv = writeLogs(fileName, dfLog)
            
            downloadButton = st.download_button("Click to Download Logs", logCsv, file_name = fileName, mime = "text/csv", key = 'Download Logs')
            # db_ref = db.collection("appRuns")
            # new_doc_ref = db_ref.add({
            #     "field": str(field),
            #     "filter": str(filterOption),
            #     "placeActionType": str(placeActionTypeFilter),
            #     "timestamp": datetime.now(),
            #     "locationCount": len(frame.index),
            #     "filterData": varElseNone(str(filterData)),
            #     "daterange": varElseNone(str(daterange)),
            #     "googleAccountNum": varElseNone(googleAccountNum),
            #     "appName": "yextgooglelocs"
            # })
            
        # streamlit_analytics.stop_tracking(st.secrets["analyticsPass"])

if __name__ == "__main__":
    asyncio.run(main())