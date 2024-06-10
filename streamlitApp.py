import streamlit as st
# import snowflake.connector
import pandas as pd
import numpy as np
import json
import requests
from io import StringIO
import sys
from datetime import date
import os
import time
import sqlConnect_pymysqlConnection as db
import streamlit_analytics

def queryDB(database):
    connection = db.ConnectToYextDB('ops-sql01.tx1.yext.com', user = st.secrets["dbUsername"], password = st.secrets["dbPassword"])
    os.write(1,  f"{connection}\n".encode())
    result = connection.query_database(allIdsQuery(pd.DataFrame({'col1': [1, 2]})  ))
    os.write(1,  f"{result}\n".encode())
    connection.close_connection()
    return

def allIdsQuery(df):
    tempList = df[df.columns[0]].values.tolist()
    tempList = [4058034, 4058035]
    tempList = ', '.join(map(str, tempList))
    
    query = f"""
        select tl.location_id, tl.externalId from alpha.tags_listings tl
        where tl.partner_id = 715
        and tl.location_id in ({tempList})
    """
    os.write(1,  f"{query}\n".encode())
    return query

def getExternalIds(df):
    return df[df.columns[0]].values.tolist()

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

def uploadFile():
    exampleSheet = 'https://docs.google.com/spreadsheets/d/18tJfjrlZFd3qQT5ZTnz3eIw5v6KSILTBN-Ol9G7sFLo/edit#gid=0'
    # st.info('It\'s recommended to use a Google Sheet [in this format](%s) and download as a CSV.' % exampleSheet, icon = "ℹ️")
    uploaded_file = st.file_uploader("Provide a file with IDs. Make sure the CSV file has headers of \"Yext ID\" (first) and \"Google ID\" (second).", 
                                    type = ['csv', 'txt'],
                                    help = 'It\'s recommended to use a Google Sheet [in this format](%s) and download as a CSV.' % exampleSheet)
    if uploaded_file is not None:
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

                dataframe = dataframe.dropna()
                dataframe = dataframe.astype(str)
                st.write(dataframe)
        return dataframe
    
def parseFile(df):
    listGoogleIds = df['Google ID'].tolist()
    listYextIds = df['Yext ID'].tolist()
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

def loopThroughIds(accountId, endpoint, id, headers):
    if endpoint == 'placeActionLinks':
        response  = placeActionGetCall(id, headers)
    elif endpoint == 'Social Posts': # this isn't catching the 401 auth token errors. Place action works because it returns the code, but social post GET is returning a dataframe--- they might be getting caught now, not sure
        response = localPostGetCall(accountId, id, headers)
    elif endpoint == 'FAQs':
        response = getQuestions(id, headers)
    elif endpoint == 'Photos':
        response = getPhotosCall(accountId, id, headers)
    elif endpoint == 'moreHours':
        response = getMoreHoursCall(id, headers)
    authStatus = authErrors(response)
    if authStatus == 0:
        return response
    return 0

def getMoreHoursCall(externalId, headers):
    fullApi = f'https://mybusinessbusinessinformation.googleapis.com/v1/locations/{str(externalId)}?readMask=moreHours'
    r_info = requests.get(fullApi, headers = headers)
    
    responseCode = r_info.status_code
    os.write(1,  f"Calling {responseCode}\n".encode())
    if responseCode != 200:
        if responseCode == 404:
            return 'Could not find location ' + str(externalId)
        elif responseCode == 401:
            return 'Need authorization token for ' + str(externalId) + '!'
        return 'Failed for ' + str(externalId)
    response = r_info.json()
    try:
        temp = response['moreHours'][0]
    except: 
        return 'No moreHours for ' + str(externalId)
    
    df = pd.DataFrame(temp)
    os.write(1,  f"{df}\n".encode())
    # print(df)
    return df

def parseMoreHours(accountNum, df, externalId, filterType, filterData, myRange):
    # df = dfCols(df, 'name', 'hoursType', 'hours', 'createTime', 'updateTime')


    hoursList = filtered_df['name'].tolist()
    return hoursList

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
    filtered_df = df[df[apiFieldKey].str.contains(filterData)]
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
    # os.write(1,  f"{logCsv}\n".encode())
    return logCsv

def progress(numRows):
    progress_text = "Operation in progress. Please wait."
    my_bar = st.progress(0, text = progress_text)
    for i in range(numRows):
        time.sleep(0.01)
        my_bar.progress((i + 1) / numRows)
    st.write('Task completed!')
    my_bar.empty()
    return

def fieldSpecificInfo(field):
    if field == 'FAQs':
        myStr = f'This only deletes questions posted by the merchant.\nIt will ensure that there is not more than 1 of the same FAQ on each listing. It will NOT delete all FAQs.'
    else:
        myStr = f'This will delete {field} that match the filter from each listing.'
    return myStr

def useWarnings():
    st.warning('Please be careful, the actions by this tool cannot be undone! This app is for internal use only and should not be shared with customers. In most cases, this tool only checks the first 100 results that Google returns.', icon = "⚠️")
    st.info('If you have a problem uploading a file, check the error messages, refresh the page, and try again. If you have an authorization token issue, contact Pubops to get a token.', icon = "ℹ️")
    return

def getQuestions(id, heads):
    call = 'https://mybusinessqanda.googleapis.com/v1/locations/'
    additional = '/questions?pageSize=10&answersPerQuestion=10'
    url = f'{call}{str(id)}{additional}'
    all_data = []
    while url:
        response_json = requests.get(url, headers=heads).json()
        
        data = response_json.get('questions', [])
        nextPageToken = response_json.get('nextPageToken')
        
        all_data.extend(data)      
        if nextPageToken:
            url = f'{call}{str(id)}/questions?pageSize=10&pageToken={nextPageToken}&answersPerQuestion=10'
        else:
            url = None
    
    df = pd.DataFrame(all_data)
    return df

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
        os.write(1, f'Google ID: {id}, {len(dupeVals)}\n'.encode())
    except: 
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
    df['name'] = df['name'].str.replace(str(accountStr), '')

    df = dfCols(df, 'name', 'sourceUrl', 'mediaFormat', 'googleUrl', 'thumbnailUrl', 'createTime')

    # Search for posts that meet the criteria
    if filterType == 'createTime':
        try:
            df['createTime'] = pd.to_datetime(df['createTime']).dt.tz_localize(None)
            filterData = pd.to_datetime(filterData)
            filtered_df = filterByDate(df, myRange, 'createTime', filterData)
        except ValueError as e:
            # print("Error:", e)
            return []

    mediaList = filtered_df['name'].tolist()
    os.write(1,  f"{len(mediaList)}".encode())
    return mediaList

def deleteMedia(accountId, mediaIdList, externalId, heads):
    baseApi = f'https://mybusiness.googleapis.com/v4/accounts/{accountId}/locations/{externalId}/media/'
    df = pd.DataFrame(columns = ['Google Location ID', 'Media ID', 'API Response Code'])
    for mediaId in mediaIdList:
        call = f"{baseApi}{mediaId}"
        r_info = requests.delete(call, headers = heads)
        response = r_info.status_code
        df.loc[len(df)] = [externalId, str(mediaId), response]
    return df

if __name__ == "__main__":
    st.session_state.state_dict = {}

    st.set_page_config(
        page_title = "Google Locations"
    )
    useWarnings()
    if check_password():
        streamlit_analytics.start_tracking()
        st.title("Google Locations")
        
        my_dict = {
                "Place Action Links": ["placeActionType", "uri", "createTime"], 
                "Social Posts": ["createTime", "Key Text Search"], 
                "FAQs": ["createTime"],
                "Photos": ["createTime"],
                "moreHours": ["Access", "Breakfast", "Brunch", "Delivery", "Dinner", "Drive Thru", "Happy Hour", "Kitchen", "Takeout", "Senior"]
            }
        col1, col2 = st.columns([2, 1])

        with col1:
            field = st.selectbox("Choose field", options = my_dict.keys(), key = 1)
        with col2:
            filterOption = st.selectbox("Choose filter option", options = my_dict[field], key = 2)
        st.write(fieldSpecificInfo(field))

        with st.form("Form"):
            frame = uploadFile()
            filterData = ''
            daterange = ''
            placeActionTypeFilter = ''
            if field == 'Social Posts' or field == 'Photos':
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
            elif filterOption == 'FAQs':
                st.write('No selections needed.')
            else: 
                if field != 'moreHours':
                    filterData = st.text_input("Enter filter (this is case sensitive):") # This would be for key text search

            token = st.text_input("Enter Google API Authorization token (No 'Bearer' included. Should start with 'ya29.'):")
            form_submitted = st.form_submit_button("Delete " +  field)
 
        if form_submitted:
            os.write(1,  f"{field}\n".encode())
            listYextIds, listGoogleIds = parseFile(frame)
            dfLog = pd.DataFrame()

            headers = {"Authorization": "Bearer " + token}
            # progress(len(frame.index))
            if field == 'Place Action Links':
                for i in listGoogleIds:
                    response = loopThroughIds(googleAccountNum, 'placeActionLinks', i, headers)
                    placeActionsToDel = parsePlaceActionResponse(response, i, filterOption, placeActionTypeFilter, filterData, daterange)
                    locationLog = deleteLink(i, placeActionsToDel, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)
                    
            elif field == 'Social Posts':
                for i in listGoogleIds:
                    if googleAccountNum == '':
                        exitApp(3)
                    else:
                        # os.write(1,  f"{i}\n".encode())
                        response = loopThroughIds(googleAccountNum, 'Social Posts', i, headers)
                        # os.write(1,  f"{response}\n".encode())
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
                        locationLog = deletePost(googleAccountNum, postsToDel, i, headers)
                        dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'FAQs':
                for i in listGoogleIds:
                    response = loopThroughIds(googleAccountNum, field, i, headers)
                    dupeQuestions = parseQuestions(response, i, filterOption, filterData, daterange)
                    locationLog = deleteDupeQuestions(i, dupeQuestions, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'Photos':
                for i in listGoogleIds:
                    response = loopThroughIds(googleAccountNum, field, i, headers)
                    photosToDel = parseMedia(googleAccountNum, response, i, filterOption, filterData, daterange)
                    locationLog = deleteMedia(googleAccountNum, photosToDel, i, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            elif field == 'moreHours':
                for i in listGoogleIds:
                    response = loopThroughIds(googleAccountNum, field, i, headers)
                    # photosToDel = parseMoreHours(googleAccountNum, response, i, filterOption, filterData, daterange)
                    # locationLog = deleteMedia(googleAccountNum, photosToDel, i, headers)
                    # dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            os.write(1,  f"Done!\n".encode())
            fileName = 'Streamlit_' + str(date.today()) + '_LogOutput.csv'
            logCsv = writeLogs(fileName, dfLog)
            
            downloadButton = st.download_button("Click to Download Logs", logCsv, file_name = fileName, mime = "text/csv", key = 'Download Logs')
        streamlit_analytics.stop_tracking(st.secrets["analyticsPass"])
