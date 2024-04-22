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
# import streamlit_analytics

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
    uploaded_file = st.file_uploader("Provide a file with IDs")
    if uploaded_file is not None:
        dataframe = pd.read_csv(uploaded_file, dtype = {'Yext ID': str, 'Google ID': str})
        st.write(dataframe)
        return dataframe
    else:
        # st.error("Provide a file!")
        return -1
    
def parseFile(df):
    listGoogleIds = df['Google ID'].tolist()
    listYextIds = df['Yext ID'].tolist()
    return listYextIds, listGoogleIds

def authErrors(response):
    try:
        if 'invalid authentication credentials' in response['error']['message']:
            st.error('Need authorization token!')
            sys.exit(1)
    except:
        return 0

def loopThroughIds(accountId, endpoint, id, headers):
    if endpoint == 'placeActionLinks':
        response  = placeActionGetCall(id, headers)
    elif endpoint == 'Social Posts': # this isn't catching the 401 auth token errors. Place action works because it returns the code, but social post GET is returning a dataframe
        response = localPostGetCall(accountId, id, headers)
    authStatus = authErrors(response)
    if authStatus == 0:
        return response
    return 0

def localPostGetCall(accountId, externalId, headers):
    base = 'https://mybusiness.googleapis.com/v4/accounts/' + str(accountId) + '/locations/'
    end = str(externalId) + '/localPosts?pageSize=100'
    fullApi = base + end
    r_info = requests.get(fullApi, headers = headers)
    responseCode = r_info.status_code
    if responseCode != 200:
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
    os.write(1,  f"{df}\n".encode())
    df['name'] = df['name'].astype(str)

    df['name'] = df['name'].str.replace(str(accountStr), '')
    
    temp1 = df['name'].tolist()
    temp2 = df['summary'].tolist()
    temp3 = df['createTime'].tolist()
    temp4 = df['topicType'].tolist()
    temp5 = df['state'].tolist()
    temp6 = df['languageCode'].tolist()
    df = pd.DataFrame(list(zip(temp1, temp2, temp3, temp4, temp5, temp6)), columns = ['name', 'summary', 'createTime', 'topicType', 'state', 'languageCode'])

    # Search for posts that meet the criteria
    if filterType == 'createTime':
        df['createTime'] = pd.to_datetime(df['createTime']).dt.tz_localize(None)
        filterData = pd.to_datetime(filterData)
        filtered_df = filterByDate(df, myRange, 'createTime', filterData)
        
    elif filterType == 'Key Text Search':
        filtered_df = filterByKeyText(df, filterData, 'summary')

    postList = filtered_df['name'].tolist()
    return postList

def deletePost(accountId, postIdList, externalId, heads):
    baseApi = 'https://mybusiness.googleapis.com/v4/accounts/' + str(accountId) + '/locations/'
    df = pd.DataFrame(columns = ['Google Location ID', 'localPostId', 'API Response Code'])
    # os.write(1,  f"{postIdList}\n".encode())
    for i in range(len(postIdList)):
        call = baseApi + str(externalId) + '/localPosts/' + str(postIdList[i])
        r_info = requests.delete(call, headers = heads)
        response = r_info.status_code
        df.loc[i] = [externalId, str(postIdList[i]), response]
    return df

def filterByKeyText(df, filterData, apiFieldKey):
    filtered_df = df[df[apiFieldKey].str.contains(filterData)]
    return filtered_df

def placeActionGetCall(id, heads):
    call = 'https://mybusinessplaceactions.googleapis.com/v1/locations/'
    additional = '/placeActionLinks'
    r_info = requests.get(call + str(id) + additional, headers = heads).json()
    return r_info

def parsePlaceActionResponse(apiResponse, id, filterOption, typeFilter, filterData, myRange):
    try:
        prx = apiResponse['placeActionLinks']
        df = pd.DataFrame(prx)
        
        temp1 = df['name'].tolist()
        temp2 = df['placeActionType'].tolist()
        temp3 = df['uri'].tolist()
        temp4 = df['createTime'].tolist()
        temp5 = df['updateTime'].tolist()
        temp6 = df['providerType'].tolist()
        df = pd.DataFrame(list(zip(temp1, temp2, temp3, temp4, temp5, temp6)), columns = ['name', 'placeActionType', 'uri', 'createTime', 'updateTime', 'providerType'])
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
    df = pd.DataFrame(columns = ['Google Location ID', 'placeActionId', 'API Response Code'])

    for i in range(len(placeActionIdList)):
        call = base + str(locationId) + additional + placeActionIdList[i]
        r_info = requests.delete(call, headers = heads)
        response = r_info.status_code
        df.loc[i] = [locationId, placeActionIdList[i], response]
    # print(df)
    return df

def filterByDate(df, option, columnName, filterData):
    filterData = pd.to_datetime(filterData).date()
    # print('Using ' + str(filterData))
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

def progress():
    progress_text = "Operation in progress. Please wait."
    my_bar = st.progress(0, text = progress_text)
    for percent_complete in range(100):
        time.sleep(0.01)
        my_bar.progress(percent_complete + 1, text=progress_text)
    time.sleep(1)
    my_bar.empty()
    return

def useWarnings():
    st.warning('Please be careful, the actions by this tool cannot be undone! This app is for internal use only and should not be shared with customers.', icon = "⚠️")
    st.warning('In most cases, this tool only checks the first 100 results that Google returns.', icon = "⚠️")
    return

if __name__ == "__main__":
    # streamlit_analytics.stop_tracking()

    st.set_page_config(
        page_title = "Google Location Updates"
    )
    useWarnings()
    if check_password():
        st.title("Google Location Updates")
        
        my_dict = {
                "Place Action Links": ["placeActionType", "uri", "createTime"], 
                # "Hours": ["accessHours", "brunchHours"],
                "Social Posts": ["createTime", "Key Text Search"]
            }
        col1, col2 = st.columns([2, 1])

        with col1:
            field = st.selectbox("Choose field", options = my_dict.keys(), key = 1)
        with col2:
            filterOption = st.selectbox("Choose filter option", options = my_dict[field], key = 2)

        with st.form("Form"):
            frame = uploadFile()

            filterData = ''
            daterange = ''
            placeActionTypeFilter = ''
            if field == 'Social Posts':
                googleAccountNum = st.text_input("Enter the Google account number (all locations must be in the same account):")
            else:
                googleAccountNum = 0
            if filterOption == 'createTime':
                daterange = st.radio(
                    "Select place action type",
                    ('Before', 'On or Before', 'After', 'On or After'))
                filterData = st.date_input("What date should we use?", value = None)
            elif filterOption == 'placeActionType':
                placeActionTypeFilter = st.radio(
                    "Select place action type",
                    ('All', 'APPOINTMENT', 'DINING_RESERVATION', 'FOOD_TAKEOUT', 'ONLINE_APPOINTMENT', 'SHOP_ONLINE', 'FOOD_ORDERING', 'FOOD_DELIVERY'))
            else: 
                filterData = st.text_input("Enter filter (this is case sensitive):")

            token = st.text_input("Enter Google API Authorization token (No 'Bearer' included. Should start with 'ya29.'):")
            form_submitted = st.form_submit_button("Delete " +  field)
 
        if form_submitted:
            os.write(1,  f"{field}\n".encode())
            listYextIds, listGoogleIds = parseFile(frame)
            dfLog = pd.DataFrame()

            headers = {"Authorization": "Bearer " + token}
            progress()
            if field == 'Place Action Links':
                for i in listGoogleIds:
                    response = loopThroughIds(googleAccountNum, 'placeActionLinks', i, headers)
                    placeActionsToDel = parsePlaceActionResponse(response, i, filterOption, placeActionTypeFilter, filterData, daterange)
                    locationLog = deleteLink(i, placeActionsToDel, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)
                    
            elif field == 'Social Posts':
                for i in listGoogleIds:
                    response = loopThroughIds(googleAccountNum, 'Social Posts', i, headers)
                    if response == 'No localPosts for ' + i:
                        locationLog = pd.DataFrame({'Google Location ID': [i], 'localPostId': [response], 'API Response Code': [200]})
                        dfLog = pd.concat([dfLog, locationLog], ignore_index = True)
                        continue
                    postsToDel = parseLocalPostsResponse(googleAccountNum, response, i, filterOption, filterData, daterange)
                    # print(postsToDel) 
                    locationLog = deletePost(googleAccountNum, postsToDel, i, headers)
                    dfLog = pd.concat([dfLog, locationLog], ignore_index = True)

            fileName = 'Streamlit_' + str(date.today()) + '_LogOutput.csv'
            logCsv = writeLogs(fileName, dfLog)
            
            downloadButton = st.download_button("Click to Download Logs", logCsv, file_name = fileName, mime = "text/csv", key = 'Download Logs')

        # streamlit_analytics.stop_tracking()
