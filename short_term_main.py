#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 11:54:23 2020

@author: kostaskonsolakis
"""

#import libraries
import itertools
import pandas as pd
import numpy as np
from pandas import datetime
import os
import os.path
import statsmodels.api as sm
import matplotlib.pyplot as plt
import ruptures as rpt
from pandas.plotting import register_matplotlib_converters
from os import listdir
from scipy.signal import butter, lfilter,argrelmax
import math
from datetime import datetime, timedelta
import mysql.connector
import json
import short_term_functions


# =============================================================================
# Specify Server and databass address for gaining access
# =============================================================================
#Sensitive information needs to be updated
host='aware-micro.ewi.utwente.nl'
user ='human_monitoring'
passwd='hee5eeYo'
db='human_monitoring'

server_read = [host, user, passwd, db]
server_store = ["linux442.ewi.utwente.nl","Short_Behaviour","Xj6kEQdF","ShortTerm_Behaviour"]


# =============================================================================
# Function for Read Data
# =============================================================================
"""
The function connects to the server and reads data from the smartphone

Input:
    - server route, smartphone='android', sensors='all', date1, date2, deviceID

Output:
"""
def read_smartphone_data(server, smartphone, sensors, date_threshold1, date_threshold2, deviceID):
    #Define df to return
    df_android_AR_2 = pd.DataFrame() 
    df_android_audio_2  = pd.DataFrame()
    df_device2 = pd.DataFrame()
    df_locations2 = pd.DataFrame()
    df_ESM2 = pd.DataFrame()
    df_calls2 = pd.DataFrame()
    df_messages2 = pd.DataFrame()
    email_id = 'None'
    
    # Open database connection
    connectionObject = mysql.connector.connect(host=server[0], user =server[1], passwd=server[2], db=server[3] )
    try:
        # prepare a cursor object using cursor() method
        cursorObject = connectionObject.cursor()
        
        if smartphone == "android":
            sql_android_AR = "SELECT * FROM human_monitoring.plugin_google_activity_recognition WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_android_AR_1 = pd.read_sql(sql_android_AR, connectionObject)
            df_android_AR_2 = df_android_AR_1['data'].apply(json.loads).apply(pd.Series)
    
            sql_android_audio = "SELECT * FROM human_monitoring.plugin_studentlife_audio_android WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_android_audio_1 = pd.read_sql(sql_android_audio, connectionObject)
            df_android_audio_2 = df_android_audio_1['data'].apply(json.loads).apply(pd.Series)
    
        elif smartphone == "ios":
            sql_ios_AR = "SELECT * FROM human_monitoring.plugin_ios_activity_recognition WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_ios_AR_1 = pd.read_sql(sql_ios_AR, connectionObject)
            df_ios_AR_2 = df_ios_AR_1['data'].apply(json.loads).apply(pd.Series)
    
            sql_ios_pedometer = "SELECT * FROM human_monitoring.plugin_ios_pedometer WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_ios_pedometer_1 = pd.read_sql(sql_ios_pedometer, connectionObject)
            df_ios_pedometer_2 = df_ios_pedometer_1['data'].apply(json.loads).apply(pd.Series)
    
            sql_ios_audio = "SELECT * FROM human_monitoring.plugin_studentlife_audio WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_ios_audio_1 = pd.read_sql(sql_ios_audio, connectionObject)
            df_ios_audio_2 = df_ios_audio_1['data'].apply(json.loads).apply(pd.Series)
        
    
        if sensors == "all":
            sql_device = "SELECT * FROM human_monitoring.aware_device WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_device1 = pd.read_sql(sql_device, connectionObject)
            # df_device1 = df_device.join(df_device['data'].apply(json.loads).apply(pd.Series))
            df_device2 = df_device1['data'].apply(json.loads).apply(pd.Series)
    
            sql_locations = "SELECT * FROM human_monitoring.locations WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_locations1 = pd.read_sql(sql_locations, connectionObject)
            df_locations2 = df_locations1['data'].apply(json.loads).apply(pd.Series)
    
            sql_ESM = "SELECT * FROM human_monitoring.esms WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_ESM1 = pd.read_sql(sql_ESM, connectionObject)
            df_ESM2 = df_ESM1['data'].apply(json.loads).apply(pd.Series)
            df_ESM3 = df_ESM2['esm_json'].apply(json.loads).apply(pd.Series)
            df_ESM2['esm_title'] = df_ESM3['esm_title']
    
            sql_calls = "SELECT * FROM human_monitoring.calls WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_calls1 = pd.read_sql(sql_calls, connectionObject)
            df_calls2 = df_calls1['data'].apply(json.loads).apply(pd.Series)
    
            sql_messages = "SELECT * FROM human_monitoring.messages WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_messages1 = pd.read_sql(sql_messages, connectionObject)
            df_messages2 = df_messages1['data'].apply(json.loads).apply(pd.Series)
            
            sql_bluetooth = "SELECT * FROM human_monitoring.bluetooth WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_bluetooth1 = pd.read_sql(sql_bluetooth, connectionObject)
            df_bluetooth2 = df_bluetooth1['data'].apply(json.loads).apply(pd.Series)
            
            sql_email = "SELECT * FROM human_monitoring.aware_device WHERE device_id='%s'"%deviceID
            df_email1 = pd.read_sql(sql_email, connectionObject)
            df_email2 = df_email1['data'].apply(json.loads).apply(pd.Series)
            email_id = df_email2['label'][0]
    
        elif sensors=="custom":
            sql_accelerometer = "SELECT * FROM human_monitoring.accelerometer WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_acc1 = pd.read_sql(sql_accelerometer, connectionObject)
            df_acc2 = df_acc1['data'].apply(json.loads).apply(pd.Series)
    
            # sql_aware_log = "SELECT * FROM human_monitoring.aware_log WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            # df_aware_log1 = pd.read_sql(sql_aware_log, connectionObject)
            # df_aware_log2 = df_aware_log1['data'].apply(json.loads).apply(pd.Series)
    
            # sql_ambient_noise = "SELECT * FROM human_monitoring.ambient_noise WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            # df_ambient_noise1 = pd.read_sql(sql_ambient_noise, connectionObject)
            # df_ambient_noise2 = df_ambient_noise1['data'].apply(json.loads).apply(pd.Series)
    
    except Exception as e:
        print("Exeception occured:{}".format(e))
    
    finally:
        cursorObject.close()
        connectionObject.close()
    #specify return dataframes
    return df_android_AR_2, df_android_audio_2, df_device2, df_locations2, df_ESM2, df_calls2, df_messages2, email_id       



# =============================================================================
# Function for Storing Processed Data
# =============================================================================
"""
The function connects to the server and uploads processed data

Input:
    - server route, data

Output:
"""
def store_processed_data(server):
    # =============================================================================
    # SQL Connection - Create a table for the first time (if there is not)
    # =============================================================================
    short_term_functions.create_table(server_store) #Create a table for the first time (if there is not)


    # =============================================================================
    # SQL Connection - Insert Data
    # =============================================================================

    connectionObject = mysql.connector.connect(host=server[0], user =server[1], passwd=server[2], db=server[3] )
    # prepare a cursor object using cursor() method
    cursor = connectionObject.cursor()

    for index,row in Physical_Activity.iterrows():

        sql="""INSERT INTO Physical_Behaviour(Timestamp_Start, Key_id, User_id, Device_id, Steps, activity_type, confidence)
        SELECT  %s, %s, %s, %s, %s, %s, %s FROM DUAL
        WHERE NOT EXISTS (SELECT Key_id FROM Physical_Behaviour WHERE Key_id=%s); """

        values = [row['Timestamp_Start'], row['Key_id'], email_id, row['Device_id'], row['Steps'], row['activity_type'], row['confidence'], row['Key_id'] ]
        cursor.execute(sql,values)
        connectionObject.commit()


    for index,row in Social_Activity.iterrows():

        sql="""INSERT INTO Social_Behaviour(Timestamp_Start, Key_id, User_id, Device_id, Detected_Social, confidence, Bluetooth, Calls, SMS, Noise, Google)
        SELECT  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM DUAL
        WHERE NOT EXISTS (SELECT Key_id FROM Social_Behaviour WHERE Key_id=%s); """

        values = [row['Timestamp_Start'], row['Key_id'], email_id, row['Device_id'], row['Detected_Social'], row['confidence'], row['Bluetooth'], row['Calls'], row['SMS'], row['Noise'], row['Google'],row['Key_id']  ]
        cursor.execute(sql,values)
        connectionObject.commit()

    sql="""INSERT INTO Logs_Physical (Key_id, Comment)
        SELECT  %s, %s FROM DUAL
        WHERE NOT EXISTS (SELECT Key_id FROM Logs_Physical WHERE Key_id=%s); """
    values = [Physical_Activity['Key_id'][-1], '%s rows inserted'%len(Physical_Activity), Physical_Activity['Key_id'][-1]  ]
    cursor.execute(sql,values)
    connectionObject.commit()

    sql="""INSERT INTO Logs_Social (Key_id, Comment)
        SELECT  %s, %s FROM DUAL
        WHERE NOT EXISTS (SELECT Key_id FROM Logs_Social WHERE Key_id=%s); """

    values = [Social_Activity['Key_id'][-1], '%s rows inserted'%len(Social_Activity) , Social_Activity['Key_id'][-1] ]
    cursor.execute(sql,values)
    connectionObject.commit()

    print (Physical_Activity['Key_id'][-1], 'and %s rows inserted / Physical Behaviour is ok'%len(Physical_Activity))
    print (Social_Activity['Key_id'][-1], 'and %s rows inserted / Social Behaviour is ok'%len(Social_Activity))

    cursor.close()
    connectionObject.close()


print("hello world, short_term_main is runnig")

# =============================================================================
#    Timestamp Condition for data acquisition
# =============================================================================
N=60 #define the datetime for one day ago
date_threshold1 = short_term_functions.date_threshold1(N) #calculates based on the timestamp N days ago
date_threshold2 = short_term_functions.date_threshold2(date_threshold1, server_store) #calculates based on the last timestamp at database


# =============================================================================
# list with user and device id
# =============================================================================
list_deviceID = ['e5c24c3c-151c-4cf5-ba0f-74852788afbc']
#list_deviceID = ['be0e0e59-59c3-421c-b66a-778a7459541f','8ab1d168-fd27-4fbb-b1d7-9a3bca3f9f82']

for deviceID in list_deviceID:
    #call function for smartphone="android" and sensors="all"
    df_AR, df_ambient_noise, df_device, df_locations, df_ESM, df_calls, df_messages, email_id =  read_smartphone_data(server_read, "android", "all", date_threshold1, date_threshold2, deviceID)

    userID = "user_%s"%(list_deviceID.index(deviceID)+1)
    #Physical_Activity = ShortTerm_Behaviour_Functions.Physical_Behaviour_Model(deviceID, userID, df_acc, df_AR)
    Physical_Activity = short_term_functions.Physical_Behaviour_Model2(deviceID, userID, df_AR) #without accelerometer-steps
    Social_Activity = short_term_functions.Social_Behaviour_Model(deviceID, userID, df_bluetooth, df_calls, df_messages, df_ambient_noise, df_AR)

    store_processed_data(server_store)
