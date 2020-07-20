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
# Specify Server and database address for gaining access
# =============================================================================
'''
Users input for server connection
Sensitive information and needs to be updated by the user
'''
server_read = None
server_store = None


# =============================================================================
# Function for Read Data
# =============================================================================
"""
The function connects to the server and reads data from the smartphone

Input:
    - server_read route, server_store route, smartphone='android', sensors='all', deviceID

Output:
    - df_android_AR, df_android_audio, df_locations, df_ESM, df_calls, df_messages, df_timezone
"""
def read_smartphone_data(server_read, server_store,  smartphone, sensors, deviceID):
    #Define df to return
    df_android_AR_2 = pd.DataFrame() 
    df_android_audio_2  = pd.DataFrame()
    df_locations2 = pd.DataFrame()
    df_ESM2 = pd.DataFrame()
    df_calls2 = pd.DataFrame()
    df_messages2 = pd.DataFrame()
    df_timezone2 = pd.DataFrame()
    
    #Timestamp condition for data acquisition for a certain deviceID
    N=1 #define the datetime for N=1 days ago
    date_threshold1 = short_term_functions.date_threshold1(N) #calculates based on the timestamp N days ago
    date_threshold2 = short_term_functions.date_threshold2(date_threshold1, server_store, deviceID) #calculates based on the last timestamp at database
    
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server_read.get('host'), user =server_read.get('user'), passwd=server_read.get('passwd'), db=server_read.get('db') )
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
            
            sql_timezone = "SELECT * FROM human_monitoring.timezone WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_timezone1 = pd.read_sql(sql_timezone, connectionObject)
            #condition to check the last timezone value in case the timestamp condition returns empty dataframe
            if df_timezone1.empty == True:
                sql_timezone = "SELECT * FROM human_monitoring.timezone WHERE device_id='%s'"%deviceID +"ORDER BY _id DESC LIMIT 1"
                df_timezone1 = pd.read_sql(sql_timezone, connectionObject)
            df_timezone2 = df_timezone1['data'].apply(json.loads).apply(pd.Series)
            
    
        elif sensors=="custom":
            sql_accelerometer = "SELECT * FROM human_monitoring.accelerometer WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_acc1 = pd.read_sql(sql_accelerometer, connectionObject)
            df_acc2 = df_acc1['data'].apply(json.loads).apply(pd.Series)
            
            sql_bluetooth = "SELECT * FROM human_monitoring.bluetooth WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_bluetooth1 = pd.read_sql(sql_bluetooth, connectionObject)
            df_bluetooth2 = df_bluetooth1['data'].apply(json.loads).apply(pd.Series)
            
            sql_ambient_noise = "SELECT * FROM human_monitoring.plugin_ambient_noise WHERE timestamp>=%s"%date_threshold1 + " AND timestamp>%s"%date_threshold2 + " AND device_id='%s'"%deviceID
            df_ambient_noise_1 = pd.read_sql(sql_ambient_noise, connectionObject)
            df_ambient_noise_2 = df_ambient_noise_1['data'].apply(json.loads).apply(pd.Series)
            
            # sql_email = "SELECT * FROM human_monitoring.aware_device WHERE device_id='%s'"%deviceID
            # df_email1 = pd.read_sql(sql_email, connectionObject)
            # df_email2 = df_email1['data'].apply(json.loads).apply(pd.Series)
            # email_id = df_email2['label'][0]
    
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
        print("Read data for DeviceID= "+deviceID)
    #specify return dataframes
    return df_android_AR_2, df_android_audio_2, df_locations2, df_ESM2, df_calls2, df_messages2, df_timezone2



# =============================================================================
# Function for Storing Processed Data
# =============================================================================
"""
The function connects to the server and uploads processed data

Input:
    - server route, email_id, Physical_Activity, Social_Activity, Emotional_Activity, Cognitive_Activity

Output:
"""
def store_processed_data(server_store, email_id, Physical_Activity, Social_Activity, Emotional_Activity, Cognitive_Activity):
    # =============================================================================
    # SQL Connection - Create a table for the first time (if there is not)
    # =============================================================================
    short_term_functions.create_table(server_store) #Create a table for the first time (if there is not)


    # =============================================================================
    # SQL Connection - Insert Data
    # =============================================================================
    
    #Physical Behaviour
    try:
         # Open database connection
        connectionObject = mysql.connector.connect(host=server_store.get('host'), user =server_store.get('user'), passwd=server_store.get('passwd'), db=server_store.get('db') )
        # prepare a cursor object using cursor() method
        cursorObject = connectionObject.cursor()
        for index,row in Physical_Activity.iterrows():
    
            sql="""INSERT INTO Physical_Behaviour(Timestamp_Start, Key_id, User_id, Device_id, Steps, activity_type, confidence)
            SELECT  %s, %s, %s, %s, %s, %s, %s FROM DUAL
            WHERE NOT EXISTS (SELECT Key_id FROM Physical_Behaviour WHERE Key_id=%s); """
    
            values = [row['Timestamp_Start'], row['Key_id'], email_id, row['Device_id'], row['Steps'], row['activity_type'], row['confidence'], row['Key_id'] ]
            cursorObject.execute(sql,values)
            connectionObject.commit()
            
        print("Store data for Physical_Activity")   
        
        sql="""INSERT INTO Logs_Physical (Key_id, Comment)
            SELECT  %s, %s FROM DUAL
            WHERE NOT EXISTS (SELECT Key_id FROM Logs_Physical WHERE Key_id=%s); """
        values = [Physical_Activity['Key_id'][-1], '%s rows inserted'%len(Physical_Activity), Physical_Activity['Key_id'][-1]  ]
        cursorObject.execute(sql,values)
        connectionObject.commit()

    except Exception as e:
        print("Exeception occured:{}".format(e))

    finally:
        cursorObject.close()
        connectionObject.close()
        print (Physical_Activity['Key_id'][-1], 'and %s rows inserted / Physical Behaviour is ok'%len(Physical_Activity))
        
    
    #Social Behaviour
    try:
         # Open database connection
        connectionObject = mysql.connector.connect(host=server_store.get('host'), user =server_store.get('user'), passwd=server_store.get('passwd'), db=server_store.get('db') )
        # prepare a cursor object using cursor() method
        cursorObject = connectionObject.cursor()
        for index,row in Social_Activity.iterrows():
    
            sql="""INSERT INTO Social_Behaviour(Timestamp_Start, Key_id, User_id, Device_id, Detected_Social, confidence, Bluetooth, Calls, SMS, Conversation, Google, ESM_Social_Minutes)
            SELECT  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM DUAL
            WHERE NOT EXISTS (SELECT Key_id FROM Social_Behaviour WHERE Key_id=%s); """
    
            values = [row['Timestamp_Start'], row['Key_id'], email_id, row['Device_id'], row['Detected_Social'], row['confidence'], row['Bluetooth'], row['Calls'], row['SMS'], row['Conversation'], row['Google'], row['ESM_Social_Minutes'], row['Key_id']  ]
            cursorObject.execute(sql,values)
            connectionObject.commit()
        print("Store data for Social_Activity")  
    
        sql="""INSERT INTO Logs_Social (Key_id, Comment)
            SELECT  %s, %s FROM DUAL
            WHERE NOT EXISTS (SELECT Key_id FROM Logs_Social WHERE Key_id=%s); """
    
        values = [Social_Activity['Key_id'][-1], '%s rows inserted'%len(Social_Activity) , Social_Activity['Key_id'][-1] ]
        cursorObject.execute(sql,values)
        connectionObject.commit()

    except Exception as e:
        print("Exeception occured:{}".format(e))

    finally:
        cursorObject.close()
        connectionObject.close()
        print (Social_Activity['Key_id'][-1], 'and %s rows inserted / Social Behaviour is ok'%len(Social_Activity))
   
    
    #Emotional Behaviour
    try:
         # Open database connection
        connectionObject = mysql.connector.connect(host=server_store.get('host'), user =server_store.get('user'), passwd=server_store.get('passwd'), db=server_store.get('db') )
        # prepare a cursor object using cursor() method
        cursorObject = connectionObject.cursor()
        for index,row in Emotional_Activity.iterrows():
    
            sql="""INSERT INTO Emotional_Behaviour(Timestamp_Start, Key_id, User_id, Device_id, ESM_Emotional_Score)
            SELECT  %s, %s, %s, %s, %s FROM DUAL
            WHERE NOT EXISTS (SELECT Key_id FROM Emotional_Behaviour WHERE Key_id=%s); """
    
            values = [row['Timestamp_Start'], row['Key_id'], email_id, row['Device_id'], row['ESM_Emotional_Score'], row['Key_id']  ]
            cursorObject.execute(sql,values)
            connectionObject.commit()
        print("Store data for Emotional_Activity")  
    
        sql="""INSERT INTO Logs_Emotional (Key_id, Comment)
            SELECT  %s, %s FROM DUAL
            WHERE NOT EXISTS (SELECT Key_id FROM Logs_Emotional WHERE Key_id=%s); """
    
        values = [Emotional_Activity['Key_id'][-1], '%s rows inserted'%len(Emotional_Activity) , Emotional_Activity['Key_id'][-1] ]
        cursorObject.execute(sql,values)
        connectionObject.commit()

    except Exception as e:
        print("Exeception occured:{}".format(e))

    finally:
        cursorObject.close()
        connectionObject.close()
        print (Emotional_Activity['Key_id'][-1], 'and %s rows inserted / Emotional Behaviour is ok'%len(Emotional_Activity)) 
        
        
        
    #Cognitive Behaviour
    try:
         # Open database connection
        connectionObject = mysql.connector.connect(host=server_store.get('host'), user =server_store.get('user'), passwd=server_store.get('passwd'), db=server_store.get('db') )
        # prepare a cursor object using cursor() method
        cursorObject = connectionObject.cursor()
        for index,row in Cognitive_Activity.iterrows():
    
            sql="""INSERT INTO Cognitive_Behaviour(Timestamp_Start, Key_id, User_id, Device_id, ESM_Cognitive_Minutes)
            SELECT  %s, %s, %s, %s, %s FROM DUAL
            WHERE NOT EXISTS (SELECT Key_id FROM Cognitive_Behaviour WHERE Key_id=%s); """
    
            values = [row['Timestamp_Start'], row['Key_id'], email_id, row['Device_id'], row['ESM_Cognitive_Minutes'], row['Key_id']  ]
            cursorObject.execute(sql,values)
            connectionObject.commit()
        print("Store data for Cognitive_Activity")  
    
        sql="""INSERT INTO Logs_Cognitive (Key_id, Comment)
            SELECT  %s, %s FROM DUAL
            WHERE NOT EXISTS (SELECT Key_id FROM Logs_Cognitive WHERE Key_id=%s); """
    
        values = [Cognitive_Activity['Key_id'][-1], '%s rows inserted'%len(Cognitive_Activity) , Cognitive_Activity['Key_id'][-1] ]
        cursorObject.execute(sql,values)
        connectionObject.commit()

    except Exception as e:
        print("Exeception occured:{}".format(e))

    finally:
        cursorObject.close()
        connectionObject.close()
        print (Cognitive_Activity['Key_id'][-1], 'and %s rows inserted / Cognitive Behaviour is ok'%len(Cognitive_Activity)) 
        
    print("Insert data for DeviceID= "+deviceID)
   
   
        
    
        
    
"""
main function which reads and uploads data for all the existing device ids 
"""   
if __name__ == '__main__':

    # =============================================================================
    # Define Server Connections
    # =============================================================================
    server_read = short_term_functions.server_connection_read(server_read)
    server_store = short_term_functions.server_connection_store(server_store)
    
    # =============================================================================
    # Device logs & list with deviceIDs and emailIDs
    # =============================================================================
    df_device = short_term_functions.device_log(server_read)
    # list_deviceID = df_device['device_id'].unique()
    list_deviceID = df_device['device_id'][5:7] # condition for testing purposes
    list_emailID = df_device['label'].unique()
    

    for deviceID in list_deviceID:
        
        print (deviceID)
        #call function to read smartphone data with parameters smartphone="android" and sensors="all"
        df_AR, df_conversations, df_locations, df_ESM, df_calls, df_messages, df_timezone =  read_smartphone_data(server_read, server_store, "android", "all", deviceID)
    
        userID = "user_%s"%(list_deviceID.tolist().index(deviceID)+1)
        emailID = df_device.loc[df_device['device_id'] ==  deviceID, 'label'].unique()
        #Physical_Activity = ShortTerm_Behaviour_Functions.Physical_Behaviour_Model(deviceID, userID, df_acc, df_AR, df_timezone)
        Physical_Activity = short_term_functions.Physical_Behaviour_Model2(deviceID, userID, df_AR, df_timezone) #without accelerometer-steps
        Social_Activity = short_term_functions.Social_Behaviour_Model(deviceID, userID, df_calls, df_messages, df_conversations, df_AR, df_ESM, df_timezone)
        Emotional_Activity = short_term_functions.Emotional_Behaviour_Model(deviceID, userID, df_ESM, df_timezone)
        Cognitive_Activity = short_term_functions.Cognitive_Behaviour_Model(deviceID, userID, df_ESM, df_timezone)
        
        #store function to upload processed data
        store_processed_data(server_store, emailID[0], Physical_Activity, Social_Activity, Emotional_Activity, Cognitive_Activity)
    
    
