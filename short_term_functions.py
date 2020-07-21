#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 10:17:46 2019

@author: kostaskonsolakis
"""
from sklearn.model_selection import GridSearchCV
import pandas as pd
import numpy as np
import os
import os.path
from os import listdir
#from sklearn.externals import joblib
import numpy
import mysql.connector
from scipy.signal import butter, lfilter,argrelmax
import matplotlib.pyplot as plt
import math

from sklearn.svm import SVC
from sklearn.svm import LinearSVC
from datetime import datetime, timedelta, date
import time
import json


# =============================================================================
# Server Connection for Reading Data from a database (based on user's input)
# =============================================================================
"""
The server_connection_read() function requests user's input with the server connection in order to read data on the server
Input:
    - server route
Output:
    - server route for reading data
"""
def server_connection_read(server):
    if server is None:
        print ('Please insert the server connection and the user credentials for reading raw data')
        host_read = input("Please enter the host:\n")
        user_read = input("Please enter the user:\n")
        passwd_read = input("Please enter the password:\n")
        db_read = input("Please enter the name of the database:\n")
        server_read = {'host': host_read, 'user': user_read, 'passwd_read': passwd_read, 'db': db_read }
    else:
        server_read = server
    return server_read

# =============================================================================
# Server Connection for Uploading Data to a database (based on user's input)
# =============================================================================
"""
The server_connection_store() function requests user's input with the server connection in order to upload data on the server
Input:
    - server route
Output:
    - server route for uploading data
"""
def server_connection_store(server):
    if server is None:
        print ('Please insert the server connection and the user credentials for uploading processed data')
        host_store = input("Please enter the host:\n")
        user_store = input("Please enter the user:\n")
        passwd_store = input("Please enter the password:\n")
        db_store = input("Please enter the name of the database:\n")
        server_store = {'host': host_store, 'user': user_store, 'passwd_read': passwd_store, 'db': db_store }
    else:
        server_store = server
    return server_store

"""
The device_log() function returns a dataframe with all the connected devices and users on the read server
Input:
    - server route for reading data
Output:
    - dataframe with all the connected devices and users on the read server
"""
def device_log(server):
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # prepare a cursor object using cursor() method
        cursorObject = connectionObject.cursor()
        
        sql_device = "SELECT * FROM human_monitoring.aware_device"
        df_device1 = pd.read_sql(sql_device, connectionObject)
        df_device2 = df_device1['data'].apply(json.loads).apply(pd.Series)
    
    except Exception as e:
        print("Exception occured:{}".format(e))
    
    finally:
        cursorObject.close()
        connectionObject.close()
    #specify return dataframes
    return df_device2      



# =============================================================================
#    Timestamp Condition for data acquisition
# =============================================================================
"""
The date_threshold1() function returns the timestamp which is equal to N days ago
Input:
    - days_number = N
Output:
    - 'threshold1' which is the timestamp for N days ago
"""
def date_threshold1 (days_number):
    days_ago = datetime.now() - timedelta(days=days_number) #define the datetime for one day ago
    d = days_ago.strftime('%s.%f')
    d_in_ms = int(float(d)*1000)
    return d_in_ms
#    return days_ago.strftime("%s")
#    return days_ago.timestamp() * 1000 #for python version Python3

"""
The date_threshold2() function returns the timestamp which is equal to the last row that is uploaded on the server_store
Input:
    - date_threshold1, server_store, deviceID
Output:
    - if data are uploaded for a certain user, then the output 'threshold2' is the timestamp for the last inserted value
      otherwise, the output 'threshold2' is equal to date_threshold1() function
"""
def date_threshold2(date_threshold1, server, deviceID):
# Open database connection
    threshold2 = date_threshold1
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # prepare a cursor object using cursor() method
        cursor = connectionObject.cursor()
        
        cursor.execute("SELECT Timestamp_Start FROM Physical_Behaviour WHERE device_id='%s'"%deviceID + "ORDER BY ID DESC LIMIT 1")
        res = cursor.fetchone()
        #condition for empty table
        if res is not None:
            res = res[0]
            s = '{:%Y/%m/%d %H:%M}'.format(res)
            threshold_physical = int(time.mktime(datetime.strptime(s, "%Y/%m/%d %H:%M").timetuple()))*1000
        else:
            threshold_physical = 0

        cursor.execute("SELECT Timestamp_Start FROM Social_Behaviour WHERE device_id='%s'"%deviceID + "ORDER BY ID DESC LIMIT 1")
        res = cursor.fetchone()
        #condition for empty table
        if res is not None:
            res = res[0]
            s = '{:%Y/%m/%d %H:%M}'.format(res)
            threshold_social = int(time.mktime(datetime.strptime(s, "%Y/%m/%d %H:%M").timetuple()))*1000
        else:
            threshold_social = 0
        
        cursor.execute("SELECT Timestamp_Start FROM Emotional_Behaviour WHERE device_id='%s'"%deviceID + "ORDER BY ID DESC LIMIT 1")
        res = cursor.fetchone()
        #condition for empty table
        if res is not None:
            res = res[0]
            s = '{:%Y/%m/%d %H:%M}'.format(res)
            threshold_emotional = int(time.mktime(datetime.strptime(s, "%Y/%m/%d %H:%M").timetuple()))*1000
        else:
            threshold_emotional = 0
        
        cursor.execute("SELECT Timestamp_Start FROM Cognitive_Behaviour WHERE device_id='%s'"%deviceID + "ORDER BY ID DESC LIMIT 1")
        res = cursor.fetchone()
        #condition for empty table
        if res is not None:
            res = res[0]
            s = '{:%Y/%m/%d %H:%M}'.format(res)
            threshold_cognitive = int(time.mktime(datetime.strptime(s, "%Y/%m/%d %H:%M").timetuple()))*1000
        else:
            threshold_cognitive = 0

        cursor.close()
        connectionObject.close()
        
        #checks the minimum timestamp among all the data
        threshold_array = [threshold_physical, threshold_social, threshold_emotional, threshold_cognitive ]
        
        if min(threshold_array) !=0 :
            threshold2 = min(threshold_array)
    
    except Exception as e:
        print("Exception occured:{}".format(e))

    return threshold2

# =============================================================================
#  Physical Behaviour
# =============================================================================
"""
The butter_lowpass() function calculates the b,a values for lowpass filtering in order to calculate the steps based on raw accelerometer data
Input:
    - cutoff, fs, order
Output:
    - b, a
"""
def butter_lowpass(cutoff, fs, order):  # function for lowpass filtering
    nyq = 0.5 * fs
    normal_cutoff = cutoff / nyq
    b, a = butter(order, normal_cutoff, btype='low', analog=False)
    return b, a

"""
The peak_accel_threshold() function calculates the peak acceleration threshold in order to calculate the steps based on raw accelerometer data
Input:
    - data, timestamps, threshold
Output:
    - crossings array with the peak acceleration thresholds 
"""
def peak_accel_threshold(data, timestamps, threshold):  # function for calculating peak acceleration
    last_state = 'below'
    crest_troughs = 0
    crossings = []
    
    for i, datum in enumerate(data):
        current_state = last_state
        if datum < threshold:
            current_state = 'below'  # below - less than threshold
        elif datum > threshold:
            current_state = 'above'  # above - above the threshold

        if current_state is not last_state:
            if current_state == 'above':
                crossing = [timestamps[i], threshold]
                crossings.append(crossing)
            else:
                crossing = [timestamps[i], threshold]
                crossings.append(crossing)

            crest_troughs += 1
        last_state = current_state

    return np.array(crossings)

"""
The counting_steps() function calculates the steps based on raw accelerometer data
Input:
    - df_acc
Output:
    - total_steps
"""
def counting_steps(df_acc):
    # Filter the data
    order = 3
    fs = 50.0  # sample rate 50Hz
    cutoff = 3.667  # desired cutoff frequency of the filter, Hz
    b, a = butter_lowpass(cutoff, fs, order)
    r = lfilter(b, a, df_acc['magnitude'])
    crossings = peak_accel_threshold(r, df_acc['time'], 12)
    total_steps =  len(crossings) / 2

    return total_steps


# =============================================================================
# SQL Connection - Check for empty tables & Table Create
# =============================================================================
"""
The create_table() function creates any missing tables on the server_store
Input:
    - server_store
Output:
    
"""
def create_table(server):

    ### Physical Behaviour###
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        sqlQuery = """CREATE TABLE IF NOT EXISTS Physical_Behaviour (ID int(11) NOT NULL AUTO_INCREMENT, Timestamp_Start datetime NOT NULL, Key_id varchar(150) NOT NULL,
        User_id varchar(150) NOT NULL, Device_id varchar(150) NOT NULL, Steps int(11) DEFAULT NULL, activity_type int(11) DEFAULT NULL, confidence int(11) DEFAULT NULL,
        UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)

    except Exception as e:

        print("Exception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()

    ### Social Behaviour###
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Social_Behaviour (ID int(11) NOT NULL AUTO_INCREMENT, Timestamp_Start datetime NOT NULL, Key_id varchar(150) NOT NULL,
        User_id varchar(150) NOT NULL, Device_id varchar(150) NOT NULL, Detected_Social int(5) DEFAULT NULL, confidence int(11) DEFAULT NULL, Bluetooth int(5) DEFAULT NULL, Calls int(5) DEFAULT NULL, SMS int(5) DEFAULT NULL, 
        Conversation int(5) DEFAULT NULL, Google int(5) DEFAULT NULL, ESM_Social_Minutes int(5) DEFAULT NULL,
        UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)

    except Exception as e:

        print("Exception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()

    ### Emotional Behaviour###
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Emotional_Behaviour (ID int(11) NOT NULL AUTO_INCREMENT, Timestamp_Start datetime NOT NULL, Key_id varchar(150) NOT NULL,
        User_id varchar(150) NOT NULL, Device_id varchar(150) NOT NULL, ESM_Emotional_Score int(5) DEFAULT NULL,
        UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)

    except Exception as e:

        print("Exception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()
        
    ### Cognitive Behaviour###
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Cognitive_Behaviour (ID int(11) NOT NULL AUTO_INCREMENT, Timestamp_Start datetime NOT NULL, Key_id varchar(150) NOT NULL,
        User_id varchar(150) NOT NULL, Device_id varchar(150) NOT NULL, ESM_Cognitive_Minutes int(5) DEFAULT NULL,
        UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)

    except Exception as e:

        print("Exception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()

    ### Logs for Physical Behaviour###
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Logs_Physical (ID int(11) NOT NULL AUTO_INCREMENT, Key_id varchar(150) NOT NULL,
        Comment varchar(150) NOT NULL, UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)

    except Exception as e:

        print("Exception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()


    ### Logs for Social Behaviour###
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Logs_Social (ID int(11) NOT NULL AUTO_INCREMENT, Key_id varchar(150) NOT NULL,
        Comment varchar(150) NOT NULL, UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)

    except Exception as e:

        print("Exception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()

    
    ### Logs for Emotional Behaviour###
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Logs_Emotional (ID int(11) NOT NULL AUTO_INCREMENT, Key_id varchar(150) NOT NULL,
        Comment varchar(150) NOT NULL, UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)

    except Exception as e:

        print("Exception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()
        
    ### Logs for Cognitive Behaviour###
    try:
        # Open database connection
        connectionObject = mysql.connector.connect(host=server.get('host'), user =server.get('user'), passwd=server.get('passwd'), db=server.get('db') )
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Logs_Cognitive (ID int(11) NOT NULL AUTO_INCREMENT, Key_id varchar(150) NOT NULL,
        Comment varchar(150) NOT NULL, UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)

    except Exception as e:

        print("Exception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()



# =============================================================================
#   START Processing FOR Physical Behaviour Model with Counting Steps
# =============================================================================
"""
The Physical_Behaviour_Model() function calculates the Physical_Activity dataframe based on steps and Activity Recognition (GOOGLE API AR)
Input:
    - deviceID, userID, df_acc, df_GOOGLE, df_timezone
Output:
    - Physical_Activity
"""
def Physical_Behaviour_Model(deviceID, userID, df_acc, df_GOOGLE, df_timezone):
    # Change column names, parse dates and create magnitude column
    df_acc['x']=df_acc["double_values_0"]
    df_acc['y']=df_acc["double_values_1"]
    df_acc['z']=df_acc["double_values_2"]
    df_acc['magnitude'] = df_acc.apply(lambda row: math.sqrt((row.x)**2 + (row.y)**2 + (row.z)**2),axis=1)
    df_acc['time'] = pd.to_datetime(df_acc['timestamp'].astype(np.int64) ,unit='ms')
    df_acc = df_acc.set_index(df_acc['time'])

    # Convert Timezone
    df_acc.index = df_acc.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
    df_acc.index= df_acc.index.tz_localize(None)

    steps = df_acc.resample('1T').sum() #resample used only for the timestamps and index - the rest of the values are calculated in a wrong way
    steps['Timestamp']=pd.to_datetime(steps.index)
    steps['Steps'] = np.nan
    df_acc['time'] = df_acc.index

    for i in range (len(steps)-1):
        df=df_acc[df_acc.time.between(steps['Timestamp'].iloc[i], steps['Timestamp'].iloc[i+1])]  #last value gets nan
        steps['Steps'].iloc[i+1] = counting_steps(df) #calls the function for steps counting


    #Google Location
    df_GOOGLE['time'] = pd.to_datetime(df_GOOGLE['timestamp'].astype(np.int64) ,unit='ms')
    df_GOOGLE = df_GOOGLE.set_index(df_GOOGLE['time'])
    df_GOOGLE.index = df_GOOGLE.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
    df_GOOGLE.index= df_GOOGLE.index.tz_localize(None)
    df_GOOGLE['time'] = df_GOOGLE.index
    df_GOOGLE['time_round'] = df_GOOGLE.index.round("T")
    df_GOOGLE1 = df_GOOGLE.copy()
#    GOOGLE_Test = df_GOOGLE1.resample('1T').sum()
    df_GOOGLE1.index = df_GOOGLE.index.round("T")

    #calculate activities based on the most performed during 1'
#    df_GOOGLE1['seconds'] = np.nan
#    for i in range (len(df_GOOGLE1)-1):
#        if df_GOOGLE1.time_round[i] == df_GOOGLE1.time_round[i+1]:
#            df_GOOGLE1['seconds'].iloc[i] = (df_GOOGLE1.time[i+1] - df_GOOGLE1.time[i]).total_seconds()
#            df_GOOGLE1['seconds'].iloc[i+1] = (df_GOOGLE1.time[i+2] - df_GOOGLE1.time[i+1]).total_seconds()
##            df_GOOGLE1['seconds'].iloc[i] = (df_GOOGLE1.time[i] - df_GOOGLE1.time[i-1]).total_seconds()
##            df_GOOGLE1['seconds'].iloc[i+1] = (df_GOOGLE1.time[i+1] - df_GOOGLE1.time[i]).total_seconds()


    df_GOOGLE1["Google"] = 0
    for i in range(len(df_GOOGLE1)):
        if (df_GOOGLE1.activity_type[i] ==0): #condition for the activity taking the Bus (Social Behaviour)
            df_GOOGLE1["Google"].iloc[[i]] = 1


    Physical_Activity = steps.copy()
    Physical_Activity = Physical_Activity[["Steps"]]
    Physical_Activity = Physical_Activity.join(df_GOOGLE1[['activity_type', 'confidence']])
    Physical_Activity["Timestamp_Start"] = Physical_Activity.index
    Physical_Activity["Device_id"] = deviceID
    Physical_Activity["User_id"] = userID
    Physical_Activity["period"] = Physical_Activity.index.strftime('%s')
    Physical_Activity["Key_id"] = Physical_Activity[['period', 'User_id']].apply(lambda x: '_'.join(x), axis=1)
    Physical_Activity = Physical_Activity.replace({pd.nan: None})
    Physical_Activity = Physical_Activity[:-1] #delete last row with null value (due to above condition)
    #Physical_Activity = Physical_Activity.fillna(0) #condition to concert null to 0
    print ("End of Physical Script")
    return Physical_Activity


# =============================================================================
#   START Processing FOR Physical Behaviour Model without Counting Steps
# =============================================================================
"""
The Physical_Behaviour_Model2() function calculates the Physical_Activity dataframe based on the Activity Recognition (GOOGLE API AR) data
Input:
    - deviceID, userID, df_GOOGLE, df_timezone
Output:
    - Physical_Activity
"""
def Physical_Behaviour_Model2(deviceID, userID, df_GOOGLE, df_timezone):

    #Google Location
    if df_GOOGLE.empty == True:
        df_GOOGLE1 = pd.DataFrame()
        df_GOOGLE1["Steps"] = 0
        df_GOOGLE1["activity_type"] = 0
        df_GOOGLE1["confidence"] = 0
    else:
        df_GOOGLE['Steps']=0 #steps are predefined with zero values
        df_GOOGLE['time'] = pd.to_datetime(df_GOOGLE['timestamp'].astype(np.int64) ,unit='ms')
        df_GOOGLE = df_GOOGLE.set_index(df_GOOGLE['time'])
        df_GOOGLE.index = df_GOOGLE.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
        df_GOOGLE.index= df_GOOGLE.index.tz_localize(None)
        df_GOOGLE['time'] = df_GOOGLE.index
        df_GOOGLE['time_round'] = df_GOOGLE.index.round("T")
        df_GOOGLE1 = df_GOOGLE.copy()
        df_GOOGLE1.index = df_GOOGLE.index.round("T")
    
        #calculate activities based on the most performed during 1'
    #    df_GOOGLE1['seconds'] = np.nan
    #    for i in range (len(df_GOOGLE1)-1):
    #        if df_GOOGLE1.time_round[i] == df_GOOGLE1.time_round[i+1]:
    #            df_GOOGLE1['seconds'].iloc[i] = (df_GOOGLE1.time[i+1] - df_GOOGLE1.time[i]).total_seconds()
    #            df_GOOGLE1['seconds'].iloc[i+1] = (df_GOOGLE1.time[i+2] - df_GOOGLE1.time[i+1]).total_seconds()
    ##            df_GOOGLE1['seconds'].iloc[i] = (df_GOOGLE1.time[i] - df_GOOGLE1.time[i-1]).total_seconds()
    ##            df_GOOGLE1['seconds'].iloc[i+1] = (df_GOOGLE1.time[i+1] - df_GOOGLE1.time[i]).total_seconds()
  
        df_GOOGLE1["Google"] = 0
        for i in range(len(df_GOOGLE1)):
            if (df_GOOGLE1.activity_type[i] ==0): #condition for the activity taking the Bus (Social Behaviour)
                df_GOOGLE1["Google"].iloc[[i]] = 1


    Physical_Activity = df_GOOGLE1.copy()
    Physical_Activity = Physical_Activity[['Steps','activity_type', 'confidence']]
    Physical_Activity["Timestamp_Start"] = Physical_Activity.index
    Physical_Activity["Device_id"] = deviceID
    Physical_Activity["User_id"] = userID
    Physical_Activity["period"] = Physical_Activity.index.strftime('%s')
    Physical_Activity["Key_id"] = Physical_Activity[['period', 'User_id']].apply(lambda x: '_'.join(x), axis=1)
    Physical_Activity = Physical_Activity.replace({np.nan: None})
    # Physical_Activity = Physical_Activity[:-1] #delete last row with null value (due to above condition)
    print ("End of Physical Script")
    return Physical_Activity



#==============================================================================
#     START Processing FOR Social Behaviour
#==============================================================================
"""
The Social_Behaviour_Model() function calculates the Social_Activity dataframe based on multimodal smartphone data and ESM questions
Specifically, it calculates the followings:
    - if there is an incoming/outgoing phone call per minute (0 for social isolation or 1 for social interaction), 
    - if there is a received/sent SMS per minute (0 for social isolation or 1 for social interaction), 
    - if there is an ongoing conversation per minute (0 for social isolation or 1 for social interaction), 
    - if there is an interaction in the vehicle, assuming that there is a social interaction in the car/bus with other passengers (0 for social isolation or 1 for social interaction), 
    - the total duration of being soccialy active based on the ESM questions (ranging from 0 to 5 hours)
Input:
    - deviceID, userID, df_calls, df_messages, df_conversations, df_GOOGLE, df_ESM, df_timezone
Output:
    - Social_Activity
"""
def Social_Behaviour_Model(deviceID, userID, df_calls, df_messages, df_conversations, df_GOOGLE, df_ESM, df_timezone):
    # # Bluetooth
    # df_bluetooth['time'] = pd.to_datetime(df_bluetooth['timestamp'].astype(np.int64), unit='ms')

    # df_bluetooth = df_bluetooth.set_index(df_bluetooth['time'])
    # df_bluetooth.index = df_bluetooth.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
    # df_bluetooth.index= df_bluetooth.index.tz_localize(None)
    # df_bluetooth1 = df_bluetooth.resample('1T').mean()

    # df_bluetooth1["Bluetooth"] = 0
    # for i in range(len(df_bluetooth1)):
    #     if (df_bluetooth1.bt_rssi[i] >-70) and (df_bluetooth1.bt_rssi[i] <0): #condition for 0<threshold<-70
    #         df_bluetooth1["Bluetooth"].iloc[[i]] = 1

    # Calls
    if df_calls.empty == True:
        df_calls1 = pd.DataFrame()
        df_calls1["Calls"] = 0
    else:
        df_calls['time'] = pd.to_datetime(df_calls['timestamp'].astype(np.int64) ,unit='ms')
        df_calls = df_calls.set_index(df_calls['time'])
        df_calls.index = df_calls.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
        df_calls.index= df_calls.index.tz_localize(None)
        df_calls['time'] = df_calls.index
        #convert string to int
        df_calls['call_duration'] = df_calls['call_duration'].astype(str).astype(int) 
        
        df_calls['call_start'] = df_calls.index.round("1T") - pd.to_timedelta(df_calls['call_duration'], unit='s') 
        df_calls['call_end']= df_calls.index.round("1T")
        call_start = df_calls.loc[df_calls['call_duration']>0 , 'call_start']
        call_end = df_calls.loc[df_calls['call_duration']>0 , 'call_end']
        
        df_calls1 = df_calls.copy()
        df_calls1 = df_calls1.resample('1T').sum()
        df_calls1["time"] = df_calls1.index
        df_calls1["Calls"] = 0
        
        for i in range(len(call_start)):
            mask = df_calls1.time.between(call_start[i], call_end[i])
            df_calls1.loc[mask, 'Calls'] = 1
  

    # Messages
    if df_messages.empty == True:
        df_messages1 = pd.DataFrame()
        df_messages1["SMS"] = 0
    else:
        df_messages['time'] = pd.to_datetime(df_messages['timestamp'].astype(np.int64) ,unit='ms')
        df_messages = df_messages.set_index(df_messages['time'])
        df_messages.index = df_messages.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
        df_messages.index= df_messages.index.tz_localize(None)
        df_messages['time'] = df_messages.index
        #convert string to int
        df_messages['message_type'] = df_messages['message_type'].astype(str).astype(int)
        
        df_messages1 = df_messages.resample('1T').sum()
        df_messages1["SMS"] = 0
        df_messages1.loc[df_messages1["message_type"]>2,"SMS"] = 1 


#     #Ambient noise based on Plugin ambient noise 
#     df_ambient_noise['time'] = pd.to_datetime(df_ambient_noise['timestamp'].astype(np.int64) ,unit='ms')
#     df_ambient_noise = df_ambient_noise.set_index(df_ambient_noise['time'])
#     df_ambient_noise.index = df_ambient_noise.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
#     df_ambient_noise.index= df_ambient_noise.index.tz_localize(None)
#     df_ambient_noise1 = df_ambient_noise.resample('1T').mean() #.sum

#     df_ambient_noise1["Noise"] = 0
#     for i in range(len(df_ambient_noise1)):
#         if (df_ambient_noise1.double_decibels[i] >30) and (df_ambient_noise1.double_rms[i] >150): #decibel threshol > 20dB
# #            df_ambient_noise1["Noise"].iloc[[i-4,i-3,i-2,i-1,i]] = 1
#             df_ambient_noise1["Noise"].iloc[i] = 1
#         if (df_ambient_noise1.double_rms[i] >200): #decibel threshol > 20dB
#             df_ambient_noise1["Noise"].iloc[[i-4,i-3,i-2,i-1,i]] = 1 #condition for sampling ambient noise every 5'
# #            df_ambient_noise1["Noise"].iloc[i] = 1 #condition for sampling ambient noise every 1'


    #Conversations based on audio Plugin StudentLife
    if df_conversations.empty == True:
        df_conversations1 = pd.DataFrame()
        df_conversations1["Conversation"] = 0
    else:
        df_conversations['time'] = pd.to_datetime(df_conversations['timestamp'].astype(np.int64) ,unit='ms')
        df_conversations["time_convo_start"] = pd.to_datetime(df_conversations["double_convo_start"].astype(np.int64) ,unit='ms')
        df_conversations["time_convo_end"] = pd.to_datetime(df_conversations["double_convo_end"].astype(np.int64) ,unit='ms')
        #removes any possible duplicates
        df_conversations = df_conversations.drop_duplicates('time',keep='first')
        df_conversations = df_conversations.set_index(df_conversations['time'])
        df_conversations.index = df_conversations.index.round("s")
        #convert string to int
        df_conversations['inference'] = df_conversations['inference'].astype(str).astype(int)
        df_conversations['datatype'] = df_conversations['datatype'].astype(str).astype(int)
        df_conversations["Conversation"] = 0
      
        convo_start = df_conversations.loc[df_conversations['datatype']==2 , "time_convo_start"]
        convo_end = df_conversations.loc[df_conversations['datatype']==2 , "time_convo_end"]
        
        for i in range(len(convo_start)):
            mask = df_conversations.time.between(convo_start[i], convo_end[i])
            df_conversations.loc[mask, 'Conversation'] = 1
        
        df_conversations1 = df_conversations.resample('1T').sum() #.sum
        df_conversations1.index = df_conversations1.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
        df_conversations1.index= df_conversations1.index.tz_localize(None)     
        df_conversations1.loc[df_conversations1["Conversation"]>0, 'Conversation'] = 1
             
         

    #Google Location
    if df_GOOGLE.empty == True:
        df_GOOGLE1 = pd.DataFrame()
        df_GOOGLE1["Google"] = 0
    else:
        df_GOOGLE['time'] = pd.to_datetime(df_GOOGLE['timestamp'].astype(np.int64) ,unit='ms')
        df_GOOGLE = df_GOOGLE.set_index(df_GOOGLE['time'])
        df_GOOGLE.index = df_GOOGLE.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
        df_GOOGLE.index= df_GOOGLE.index.tz_localize(None)
        df_GOOGLE.index = df_GOOGLE.index.round("1T")
        df_GOOGLE['time'] = df_GOOGLE.index
        df_GOOGLE1 = df_GOOGLE.copy()
        #removes any possible duplicates
        df_GOOGLE1 = df_GOOGLE1.drop_duplicates('time',keep='first')
    
        df_GOOGLE1["Google"] = 0
        for i in range(len(df_GOOGLE1)):
            if (df_GOOGLE1.activity_type[i] ==0): #condition for the activity taking the Bus (Social Behaviour)
                df_GOOGLE1["Google"].iloc[[i]] = 1
            
    #ESM Social
    if df_ESM.empty == True:
        df_ESM_Social1 = pd.DataFrame()
        df_ESM_Social1['ESM_Social_Minutes'] = 0
    else:
        df_ESM_Social = df_ESM.copy()
        df_ESM_Social['time'] = pd.to_datetime(df_ESM_Social['timestamp'].astype(np.int64) ,unit='ms')
        df_ESM_Social = df_ESM_Social.set_index(df_ESM_Social['time'])
        df_ESM_Social.index = df_ESM_Social.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
        df_ESM_Social.index= df_ESM_Social.index.tz_localize(None)
        df_ESM_Social['time'] = df_ESM_Social.index
        df_ESM_Social.index = df_ESM_Social.index.round("T")
        
        df_ESM_Social1 = df_ESM_Social.copy()
        mask = (df_ESM_Social['esm_title']== 'Q2: Social') | (df_ESM_Social['esm_title']== 'Q5: Social') | (df_ESM_Social['esm_title']== 'Q8: Social')
        df_ESM_Social1 = df_ESM_Social.loc[mask, ['esm_title', 'esm_user_answer', 'time']]
        #remove duplicates
        df_ESM_Social1 = df_ESM_Social1.drop_duplicates('time',keep='first')
        df_ESM_Social1['ESM_Social_Minutes'] = df_ESM_Social1['esm_user_answer']
        df_ESM_Social1['ESM_Social_Minutes'].replace('', '0', inplace=True)
        #convert string to int
        df_ESM_Social1['ESM_Social_Minutes'] = df_ESM_Social1['ESM_Social_Minutes'].astype(str).astype(int)
        #convert duration from hours to minutes
        df_ESM_Social1['ESM_Social_Minutes'] = df_ESM_Social1['ESM_Social_Minutes']*60
    

    Social_Activity = pd.DataFrame()
    Social_Activity = pd.concat([Social_Activity, df_conversations1["Conversation"]], axis=1)#, join_axes=[df_conversations1.index])
    # Social_Activity = pd.concat([Social_Activity, df_bluetooth1["Bluetooth"]], axis=1, join_axes=[df_bluetooth1.index])
    Social_Activity = pd.concat([Social_Activity, df_calls1["Calls"]], axis=1)
    Social_Activity = pd.concat([Social_Activity, df_messages1["SMS"]], axis=1)
    # Social_Activity = pd.concat([Social_Activity, df_ambient_noise1["Noise"]], axis=1)
    Social_Activity = pd.concat([Social_Activity, df_GOOGLE1["Google"]], axis=1)
    # Social_Activity = Social_Activity.join(df_GOOGLE1["Google"])
    Social_Activity = pd.concat([Social_Activity, df_ESM_Social1['ESM_Social_Minutes']], axis=1)
    Social_Activity = Social_Activity.fillna(0) # condition to convert null to 0 [WRONG approach since it should be mentioned when in NaN]
    #convert float to int
    Social_Activity['ESM_Social_Minutes'] = Social_Activity['ESM_Social_Minutes'].astype(float).astype(int)
    
    Social_Activity["Bluetooth"] = 0  #for the current sensors   
    Social_Activity["Detected_Social"] = 0
    Social_Activity["Detected_Social"] = Social_Activity['Calls'] + Social_Activity['SMS'] + Social_Activity['Conversation'] + Social_Activity['Google'] + Social_Activity["Bluetooth"]
    Social_Activity["confidence"] = Social_Activity["Detected_Social"] *20
    Social_Activity.loc[Social_Activity["Detected_Social"] >0, "Detected_Social"] =1
    Social_Activity["Timestamp_Start"] = pd.to_datetime(Social_Activity.index)
    #Social_Activity ["Timestamp_End"] = Social_Activity.index + datetime.timedelta(minutes = 1)
    Social_Activity["Device_id"] = deviceID
    Social_Activity["User_id"] = userID
    Social_Activity["period"] = pd.to_datetime(Social_Activity.index).strftime('%s')
    Social_Activity["Key_id"] = Social_Activity[['period', 'User_id']].apply(lambda x: '_'.join(x), axis=1)
    Social_Activity = Social_Activity.replace({np.nan: None})

    print ("End of Social Script")
    return Social_Activity


#==============================================================================
#     START Processing FOR Emotional Behaviour
#==============================================================================
"""
The Emotional_Behaviour_Model() function calculates the Emotional_Activity dataframe based on the ESM questions
Specifically, it calculates the emotional score (very negative, negative, neutral, happy, verry happy) based on the ESM questions that ranges from -2 to 2
Input:
    - deviceID, userID, df_ESM, df_timezone
Output:
    - Emotional_Activity
"""
def Emotional_Behaviour_Model(deviceID, userID, df_ESM, df_timezone):
    #ESM Emotional
    if df_ESM.empty == True:
        df_ESM_Emotional1 = pd.DataFrame()
        df_ESM_Emotional1['ESM_Emotional_Score'] = 0
    else:
        df_ESM_Emotional = df_ESM.copy()
        df_ESM_Emotional['time'] = pd.to_datetime(df_ESM_Emotional['timestamp'].astype(np.int64) ,unit='ms')
        df_ESM_Emotional = df_ESM_Emotional.set_index(df_ESM_Emotional['time'])
        df_ESM_Emotional.index = df_ESM_Emotional.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
        df_ESM_Emotional.index= df_ESM_Emotional.index.tz_localize(None)
        df_ESM_Emotional['time'] = df_ESM_Emotional.index
        df_ESM_Emotional.index = df_ESM_Emotional.index.round("T")
        
        df_ESM_Emotional1 = df_ESM_Emotional.copy()
        mask = (df_ESM_Emotional['esm_title']== 'Q1: Emotional') | (df_ESM_Emotional['esm_title']== 'Q4: Emotional') | (df_ESM_Emotional['esm_title']== 'Q7: Emotional')
        df_ESM_Emotional1 = df_ESM_Emotional.loc[mask, ['esm_title', 'esm_user_answer', 'time']]
        #remove duplicates
        df_ESM_Emotional1 = df_ESM_Emotional1.drop_duplicates('time',keep='first')
        df_ESM_Emotional1['ESM_Emotional_Score'] = df_ESM_Emotional1['esm_user_answer']
        df_ESM_Emotional1['ESM_Emotional_Score'].replace('', '0', inplace=True)
        #convert string to int
        df_ESM_Emotional1['ESM_Emotional_Score'] = df_ESM_Emotional1['ESM_Emotional_Score'].astype(str).astype(int)
    
    Emotional_Activity = df_ESM_Emotional1[['ESM_Emotional_Score']].copy()
    Emotional_Activity["Timestamp_Start"] = Emotional_Activity.index
    Emotional_Activity["Device_id"] = deviceID
    Emotional_Activity["User_id"] = userID
    Emotional_Activity["period"] = Emotional_Activity.index.strftime('%s')
    Emotional_Activity["Key_id"] = Emotional_Activity[['period', 'User_id']].apply(lambda x: '_'.join(x), axis=1)
    Emotional_Activity = Emotional_Activity.replace({np.nan: None})
    
    print ("End of Emotional Script")
    return Emotional_Activity

#==============================================================================
#     START Processing FOR Cognitive Behaviour
#==============================================================================
"""
The Cognitive_Behaviour_Model() function calculates the Cognitive_Activity dataframe based on the ESM questions
Specifically, it calculates the duration of being involved into cognitive tasks (such as reading, playing a board game, etc) based on the ESM questions (ranging from 0 to 5 hours)
Input:
    - deviceID, userID, df_ESM, df_timezone
Output:
    - Cognitive_Activity
"""
def Cognitive_Behaviour_Model(deviceID, userID, df_ESM, df_timezone):
    #ESM Cognitive
    if df_ESM.empty == True:
        df_ESM_Cognitive1 = pd.DataFrame()
        df_ESM_Cognitive1['ESM_Cognitive_Minutes'] = 0
    else:
        df_ESM_Cognitive = df_ESM.copy()
        df_ESM_Cognitive['time'] = pd.to_datetime(df_ESM_Cognitive['timestamp'].astype(np.int64) ,unit='ms')
        df_ESM_Cognitive = df_ESM_Cognitive.set_index(df_ESM_Cognitive['time'])
        df_ESM_Cognitive.index = df_ESM_Cognitive.index.tz_localize('UTC').tz_convert(df_timezone["timezone"][0])
        df_ESM_Cognitive.index= df_ESM_Cognitive.index.tz_localize(None)
        df_ESM_Cognitive['time'] = df_ESM_Cognitive.index
        df_ESM_Cognitive.index = df_ESM_Cognitive.index.round("T")
        
        df_ESM_Cognitive1 = df_ESM_Cognitive.copy()
        mask = (df_ESM_Cognitive['esm_title']== 'Q3: Cognitive') | (df_ESM_Cognitive['esm_title']== 'Q6: Cognitive') | (df_ESM_Cognitive['esm_title']== 'Q9: Cognitive')
        df_ESM_Cognitive1 = df_ESM_Cognitive.loc[mask, ['esm_title', 'esm_user_answer', 'time']]
        #remove duplicates
        df_ESM_Cognitive1 = df_ESM_Cognitive1.drop_duplicates('time',keep='first')
        df_ESM_Cognitive1['ESM_Cognitive_Minutes'] = df_ESM_Cognitive1['esm_user_answer']
        df_ESM_Cognitive1['ESM_Cognitive_Minutes'].replace('', '0', inplace=True)
        #convert string to int
        df_ESM_Cognitive1['ESM_Cognitive_Minutes'] = df_ESM_Cognitive1['ESM_Cognitive_Minutes'].astype(str).astype(int)
        #convert duration from hours to minutes
        df_ESM_Cognitive1['ESM_Cognitive_Minutes'] = df_ESM_Cognitive1['ESM_Cognitive_Minutes']*60
    
    Cognitive_Activity = df_ESM_Cognitive1[['ESM_Cognitive_Minutes']].copy()
    Cognitive_Activity["Timestamp_Start"] = Cognitive_Activity.index
    Cognitive_Activity["Device_id"] = deviceID
    Cognitive_Activity["User_id"] = userID
    Cognitive_Activity["period"] = Cognitive_Activity.index.strftime('%s')
    Cognitive_Activity["Key_id"] = Cognitive_Activity[['period', 'User_id']].apply(lambda x: '_'.join(x), axis=1)
    Cognitive_Activity = Cognitive_Activity.replace({np.nan: None})
    
    print ("End of Cognitive Script")
    return Cognitive_Activity


    
