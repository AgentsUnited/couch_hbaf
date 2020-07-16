#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 10:17:46 2019

@author: kostaskonsolakis
"""
from sklearn.model_selection import GridSearchCV
import pandas as pd
import os
import os.path
from os import listdir
import sklearn.externals 
import joblib
import numpy as np
import mysql.connector
from scipy.signal import butter, lfilter,argrelmax
import matplotlib.pyplot as plt
import math

from sklearn.svm import SVC
from sklearn.svm import LinearSVC
from datetime import datetime, timedelta, date
import time

print("hello world, short_term_funtion is running")
# =============================================================================
#    Timestamp Condition for data acquisition
# =============================================================================
def date_threshold1 (days_number):
    days_ago = datetime.now() - timedelta(days=days_number) #define the datetime for one day ago
    d = days_ago.strftime('%s.%f')
    d_in_ms = int(float(d)*1000)
    return d_in_ms
#    return days_ago.strftime("%s")
#    return days_ago.timestamp() * 1000 #for python version Python3

def date_threshold2(date_threshold1,server):
# Open database connection
    try:
        connectionObject = mysql.connector.connect(host=server[0], user =server[1], passwd=server[2], db=server[3] )

        # prepare a cursor object using cursor() method
        cursor = connectionObject.cursor()
        cursor.execute("SELECT Timestamp_Start FROM Physical_Behaviour ORDER BY ID DESC LIMIT 1")
        res = cursor.fetchone()
        res = res[0]
        s = '{:%Y/%m/%d %H:%M}'.format(res)
        threshold_physical = int(time.mktime(datetime.strptime(s, "%Y/%m/%d %H:%M").timetuple()))*1000

        #cursor = db.cursor()
        cursor.execute("SELECT Timestamp_Start FROM Social_Behaviour ORDER BY ID DESC LIMIT 1")
        res = cursor.fetchone()
        res = res[0]
        s = '{:%Y/%m/%d %H:%M}'.format(res)
        threshold_social = int(time.mktime(datetime.strptime(s, "%Y/%m/%d %H:%M").timetuple()))*1000

        cursor.close()
        connectionObject.close()

        if threshold_physical <= threshold_social:
            threshold2 = threshold_physical
        else:
            threshold2 = threshold_social
    except Exception as e:
        threshold2 = date_threshold1
        print("Exeception occured:{}".format(e))

    return threshold2
# =============================================================================
#  Physical Behaviour
# =============================================================================

def butter_lowpass(cutoff, fs, order):  # function for lowpass filtering
    nyq = 0.5 * fs
    normal_cutoff = cutoff / nyq
    b, a = butter(order, normal_cutoff, btype='low', analog=False)
    return b, a

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
            if current_state is 'above':
                crossing = [timestamps[i], threshold]
                crossings.append(crossing)
            else:
                crossing = [timestamps[i], threshold]
                crossings.append(crossing)

            crest_troughs += 1
        last_state = current_state

    return np.array(crossings)

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
def create_table(server):
# =============================================================================
# SQL CONNECTION
# =============================================================================

    ### Physical Behaviour###
    connectionObject = mysql.connector.connect(host=server[0], user =server[1], passwd=server[2], db=server[3] )
    try:
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        sqlQuery = """CREATE TABLE IF NOT EXISTS Physical_Behaviour (ID int(11) NOT NULL AUTO_INCREMENT, Timestamp_Start datetime NOT NULL, Key_id varchar(150) NOT NULL,
        User_id varchar(150) NOT NULL, Device_id varchar(150) NOT NULL, Steps int(11) DEFAULT NULL, activity_type int(11) DEFAULT NULL, confidence int(11) DEFAULT NULL,
        UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""

        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)
        # SQL query string
        sqlQuery = "show tables"
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)
        #Fetch all the rows
        rows = cursorObject.fetchall()

        for row in rows:
            print(row)

    except Exception as e:

        print("Exeception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()

    ### Social Behaviour###
    connectionObject = mysql.connector.connect(host=server[0], user =server[1], passwd=server[2], db=server[3] )
    try:
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Social_Behaviour (ID int(11) NOT NULL AUTO_INCREMENT, Timestamp_Start datetime NOT NULL, Key_id varchar(150) NOT NULL,
        User_id varchar(150) NOT NULL, Device_id varchar(150) NOT NULL, Detected_Social int(5) DEFAULT NULL, confidence int(11) DEFAULT NULL, Bluetooth int(5) DEFAULT NULL, Calls int(5) DEFAULT NULL, SMS int(5) DEFAULT NULL, Noise int(5) DEFAULT NULL, Google int(5) DEFAULT NULL,
        UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""

        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)
        # SQL query string
        sqlQuery = "show tables"
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)
        #Fetch all the rows
        rows = cursorObject.fetchall()

        for row in rows:
            print(row)

    except Exception as e:

        print("Exeception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()


    ### Logs for Physical Behaviour###
    connectionObject = mysql.connector.connect(host=server[0], user =server[1], passwd=server[2], db=server[3] )
    try:
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Logs_Physical (ID int(11) NOT NULL AUTO_INCREMENT, Key_id varchar(150) NOT NULL,
        Comment varchar(150) NOT NULL, UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""

        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)
        # SQL query string
        sqlQuery = "show tables"
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)
        #Fetch all the rows
        rows = cursorObject.fetchall()

        for row in rows:
            print(row)

    except Exception as e:

        print("Exeception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()


    ### Logs for Social Behaviour###
    connectionObject = mysql.connector.connect(host=server[0], user =server[1], passwd=server[2], db=server[3] )
    try:
        # Create a cursor object
        cursorObject = connectionObject.cursor()
        # SQL query string
        sqlQuery = """CREATE TABLE IF NOT EXISTS Logs_Social (ID int(11) NOT NULL AUTO_INCREMENT, Key_id varchar(150) NOT NULL,
        Comment varchar(150) NOT NULL, UNIQUE KEY ID (ID), PRIMARY KEY (Key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1"""

        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)
        # SQL query string
        sqlQuery = "show tables"
        # Execute the sqlQuery
        cursorObject.execute(sqlQuery)
        #Fetch all the rows
        rows = cursorObject.fetchall()

        for row in rows:
            print(row)

    except Exception as e:

        print("Exeception occured:{}".format(e))

    finally:

        cursorObject.close()
        connectionObject.close()



# =============================================================================
#   START Processing FOR Physical Behaviour Model with Counting Steps
# =============================================================================

def Physical_Behaviour_Model(deviceID, userID, df_acc, df_GOOGLE):
    # Change column names, parse dates and create magnitude column
    df_acc['x']=df_acc["double_values_0"]
    df_acc['y']=df_acc["double_values_1"]
    df_acc['z']=df_acc["double_values_2"]
    df_acc['magnitude'] = df_acc.apply(lambda row: math.sqrt((row.x)**2 + (row.y)**2 + (row.z)**2),axis=1)
    df_acc['time'] = pd.to_datetime(df_acc['timestamp'].astype(np.int64) ,unit='ms')
    df_acc = df_acc.set_index(df_acc['time'])

    # Convert Timezone
    df_acc.index = df_acc.index.tz_localize('UTC').tz_convert('Europe/Amsterdam')
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
    df_GOOGLE.index = df_GOOGLE.index.tz_localize('UTC').tz_convert('Europe/Brussels')
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
    Physical_Activity = Physical_Activity.replace({pd.np.nan: None})
    Physical_Activity = Physical_Activity[:-1] #delete last row with null value (due to above condition)
    #Physical_Activity = Physical_Activity.fillna(0) #condition to concert null to 0
    print ("End of Physical Script")
    return Physical_Activity


# =============================================================================
#   START Processing FOR Physical Behaviour Model without Counting Steps
# =============================================================================

def Physical_Behaviour_Model2(deviceID, userID, df_GOOGLE):

    #Google Location
    df_GOOGLE['time'] = pd.to_datetime(df_GOOGLE['timestamp'].astype(np.int64) ,unit='ms')
    df_GOOGLE = df_GOOGLE.set_index(df_GOOGLE['time'])
    df_GOOGLE.index = df_GOOGLE.index.tz_localize('UTC').tz_convert('Europe/Brussels')
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


    Physical_Activity = df_GOOGLE1.copy()
    Physical_Activity = Physical_Activity[['activity_type', 'confidence']]
    Physical_Activity["Timestamp_Start"] = Physical_Activity.index
    Physical_Activity["Device_id"] = deviceID
    Physical_Activity["User_id"] = userID
    Physical_Activity["period"] = Physical_Activity.index.strftime('%s')
    Physical_Activity["Key_id"] = Physical_Activity[['period', 'User_id']].apply(lambda x: '_'.join(x), axis=1)
    Physical_Activity = Physical_Activity.replace({pd.np.nan: None})
    Physical_Activity = Physical_Activity[:-1] #delete last row with null value (due to above condition)
    print ("End of Physical Script")
    return Physical_Activity


# The term bfill means that we use the value before filling in missing values
#y = y.fillna(y.bfill())

#==============================================================================
#     START Processing FOR Social Behaviour
#==============================================================================
def Social_Behaviour_Model(deviceID, userID, df_bluetooth, df_calls, df_messages, df_ambient_noise, df_GOOGLE):
    # Bluetooth
    df_bluetooth['time'] = pd.to_datetime(df_bluetooth['timestamp'].astype(np.int64), unit='ms')

    df_bluetooth = df_bluetooth.set_index(df_bluetooth['time'])
    df_bluetooth.index = df_bluetooth.index.tz_localize('UTC').tz_convert('Europe/Brussels')
    df_bluetooth.index= df_bluetooth.index.tz_localize(None)
    df_bluetooth1 = df_bluetooth.resample('1T').mean()

    df_bluetooth1["Bluetooth"] = 0
    for i in range(len(df_bluetooth1)):
        if (df_bluetooth1.bt_rssi[i] >-70) and (df_bluetooth1.bt_rssi[i] <0): #condition for 0<threshold<-70
            df_bluetooth1["Bluetooth"].iloc[[i]] = 1


    # Calls
    df_calls['time'] = pd.to_datetime(df_calls['timestamp'].astype(np.int64) ,unit='ms')
    df_calls = df_calls.set_index(df_calls['time'])
    df_calls.index = df_calls.index.tz_localize('UTC').tz_convert('Europe/Brussels')
    df_calls.index= df_calls.index.tz_localize(None)
    df_calls1 = df_calls.resample('1T').sum()

    df_calls1["Calls"] = 0

    for i in range(len(df_calls1)): #condition is missing for
        if (df_calls1.call_duration[i] >1) and (df_calls1.call_duration[i]) <60:
            df_calls1["Calls"].iloc[[i]] = 1
        if (df_calls1.call_duration[i] >=60) and (df_calls1.call_duration[i]) <120:
            df_calls1["Calls"].iloc[[i,i+1]] = 1
        if (df_calls1.call_duration[i] >=120) and (df_calls1.call_duration[i]) <180:
            df_calls1["Calls"].iloc[[i,i+1,i+2]] = 1
        if (df_calls1.call_duration[i] >=180) and (df_calls1.call_duration[i]) <240:
            df_calls1["Calls"].iloc[[i,i+1,i+2, i+3]] = 1
        if (df_calls1.call_duration[i] >=240) and (df_calls1.call_duration[i]) <300:
            df_calls1["Calls"].iloc[[i,i+1,i+2, i+3, i+4]] = 1
        if (df_calls1.call_duration[i] >=300) and (df_calls1.call_duration[i]) <360:
            df_calls1["Calls"].iloc[[i,i+1,i+2, i+3, i+4, i+5]] = 1
        if (df_calls1.call_duration[i] >=360) and (df_calls1.call_duration[i]) <420:
            df_calls1["Calls"].iloc[[i,i+1,i+2, i+3, i+4, i+5, i+6]] = 1
        if (df_calls1.call_duration[i] >=420) and (df_calls1.call_duration[i]) <480:
            df_calls1["Calls"].iloc[[i,i+1,i+2, i+3, i+4, i+5, i+6, i+7]] = 1


    # Messages
    df_messages['time'] = pd.to_datetime(df_messages['timestamp'].astype(np.int64) ,unit='ms')
    df_messages = df_messages.set_index(df_messages['time'])
    df_messages.index = df_messages.index.tz_localize('UTC').tz_convert('Europe/Brussels')
    df_messages.index= df_messages.index.tz_localize(None)
    df_messages1 = df_messages.resample('1T').sum()

    df_messages1["SMS"] = 0
    for i in range(len(df_messages1)):
        if (df_messages1.message_type[i] >2): #condition for both received and sent message
            df_messages1["SMS"].iloc[[i]] = 1


    #Ambient noise
    df_ambient_noise['time'] = pd.to_datetime(df_ambient_noise['timestamp'].astype(np.int64) ,unit='ms')
    df_ambient_noise = df_ambient_noise.set_index(df_ambient_noise['time'])
    df_ambient_noise.index = df_ambient_noise.index.tz_localize('UTC').tz_convert('Europe/Brussels')
    df_ambient_noise.index= df_ambient_noise.index.tz_localize(None)
    df_ambient_noise1 = df_ambient_noise.resample('1T').mean() #.sum

    df_ambient_noise1["Noise"] = 0
    for i in range(len(df_ambient_noise1)):
        if (df_ambient_noise1.double_decibels[i] >30) and (df_ambient_noise1.double_rms[i] >150): #decibel threshol > 20dB
#            df_ambient_noise1["Noise"].iloc[[i-4,i-3,i-2,i-1,i]] = 1
            df_ambient_noise1["Noise"].iloc[i] = 1
        if (df_ambient_noise1.double_rms[i] >200): #decibel threshol > 20dB
            df_ambient_noise1["Noise"].iloc[[i-4,i-3,i-2,i-1,i]] = 1 #condition for sampling ambient noise every 5'
#            df_ambient_noise1["Noise"].iloc[i] = 1 #condition for sampling ambient noise every 1'

    #Google Location
    df_GOOGLE['time'] = pd.to_datetime(df_GOOGLE['timestamp'].astype(np.int64) ,unit='ms')
    df_GOOGLE = df_GOOGLE.set_index(df_GOOGLE['time'])
    df_GOOGLE.index = df_GOOGLE.index.tz_localize('UTC').tz_convert('Europe/Brussels')
    df_GOOGLE.index= df_GOOGLE.index.tz_localize(None)
    #df_GOOGLE = df_GOOGLE.loc[df_GOOGLE['device_id'] == 'f4e55e15-49ce-411c-ae41-d47a76497fa5']
    df_GOOGLE1 = df_GOOGLE.copy()
    df_GOOGLE1.index = df_GOOGLE.index.round("T")

    df_GOOGLE1["Google"] = 0
    for i in range(len(df_GOOGLE1)):
        if (df_GOOGLE1.activity_type[i] ==0): #condition for the activity taking the Bus (Social Behaviour)
            df_GOOGLE1["Google"].iloc[[i]] = 1

    Social_Activity = pd.DataFrame()
    Social_Activity = pd.concat([Social_Activity, df_bluetooth1["Bluetooth"]], axis=1, join_axes=[df_bluetooth1.index])
    Social_Activity = pd.concat([Social_Activity, df_calls1["Calls"]], axis=1)
    Social_Activity = pd.concat([Social_Activity, df_messages1["SMS"]], axis=1)
    Social_Activity = pd.concat([Social_Activity, df_ambient_noise1["Noise"]], axis=1)
    Social_Activity = Social_Activity.join(df_GOOGLE1["Google"])

    Social_Activity = Social_Activity.fillna(0) # condition to convert null to 0 [WRONG approach since it should be mentioned when in NaN]

    Social_Activity["Detected_Social"] = 0
    Social_Activity["Detected_Social"] = Social_Activity.Bluetooth + Social_Activity.Calls + Social_Activity.SMS+ Social_Activity.Noise + Social_Activity.Google
    Social_Activity["confidence"] = Social_Activity["Detected_Social"] *20
    #missing confidence score when all values are null, then confidence score should be nan?
    Social_Activity1 = Social_Activity.resample('1T').mean() #avoid duplicates!!!!
    Social_Activity = Social_Activity1.copy()
    Social_Activity["Timestamp_Start"] = Social_Activity.index
    #Social_Activity ["Timestamp_End"] = Social_Activity.index + datetime.timedelta(minutes = 1)
    Social_Activity["Device_id"] = deviceID
    Social_Activity["User_id"] = userID
    Social_Activity["period"] = Social_Activity.index.strftime('%s')
    Social_Activity["Key_id"] = Social_Activity[['period', 'User_id']].apply(lambda x: '_'.join(x), axis=1)


    for i in range(len(Social_Activity)):
        if Social_Activity.Detected_Social[i] >0:
                Social_Activity.Detected_Social[i] = 1

    print ("End of Social Script")
    return Social_Activity

