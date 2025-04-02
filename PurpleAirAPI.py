# -*- coding: utf-8 -*-
"""
Created on Fri Mar 28 09:54:45 2025

Download PurpleAir data from Love my Air Wisconsin using API key

Automatically applies both stanard & fire&smoke EPA corrections

@author: okorn
"""
#import helpful toolboxes etc
import requests
import os
import pandas as pd 
import numpy as np
#--------------------------------------------
#user inputs - edit this section only

#specific timestamps to pull in? if not, pulls entire dataset
tstamps = 'yes'

#put the start & end dates/times if so (mm/dd/yyyy HH:MM)
start_timestamp = '4/2/2025 9:00'
end_timestamp =  '4/2/2025 10:00'

#list of sensors to read in
#sensor_ids = ['189679', '237181', '217605', '217599', '217611', '189683', '189701', '217597', '217627', '189663']
sensor_ids = ['189679']

#get the api key for this project
API_KEY = '056020E1-0BF4-11F0-81BE-42010A80001F'

#specify the save directory for later
outPath = 'C:\\Users\\okorn\\Documents\\Sensor Database\\Love my Air Wisconsin'

#--------------------------------------------
#main body of code - don't edit below this line

for sensor in sensor_ids:
    
    if tstamps == 'yes':
        #Convert user-selected times to seconds / epoch
        start_timestamp = pd.to_datetime(start_timestamp, format='%m/%d/%Y %H:%M').timestamp()
        end_timestamp = pd.to_datetime(end_timestamp, format='%m/%d/%Y %H:%M').timestamp()
    
        #start reading in the data
        url = 'https://api.purpleair.com/v1/sensors/{}/history?fields=humidity_a,pm2.5_cf_1_a,pm2.5_cf_1_b&start_timestamp={}&end_timestamp={}'.format(sensor,start_timestamp,end_timestamp)
    else:
        #read in without timecaps if we want the entire archive
        url = 'https://api.purpleair.com/v1/sensors/{}/history?fields=humidity_a,pm2.5_cf_1_a,pm2.5_cf_1_b'.format(sensor)
   
    headers = {
        "X-API-Key": API_KEY,
        "Accept": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        #makes a dictionary with our data
        data = response.json()
        #Convert to dataframe - easier for csv later
        df = pd.DataFrame(data['data'], columns=data['fields'])
        #Change the datetime column name
        df.columns.values[0] = 'datetimeUTC'
        #Convert epoch to standard datetime
        df['datetimeUTC'] = pd.to_datetime(df['datetimeUTC'], unit='s')
        #Make the datetime the index
        df = df.set_index('datetimeUTC')
        #make sure it's in the right order
        df = df.sort_index()
        
    else:
        data = None
        print("Error:", response.status_code, response.text)   
    
    #-----------------------------------
    #now do a one-time API pull to grab the lat/lon
    #one-time url
    url2 = 'https://api.purpleair.com/v1/sensors/{}?fields=latitude,longitude'.format(sensor)
    
    response2 = requests.get(url2, headers=headers)
    
    if response2.status_code == 200:
        #makes a dictionary with our data
        latlon = response2.json()
        #copy the lat/lon over to our pre-existing dataframe
        df = df.join(pd.DataFrame([latlon['sensor']] * len(df), index=df.index))
            
    else:
        latlon = None
        print("Error:", response2.status_code, response2.text)   

    #-----------------------------------
    #apply the standard + fire&smoke corrections here
    
    #need the average of cf1 a&b
    df['pm2.5_cf_1_avg'] = (df['pm2.5_cf_1_a'] + df['pm2.5_cf_1_b'])/2
    #calculate the standard https://doi.org/10.5194/amt-14-4617-2021
    df['standard'] = (0.524*df['pm2.5_cf_1_avg'])-(0.0862*df['humidity_a'])+5.75
    
    #different zones for wildfire correction - based on raw readings
    #https://doi.org/10.3390/s22249669
    #Define conditions
    conditions = [(df['pm2.5_cf_1_avg'] < 570),
                  (df['pm2.5_cf_1_avg'] >= 570) & (df['pm2.5_cf_1_avg'] <= 611),
                  (df['pm2.5_cf_1_avg'] > 611)]

    #Define corresponding values based on the conditions
    values = [(df['pm2.5_cf_1_avg']*0.524) - (0.0862*df['humidity_a']) +5.75,  
              (0.0244*df['pm2.5_cf_1_avg'] - 13.9) * (df['pm2.5_cf_1_avg']**2 * 4.21e-4 + df['pm2.5_cf_1_avg']*0.392 +3.44) + (1-(0.0244*df['pm2.5_cf_1_avg'] - 13.9))*((df['pm2.5_cf_1_avg']*0.524) - (0.0862*df['humidity_a']) +5.75), 
              (df['pm2.5_cf_1_avg']**2 * 4.21e-4) + (df['pm2.5_cf_1_avg']*0.392) +3.44 ]

    #Apply the conditions
    df['wildfire'] = np.select(conditions, values)
    
    #-----------------------------------
    #save out the file
    filePath = os.path.join(outPath, 'PA_{}.csv'.format(sensor))
    df.to_csv(filePath, index=True)