#!/usr/bin/env python
# coding: utf-8

# https://www.visualcrossing.com/legacy/weather/weather-data-services#/editDataDefinition

# In[1]:


import pandas as pd
import numpy as np
import datetime
import os


# In[2]:


# Pull in current data file
current_data = pd.read_excel('Records.xlsx')
# Set new to false bc it's no longer new
current_data['New'] = False
current_data = current_data.astype({'Day': 'datetime64', 'Hour': int, 'Temperature': int})
current_data


# In[3]:


# Check if there's any missing dates
def check_missing(df):
    last_date_str = str(df.iloc[0]['Day'])
    last_date = datetime.date(year = int(last_date_str[:4]),
                              month = int(last_date_str[5:7]),
                              day = int(last_date_str[8:10]))
    first_date = datetime.date(year = 2022, month = 1, day = 1)
    
    # difference between days
    date_delta = last_date - first_date
    number_of_days = date_delta.days
    length = len(df) / 24
    
    return (number_of_days + 1) == length


# In[4]:


if not check_missing(current_data):
    raise Exception("Missing data")


# In[5]:


# Delete stitch counts for rows that have been finished
def update_count(df):
    for row in range(len(df)):
        done = df.iloc[row]['Finished']
        if done:
            df.at[row, 'Count'] = None
    return df


# In[6]:


# Run update_count
current_data = update_count(current_data)
current_data


# In[7]:


# Assuming new data was just downloaded from site, read from downloads folder
in_file = '../../../Downloads/history_data.csv'
data = pd.read_csv(in_file)
# Last row is first hour of next day, omit
data = data[['Date time', 'Temperature']][:-1]
data


# In[16]:


def pull_time(val):
    space_ind = val.index(' ')
    return val[space_ind + 1:-6]


# In[17]:


# Pull day and hour from date
data['Day'] = data['Date time'].str.slice(0, 10)
data['Hour'] = data['Date time'].map(lambda x: pull_time(x))


# In[19]:


# Cast data types
data = data.astype({'Day': 'datetime64', 'Hour': int, 'Temperature': float})


# In[20]:


# Only keep new data (after last date in current_data)
data = data[data['Day'] > current_data.iloc[0]['Day']]


# In[21]:


# Save data in data archive folder, named appropriately
first_date = data.iloc[0]['Date time'][:10].replace('/', '-')
last_date = data.iloc[-2]['Date time'][:10].replace('/', '-')
if first_date == last_date:
    out_file = 'Data/' + first_date + '.csv'
else:
    out_file = 'Data/' + first_date + ' to ' + last_date + '.csv'
data[['Date time', 'Temperature']].to_csv(out_file, index = False)


# In[22]:


if data.iloc[0]['Day'] - current_data.iloc[0]['Day'] != datetime.timedelta(days = 1):
    raise Exception("Missing data")


# In[23]:


# Get indices of null values to replace
nulls = list(data[data['Temperature'].isnull()].index)
print(len(nulls))


# In[24]:


# Function to determine the color for a given temperature
def pick_color(val):
    if val < 19:
        return 'Off White'
    elif val < 25:
        return 'Orchid'
    elif val < 31:
        return 'Soft Blue'
    elif val < 37:
        return 'Light Country Blue'
    elif val < 43:
        return 'Robin\'s Egg'
    elif val < 49:
        return 'Sage'
    elif val < 55:
        return 'Pistachio'
    elif val < 61:
        return 'Chartreuse'
    elif val < 67:
        return 'Lemonade'
    elif val < 73:
        return 'Gold'
    elif val < 79:
        return 'Neon Orange'
    elif val < 85:
        return 'Pumpkin'
    elif val < 91:
        return 'Red'
    return 'Autumn Red'


# In[25]:


# Add color values, mark new as true and finished as false, convert temps to ints
data['Color'] = data['Temperature'].map(lambda x: pick_color(x))
data['New'] = True
data['Finished'] = False


# In[26]:


# Sort new data by day decreasing (more recent at the top), and by hour increasing
data = data.sort_values(by = ['Day', 'Hour'], ascending = [False, True])
data


# In[27]:


# Function to count stitches
def number(df):
    count = 1
    df.at[0, 'Count'] = count
    for row in range(1, len(df)):
        if (df.iloc[row]['Day'] == df.iloc[row - 1]['Day']) and (df.iloc[row]['Color'] == df.iloc[row - 1]['Color']):
            count += 1
        else:
            count = 1
        df.at[row, 'Count'] = count
    return df


# In[28]:


data = data.reset_index()
data = number(data)
data


# In[29]:


# Combine new and old data
all_data = pd.concat([data, current_data], sort = False)\
[['Day', 'Hour', 'Temperature', 'Color', 'New', 'Finished', 'Count']]
all_data


# In[30]:


all_data.to_excel('Records.xlsx', index = False)


# In[31]:


# Delete input file from downloads
os.remove(in_file)


# In[ ]:




