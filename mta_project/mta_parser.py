"""
Created on Thu Sep 24 11:37:03 2015

@author: gregoryfriedman
"""

import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt

def get_file_names(start, end):
    list_of_files = []
    start_dow = start.weekday()
    #print start_dow
    start += (timedelta((5 - start_dow) % 7))
    while start < end:
        list_of_files.append('../turnstile_data_2015/turnstile_' + start.strftime('%y%m%d') + '.txt')
        start += timedelta(7)
    return list_of_files

def read_file(filename):
    cols = ['ca', 'unit', 'scp', 'station', 
            'linename', 'division', 'date', 'time', 
            'desc', 'entries', 'exits']
    traffic = pd.read_csv(filename, 
                          header = True, 
                          names = cols, 
                          converters = {'linename': lambda x: ''.join(sorted(x))})
    #print traffic.head()
    return traffic

def makeCols(df):
    '''
    Convert from cumulative entries/exits to entries/exits per measurement
    '''
    df['deltaEntries'] = df.groupby(['ca', 'unit', 'scp', 'station', ]).entries.diff()
    df['deltaExits'] = df.groupby(['ca', 'unit', 'scp', 'station']).exits.diff()
    df = clean_frame(df)
    return df

def clean_frame(df):
    '''
    Reorganize columns to:
    station-date (tuple), datetime(timestamp), date & time (strings), entries & exits (int)
    '''
    df2 = pd.DataFrame()
#    df2['stile'] = zip(df.ca, df.unit, df.scp, df.station)
    df2['station'] = zip(df.station, df.linename)
    df2['datetime'] = pd.to_datetime(df.date+df.time, format = '%m/%d/%Y%H:%M:%S')
    df2['date'] = df.date
    df2['time'] = df.time
    df2['entries'] = df.deltaEntries
    df2['exits'] = df.deltaExits
    df2 = df2[df2.entries < 5000]
    df2 = df2[df2.entries >= 0]
    df2 = df2[df2.exits < 5000]
    df2 = df2[df2.exits >= 0]
    return df2

def filter_times(df, start = 12, end = 20):
    """
    Returns all entries between start and end time, inclusive.
    """
    filtered = df[df['datetime'].apply(lambda x: x.hour >= start and x.hour < end)]
    return filtered

def n_busiest_stations(df, n):
    '''
    Returns a list of the 'n' stations with the most total exits over the entire Data Fram
    '''
    s= df.groupby('station')['exits'].agg(np.sum)
    return s.nlargest(n)

def aggregate_turnstiles(df):
    '''
    return a DataFrame that sums all exits per station for each recorded timestamp at that station
    '''
    count = df.groupby(['station', 'date', 'datetime'])['exits'].sum().reset_index()
    return count


def hourly_exits(df):
    '''
    create a column that tracks the hours between consecutive timestamps
    create a column that tracks exits per hour as an integer
    '''
    df = aggregate_turnstiles(df)
    df.reset_index(pd.DatetimeIndex(df['datetime'])) #setup datetime index
    df['delta'] = (df['datetime']-df['datetime'].shift()).fillna(0)
    df['delta'] = df['delta'].apply(lambda x: x  / np.timedelta64(1,'h')).astype('float64')
    df = df[df.delta > 1]

    #creates a column of hourly differences between timestamps
    df['exitshourly'] = (df['exits']/df['delta']).astype('int64') #create 'exits/hour' column
    df.reset_index()
    return df

def daily_exit_rate(df):
    '''
    create a DataFrame of the daily average exits/hour for each station ordered by date
    '''
    #return df.groupby(['station', 'date'])['exitshourly'].agg(np.mean)
    by_station = df.groupby(['station', 'date']).agg({'exitshourly': np.mean, 'exits': np.sum})
    return by_station

def exits_by_day(df):
    '''
    create a DataFrame that lists for a given date, the exit rate & cummulative daily exits
    for each station 
    '''

    by_date = df.groupby(['date', 'station']).agg({'exitshourly': np.mean, 'exits': np.sum})
    return by_date


def busiest_exits_by_day(df, date = '04/17/2015', n = 100):
    '''
    lists daily readings for top 'n' stations on given date

    '''
    station_list = exits_by_day(df)
    daily = station_list.ix[date].sort('exitshourly', ascending=False)[:n]
    return daily
    

def main():
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 200)

    list_of_files = []
    list_of_frames = []

    folder_path = '../turnstile_data_2015' #change to folder where mta data is located

    #the block below reads csv data as needed from the mta files & changes the columns to make analysis easier

    currDate = date(2015, 1, 3)
    while currDate < date(2015, 2, 1):
        list_of_files.append(folder_path'/turnstile_' + currDate.strftime('%y%m%d') + '.txt')
        currDate += timedelta(7)
    list_of_frames = [read_file(filename) for filename in list_of_files]
    big = pd.concat(list_of_frames, ignore_index = True)

    start = date(2015, 3, 1)
    end = date(2015, 6, 1)
    files = get_file_names(start, end)
    frames = [read_file(file) for file in files]
    big = pd.concat(frames, ignore_index = True)
    big = big.dropna(subset = ['entries', 'exits'])
    big = makeCols(big)

    #the line below sums the turnstiles for each station and adds the exits/hour column 

    station_rates = hourly_exits(big)

    #the line below creates a time series of daily readings for each station

    daily_exits = daily_exit_rate(station_rates)

    #the 2 lines below creates a timeline of daily exit rates for the given the station

    station_input = ('LEXINGTON-53 ST', '6EM')
    timeline = daily_exits.ix[station_input]

    #the line below lists each the daily readings for each station on my wife bday

    crowded_exits_by_day = busiest_exits_by_day(station_rates)

    #the 2 lines below first filters for the desired time intervals then
    #returns the list of the top 50 stations and their total exits over the dates

    peak = filter_times(big)
    busy_stations = n_busiest_stations(peak, 50)



if __name__ == '__main__':
    main()