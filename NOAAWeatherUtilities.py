'''
Class to encapsulate utilities for reading data from NOAA Weather files.
'''
import os
import re
import tarfile
# data tools
import numpy as np
import pandas as pd

class GHCNUtilities:

    def _read_countries( self ):
        '''
        Read the countries list from the NOAA ghcn daily folder
        and return the information in a Pandas Series.
        '''
        colnames = ['Code', 'Country']
        df = pd.read_fwf( os.path.join(self.WeatherDir, 'ghcnd-countries.txt'), 
                          header=None,
                          widths=[2,48], 
                          names=colnames)
        df.set_index('Code', inplace=True)
        self.srCountries = df['Country']
        return 
        
    def _read_states( self ):
        '''
        Read the states list from the NOAA ghcn daily folder
        and return the information in a Pandas Series.
        '''
        colnames = ['Code', 'State']
        df = pd.read_fwf( os.path.join(self.WeatherDir, 'ghcnd-states.txt'), 
                          header=None,
                          widths=[2,48], 
                          names=colnames)
        df.set_index('Code', inplace=True)
        self.srStates = df ['State']   
        return

    def _read_stations( self ):
        '''
        Read the weather stations list from the NOAA ghcn daily folder.
        '''
        colnames = ['WeatherStnId', 'Latitude', 'Longitude', 'Elevation_m',
                    'State', 'StationName', 
                    'GSNFlag', 'HCNCat', 'WMOId']
        dtypes   = {0:'str', 1:'float', 2:'float', 3:'float', 
                    4:'str', 5:'str', 
                    6:'bool', 7:'category', 8:'str'} 
        
        df = pd.read_fwf( os.path.join(self.WeatherDir, 'ghcnd-stations.txt'), 
                          header=None, keep_default_na=False,
                          na_values=('-999.9'),
                          colspecs=[ [0,11], [12,20], [21, 30], [31, 37], [38,40], 
                                     [41,71], [72,75], [76,79], [80,85]],
                          names=colnames, dtype=dtypes)
        df.set_index(colnames[0], inplace=True)
        df['WMOId'] = pd.to_numeric(df['WMOId'], errors='coerce')
        self.dfStations = df
        return
        
    def __init__(self, WeatherDir ):
        self.WeatherDir = WeatherDir
        self.srCountries = None
        self.srStates = None
        self.dfStations = None
        if not os.path.exists(WeatherDir):
            raise ValueError('Weather directory does not exist: {}'.format(WeatherDir))
        self._read_countries()
        self._read_states()
        self._read_stations()
        return

    def find_stations( self, station='', state=''):
        '''
        Find one or more stations in the NOAA ghcn data whose station name
        contains a given string and optionally matches a specified state.
        dfw     - the dataframe result from ghcn_read_stations(). 
        station - optional string. If supplied, find this anywhere in the StationName column. Ignore case.
                  Raise a ValueError exception if this is not a string
        state   - optional text string containing the two letter state designation. 
                  Raise a ValueError exception if this is not a string of exactly 2 letters.
        '''
        dfw = self.dfStations
        if (not isinstance(state,str)) or (len(state) > 0 and (len(state) != 2 or not re.match('[a-z|A-Z]{2}', state))):
            raise ValueError("state must be a string two letters")
        if (not isinstance(station,str)):
            raise ValueError("station must be a string")
        if len(state) > 0 and len(station) > 0:
            df = dfw[dfw['StationName'].str.contains(station, case=False, regex=False) & dfw['State'].str.match(state, case=False)]
        elif len(station) > 0:        
            df = dfw[dfw['StationName'].str.contains(station, case=False, regex=False)]
        elif len(state) > 0:
            df = dfw[dfw['State'].str.match(state, case=False)]
        else:
            df = dfw
        return df.copy()

    def get_station_info(self, stationid):
        '''
        Get the station information for the given station id.
        Returns an empty Series if the station is not found.
        '''
        try:
            sr = self.dfStations.loc[ stationid ]
        except:
            sr = pd.Series()
        return sr


    def read_weather_byyear( self, year ):
        '''
        Read the gchn data for a given year from the by_year directory.
        See the Readme.txt file for the names of all available measurements.
        The most commonly used measurements include the following. 
            PRCP = Precipitation (mm)
            SNOW = Snowfall (mm)
            SNWD = Snow depth (mm)
            TMAX = Maximum temperature (degrees C)
            TMIN = Minimum temperature (degrees C)
            TAVG = Average temperature (degrees C)
        For many of the elements, the data in the file is in tenths of the unit.
        All such elements have been scaled to normal (e.g. tenths degC is now degC).
    
        Raises a ValueError exception if WeatherDir does not exist.
        Returns an empty dataframe if there is no data for the given year
        '''
            
        colnames = ['WeatherStnId', 'Date',  'Element', 'Value',
                    'MFlag',        'QFlag', 'SFlag',   'Time'] 
    
        dtypes =   {0:'str',      1:'str',       2:'str',      3:'float', 
                    4:'category', 5:'category',  6:'category', 7:'str'}    
    
        # Read the weather data for all stations for the given year
        # If there is no data for the year, return an empty dataframe
        filepath = os.path.join(self.WeatherDir,'by_year', '{}.csv.gz'.format(year) )
        if not os.path.isfile(filepath):
            return pd.DataFrame()
    
        df = pd.read_csv(filepath, compression='gzip', header=None, 
                         sep=',', quotechar='"', keep_default_na=False, 
                         names=colnames, dtype=dtypes,
                         usecols=colnames[0:7])
    
        # convert the dates to pandas datetime
        df.Date = pd.to_datetime(df.Date)
        # Several of the element values are measured in tenths.
        # Convert these to standard unit values.
        lstTenths = ['PRCP','TMAX','TMIN','AWND','EVAP','MDEV','MDPR','MDTN','MDTX','MNPN','MXPN',
                     'SN*#','SX*#','TAVG','THIC','TOBS','WESD','WESF','WSF1','WSF2','WSF5','WSFG',
                     'WSFI','WSFM']
        df.loc[df['Element'].isin(lstTenths), ['Value']] /= 10.0
    
        # add columns for the date components for easier indexing later
        df['Year']  = year
        df['Month'] = pd.DatetimeIndex(df['Date']).month
        df['Day']   = pd.DatetimeIndex(df['Date']).day
        return df
