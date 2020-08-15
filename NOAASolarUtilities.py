'''
Class to encapsulate utilities for reading data from NOAA Solar files.
'''
import os
import re
import tarfile
# data tools
import numpy as np
import pandas as pd

class NSRBUtilities:

    def _read_stations( self ):
        '''
        Read the solar stations list from the NOAA nsrb folder.
        The column names are intended to be consistent with the weather functions
        '''
        colnames = ['SolarStnId', 'SiteClass', 'SolarFlag', 'StationName', 'State', 
                    'Latitude', 'Longitude', 'Elevation_m', 'TimeZoneOffset']
        dtypes = { 0:'str', 1:'category', 2:'bool', 3:'str', 4:'str',
                   5:'float', 6:'float', 7:'float', 8:'float'}
        df = pd.read_csv( os.path.join(self.SolarDir,'documentation','NSRDB_StationsMeta.csv'), 
                          header=0, keep_default_na=True,
                          na_values=('-999.9'),
                          names=colnames,
                          dtype=dtypes,
                          usecols=colnames )
        df.set_index(colnames[0], inplace=True)
        self.dfStations = df
        return df

    def __init__(self, SolarDir ):
        self.SolarDir = SolarDir
        self.dfStations = None
        if not os.path.exists(SolarDir):
            raise ValueError('Solar directory does not exist: {}'.format(SolarDir))
        self._read_stations()
        return

    def find_stations( self, station='', state='', solar=''):
        '''
        Find one or more stations in the NOAA nsrb solar data whose station name
        contains a given string and optionally matches a specified state.
        dfs     - the dataframe result from nsrb_read_stations()
        station - optional string. If supplied, find this anywhere in the StationName column. Ignore case.
                  Raise a ValueError exception if this is not a string
        state   - optional string containing the two letter state designation.
                  Raise a ValueError exception if this is not a string of exactly 2 letters.
        solar   - optional bool. If true, only include sites with measured data.
                  If false, include only sites with modeled data.
        '''
        # Check inputs
        if (not isinstance(state,str)) or (len(state) > 0 and (len(state) != 2 or not re.match('[a-z|A-Z]{2}', state))):
            raise ValueError("state must be a string two letters")
        if (not isinstance(station,str)):
            raise ValueError("station must be a string")
        
        # build up an index based on input  
        # start by assuming we will include everything
        dfs = self.dfStations
        idx = [True] * dfs.shape[0]    
        if len(state) > 0:    
            idx = idx & dfs['State'].str.match(state, case=False)
        if (len(station) > 0):
            idx = idx & dfs['StationName'].str.contains(station, case=False, regex=False)
        if isinstance(solar,bool):
            idx = idx &  dfs['SolarFlag'] == solar
        
        df = dfs[idx]
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


    def read_solar_byyear(self, stationid, year):
        '''
        Reads the solar data for a single station for a given year.
        Assumes the NOAA nsrb data has been unzipped into directories
        with the station id as the directory name.
        '''    
        if not isinstance(stationid, str):
            raise ValueError('Station ID must be a string')
            
        colnames = ['Date',           'Time_LST',       'Zenith_deg', 
                    'Azimuth_deg',    'ETR_Wpm2',       'ETRN_Wpm2', 
                    'GloMod_Wpm2',    'GloModUnc_pct',  'GloModSrc', 
                    'DirMod_Wpm2',    'DirModUnc_pct',  'DirModSrc',
                    'DifMod_Wpm2',    'DifModUnc_pct',  'DifModSrc', 
                    'MeasGlo_Wpm2',   'MeasGloQualFlg', 'MeasDir_Wpm2', 
                    'MeasDirQualFlg', 'MeasDif_Wpm2',   'MeasDifQualFlg']

        dtypes =   { 0:'str',    1:'str',    2:'float', 
                     3:'float',  4:'float',  5:'float', 
                     6:'float',  7:'float',  8:'float', 
                     9:'float', 10:'float', 11:'float',
                    12:'float', 13:'float', 14:'float',
                    15:'float', 16:'int',   17:'float', 
                    18:'int',   19:'float', 20:'int' }
    
        # If the tar.gz file exists, then the station id is valid
        tarfilepath = os.path.join(self.SolarDir, stationid+".tar.gz")
        if not os.path.isfile(tarfilepath):
            raise ValueError('File not found: {}'.format(tarfilepath))
        # This will be inside the tar file    
        datfilepath = 'nsrdb_solar/{}/{}_{}.csv'.format(stationid, stationid, year)
        with tarfile.open(tarfilepath, 'r:gz') as f:
            try:
                datfile = f.extractfile(datfilepath)
            except:    
                # No data for the given year
                print('No data for {}'.format(year))
                df = pd.DataFrame()
            else:
                df = pd.read_csv(datfile, header=0, sep=',', quotechar='"', 
                                 keep_default_na=True, na_values='-9900',
                                 names=colnames, dtype=dtypes, usecols=colnames )
                # convert the dates to pandas datetime
                # This data uses 24:00 at midnight to indicate the last
                # hour of the day. 1:00 is the first hour of a day.
                # For now, leave the time column as a string
                df.Date = pd.to_datetime(df.Date)
                # put station id in first column
                df.insert(loc=0, column='SolarStnId', value=stationid)
                # add columns for the date components for easier indexing later
                df['Year']  = year
                df['Month'] = pd.DatetimeIndex(df['Date']).month
                df['Day']   = pd.DatetimeIndex(df['Date']).day
        return df
