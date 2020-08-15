import os
import string
import sys
import pandas as pd

# indirectly referenced libraries
import pyarrow
import fastparquet

# utility functions to save and read a dataframe for later use

def save_dataframe(df, name, savedir='./', makeunique=False):
    '''
    df - the dataframe to save
    name - the desired file name as a string (.pqt will be added as the extension) 
           Characters other than letters and digits will be stripped.
    savedir - optional path to a directory  
    makeunique - if True, prepend date and time to file name.
    '''
    validFilenameChars = '-_(){}{}'.format(string.ascii_letters, string.digits)
    if not isinstance(name, str):
        print('name must be a string')
        return ""

    dfempty = pd.DataFrame()
    if type(df) != type(dfempty):
        print('only works for dataframes')
        return ""

    # strip out invalid characters
    cleanName = ''.join(c for c in name if c in validFilenameChars)
    # format a date and time
    ftime = pd.to_datetime('today').strftime('%Y%m%dT%H%M%S')
    # put them all together
    if makeunique:
        finalName = os.path.join(savedir, '{}_{}.pqt'.format(ftime,cleanName))
    else:
        finalName = os.path.join(savedir, '{}.pqt'.format(cleanName))
    # save using parquet format
    print('Saving to parquet file: {}'.format(finalName))
    df.to_parquet(finalName, index=True, engine='fastparquet', compression='gzip')
    # return the filename used       
    return finalName

def read_dataframe( fName, savedir='./' ):
    '''
    Read a dataframe using the same engine as save_dataframe()
    '''
    fullpath = os.path.join(savedir, fName)
    if os.path.exists(fullpath):
        print('Reading from parquet file: {}'.format(fullpath))
        sys.stdout.flush()
        df = pd.read_parquet( fullpath, engine='fastparquet' )
        return df 
    else:
        print('File not found: {}'.format(fullpath))
    return None

# approximate distance between two points on the globe
from math import radians, cos, sin, asin, sqrt

def haversine_km(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two  
    locations, specified in decimal degrees.
    From: https://rosettacode.org/wiki/Haversine_formula#Python
    """
    #R = 3959.87433 # Radius of the earth in miles
    R = 6372.8 # Radius of the earth in kilometers
 
    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
 
    a = sin(dLat / 2)**2 + cos(lat1) * cos(lat2) * sin(dLon / 2)**2
    c = 2 * asin(sqrt(a))
 
    return R * c


#test

#df1 = pd.DataFrame({'a':(1,2,3),'b':(4,5,6)})
#fname = utl.save_dataframe( df1, 'xyzzy ??', savedir)
#df2 = utl.read_dataframe( fname )
#df1.equals(df2)  


#test. Distance between airports BNA and LAX is 2887.26 kilometers
#BNA = (36.12,  -86.67)
#LAX = (33.94, -118.40)
#print('Distance between BNA and LAX = {} km'.format(haversine( BNA[0], BNA[1], LAX[0], LAX[1])))


