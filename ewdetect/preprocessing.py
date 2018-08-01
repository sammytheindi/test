# Preprocessing


# Imports

import numpy as np


# Functions

# For handling mongo objects
def reformat_timeseries( data, start_time = 0 ):
    t = np.array( [int( k ) for k in data.keys()] )
    v = np.array( [data[k] for k in data.keys()] )
    
    ti = np.argsort( t )
    t = t[ti] - start_time
    v = v[ti]
    
    return t, v

def reformat_timeseries_continuous( data, start_time = 0 ):
    t = np.array( [ d['t'] for d in data ] )
    v = np.array( [ [ d['acceleration']['x'],
                      d['acceleration']['y'],
                      d['acceleration']['z'] ]
                    for d in data ] )
    
    ti = np.argsort( t )
    t = t[ti] - start_time
    v = v[ti]
    
    return t, v

def reformat_hr_continuous( data, start_time = 0 ):
    t = np.array( [ d['t'] for d in data ] )
    h = np.array( [ d['heartrate'] for d in data ] )
    
    ti = np.argsort( t )
    t = t[ti] - start_time
    h = h[ti]
    
    return t, h

def stitch_continuous( data_arrays ):
    data = []
    
    last_end = None
    
    for data_array in data_arrays:
        for d in data_array:
            if last_end is None:
                data.append( d )
                continue
            if d['t'] <= last_end:
                continue
            
            data.append( d )
        
        last_end = data_array[-1]['t']
    
    return data

def reformat_timeseries_pg( data, resort = False, start_time = 0 ):
    t = data['dateTime']
    v = np.c_[ data['x'], data['y'], data['z'] ]
    
    if resort:
        ti = np.argsort( t )
        t = t[ti]
        v = v[ti]
    
    t = t - start_time
    
    return t, v

def reformat_hr_pg( data, start_time = 0 ):
    t = data['dateTime']
    h = data['hr1']
    
    ti = np.argsort( t )
    t = t[ti] - start_time
    h = h[ti]
    
    return t, h

# Helper to deal with uneven sampling of data
# NOTE: Assumes time data in *ms*
def uniformize( t, x, fs = 100.0 ):
    
    i_sort = np.argsort( t, axis = 0 )
    t = t[i_sort]
    x = x[i_sort, :]
    
    Ts = 1000.0 / fs
    
    t_uniform = np.arange( t[0], t[-1], Ts )
    x_uniform = np.zeros( (t_uniform.shape[0], x.shape[1]) )
    
    slices = [ [] for t_cur in t_uniform ]
    i_uniform_cur = 0
    t_uniform_cur = t_uniform[i_uniform_cur]
    
    for i_cur, t_cur in enumerate( t ):
        
        while not ( t_uniform_cur <= t_cur and t_cur < (t_uniform_cur + Ts) ):
            i_uniform_cur += 1
            t_uniform_cur = t_uniform[i_uniform_cur]
        
        slices[i_uniform_cur].append( i_cur )
    
    for i_cur, t_cur in enumerate( t_uniform ):
        slice_cur = np.array( slices[i_cur] )
        
        if slice_cur.shape[0] == 0:
            x_uniform[i_cur, :] = x_uniform[i_cur-1, :]
            continue
        
        x_uniform[i_cur, :] = x[slice_cur[-1], :]
        
        cur_start_index = slice_cur[-1]
    
    return t_uniform, x_uniform





