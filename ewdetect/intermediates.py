# EpiWatch Detection
# Intermediates


# Imports

import os
import fnmatch
import time
import json

import collections

import numpy as np


# Globals

with open( '.ewdetect_config', 'r' ) as config_file:
    config_data = json.load( config_file )
    
    intermediates_dir = config_data.get( 'intermediatesPath', os.path.join( '.', 'intermediates' ) )
    run_prefix = config_data.get( 'runPrefix', 'run' )
    
    run_identifier = config_data.get( 'runIdentifier', 'r' )
    user_identifier = config_data.get( 'userIdentifier', 'u' )
    segment_identifier = config_data.get( 'segmentIdentifier', 's' )
    

# Helpers

def _is_list( x ):
    if isinstance( x, str ):
        return False
    return isinstance( x, collections.Sequence )


# Functions

def path_for_run( run ):
    return os.path.join( intermediates_dir, '{0}_{1}'.format( run_prefix, run ) )

def get_latest_run():
    
    run_start = '{0}_'.format( run_prefix )
    
    runs = [ int( x.split( run_start )[1] )
             for x in os.listdir( intermediates_dir )
             if fnmatch.fnmatch( x, '{0}*'.format( run_start ) ) ]
    
    return max( runs )

def make_new_run():
    
    cur_timestamp = int( time.time() ) # In seconds
    os.makedirs( path_for_run( cur_timestamp ), exist_ok = True )

    return cur_timestamp

def path_for_intermediate( name,
                           run = None,
                           user = None,
                           segment = None,
                           extension = 'npz' ):
    
    if run is None:
        run = get_latest_run()
        
    # Determine if nested
    ret = os.path.join( path_for_run( run ), name )
    if os.path.isdir( ret ):
        ret = os.path.join( ret, name )
    ret += '_{0}{1}'.format( run_identifier, run )
    if user is not None:
        ret += '_{0}{1}'.format( user_identifier, user )
    if segment is not None:
        ret += '_{0}{1}'.format( segment_identifier, segment )
    if extension is not None:
        ret += '.{0}'.format( extension.lower() )
    
    return ret

def save_intermediates( data, name,
                        nested = False,
                        run = None,
                        user = None,
                        segment = None ):
    
    if run is None:
        run = get_latest_run()
    
    if nested:
        save_dir = os.path.join( path_for_run( run ), name )
        os.makedirs( save_dir, exist_ok = True )
    else:
        save_dir = path_for_run( run )
        
    save_filename = '{0}_{1}{2}'.format( name, run_identifier, run )
    if user is not None:
        save_filename += '_{0}{1}'.format( user_identifier, user )
    if segment is not None:
        save_filename += '_{0}{1}'.format( segment_identifier, segment )
    save_filename += '.npz'
    
    save_path = os.path.join( save_dir, save_filename )
    
    with open( save_path, 'wb' ) as save_file:
        np.savez( save_file, **data )

def intermediate_names( run = None ):
    
    if run is None:
        run = get_latest_run()
    
    ret = []
    for item in os.listdir( path_for_run( run ) ):
        if os.path.isdir( os.path.join( path_for_run( run ), item ) ):
            named_files = [ x for x in os.listdir( os.path.join( path_for_run( run ), item ) )
                            if fnmatch.fnmatch( x, '{0}_*.npz'.format( item ) ) ]
            if len( named_files ) > 0:
                ret.append( item )
                continue
        
        named_files = [ x for x in os.listdir( path_for_run( run ) )
                        if fnmatch.fnmatch( x, '{0}_*.npz'.format( item ) ) ]
        if len( named_files ) > 0:
            ret.append( item )
    
    return ret
        
def find_intermediates( name,
                        run = None,
                        user = None,
                        segment = None ):
    
    if run is None:
        run = get_latest_run()
    
    if not _is_list( run ):
        run = [ run ]
    
    intermediates_dir_contents = os.listdir( intermediates_dir )
    matched_runs = [ r for r in run
                     if '{0}_{1}'.format( run_prefix, r ) in intermediates_dir_contents ]
    
    ret = []
    
    for r in matched_runs:
        
        # First check if name is a nested intermediate
        if os.path.isdir( os.path.join( path_for_run( r ), name ) ):
            search_dir = os.path.join( path_for_run( r ), name )
        else:
            search_dir = path_for_run( r )
        
        run_matches = [ x for x in os.listdir( search_dir )
                        if fnmatch.fnmatch( x, '{0}_*.npz'.format( name ) ) ]
        
        if user is not None:
            user_matches = []
            if not _is_list( user ):
                user = [ user ]
            for u in user:
                user_matches += [ x for x in run_matches
                                  if fnmatch.fnmatch( x, '*_{0}{1}_*'.format( user_identifier, u ) ) ]
        else:
            user_matches = run_matches
        
        if segment is not None:
            segment_matches = []
            if not _is_list( segment ):
                segment = [ segment ]
            for s in segment:
                segment_matches += [ x for x in user_matches
                                     if fnmatch.fnmatch( x, '*_{0}{1}_*'.format( segment_identifier, s ) ) ]
        else:
            segment_matches = user_matches
        
        match_specs = []
        for match in segment_matches:
            match_name = name
            match_run = None
            match_user = None
            match_segment = None
            
            match_components = os.path.splitext( match )[0].split( '_' )[1:]
            
            for component in match_components:
                if component[0] == run_identifier:
                    match_run = int( component[1:] )
                    continue
                if component[0] == user_identifier:
                    match_user = int( component[1:] )
                    continue
                if component[0] == segment_identifier:
                    match_segment = int( component[1:] )
            
            match_specs += [ {
                'name': match_name,
                'run': match_run,
                'user': match_user,
                'segment': match_segment
            } ]
        
        ret += match_specs
        
    return ret
        
def load_intermediates( name,
                        run = None,
                        user = None,
                        segment = None ):
    
    if run is None:
        run = get_latest_run()
        
    # Determine if nested
    load_path = path_for_intermediate( name, run = run, user = user, segment = segment, extension = 'npz' )
    
    return np.load( load_path )