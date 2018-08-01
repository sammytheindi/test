from getpass import getpass

import numpy as np

from pymongo import MongoClient
from bson import ObjectId

import psycopg2 as pg

import PostgresDriver as pd
import binary

def connect_mongo( username,
                   host = 'localhost' ):
    
    client = MongoClient( host )
    
    print( 'Password for mongo instance at {0}'.format( host ) )
    client.epiwatch.authenticate( username, getpass() )
    print( 'Success' )
    
    # TODO Parameterize db name
    return client.epiwatch

def connect_pg( username,
                host = None,
                backend = 'stage',
                db = 'epiwatch' ):
    
    if host is None:
        if backend.lower() == 'prod':
            host = 'epiwatch.cxoyodq5cuh1.us-east-1.rds.amazonaws.com'
        elif backend.lower() == 'stage':
            host = 'epiwatch-stage.cxoyodq5cuh1.us-east-1.rds.amazonaws.com'
        else:
            raise ValueError( 'Unknown backend: {0}'.format( backend ) )
    
    print( 'Password for pgSQL instance at {0}'.format( host ) )
    connect_string = 'host={0} dbname={1} user={2} password={3}'.format( host, db, username, getpass() )
    connection = pg.connect( connect_string )
    print( 'Success' )
    
    del connect_string
    
    return connection

def connect_s3( backend = 'stage' ):
    print( 'Password for PostgresDriver connection ({0})'.format( backend ) )
    # Password is requested internally here
    pg_driver = pd.PostgresDriver( backend = backend )
    pg_driver.connectToPostgres()
    s3_driver = binary.S3Driver( backend = backend )
    s3_driver.debug = True
    print( 'Success' )
    
    return pg_driver, s3_driver