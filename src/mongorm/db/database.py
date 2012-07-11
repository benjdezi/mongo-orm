'''
Created on Feb 22, 2012

@requires: pyMongo (pip install pymongo)
@requires: py-utils (https://github.com/benjdezi/Python-Utils)
@author: Benjamin Dezile
'''

#from pyutils.utils.logging import Logger
#from pyutils.utils.config import Config
import pymongo as Mongo
import time

DEFAULT_PORT = 27017

def query_logging(f):
    ''' Decorator for query logging '''
    def new_f(*kargs, **kwargs):
        if Database.query_logging is True:
            start_time = time.time()
            res = f(*kargs, **kwargs)
            dt = round((time.time() - start_time) * 1000, 2)
            query_type = f.__name__
            col_name = kargs[1]
            print "Query: %s %s in %s ms" % (query_type, col_name, dt)
            return res
        else:
            return f(*kargs, **kwargs)
    return new_f

class Database:
    ''' MongoDB wrapper for database access '''
    
    query_logging = True
    config = None
    connection = None
    db = None
    
    @classmethod
    def get_instance(cls, host, port, model, db_name=None, user=None, pwd=None):
        ''' Get a connection to a database instance 
        host:      Server address
        port:      Server port
        model:     Data model config map
        db_name:   Database name
        user:      User name
        pwd:       Password
        '''
        cls.config = { 'host': host, 'port': port, 'model': model }
        return cls._get_db(db_name, user, pwd)
    
    @classmethod
    def enable_query_logging(cls, state):
        ''' Enable or disable query logging '''
        cls.query_logging = state
    
    @classmethod
    def _get_connection(cls):
        ''' Establish connection to the database '''
        if not cls.connection:
            host = cls.config['host']
            port = cls.config.get('port', DEFAULT_PORT)
            cls.connection = Mongo.Connection(host, port)
            print "Connected to MongoDB @ %s:%s" % (host, port)
        return cls.connection
    
    @classmethod    
    def _get_db(cls, db_name, db_user, db_pwd):
        ''' Create a database instance if not already existing '''
        if not cls.db:
            conn = cls._get_connection()
            cls.db = conn[db_name]
            if db_user and db_pwd:
                cls.db.authenticate(db_user, db_pwd)
            print "Using database %s" % db_name
        return cls.db
    
    @classmethod
    def _get_collection(cls, name):
        ''' Return the collection for the given name '''
        db = cls._get_db()
        return db[name]
        
    @classmethod
    def build_indexes(cls):
        ''' Build indexes '''
        print "Building indexes"
        model = cls.config['model']
        for class_name in model.keys():
            fields = model[class_name]["fields"]
            for field_name in fields.keys():
                index = fields[field_name].get("index", None)
                if index:
                    col = cls._get_collection(class_name)
                    index_type = Mongo.ASCENDING
                    if index == -1:
                        index_type = Mongo.DESCENDING
                    elif index == "2d":
                        index_type = Mongo.GEO2D
                    col.ensure_index([(field_name, index_type)])
                    print "Ensured index %s for %s.%s" % (index_type, class_name, field_name)
        
    @classmethod
    def info(cls):
        ''' Return database info '''
        return cls._get_connection().server_info()    
        
    @classmethod
    def stats(cls):
        ''' Return database stats '''
        db = cls._get_db()
        return db.eval("db.stats()")
        
    @classmethod
    def drop(cls):
        ''' Drop the entire database '''
        db = cls._get_db()
        db_name = db.name
        cls._get_connection().drop_database(db_name)
        print "Dropped database %s" % db_name
        