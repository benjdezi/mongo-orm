'''
Created on Feb 22, 2012

@requires: pyMongo (pip install pymongo)
@author: Benjamin Dezile
'''

import pymongo as Mongo

DEFAULT_PORT = 27017
DEFAULT_HOST = "localhost"

class Database:
    ''' MongoDB wrapper for database access '''
    
    query_logging = True
    config = None
    connection = None
    db = None
    
    @classmethod
    def get_instance(cls, host, port=None, db_name=None, user=None, pwd=None):
        ''' Get a connection to a database instance 
        host:      Server address
        port:      Server port
        model:     Data model config map
        db_name:   Database name
        user:      User name
        pwd:       Password
        '''
        cls.config = { 'host': host, 'port': port }
        return cls._get_db(db_name, user, pwd)
    
    @classmethod
    def enable_query_logging(cls, state):
        ''' Enable or disable query logging '''
        cls.query_logging = state
    
    @classmethod
    def _get_connection(cls):
        ''' Establish connection to the database '''
        if not cls.connection:
            host = cls.config.get('host', DEFAULT_HOST)
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
    def build_indexes(cls, model_config):
        ''' Build indexes '''
        print "Building indexes"
        for class_name in model_config.keys():
            fields = model_config[class_name]["fields"]
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
        
