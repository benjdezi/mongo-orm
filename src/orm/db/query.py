'''
Created on Mar 10, 2012

@requires: pyMongo (pip install pymongo)
@author: Benjamin Dezile
'''

from __future__ import with_statement

from orm.db.database import Database
from time import time
import pymongo

# TODO: Add query caching

class QueryMonitor(object):
    ''' Decorator that logs queries '''
    
    def __init__(self, query_inst, query_name):
        self.start_time = 0
        self.inst = query_inst
        self.name = query_name
        
    def __enter__(self):
        if Database.query_logging is True:
            self.start_time = time()
        
    def __exit__(self, t, value, tb):
        if Database.query_logging is True:
            if t is not None:
                pass # Exception occurred
            self.inst._clean()
            dt = (time() - self.start_time) * 1000
            extra = list()
            if self.inst.distinct_field:
                extra.append("distinct %s" % self.inst.distinct_field)
            if self.inst.order_field:
                extra.append("sort by %s" % self.inst.order_field)
            if self.inst.lim:
                extra.append("lim=%d" % self.inst.lim)
            print "Query: %s %s%s in %.2f ms" % (self.name, "(%s) " % ", ".join(extra) if extra else "", self.inst.col_name, dt)


ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING


class Query:
    ''' MongoDB query wrapper '''
    
    def __init__(self, collection, db_inst=None):
        self.reset()
        self.db = db_inst
        self.has_ext_conn = (db_inst is not None)
        t = type(collection)
        if t is str:
            self.col_name = collection
        elif hasattr(collection, 'get_class_name'):
            self.col_name = collection.get_class_name()
        else:
            raise Exception("Collection should be a string or a class extending BaseObject: %s" % t)
        
    def insert(self, **values):
        ''' Insert values '''
        self.insert_values = values
        return self
    
    def select(self, *fields):
        ''' Specify the fields to select '''
        self.selected_fields = fields
        return self
    
    def distinct(self, field):
        ''' Distinct selection on the given field '''
        self.distinct_field = field
        return self
    
    def where(self, **params):
        ''' Conditions to match '''
        self.conditions = params
        return self
    
    def and_where(self, **params):
        ''' Complementary conditions to satisfy '''
        if not self.conditions.has_key("$and"):
            self.conditions["$and"] = list()
        for k in params:
            self.conditions["$and"].append({ k: params[k] })
    
    def or_where(self, **params):
        ''' Exclusive conditions to satisfy '''
        if not self.conditions.has_key("$or"):
            self.conditions["$or"] = list()
        for k in params:
            self.conditions["$or"].append({ k: params[k] })
        return self
    
    def where_in(self, field, values):
        ''' The given field value must match one of the given values '''
        self.conditions[field] = { "$in" : values if type(values) is list else list(values) }
        return self
        
    def where_not_in(self, field, values):
        ''' The given field value must not match one of the given values '''
        self.conditions[field] = { "$nin": values if type(values) is list else list(values) }
        return self
    
    def where_exist(self, *fields):
        ''' The given fields must exist '''
        for field in fields:
            self.conditions[field] = { "$exists": True }
        return self
    
    def where_not_exist(self, *fields):
        ''' The given fields must not exist '''
        for field in fields:
            self.conditions[field] = { "$exists": False }
        return self
    
    def where_not(self, **params):
        ''' Inequality conditions to match '''
        for k in params:
            self.conditions[k] = { "$ne": params[k] }
        return self
    
    def where_regex(self, field, pattern):
        ''' Pattern to match on field values '''
        self.conditions[field] = { "$regex": pattern }
        return self
    
    def where_gt(self, **params):
        ''' Field value must be greater than the given value '''
        for k in params:
            self.conditions[k] = { "$gt": params[k] }
        return self

    def where_gte(self, **params):
        ''' Field value must be greater or equal to the given value '''
        for k in params:
            self.conditions[k] = { "$gte": params[k] }
        return self
    
    def where_lt(self, **params):
        ''' Field value must be lesser than the given value '''
        for k in params:
            self.conditions[k] = { "$lt": params[k] }
        return self

    def where_lte(self, **params):
        ''' Field value must be lesser or equal to the given value '''
        for k in params:
            self.conditions[k] = { "$lte": params[k] }
        return self
    
    def unset(self, field):
        ''' Unset a given field '''
        if not self.update_rules.has_key('$unset'):
            self.update_rules['$unset'] = dict()
        self.update_rules['$unset'][field] = 1
        return self
                
    def incr(self, field, value=1):
        ''' Increment a given field '''
        if not self.update_rules.has_key('$inc'):
            self.update_rules['$inc'] = dict()
        self.update_rules['$inc'][field] = value
        return self
    
    def decr(self, field):
        ''' Decrement a given field '''
        self.incr(field, -1)
        return self
    
    def limit(self, n):
        ''' Specify the maximum number of results to return '''
        self.lim = n
        return self
    
    def sort(self, field, direction=None):
        ''' Sort the results according to the given field and direction '''
        self.order_field = field
        self.order_dir = direction
        return self
    
    def _get_db_inst(self):
        ''' Return the database instance '''
        if not self.db:
            self.db = Database.get_instance()
        return self.db
    
    def _get_collection(self):
        ''' Return the corresponding collection for this class '''
        db = self._get_db_inst()
        return db[self.col_name]
    
    def _clean(self):
        ''' Clean up '''
        if not self.has_ext_conn:
            self.db.connection.close()
    
    def execute(self):
        ''' Execute this query '''
        if self.insert_values:
            with QueryMonitor(self, "Insert %d fields into" % len(self.insert_values)):
                if self.insert_values.has_key("id"):
                    self.insert_values["_id"] = self.insert_values["id"]
                    del self.insert_values["id"]
                col = self._get_collection()
                return col.insert(self.insert_values)
        else:
            with QueryMonitor(self, "Get %sfrom" % ("%d fields " % len(self.selected_fields) if self.selected_fields else "")):
                col = self._get_collection()
                params = dict(map(lambda x: (x, 1), self.selected_fields)) if self.selected_fields else None                    
                res = col.find(self.conditions, params)
                if self.distinct_field:
                    res = res.distinct(self.distinct_field)
                if self.order_field:
                    res = res.sort(self.order_field, 
                                   self.order_dir if self.order_dir is not None else DESCENDING)
                if self.lim:
                    res = res.limit(self.lim)
                return res
    
    def fetch_one(self):
        ''' Execute a get query limited to the first result only '''
        results = self.execute()
        if results and results.count():
            return results[0]
    
    def count(self):
        ''' Execute a count query on the associated collection '''
        with QueryMonitor(self, "Count from"):
            col = self._get_collection()
            if self.conditions:
                n = col.find(self.conditions).count()
            else:
                n = col.count() 
            return n
    
    def update(self, **params):
        ''' Execute an update with the given values '''
        with QueryMonitor(self, "Update %d fields from" % len(params)):
            self.update_rules['$set'] = params
            return self._get_collection().update(self.conditions, self.update_rules)
    
    def delete(self):
        ''' Execute a delete query '''
        with QueryMonitor(self, "Delete from"):
            resp = self._get_collection().remove(self.conditions, True)
            if resp.get('err', None):
                raise Exception(resp)
            return resp['n']
    
    def copy(self):
        ''' Return a copy of this query '''
        q = Query(self.col_name, self.db)
        if self.insert_values:
            q.insert(**self.insert_values)
        if self.selected_fields:
            q.select(*self.selected_fields)
        if self.conditions:
            q.where(**self.conditions)
        if self.lim:
            q.limit(self.lim)
        if self.order_field:
            q.sort(self.order_field, self.order_dir)
        if self.distinct_field:
            q.distinct(self.distinct_field)
        return q
    
    def reset(self):
        ''' Reset all query parameters '''
        self.selected_fields = None
        self.conditions = dict()
        self.update_rules = dict()
        self.lim = None
        self.order_field = None
        self.order_dir = None
        self.insert_values = None
        self.distinct_field = None
        return self
