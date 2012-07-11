'''
Created on Jun 16, 2012

@author: Benjamin Dezile
'''

from mongorm.db.database import Database
from mongorm.db.query import Query
import json

class BaseObjectArray(list):
    ''' Array of BaseObjects '''
    
    def __init__(self, objs=None):
        ''' Create a new array of objects '''
        if objs:
            if not hasattr(objs[0], 'getClassName'):
                raise Exception("Objects must extend BaseObject")
            list.__init__(objs)
    
    def save(self):
        ''' Save the list of object in bulk '''
        if len(self) > 0:
            cls = self.__getitem__(0).__class__
            if not hasattr(cls, 'getClassName'):
                raise Exception("Objects must extend BaseObject")
            col = Database._get_collection(cls.getClassName())
            objs = list()
            for o in self:
                doc = o.toDict()
                doc['_id'] = o.getId()
                del doc['id']
                objs.append(doc)
            col.insert(objs)
            
    def delete(self):
        ''' Delete the persisted versions of this objects '''
        if len(self) > 0:
            cls = self.__getitem__(0).__class__
            if not hasattr(cls, 'getClassName'):
                raise Exception("Objects must extend BaseObject")
            obj_ids = map(lambda x: x.getId())
            q = Query(cls)
            q.where_in("_id", obj_ids)
            q.delete()
            
    def to_json(self):
        ''' Return a JSON representation of this array '''
        l = list()
        for o in self:
            l.append(o.to_dict())
        return json.dumps(l)
            