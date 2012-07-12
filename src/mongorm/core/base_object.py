'''
Created on Feb 20, 2012

@requires: py-utils (https://github.com/benjdezi/Python-Utils)
@author: Benjamin Dezile
'''

from time import time
from types import InstanceType
from __builtin__ import __import__
from mongorm.db.query import Query
from pyutils.utils.helpers import camel_to_py_case 
import uuid
import json
import hashlib

PRIMITIVE_TYPES = (bool, int, long, float, str)
ITERATIVE_TYPES = (list, tuple, set)
INSTANCE = InstanceType
ID_ALIAS = "_id"

def _get_class_from_name(cls_name):
    ''' Get the class for the given name '''
    g = globals()
    if g.has_key(cls_name):
        return g[cls_name]
    else:
        mod_name = camel_to_py_case(cls_name)
        m = __import__("model.classes.%s" % mod_name)
        cls = getattr(m.classes, mod_name)
        cls = getattr(cls, cls_name)
        g[cls_name] = cls
        return cls


class Serializable(object):
    ''' Interface for serializable objects '''
    
    _serializable = True
    _has_sub_classes = False
    CLASS_KEY = "_class"
    
    def __init__(self):
        ''' Create a new instance '''
        return
        
    
    ##  INTERNAL METHODS  #######################
        
    def __getattr__(self, name):
        return None
    
    def _is_serializable(self, obj):
        ''' Return whether this object is serializable '''
        try:
            return (obj._serializable == True)
        except AttributeError:
            return False  
    
    def _format_field_value(self, val, field_type, sub_type=None):
        '''  Format the given field value as per the specified type and sub type '''
        if val is None:
            return val
        t = type(val)
        if t is not field_type:
            if t is INSTANCE and val.__class__ is not field_type:
                # Instance of the wrong kind
                raise ValueError("Expected instance of %s, but got %s" % (field_type, val.__class__.__name__))
            if field_type in PRIMITIVE_TYPES or (field_type in ITERATIVE_TYPES and t in ITERATIVE_TYPES):
                # Primitive types or permutation of iterative types
                try:
                    return field_type(val)
                except ValueError:
                    raise ValueError("'%s' is not a valid %s" % (val, field_type))
            else:
                # Other, non castable
                raise ValueError("Expected value of type %s but got %s" % (field_type, t))
        elif field_type is list and sub_type is not None and len(val) > 0 and type(val[0]) is not sub_type:
            # List of items of the wrong type
            t = type(val[0])
            if sub_type in PRIMITIVE_TYPES:
                l = list()
                for i in range(len(val)):
                    try:
                        l.append(t(val[i]))
                    except ValueError:
                        raise ValueError("'%s' is not a valid %s" %(val[i], t)) 
                return l
            else:
                raise ValueError("Expected list of %s but got %s" % (sub_type, t))
        else:
            # No formatting needed
            return val
    
    
    ##  PUBLIC METHODS  #######################

    def to_json(self):
        ''' Return a JSON representation of this object '''
        d = self.toDict()
        if d.has_key("id"):
            d["id"] = str(d["id"])
        return json.dumps(d, False, False) # False = not ensuring ascii
    
    def copy(self):
        ''' Return a copy of this object '''
        d = self.toDict()
        cls = self.__class__
        o = cls.from_dict(d, True)
        return o
    
    def to_dict(self):
        ''' Export this object's properties to a dictionary '''
        values = dict()
        values[Serializable.CLASS_KEY] = self.__class__.__name__
        for k in self.__dict__.keys():
            if not k.startswith('_') and self.__dict__[k] is not None:
                val = self.__dict__[k]
                t = type(val)
                if t in (list, tuple)  and len(val) > 0 and self._is_serializable(val[0]):
                    # List of objects
                    l = list()
                    for item in val:
                        l.append(item.toDict())
                    values[k] = l
                elif self._is_serializable(val):
                    # Serializable object
                    values[k] = val.toDict()
                elif t is str:
                    # String value
                    values[k] = unicode(val)
                else:
                    # Other type of value
                    values[k] = val
        return values
    
    @classmethod
    def from_dict(cls, d, is_new=False):
        ''' Create a new instance from a dictionary of properties '''
        if not cls._has_sub_classes:
            # FIXME: Dynamically import subclasses when needed instead of hard-coding it
            pass
        inst = cls(is_new) if not hasattr(cls, "_embedded") else cls()
        for k in d:
            _name = k
            _t = type(d[k])
            if _name in (ID_ALIAS, "id"):
                if not is_new:
                    # Set the id only if not a new instance
                    inst.__setattr__("id", int(d[k]) if _t in (str, unicode) else d[k])
            elif _t is dict and d[k].has_key(Serializable.CLASS_KEY):
                # Embedded object
                cls_name = d[k][Serializable.CLASS_KEY]
                clazz = eval("%s" % cls_name)
                inst.__setattr__(_name, clazz.from_dict(d[k]))
            elif _t is list and len(d[k]) > 0 and type(d[k][0]) is dict and d[k][0].has_key(Serializable.CLASS_KEY):
                # List of objects
                cls_name = d[k][0][Serializable.CLASS_KEY]
                clazz = eval("%s" % cls_name)
                l = list()
                for item in d[k]:
                    l.append(clazz.from_dict(item))
                inst.__setattr__(_name, l)
            elif _t is str:
                # String value
                inst.__setattr__(_name, unicode(d[k]))
            else:     
                # Other type of value
                inst.__setattr__(_name, d[k])
        if hasattr(inst, "_change_set") and inst._change_set is not None:
            # Reset change map
            inst._change_set.clear()
        return inst
    

class BasePersistentObject(Serializable):
    ''' Base for persistent objects '''
    
    def __init__(self, is_new=False):
        ''' Create a new instance '''
        self._new = is_new
        self._change_set = set()
        self.id = None
        if is_new:
            self.id = self._generate_uid()
    
    ## GETTERS and SETTERS  #####################
    
    def get_id(self):
        ''' Return this object's id '''
        return self.id
    
    def set_id(self, obj_id):
        ''' Set this object's id '''
        self.id = obj_id
    
    ## HELPERS  #################################
    
    def is_new(self):
        ''' Return whether this object is new (i.e. not persisted yet) '''
        return self._new
    
    def __str__(self):
        ''' Return  a string representation of this object '''
        return "%s #%s" % (self.__class__.__name__, self.id)
    
    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)
        if not name.startswith("_") and not self._new:
            self._notify_change(name)
    
    def _notify_change(self, field):
        ''' Register a change of value for a given field '''
        self._change_set.add(field)
    
    def _generate_uid(self):
        ''' Generate a random UID '''
        return uuid.uuid4().node

    def _generate_guid(self):
        ''' Generate a random GUID '''
        return uuid.uuid4().hex
            
    def equals(self, inst):
        ''' Assert whether this object is the same as the given instance '''
        if not inst or type(self) != type(inst):
            return False
        return self.toDict() == inst.toDict()
    
    @classmethod
    def get_class_name(cls):
        ''' Return the name of the class of this object '''
        name = cls.__name__
        if name.find("Base") == 0 and name != "BaseObject":
            name = name[4:]
        return name
    
    def to_dict(self):
        ''' Export this object to a dictionary '''
        d = Serializable.to_dict(self)
        if d.has_key(Serializable.CLASS_KEY):
            del d[Serializable.CLASS_KEY]
        return d
    

class BaseObject(BasePersistentObject):
    ''' Abstract base for all model objects '''
    
    def __init__(self, is_new=False, timestampable=False, softdeletable=False):
        ''' Create a new instance '''
        BasePersistentObject.__init__(self, is_new)
        self._timestampable = timestampable
        self._softdeletable = softdeletable
        self._col_name = self.__class__.__name__
        if is_new:
            self.deleted = None
            if self._timestampable:
                self.created = int(time())
                self.updated = int(time())
    
    
    ## GETTERS and SETTERS  #####################
    
    def get_created(self):
        ''' Return the creation timestamp '''
        return self.created
    
    def set_created(self, t):
        ''' Set the creation timestamp '''
        self.created = t
    
    def get_updated(self):
        ''' Return the last update timestamp '''
        return self.updated
    
    def set_updated(self, t):
        ''' Set the update timestamp '''
        self.updated = t
    
    def get_deleted(self):
        ''' Return the deletion timestamp when applicable '''
        return self.deleted
    
    def set_deleted(self, t):
        ''' Set the deletion timestamp '''
        self.deleted = t
    
    def _get_related(self, related_cls_name, value, relation_name=ID_ALIAS):
        ''' Fetch a related object 
        related_cls_name:     Name of the class that is referenced
        value:                Reference value
        relation_name:        Foreign parameter that is referenced
        '''
        if value is None:
            return None
        related_cls = _get_class_from_name(related_cls_name)
        if relation_name == ID_ALIAS:
            return related_cls.find(value)
        else:
            params = dict()
            params[relation_name] = value
            return related_cls.findOneBy(**params)
    
    def _set_related(self, related_obj, relation_name, foreign_relation_name=ID_ALIAS):
        ''' Set a relation value 
        related_obj:              Related object instance
        relation_name:            Local parameter used for the relation
        foreign_relation_name:    Foreign parameter being referenced locally
        '''
        if related_obj is not None:
            if foreign_relation_name == ID_ALIAS:
                ref = related_obj.getId()
            else:
                getter = getattr(related_obj, "get%s" % (foreign_relation_name[0].upper() + foreign_relation_name[1:]))
                ref = getter()
        else:
            ref = None
        setter = getattr(self, "set%s" % (relation_name[0].upper() + relation_name[1:]))
        setter(ref)
        
    def _get_related_array(self, related_cls_name, values, relation_name=ID_ALIAS):
        ''' Fetch an array of related objects 
        related_cls_name:     Name of the class that is referenced
        values:               Reference values
        relation_name:        Foreign parameter that is referenced
        '''
        if not values:
            return None
        related_cls = _get_class_from_name(related_cls_name)
        q = Query(related_cls)
        q.where_in(relation_name, values)
        res = q.execute()
        if res and res.count() > 0:
            objs = list()
            for item in res:
                objs.append(related_cls.from_dict(item))
            return objs
    
    def _set_related_array(self, related_objs, relation_name, foreign_relation_name=ID_ALIAS):
        ''' Set a relation array 
        related_objs:             Related object instances
        relation_name:            Local parameter used for the relation
        foreign_relation_name:    Foreign parameter being referenced locally
        '''
        if related_objs:
            refs = list()
            if foreign_relation_name == ID_ALIAS:
                getter_name = "getId"
            else:
                getter_name = "get%s" % (foreign_relation_name[0].upper() + foreign_relation_name[1:])
            for related_obj in related_objs:
                getter = getattr(related_obj, getter_name)
                refs.append(getter())
        else:
            refs = None
        setter = getattr(self, "set%s" % (relation_name[0].upper() + relation_name[1:]))
        setter(refs)
        
    
    ## INTERNAL METHODS  ########################
                    
    def __hash__(self):
        ''' Compute the MD5 hash value of this object '''
        m = hashlib.md5()
        m.update(str(self.id) + str(self.created))
        return m.hexdigest()
    
    
    ## DATA ACCESS METHODS  #####################
    
    def refresh(self):
        ''' Sync this object with its persisted version '''
        if not self._new:
            res = Query(self._col_name).where(_id=self.getId()).execute()
            if not res or not res.count():
                raise Exception("%s does not exist any more" % self)
            o = res[0]
            for field in o:
                setattr(self, field, o[field])      
        
    def save(self):
        ''' Persist this object '''
        if not self._new:
            return self.update()
        else:
            res = Query(self._col_name).insert(**self.toDict()).execute()
            self._new = False
            return res
    
    def update(self):
        ''' Update the persisted version of this object or save it if not already persisted '''
        values = dict()
        for k in self._change_set:
            val = self.__getattribute__(k)
            if hasattr(val, '_serializable'):
                val = val.toDict()
            values[str(k)] = val
        return Query(self._col_name).where(_id=self.id).update(**values)
    
    def delete(self):
        ''' Delete the persisted version of this object '''
        if self._new:
            # This object was never saved
            return
        if self._softdeletable is True:
            self.setDeleted(int(time()))
            self.save()
        else:
            return Query(self._col_name).where(_id=self.id).delete()
    
    @classmethod
    def delete_all(cls):
        ''' Delete all objects of this class '''
        Query(cls).delete()
    
    @classmethod
    def count(cls, **params):
        ''' Return the count of objects of this class '''
        return Query(cls).where(**params).count()

    @classmethod
    def find_all(cls, hydrate=True):
        ''' Get all the objects of this class '''
        objs = Query(cls).execute()
        if hydrate and objs:
            hydrated_objs = list()
            for obj in objs:
                hydrated_objs.append(cls.from_dict(obj))
            return hydrated_objs
        else:
            return objs
        
    @classmethod
    def find(cls, obj_id, hydrate=True):
        ''' Find a given object '''
        if type(obj_id) in [str, unicode]:
            obj_id = int(obj_id)
        if obj_id:
            obj = Query(cls).where(_id=obj_id).fetch_one()
            return (cls.from_dict(obj) if hydrate and obj else obj)

    @classmethod
    def find_by(cls, hydrate=True, **params):
        ''' Find objects based on a set of parameters '''
        objs = Query(cls).where(**params).execute()
        if hydrate and objs:
            hydrated_objs = []
            for obj in objs:
                hydrated_objs.append(cls.from_dict(obj))
            return hydrated_objs
        return objs
    
    @classmethod
    def find_one_by(cls, hydrate=True, **params):
        ''' Find the first object that matches the given parameters '''
        obj = Query(cls).where(**params).fetch_one()
        return cls.from_dict(obj) if hydrate and obj else obj
    