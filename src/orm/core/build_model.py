'''
Created on Feb 21, 2012

Build all the base classes based on the configuration
found in config/model.yml

@requires: py-utils (https://github.com/benjdezi/Python-Utils)
@author: Benjamin Dezile
'''

from cStringIO import StringIO
from pyutils.utils.helpers import camel_to_py_case
from orm.db.database import Database
import os
import datetime
import traceback
 
BASE_TYPES = ("int", "str", "float", "long", "bool", "list", "dict")

def generate_htag(data):
    ''' Generate a hash tag for the given data '''
    h = hash(data)
    return hex(h).strip('0x')

def build_model_class(class_path, class_name, class_info, overwrite=False):
    ''' Build the model class file for a given class '''
    
    filename = "base_%s.py" % camel_to_py_case(class_name) 
    filepath = os.path.join(class_path, filename)
    
    old_htag = None
    if os.path.exists(filepath):
        fp = open(filepath, 'r')
        for line in fp:
            if line.find("#tag:") >= 0:
                old_htag = line.split(":")[1].strip()
        fp.close()
    
    fp = None
    fields = class_info.get("fields")
    relations = class_info.get("relations", None)
    field_names = fields.keys()
    field_as = class_info.get("as", dict())
    is_ts = field_as.has_key("timestampable")
    is_sd = field_as.has_key("softdeletable")
    is_emb = field_as.has_key("embedded")
    
    header = StringIO()
    buf = StringIO()
    import_set = set()
    
    header.write("'''\nUpdated on " + datetime.date.today().strftime("%B %d, %Y") + "\n\n")
    header.write("Base class for " + class_name + "\n\n")
    header.write("@author: Auto-generated\n'''\n\n")
    
    root_class = "Serializable" if is_emb else "BaseObject"
    
    buf.write("#tag: %s\n\n")
    buf.write("from mongorm.core.base_object import " + root_class + "\n")
    buf.write("{{other_imports}}\n")
    buf.write("class Base" + class_name + "(" + root_class + "):\n")
    buf.write("\n")
    
    # Constructor
    if is_emb:
        buf.write("    _embedded = True\n\n")
        buf.write("    def __init__(self):\n")
        buf.write("        Serializable.__init__(self)\n")
    else:
        buf.write("    def __init__(self, is_new = False):\n")
        buf.write("        BaseObject.__init__(self, is_new, " + ("True" if is_ts else "False") + ", " + ("True" if is_sd else "False") + ")\n")
    for field in field_names:
        default_val = fields[field].get("default", None)
        if type(default_val) == str:
            default_val = "\"" + default_val + "\""
        buf.write("        self." + field + " = " + str(default_val) + "\n")
    buf.write("\n\n")
    
    # Getters and setters
    buf.write("    ##  GETTERS AND SETTERS  #########################\n\n")
    for field_name in field_names:
        field_info = fields[field_name]
        field_type = field_info.get("type", None)
        a = field_type.find("[")
        if a > 0:
            field_type = field_type[:a]
        if field_type is not None and field_type not in BASE_TYPES:
            import_set.add(field_type)
        buf.write(_make_getter_code(field_name))
        buf.write(_make_setter_code(field_name, field_info))
    
    # Relational getters and setters
    if relations and len(relations) > 0:
        buf.write("\n")
        buf.write("    ##  RELATIONAL GETTERS AND SETTERS  ##########\n\n")
        for rel_name in relations.keys():
            rel_info = relations[rel_name]
            buf.write(_make_relational_getter_code(rel_name, rel_info))
            buf.write(_make_relational_setter_code(rel_name, rel_info))
    
    # Management methods
    if not is_emb:
        buf.write("\n")
        buf.write("    ##  MANAGEMENT METHODS  ######################\n\n")
        buf.write("    def save(self):\n")
        for field_name in field_names:
            field_info = fields[field_name]
            if field_info["required"] is True:
                buf.write("        if self." + field_name + " is None: raise ValueError('" + class_name + "." + field_name + " is required')\n")
        buf.write("        " + root_class + ".save(self)\n")
    
    buf.write("\n")
    
    # Generate additional imports
    imports = ""
    for t in import_set:
        imports += "from model.classes.%s import %s\n" % (camel_to_py_case(t), t)
    
    # Save to file
    data = buf.getvalue().replace("{{other_imports}}", imports)
    new_htag = generate_htag(data)
    if old_htag != new_htag or overwrite:
        print "Built class for %s (%s)" % (class_name, filepath)
        try:
            fp = open(filepath, 'w+')
            fp.write(header.getvalue())
            fp.write(data % new_htag)
        except Exception, e:
            print "Could not write model class: %s" % e
            traceback.print_stack()
        finally:
            if fp: fp.close()
            
    buf.close()
    header.close()
            
    return filename

def _make_getter_code(field_name):
    ''' Generate the code for the getter of the given field '''
    code = "    def get_" + camel_to_py_case(field_name) + "(self):\n"
    code += "        return self." + field_name + "\n\n"
    return code

def _make_setter_code(field_name, field_info):
    ''' Generate the code for the setter for the given field '''
    field_type = field_info.get('type', None)
    code = "    def set_" + camel_to_py_case(field_name) + "(self, val):\n"
    if field_type:
        # Enforce type constraint
        sub_type = None
        a = field_type.find("[")
        if a > 0:
            b = field_type.find("]", a)
            sub_type = field_type[a+1:b]
            field_type = field_type[:a]
        if not sub_type:
            code += "        self.%s = self._format_field_value(val, %s)" % (field_name, field_type)
        else:
            code += "        self.%s = self._format_field_value(val, %s, %s)" % (field_name, field_type, sub_type)
    else:
        # No type constraint
        code += "        self.%s = val" % field_name
    code += "\n\n"
    return code

def _make_relational_getter_code(relation_name, relation_info):
    ''' Generate the code for a given relational getter '''
    cls_name = relation_info["class"]
    foreign_rel_name = relation_info["foreign"]
    if foreign_rel_name == "id":
        foreign_rel_name = "_id"
    local_rel_name = relation_info["local"] 
    code = "    def get_" + camel_to_py_case(relation_name) + "(self):\n"
    code += "        ref = self.get_" + camel_to_py_case(local_rel_name) + "()\n"
    if relation_info.get("multi", False):
        code += "        return self._get_related_array('%s', ref, '%s')" % (cls_name, foreign_rel_name)
    else:
        code += "        return self._get_related('%s', ref, '%s')" % (cls_name, foreign_rel_name)
    code += "\n\n"
    return code

def _make_relational_setter_code(relation_name, relation_info):
    ''' Generate the code for a given relational setter '''
    foreign_rel_name = relation_info["foreign"]
    if foreign_rel_name == "id":
        foreign_rel_name = "_id"
    local_rel_name = relation_info["local"]
    multi = relation_info.get("multi", False) 
    code = "    def set_" + camel_to_py_case(relation_name) + "(self, obj" + ("s" if multi else "") + "):\n"
    if multi:
        code += "        return self._set_related_array(objs, '%s', '%s')" % (local_rel_name, foreign_rel_name)
    else:
        code += "        return self._set_related(obj, '%s', '%s')" % (local_rel_name, foreign_rel_name)
    code += "\n\n"
    return code


if __name__ == "__main__":
    
    from pyutils.utils.config import ConfigParser
    import time
    import sys
    
    model_file = sys.argv[sys.argv.index('-m') + 1]
    class_path = sys.argv[sys.argv.index('-p') + 1]
    
    over_write = "--force" in sys.argv
    if over_write:
        print "WARNING: override enabled"
    
    # Build model from config
    start_time = time.time()
    
    model = ConfigParser().parse(model_file).get()
    print "Building model"
    
    filenames = dict()
    for cls_name in model.keys():
        file_name = build_model_class(class_path, cls_name, model.get(cls_name), over_write)
        filenames[file_name] = True
        
    # Clean old file
    print "Cleaning up"
    for filename in os.listdir(class_path):
        if filename.find("__") < 0 and not filenames.has_key(filename):
            os.remove(os.path.join(class_path, filename))
            print "Deleted %s" % filename
            
    # Ensure indexes
    Database.build_indexes(model)
        
    print "Built model in %.3f seconds" % (time.time() - start_time)

