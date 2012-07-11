'''
Created on Jun 29, 2012

@requires: py-utils (https://github.com/benjdezi/Python-Utils)
@author: Benjamin Dezile
'''

from pyutils.lib.unit_test import TestSuite, test_case
from mongorm.core.base_object import BaseObject

class TestBaseObject(TestSuite):
    ''' TEst basic object functionalities '''
    
    def teardown(self):
        BaseObject.deleteAll()
    
    @test_case
    def test1_id(self):
        ''' Test id assignment and generation '''
        o1 = BaseObject(True)
        o1.save()
        
        o2 = BaseObject.find(o1.getId())
        self.assertNotNone(o2)
        self.assertHasAttr(o2, "id")
        self.assertEqual(o1.getId(), o2.getId())
    
    @test_case
    def test2_to_and_from_dict(self):
        ''' Test exporting to and instanciating from dictionaries '''
        o1 = BaseObject(True)
        o2 = BaseObject.fromDict(o1.toDict())
        self.assertNotNone(o2)
        self.assertEqual(o1.getId(), o2.getId())

if __name__ == "__main__":
    TestBaseObject().run()
