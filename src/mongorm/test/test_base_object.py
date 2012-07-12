'''
Created on Jun 29, 2012

@requires: py-utils (https://github.com/benjdezi/Python-Utils)
@author: Benjamin Dezile
'''

from pyutils.lib.unit_test import TestSuite, test_case
from mongorm.core.base_object import BaseObject

class TestBaseObject(TestSuite):
    ''' Test basic object functionalities '''
    
    def teardown(self):
        BaseObject.delete_all()
    
    @test_case
    def test1_id(self):
        ''' Test id assignment and generation '''
        o1 = BaseObject(True)
        o1.save()
        
        o2 = BaseObject.find(o1.get_id())
        self.assert_not_none(o2)
        self.assert_has_attr(o2, "id")
        self.assert_equal(o1.get_id(), o2.get_id())
    
    @test_case
    def test2_to_and_from_dict(self):
        ''' Test exporting to and instanciating from dictionaries '''
        o1 = BaseObject(True)
        o2 = BaseObject.from_dict(o1.to_dict())
        self.assert_not_none(o2)
        self.assert_equal(o1.get_id(), o2.get_id())

if __name__ == "__main__":
    TestBaseObject().run()
