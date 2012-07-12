'''
Created on Jun 29, 2012

@requires: py-utils (https://github.com/benjdezi/Python-Utils)
@author: Benjamin Dezile
'''

from pyutils.lib.unit_test import TestSuite, test_case
from mongorm.db.database import Database
from mongorm.db.query import Query, DESCENDING

class TestQuery(TestSuite):
    ''' Test various types of queries '''
    
    N = 100
    
    def setup(self):
        
        db = Database.get_instance()
        Database.drop()
        
        for k in range(self.N):
            db['test'].insert({ 'param1': k, "param2": "value%d" % k, "param3": (k%2==0) })
        
    @test_case
    def test01_get_all_query(self):
        ''' Test getAll '''
        res = Query("test").execute()
        self.assert_equal(res.count(), self.N)
        for k in range(self.N):
            item = res[k]
            self.assert_equal(item['param1'], k)
            self.assert_equal(item['param2'], "value%d" % k)
            self.assert_equal(item['param3'], (k%2==0))
        
    @test_case
    def test02_get_sorted_and_limit_query(self):
        ''' Test sorted queries and limits '''
        q = Query("test")
        q.sort("param1", DESCENDING)
        q.limit(10)
        res = q.execute()
        self.assert_not_none(res)
        self.assert_equal(res.count(True), 10)
        for k in range(10):
            item = res[k]
            k2 = self.N - (k+1)
            self.assert_equal(item['param1'], k2)
            self.assert_equal(item['param2'], "value%d" % k2)
            self.assert_equal(item['param3'], (k2%2==0))
    
    @test_case
    def test1_count_query(self):
        ''' Test count queries '''
        self.assert_equal(Query("test").count(), self.N)
        self.assert_equal(Query("test").where(param3=True).count(), self.N / 2)
        
    @test_case
    def test2_insert_query(self):
        ''' Test inserts '''
        q = Query("test")
        q.insert(_id=1, success=True).execute()
        res = q.reset().where(_id=1).execute()
        self.assert_not_none(res)
        self.assert_equal(res.count(), 1)
        self.assert_equal(res[0]['_id'], 1)
        self.assert_equal(res[0]['success'], True)
                    
    @test_case
    def test3_update_query(self):
        ''' Test updates '''
        Query("test").where(_id=1).update(success=False)
        item = Query("test").where(_id=1).fetch_one()
        self.assert_not_none(item)
        self.assert_equal(item['success'], False)
            
    @test_case
    def test4_delete_query(self):
        ''' Test deletes '''
        Query("test").where(_id=1).delete()
        self.assert_equal(Query("test").count(), self.N)
        Query("test").delete()
        self.assert_equal(Query("test").count(), 0)

if __name__ == "__main__":
    TestQuery().run()
