#!/usr/bin/env python3

import json
import unittest
from cassandra_repair import *

def clean_redis(r):
    r.delete("REPAIR_STATUS")
    r.delete("REPAIR_START_TIME")
    r.delete("REPAIR_CURRENT_JOB")
    r.delete("REPAIR_FAILED_JOBS")
    r.delete("REPAIR_TOTAL_TIME")
    r.delete("REPAIR_COMPLETED_JOBS")

class RepairManagerTest(unittest.TestCase):

    def setUp(self):
        self.manager = RepairManager("dev.yaml")

    def tearDown(self):
        clean_redis(self.manager._redis)

    def test_starting_redis(self):
        r = self.manager._redis
        self.assertEquals(r.get("REPAIR_STATUS"), b"running")
        self.assertIsNone(r.get("REPAIR_START_TIME"))
        self.assertIsNone(r.get("REPAIR_CURRENT_JOB"))
        self.assertIsNone(r.get("REPAIR_FAILED_JOBS"))
        self.assertIsNone(r.get("REPAIR_TOTAL_TIME"))
        self.assertIsNone(r.get("REPAIR_COMPLETED_JOBS"))

    def test_add_failures(self):
        j1 = RepairJob('1.2.3.4', 'keyspace1', 'cf1')
        j2 = RepairJob('1.2.3.5', 'keyspace1', 'cf2')
        self.manager._add_failure(j1)
        self.manager._add_failure(j2)

        self.assertEqual(len(self.manager._failures), 2)
        self.assertIn(j1.format(), self.manager._failures)
        self.assertIn(j1.format(), self.manager._failures)

        failures_in_redis = json.loads(
                self.manager._redis.get("REPAIR_FAILED_JOBS"))
        self.assertIn(j1.format(), failures_in_redis)
        self.assertIn(j1.format(), failures_in_redis)

    def test_add_completed(self):
        self.manager._redis.delete("REPAIR_COMPLETED_JOBS")

        j1 = RepairJob('1.2.3.4', 'keyspace1', 'cf1')
        j2 = RepairJob('1.2.3.5', 'keyspace1', 'cf2')
        self.manager._add_completed(j1)
        self.manager._add_completed(j1)

        self.assertEqual(len(self.manager._completed_jobs), 2)
        self.assertIn(j1.format(), self.manager._completed_jobs)
        self.assertIn(j1.format(), self.manager._completed_jobs)

        completed_in_redis = json.loads(
                self.manager._redis.get("REPAIR_COMPLETED_JOBS"))
        self.assertIn(j1.format(), completed_in_redis)
        self.assertIn(j1.format(), completed_in_redis)

class RepairManagerExisting(unittest.TestCase):

    existing_job = RepairJob('10.1.1.1', 'keyspace1', 'cf1')

    def setUp(self):
        self.redis = redis.StrictRedis(host='localhost', port='6379', db=0)
        clean_redis(self.redis)
        self.redis.set('REPAIR_COMPLETED_JOBS',
                json.dumps([self.existing_job.format()]))

        self.manager = RepairManager("dev.yaml")

    def tearDown(self):
        clean_redis(self.redis)

    def test_existing_job(self):
        self.assertEquals(len(self.manager._completed_jobs), 1)
        self.assertIn(self.existing_job.format(), self.manager._completed_jobs)

    def test_was_completed(self):
        self.assertTrue(
                self.manager._was_completed(self.existing_job))
        self.assertFalse(
                self.manager._was_completed(RepairJob('1.1.1.1', 'ks1', 'cf1')))

if __name__ == '__main__':
    unittest.main()
