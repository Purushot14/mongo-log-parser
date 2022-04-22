"""
    Created by prakash at 15/02/22
"""
__author__ = 'Prakash14'

import logging

from mongo_log_parser.log_paser import LogBase
from test import TestMongoBase


class TestLogBase(TestMongoBase):

    def test_001_parse_log_str(self):
        log_str = '2022-02-15T02:45:00.080+0000 I  INDEX    [repl-writer-worker-12345] index build: starting on ' \
                  'Database001.Collection001 properties: { v: 2, key: { Field001: 1, Field002: 1 }, name: ' \
                  '"idx_field001_field002", background: true, ns: "Database001.Collection001" } using method: Hybrid'
        lb = LogBase(log_str, 0)
        assert lb.category == "INDEX"

    def test_002_load_and_parse_log_str(self):
        logs = {}
        line_number = 0
        with open(self.log_file_path, encoding="latin-1") as fp:
            while True:
                line_number += 1
                line = fp.readline()
                if line == "":
                    break
                try:
                    lb = LogBase(line, line_number)
                    logging.info(f"line number {line_number} parsed")
                    # if lb.category.lower() == "index" or lb.category.lower() == "Network":
                    if not lb.sub_category:
                        logging.info("Yes")
                    logs.setdefault(lb.category, []).append(lb)
                except Exception as e:
                    logging.exception("ERROR: %s ", e)
                    logging.info("LINE: %s", line)
        logging.info(logs)

    def test_003_parse_log_str_aggregate(self):
        log = '2022-02-15T02:58:51.305+0000 I  COMMAND  [conn354100] command Database001.UsersCollection command: ' \
              'aggregate { aggregate: "UsersCollection", pipeline: [ { $match: { _is_deleted: null, Type: "User", ' \
              '_id: { $nin: [ "bot001", "bot002", "guest" ] }, $and: [ { userName: { $eq: ' \
              '"UserName001" } } ] } }, { $project: { _id: 0, externalId: "$externalId", ' \
              'groups: "$groups", id: "$_id", lastModified: "$_modified_at.v", created: "$_created_at.v", ' \
              'nickName: "$nickName", name: { givenName: "$givenName", familyName: "$familyName", formatted: ' \
              '"$fullName" }, ' \
              'title: "$title", emails: [ { value: "$email", display: "$email", type: "work", primary: true } ' \
              '], phoneNumbers: { $cond: [ { $not: [ "$phoneNumber" ] }, [], [ { value: "$phoneNumbers", ' \
              'display: "$phoneNumbers", ' \
              'type: "work", primary: true } ] ] }, userName: "$userName", active: { $cond: [ { $eq: [ ' \
              '"$Status", "Active" ] }, true, false ] }, urn:kissflow:scim:schemas:extension:Database001:2:User: { ' \
              'CreatedBy: { display: "$CreatedBy.Name", value: "$CreatedBy._id", ref: { $concat: [ "Users/", ' \
              '"$CreatedBy._id" ] } }, Manager: { display: "$Manager.Name", value: "$Manager._id", ref: { $concat: ' \
              '[ "Users/", "$Manager._id" ] } }, UserType: "$UserType" } } }, { $match: { userName: { $eq: ' \
              '"UserName001" } } }, { $count: "Count" } ], allowDiskUse: false, cursor: {}, ' \
              'lsid: { id: UUID("7151a1e1-15bf-4148-bd45-d0637c35da10") }, $clusterTime: { clusterTime: Timestamp(' \
              '1644893926, 1), signature: { hash: BinData(0, 9CF944FA2E8747677F6318DE1145384C430F9403), ' \
              'keyId: 7021195893055422468 } }, $db: "Database001", $readPreference: { mode: "secondaryPreferred" } } ' \
              'planSummary: IXSCAN { _id: 1 } keysExamined:16 docsExamined:15 cursorExhausted:1 numYields:0 ' \
              'nreturned:0 queryHash:8EF5E40B planCacheKey:97E4EA5B reslen:239 locks:{ ReplicationStateTransition: { ' \
              'acquireCount: { w: 2 } }, Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 2 } }, ' \
              'Collection: { acquireCount: { r: 2 } }, Mutex: { acquireCount: { r: 2 } } }' \
              ' storage:{} protocol:op_msg ' \
              '233ms '
        lb = LogBase(log, 1)
        assert not lb.allow_disk_use
        self.assertEqual(lb.namespace, 'Database001.UsersCollection')
        assert lb.query and lb.sub_category == 'aggregate' and lb.category == 'COMMAND' and lb.thread and 'conn354100'
        logging.info(lb)
