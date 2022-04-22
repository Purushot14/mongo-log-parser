"""
    Created by prakash at 15/02/22
"""
__author__ = 'Prakash14'

import logging

from mongo_log_parser.log_paser import LogBase
from test import TestMongoBase

USER_COLLECTION_NAMESPACE = "Database001.UserCollection"


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
                    raise e
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
        assert lb.keysExamined == 16 and lb.docsExamined == 15 and lb.cursorExhausted == 1 and lb.numYields == 0
        assert lb.nreturned == 0 and lb.queryHash == "8EF5E40B" and lb.planCacheKey == "97E4EA5B"
        assert lb.reslen == 239
        logging.info(lb)
        
    def test_004_parse_log_str_repl(self):
        log_str = '2022-02-15T02:45:00.569+0000 I  REPL     [repl-writer-worker-2243] applied op: command { ts: ' \
                  'Timestamp(1644893100, 24), t: 760, h: 0, v: 2, op: "c", ns: "Database001.$cmd", ' \
                  'ui: UUID("964586f8-cad9-4d62-bbc1-6584627d2507"), wall: new Date(1644893100469), ' \
                  'o: { createIndexes: "UserCollection", v: 2, key: { Name: 1 }, name: "idx_name", background: true } ' \
                  '}, took 69ms '
        lb = LogBase(log_str, 1)
        assert lb.namespace == USER_COLLECTION_NAMESPACE and lb.sub_category == 'replica_update'
        log_str = '2022-02-15T02:44:58.377+0000 I  REPL     [repl-writer-worker-2244] applied op: ' \
                  'command { ts: Timestamp(1644893098, 1), t: 760, h: 0, v: 2, op: "c", ns: "Database001.$cmd", ' \
                  'ui: UUID("964586f8-cad9-4d62-bbc1-6584627d2507"), wall: new Date(1644893098253), o: { create: ' \
                  '"UserCollection", idIndex: { v: 2, key: { _id: 1 }, name: "_id_",' \
                  ' ns: "Database001.UserCollection" } ' \
                  '} }, took 114ms '
        lb = LogBase(log_str, 1)
        assert lb.namespace == USER_COLLECTION_NAMESPACE and lb.sub_category == 'replica_update'
        log_str = '2022-02-15T02:49:41.412+0000 I  REPL     [repl-writer-worker-2244] applied op: CRUD { ts: ' \
                  'Timestamp(1644893381, 8), t: 760, h: 0, v: 2, op: "d", ns: "config.system.sessions", ' \
                  'ui: UUID("2d88d47c-8094-4674-96f1-ed2a7e8b5a00"), wall: new Date(1644893381389), o: { _id: { id: ' \
                  'UUID("7ec697d9-be44-4d55-a6d8-2730f7af77d6"), uid: BinData(0, ' \
                  'B523746E16FE1A21D1721ECD36B321FB8B0B06F860AF499D095C8666AEC58C6E) } } }, took 19ms '
        lb = LogBase(log_str, 1)
        assert lb.namespace == 'config.system.sessions' and lb.sub_category == 'replica_update'
        log_str = '2022-02-15T02:50:44.332+0000 I  REPL     [repl-writer-worker-2243] applied op: CRUD { ts: ' \
                  'Timestamp(1644893444, 3), t: 760, h: 0, v: 2, op: "i", ns: "Database001.UserCollection", ' \
                  'ui: UUID("d0cd4773-00a7-40c6-bdee-cb23cf8ab449"), wall: new Date(1644893444206), lsid: { id: UUID(' \
                  '"890324d8-8696-4c57-b491-617d1d282669"), uid: BinData(0, ' \
                  'FD39643E517FA4C6A10C5151564CBE2ACD5E8CE9090051A227BD594E3DC22689) }, txnNumber: 8, stmtId: 0, ' \
                  'prevOpTime: { ts: Timestamp(0, 0), t: -1 }, o: { _id: "User001" } }, took 118ms '
        lb = LogBase(log_str, 1)
        assert lb.namespace == USER_COLLECTION_NAMESPACE and lb.sub_category == 'replica_update'

    def test_005_parse_log_str_storage(self):
        log_str = '2022-02-15T02:45:00.184+0000 I  STORAGE  [IndexBuildsCoordinatorMongod-656] Index build completed ' \
                  'successfully: 749878cb-2758-4818-ac18-35be77ae1fcf: Database001.UserCollection ( ' \
                  'fef077b5-c3f9-4339-b9d7-ade1311c3b01 ). Index specs built: 1. Indexes in catalog before build: 2. ' \
                  'Indexes in catalog after build: 7 '
        lb = LogBase(log_str, 1)
        assert lb.namespace == USER_COLLECTION_NAMESPACE and lb.sub_category == 'index_repl_writer'
        log_str = '2022-02-15T02:45:00.080+0000 I  STORAGE  [repl-writer-worker-2243] Index build initialized: ' \
                  'e0cba798-94e7-462f-bc18-953f9aa1d8e5: Database001.UserCollection' \
                  ' (fef077b5-c3f9-4339-b9d7-ade1311c3b01 ): ' \
                  'indexes: 1 '
        lb = LogBase(log_str, 1)
        assert lb.namespace == USER_COLLECTION_NAMESPACE and lb.sub_category == 'index_repl_writer'
    
    def test_005_parse_log_str_dbStats(self):
        log_str = '2022-02-15T02:47:26.499+0000 I  COMMAND  [conn355537] command Database001 appName: "MongoDB ' \
                  'Monitoring Module v11.10.5.7326 (git: 95a13eb798f22394b19dfc9181f5744b33d82bd4)" command: dbStats ' \
                  '{ dbstats: 1, lsid: { id: UUID("c1a89ec5-6fb2-4b1e-a648-72d070d8ba7f") }, $clusterTime: { ' \
                  'clusterTime: Timestamp(1644893239, 1), signature: { hash: BinData(0, ' \
                  'C6FD7BE1720665ED26C9D4EBE479C77C9492C442), keyId: 7021195893055422468 } }, $db: "Database001", ' \
                  '$readPreference: { mode: "primaryPreferred" } } numYields:0 reslen:395 locks:{ ' \
                  'ParallelBatchWriterMode: { acquireCount: { r: 1 } }, ReplicationStateTransition: { acquireCount: { ' \
                  'w: 1 } }, Global: { acquireCount: { r: 1 } }, Database: { acquireCount: { r: 1 } }, Collection: { ' \
                  'acquireCount: { r: 1284 } }, Mutex: { acquireCount: { r: 1 } } } storage:{} protocol:op_msg 48ms '
        lb = LogBase(log_str, 1)
        assert lb.namespace == 'Database001.' and lb.sub_category == 'dbStats'
        log_str = '2022-02-15T02:49:00.291+0000 I  COMMAND  [conn358038] command admin.$cmd appName: "MongoDB ' \
                  'Automation Agent v11.10.5.7326 (git: 95a13eb798f22394b19dfc9181f5744b33d82bd4)" command: ' \
                  'saslContinue { saslContinue: 1, conversationId: 1, payload: "xxx", $clusterTime: { clusterTime: ' \
                  'Timestamp(1644893339, 1), signature: { hash: BinData(0, 8B4E9B054D563553D1D395A1D2CC1B1033DA7927), ' \
                  'keyId: 7021195893055422468 } }, $db: "admin", $readPreference: { mode: "primaryPreferred" } } ' \
                  'numYields:0 reslen:204 locks:{} protocol:op_msg 17ms '
        lb = LogBase(log_str, 1)
        assert lb.namespace == 'admin.$cmd' and lb.sub_category == 'saslContinue'
