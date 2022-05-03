"""
    Created by prakash at 15/02/22
"""
__author__ = 'Prakash14'

import json
import logging
from json import JSONDecodeError
from typing import List

from .utils.json_utils.json_decoder import JSONDecoder
from .utils.json_utils.str_to_json import get_json_from_long_text, get_json_array_str_from_long_text, \
    get_json_str_from_long_text
from .utils.regex_helper import get_ip_addresses_from_text

counters = ["keysExamined", "docsExamined", "cursorExhausted", "numYields", "nreturned", "queryHash", "reslen",
            "planCacheKey", "ninserted", "keysInserted", ]


class LogBase:

    def __init__(self, line_str: str, line_number: int):
        self._line_str = line_str.rstrip('\n')
        self.line_number = line_number
        self.time = None
        self.log_level = None
        self.category = None
        self.thread = None
        self.msg = None
        self.tokens: list = []
        self.namespace = None
        self.index_key = None
        self.index_name = None
        self.index_background = None
        self.scanned_records = None
        self.taken_time = None
        self.sub_category = None
        self.inserted = None
        self.conn_ip = None
        self.conn_id = None
        self.conn_number = None
        self.driver_str = None
        self.client_details = None
        self.database = None
        self.collection = None
        self.allow_disk_use = None
        self.cursor = None
        self.cluster_time = None
        self.read_preference = None
        self.query = None
        self.keysExamined = None
        self.docsExamined = None
        self.cursorExhausted = None
        self.numYields = None
        self.nreturned = None
        self.queryHash = None
        self.reslen = None
        self.planCacheKey = None
        self.parse_log_str()

    def __setattr__(self, key, value):
        if key == "namespace" and value:
            self.database, self.collection = value.split('.', 1)
        return super().__setattr__(key, value)

    @staticmethod
    def _parse_msg(msg: list):
        return " ".join(msg)

    def _convert_to_dict(self, json_str: str) -> dict:
        loaded_json = json.loads(json_str.strip(), cls=JSONDecoder)
        #     logging.info(f"json_str: {json_str}")
        #     start_idx = json_str.find("key: {") + 6
        #     end_idx = json_str.find("}")
        #     index_key = json_str[start_idx:end_idx].strip()
        #     updated_key = index_key
        #     idx_key = json_str[start_idx:end_idx].strip()
        #     for item in [item.split(":")[0].strip() for item in index_key.split(",")]:
        #         updated_key = updated_key.replace(item, f'"{item}"')
        #     return self._convert_to_dict(json_str.replace(index_key, updated_key))
        return loaded_json

    def _update_counter_values(self, tokens: List[str]):
        for token in tokens:
            for counter in counters:
                if token.startswith(f'{counter}:'):
                    value = token.split(":")[-1]
                    self.__setattr__(counter, int(value) if value.isdigit() else value)
                    break

    def _parse_index_msg(self, msg: list):
        if msg[0] == "index" and msg[1] == "build:":
            if msg[2] == "starting":
                self.sub_category = "index_build_starting"
                properties: dict = get_json_from_long_text(self._line_str.split("properties:")[1])
                # start_idx = properties.find("key: {") + 6
                # end_idx = properties.find("}")
                # index_key = properties[start_idx:end_idx].strip()
                # updated_key = index_key
                # for item in [item.split(":")[0].strip() for item in index_key.split(",")]:
                #     updated_key = updated_key.replace(item, f'"{item}"')
                # properties: dict = self._convert_to_dict(properties.replace(index_key, updated_key))
                self.namespace = properties["ns"]
                self.index_key = properties["key"]
                self.index_name = properties["name"]
                self.index_background = properties.get("background", False)
            elif msg[2] == "collection":
                self.sub_category = "collection_scan"
                self.scanned_records = int(msg[6])
                self.taken_time = int(msg[-2]) * 1000
            elif msg[2] == "inserted":
                self.sub_category = "collection_scan"
                self.inserted = int(msg[3])
                self.taken_time = int(msg[-2]) * 1000
            elif msg[2] == "done":
                self.sub_category = "index_created"
                self.index_name = msg[5]
                self.namespace = msg[-1]
            else:
                logging.info(msg)
        elif self.thread.startswith("repl-writer-worker"):
            self.sub_category = "index_repl_writer"
        else:
            logging.info(msg)
        # return " ".join(msg)

    def _parse_network_msg(self, msg: List[str]):
        if msg[0] == "connection" and msg[1] == "accepted":
            self.sub_category = "connection_open"
            self.conn_ip = get_ip_addresses_from_text(msg[3])[0]
            self.conn_id = f"conn{msg[4][1:]}"
            self.conn_number = int(msg[5][1:])
        elif msg[0] == 'received' and msg[1] == "client":
            self.sub_category = "client"
            self.conn_ip = get_ip_addresses_from_text(msg[4])[0]
            self.conn_id = msg[5].replace(":", "")
            self.client_details = self._convert_to_dict(self.msg.split(msg[5])[1])
            driver = self.client_details.get('driver') or {}
            self.driver_str = f"{driver.get('name')} - {driver.get('version')}"
        elif msg[1] == "connection" and msg[0] == "end":
            self.sub_category = "connection_open_end"
            self.conn_ip = get_ip_addresses_from_text(msg[2])[0]
            self.conn_number = int(msg[3][1:])
        elif msg[0] == "Error":
            self.sub_category = "network_error"
            self.conn_ip = get_ip_addresses_from_text(self.msg)[0]
            self.conn_id = f"conn{msg[-1][:-1]}"
        elif msg[0] in ('Starting', 'Confirmed') and (msg[2] == 'replica' or msg[1] == "replica"):
            self.sub_category = "new_replica"
        else:
            logging.info(msg)
            pass
        return self._parse_msg(msg)

    def _parse_access_msg(self, msg: List[str]):
        if msg[0] == 'Successfully':
            self.sub_category = msg[1]
            self.conn_ip = get_ip_addresses_from_text(msg[-1])[0]
        return self._parse_msg(msg)

    def _parse_storage_msg(self, msg: List[str]):
        if msg[0] == 'createCollection:':
            if not self.thread.startswith("repl-writer-worker"):
                logging.info(msg)
            self.sub_category = msg[0]
            self.namespace = msg[1]
        elif msg[0] == "Index" and msg[1] == "build":
            self.sub_category = "index_repl_writer"
            self.namespace = msg[5] if msg[2] == "completed" else msg[4]
        else:
            logging.info(msg)
        return self._parse_msg(msg)

    def _parse_repl_msg(self, msg: List[str]):
        if msg[0] == 'applied' and msg[2] in ('command', 'CRUD'):
            self.sub_category = "replica_update"
            try:
                replica_update = get_json_from_long_text(self._line_str)
            except JSONDecodeError:
                object_str = get_json_str_from_long_text(self._line_str[self._line_str.find("o:") + 2:])
                replica_update = get_json_from_long_text(self._line_str.replace(object_str, '{}'))
            if replica_update:
                if msg[2] == "CRUD":
                    self.namespace = replica_update['ns']
                else:
                    collection = replica_update['o'].get('createIndexes') or replica_update['o'].get('drop')
                    database = replica_update['ns'].split('.')[0]
                    if collection:
                        self.namespace = f'{database}.{collection}'
                    else:
                        self.namespace = replica_update['o']['idIndex']['ns']
            else:
                self.namespace = msg[1]
        elif msg[0] in ('Canceling', 'Scheduling', "Restarting", "Scheduled", "transition", "Resetting", "Entering"):
            pass
        else:
            # Note: all REPL unhandled logs are skipped tamp
            pass
            # raise
        return self._parse_msg(msg)

    def _parse_command_msg(self, msg: List[str]):
        if msg[0] == "warning:" and msg[14] == 'command':
            msg = msg[14:]
        if msg[0] == 'command':
            if msg[2] == 'appName:':
                self.client_details = {"application": {"name": self.msg[self.msg.find("appName:") + 8: self.msg.find("command:")]}}
                self.sub_category = msg[msg.index("command:") + 1]
                if msg[1].startswith("admin"):
                    self.namespace = msg[1]
                else:
                    self.namespace = f'{msg[1]}.'
            elif msg[3] in ('aggregate', 'find', "getMore", "findAndModify", "count"):
                self.sub_category = msg[3]
                self.namespace = msg[1]
                query_json_str = None
                update_json_str = None
                fields_json_str = None
                sort_json_str = None
                if msg[3] in ('find', 'findAndModify', "count"):
                    if "filter:" in self.msg:
                        query_json_str = get_json_str_from_long_text(self.msg[self.msg.find("filter"):])
                    if "fields:" in self.msg:
                        fields_json_str = get_json_str_from_long_text(self.msg[self.msg.find("fields:"):])
                    if "sort:" in self.msg:
                        sort_json_str = get_json_str_from_long_text(self.msg[self.msg.find("sort:"):])
                    if "query:" in self.msg:
                        query_json_str = get_json_str_from_long_text(self.msg[self.msg.find("query"):])
                    if "update:" in self.msg:
                        update_json_str = get_json_str_from_long_text(self.msg[self.msg.find("update:"):])
                elif "pipeline:" in self.msg:
                    query_json_str = get_json_array_str_from_long_text(self.msg[self.msg.find("pipeline"):])
                msg_str = self.msg.replace(query_json_str, '"filter_key"') if query_json_str else self.msg
                msg_str = self.msg.replace(update_json_str, '"filter_key"') if update_json_str else msg_str
                msg_str = self.msg.replace(fields_json_str, '"filter_key"') if fields_json_str else msg_str
                msg_str = self.msg.replace(sort_json_str, '"filter_key"') if sort_json_str else msg_str
                query = get_json_from_long_text(msg_str)
                self.allow_disk_use = query.get('allowDiskUse') or False
                self.cursor = query.get('cursor')
                self.cluster_time = query.get('$clusterTime')
                self.read_preference = query.get('$readPreference', {}).get('mode')
                self.query = query_json_str
                self.sort = sort_json_str
                self.projection = fields_json_str
                self.update_query = update_json_str
                self.limit = query.get("limit")
                self.batch_size = query.get('batchSize')
                self.client_details = query.get("$client")
                self._update_counter_values(self.tokens)
            elif msg[3] in ('update', 'delete', 'insert', "createIndexes", "create"):
                self.sub_category = msg[3]
                query_json_str = None
                update_json_str = None
                updated_update_json_str = None
                if "updates:" in self.msg:
                    update_json_str = get_json_array_str_from_long_text(self.msg[self.msg.find("updates:"):])
                    query_json_str = get_json_str_from_long_text(update_json_str[update_json_str.find(" q: "):])
                    update_query = get_json_str_from_long_text(update_json_str[update_json_str.find(" u: "):])
                    updated_update_json_str = update_json_str
                    if update_query:
                        updated_update_json_str = updated_update_json_str.replace(update_query, '"UpdateQuery"')
                    if query_json_str:
                        updated_update_json_str = updated_update_json_str.replace(query_json_str, '"Query"')
                msg_str = self.msg.replace(update_json_str, updated_update_json_str) if update_json_str else self.msg
                query = get_json_from_long_text(msg_str)
                self.namespace = f"{query['$db']}.{query[self.sub_category]}"
                if self.sub_category == "createIndexes":
                    self.indexes = query['indexes']
                self.op_ordered = query.get("ordered")
                self.read_preference = query.get('$readPreference', {}).get('mode')
                self._update_counter_values(msg)
            elif msg[3] in ('listIndexes', 'saslStart', "isMaster", "saslContinue",
                                                    "replSetRequestVotes", "replSetUpdatePosition", "killCursors"):
                # TODO: need to handle 'isMaster'
                self.sub_category = msg[3]
                self.namespace = msg[1]
            else:
                raise
        elif msg[0] in ("CMD:",):
            self.sub_category = msg[1]
            self.namespace = msg[2]
        elif msg[0] in ("task:", "serverStatus", "Unable"):
            # TODO: Temp skipped
            pass
        else:
            # logging.info(msg)
            raise
        return self._parse_msg(msg)

    def _parse_write_msg(self, msg: List[str]):
        # todo: need to update later
        self.msg

    def _parse_election_msg(self, msg: List[str]):
        # todo: need to update later
        self.msg

    def _parse_connpool_msg(self, msg: List[str]):
        # todo: need to update later
        self.msg

    def _parse_query_msg(self, msg: List[str]):
        # todo: need to update later
        self.msg

    def _parse_control_msg(self, msg: List[str]):
        # todo: need to update later
        self.msg

    def _parse_log_msg(self, msg):
        if self.category == '-':
            return
        if msg:
            raise Exception("")

    def parse_log_str(self):
        # fp.seek(0)
        # line = None
        # while not line == "":
        #     line = fp.readline()
        # try:
        self.tokens = self._line_str.split()
        self.time = self.tokens[0]
        self.log_level = self.tokens[1]
        self.category = self.tokens[2]
        self.thread = self.tokens[3].replace("[", "").replace(']', '')
        if self.tokens[-1].endswith("ms"):
            self.taken_time = int(self.tokens[-1][:-2])
        self.msg = self._parse_msg(self.tokens[4:])
        getattr(self, f"_parse_{self.category.lower()}_msg", self._parse_log_msg)(self.tokens[4:])
        if not self.sub_category:
            logging.info("Yes")
