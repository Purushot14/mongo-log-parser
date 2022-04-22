"""
    Created by prakash at 22/04/22
"""
__author__ = 'Prakash14'

import json
import re


def get_json_from_long_text(search_str: str):
    start_idx = search_str.find("{")
    stop_idx = 0
    brace_counter = 0
    for match in re.finditer(r'{|}', search_str):
        stop_idx = match.start()
        if search_str[stop_idx] == '{':
            brace_counter += 1
        else:
            brace_counter -= 1
        if brace_counter == 0:
            break
    from .json_decoder import JSONDecoder
    json_str = search_str[start_idx:stop_idx + 1]
    # loaded_json: dict = pyjson5.loads(json_str.strip())
    out = json.loads(json_str, cls=JSONDecoder)
    # out = json_util.loads(json_str, cls=JSONDecoder)
    return out


def get_json_array_str_from_long_text(search_str: str):
    start_idx = search_str.find("[")
    stop_idx = 0
    brace_counter = 0
    for match in re.finditer(r'\[|\]', search_str):
        stop_idx = match.start()
        if search_str[stop_idx] == '[':
            brace_counter += 1
        else:
            brace_counter -= 1
        if brace_counter == 0:
            break
    json_str = search_str[start_idx:stop_idx + 1]
    return json_str
