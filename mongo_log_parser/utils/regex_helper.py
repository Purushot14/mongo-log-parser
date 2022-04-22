"""
    Created by prakash at 22/04/22
"""
__author__ = 'Prakash14'

import re


class REGEX:
    V4IP_ADDRESS = r"[0-9]+(?:\.[0-9]+){3}"
    SCIM_SCHEMA = "(?:[a-zA-Z0-9_-]+:){8}"


def get_ip_addresses_from_text(text):
    return re.findall(REGEX.V4IP_ADDRESS, text)
