"""
    Created by prakash at 22/04/22
"""
__author__ = 'Prakash14'

import logging
import unittest
logging.getLogger().setLevel(logging.DEBUG)


class TestMongoBase(unittest.TestCase):

    def setUp(self):
        super(TestMongoBase, self).setUp()
        self.log_file_path = "mock/mongo2.log"
