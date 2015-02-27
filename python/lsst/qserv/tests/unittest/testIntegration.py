# LSST Data Management System
# Copyright 2014 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
Module defining TestIntegration class

Launch each integration tests, using unittest framework.

@author  Fabrice Jammes, IN2P3/SLAC
"""
import logging
import os
import unittest

from lsst.qserv.admin import commons
from lsst.qserv.tests.benchmark import Benchmark


class TestIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestIntegration, cls).setUpClass()
        TestIntegration.config = commons.getConfig()
        TestIntegration.logger = logging.getLogger(__name__)
        TestIntegration.modeList = ['mysql', 'qserv']
        TestIntegration.loadData = True

        if os.environ.get('QSERV_TESTDATA_DIR') is not None:
            TestIntegration.testdata_dir = os.path.join(os.environ.get('QSERV_TESTDATA_DIR'),
                                             "datasets")
        else:
            current_file = os.path.dirname(os.path.realpath(__file__))
            fragile_testdata_dir = os.path.join(current_file, os.pardir,
                                                  os.pardir, os.pardir,
                                                  os.pardir, os.pardir,
                                                  "datasets"
                                         )
            TestIntegration.testdata_dir = os.path.abspath(fragile_testdata_dir)


    def _runTestCase(self, case_id):
        self.assertTrue(os.path.exists(self.testdata_dir),
                   msg="non existing testdata_dir {0}".format(self.testdata_dir))
        bench = Benchmark(case_id, self.testdata_dir)
        bench.run(self.modeList, self.loadData)
        failed_queries = bench.analyzeQueryResults()
        return not failed_queries

    def test_case01(self):
        case_id = "01"
        self._runTestCase(case_id)

    def test_case02(self):
        case_id = "02"
        self._runTestCase(case_id)

    def test_case03(self):
        case_id = "03"
        self._runTestCase(case_id)

    def test_case04(self):
        case_id = "04"
        self._runTestCase(case_id)

    def test_case05(self):
        case_id = "05"
        self._runTestCase(case_id)


def suite():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIntegration)
    return suite
