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
Module defining TestQservLoader class

Unit tests for Qserv loader for integration tests.

@author  Fabrice Jammes, IN2P3/SLAC
"""

from lsst.qserv.admin import commons, logger
from lsst.qserv.tests.qservloader import QservLoader
from lsst.qserv.tests.dataconfig import DataConfig
import os
import sys
import unittest

class TestQservLoader(unittest.TestCase):

    def setUp(self):
        self.config = commons.read_user_config()
        self.logger = logger.init_default_logger(
            "TestQservLoader",
            log_path=self.config['qserv']['log_dir']
            )


    def test_alterTable(self):
        case_id_list = ["01","02","03"]

        for case_id in case_id_list:

            base_dir = os.getenv("QSERV_TESTDATA_DIR")

            if base_dir is None:
                self.logger.fatal("QSERV_TESTDATA_DIR environment missing.")
                sys.exit(1)

            qserv_tests_dirname = os.path.join(
                base_dir,
                'datasets',
                "case%s" % case_id
            )
            input_dirname = os.path.join(qserv_tests_dirname,'data')

            dataReader = DataConfig(input_dirname, "case%s" % case_id)
            dataReader.analyzeInputData()

            testDbName = "TestQservLoader%s" % case_id
            out_dir = os.path.join(self.config['qserv']['tmp_dir'],testDbName)
            qservDataLoader = QservLoader(
                self.config,
                dataReader,
                testDbName,
                out_dir,
                "TestQservLoader"
                )
            qservDataLoader.connectAndInitDatabase()
            for table_name in dataReader['partitioned-tables']:
                schema_filename = dataReader.getSchemaFile(table_name)
                qservDataLoader._sqlInterface['cmd'].executeFromFile(schema_filename)
                qservDataLoader.alterTable(table_name)

def suite():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestQservLoader)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
