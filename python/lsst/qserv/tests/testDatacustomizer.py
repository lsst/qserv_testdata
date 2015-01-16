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
Coverage needs to be extended.

@author  Fabrice Jammes, IN2P3/SLAC
"""
import logging
import os
import unittest


from lsst.qserv.admin import commons
from lsst.qserv.admin import logger
from lsst.qserv.tests.dataCustomizer import DataCustomizer
from lsst.qserv.tests.dataConfig import DataConfig

class TestDataCustomizer(unittest.TestCase):

    def setUp(self):
        self._config = commons.read_user_config()
        self._logger = logging.getLogger(__name__)
        
        
        self._url = ("lsst-dev.ncsa.illinois.edu:"
               "/lsst/home/fjammes/public/qserv_testdata"
               "/datasets/case04/data/DeepSource.csv.gz")
        self._dest_file = os.path.join("/", "tmp", os.path.basename(self._url))
        try:
            os.remove(self._dest_file)
        except:
            pass
        
    def tearDown(self):
        try:
            os.remove(self._dest_file)
        except:
            pass

    def test_rsync(self):
        self._logger.info("Test rsync call on %s", self._url)
        DataCustomizer._rsync(self._url, self._dest_file)
        assert(os.path.exists(self._dest_file))

def suite():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDataCustomizer)
    return suite

if __name__ == '__main__':
    logger.setup_logging(logger.get_default_log_conf())
    unittest.TextTestRunner(verbosity=2).run(suite())
