#!/usr/bin/env python
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
Launch units tests related to integration test framework

@author  Fabrice Jammes, IN2P3/SLAC
"""
import sys
import unittest

from lsst.qserv.tests.unittest import testDataConfig
from lsst.qserv.tests.unittest import testDataCustomizer

from lsst.qserv.admin import logger

if __name__ == '__main__':

    logger.setup_logging(logger.get_default_log_conf())

    modules = [testDataConfig, testDataCustomizer]

    retcode = 0
    for m in modules:
        result = unittest.TextTestRunner(verbosity=2).run(m.suite())
        if not result.wasSuccessful():
            retcode = 1

    sys.exit(retcode)
