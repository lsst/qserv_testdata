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
Launch integration tests for Qserv, using python unittest framework:

- load datasets in MySQL/Qserv
- query these dataset for MySQL/Qserv
- compare results

@author  Fabrice Jammes, IN2P3/SLAC
"""

import argparse
import logging
import os
import sys
import unittest

from lsst.qserv.admin import commons, logger
from lsst.qserv.tests import benchmark
from lsst.qserv.tests.testintegration import TestIntegration, suite

def parseArgs():

    parser = argparse.ArgumentParser(
        description='''Qserv integration tests suite. Relies on python unit
testing framework, provide test meta-data which can be used for example in a
continuous integration framework or by a cluster management tool. Configuration values
are read from ~/.lsst/qserv.conf.''',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser = logger.add_logfile_opt(parser)
    parser = benchmark.add_testdatadir_opt(parser)

    args = parser.parse_args()

    return args

if __name__ == '__main__':
    args = parseArgs()

    logger.setup_logging(args.log_conf)

    benchmark.init(args)
    result = unittest.TextTestRunner(verbosity=2).run(suite())
    retcode = int(not result.wasSuccessful())
    sys.exit(retcode)
