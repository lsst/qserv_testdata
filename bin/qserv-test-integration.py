#!/usr/bin/env python

#
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

from __future__ import absolute_import, division, print_function

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import logging
import os
import sys
import unittest

# ----------------------------
# Imports for other modules --
# ----------------------------
import lsst.log
from lsst.qserv.admin import commons
from lsst.qserv.admin import logger
from lsst.qserv.tests import benchmark
from lsst.qserv.tests.unittest import testIntegration, testCall

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger()


def _parse_args():

    parser = argparse.ArgumentParser(
        description='''Qserv integration tests suite. Relies on python unit
testing framework, provide test meta-data which can be used for example in a
continuous integration framework or by a cluster management tool. Configuration values
are read from ~/.lsst/qserv.conf.''',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser = logger.add_logfile_opt(parser)
    _args = parser.parse_args()

    return _args


def verify(result):
    if result.wasSuccessful():
        _LOG.info("Integration test succeeded")
    else:
        _LOG.fatal("Integration test failed")
        sys.exit(1)


# -----------------------
# Exported definitions --
# -----------------------
if __name__ == '__main__':

    args = _parse_args()
    logger.setup_logging(args.log_conf)

    # configure log4cxx logging based on the logging level of Python logger
    levels = {logging.ERROR: lsst.log.ERROR,
              logging.WARNING: lsst.log.WARN,
              logging.INFO: lsst.log.INFO,
              logging.DEBUG: lsst.log.DEBUG}
    lsst.log.setLevel('', levels.get(_LOG.level, lsst.log.DEBUG))

    config = commons.read_user_config()
    run_dir = config['qserv']['qserv_run_dir']
    config_file = os.path.join(run_dir, "qserv-meta.conf")

    multi_node = benchmark.is_multi_node()

    testRunner = unittest.TextTestRunner(verbosity=2)

    tests = [testCall.suite(),
             testIntegration.suite(multi_node)]

    for test in tests:
        verify(testRunner.run(test))

    # result = testRunner.run(testIntegration.suite(multi_node))
    # verify(result)

    sys.exit(0)
