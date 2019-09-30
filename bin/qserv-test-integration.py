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


# To add test suites: add a name (key) and a factory lambda function (value) to the all_tests dictionary.
# Then it will be available in the arguments to the script, and will be run by default if test names aren't
# explicitly passed in by the caller.
# WARNING the testIntegration suite will run the data loader, and if data has not yet been loaded, any tests
# that need it will fail if testIntegration has not been run first. That is to say, tests should usually be
# added below/after testIntegration.
# At some point the data loader step should be pulled out of testIntegration and executed before running any
# tests, probably in this file but maybe somewhere else? TBD by you, dear reader.
all_tests = {'testIntegration': lambda: testIntegration.suite(multi_node),
             'testCall': lambda : testCall.suite()}


def _parse_args():

    parser = argparse.ArgumentParser(
        description=("Qserv integration tests suite. Relies on python unit testing framework, provide test "
                     "meta-data which can be used for example in a continuous integration framework or by a "
                     "cluster management tool. Configuration values are read from ~/.lsst/qserv.conf."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('-t', '--run-tests',
        help="The name of one or more test suites to execute. May include one or more of {}".format(
             list(all_tests.keys())),
        choices = all_tests.keys(),
        nargs = '+',
        default = all_tests.keys(),
        dest='run_tests')
    parser = logger.add_logfile_opt(parser)
    parser.add_argument('-q', action='store', dest='qserv_server', default='',
                        help='qserv server')
    parser.add_argument('-z', action='store_true', dest='multi_czar',
                        help='test on multi_czar configuration')
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
    qservServer = args.qserv_server
    multi_czar = args.multi_czar

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

#<<<<<<< HEAD
#<<<<<<< HEAD
#<<<<<<< HEAD
#<<<<<<< HEAD &&&
#    testRunner = unittest.TextTestRunner(verbosity=2)
#=======
#    result = unittest.TextTestRunner(verbosity=2).run(suite(multi_node, qservServer))
#>>>>>>> Made changes to add czar server as command line argument.
    
#=======
#    _LOG.error("&&& qservServer=%s", qservServer)
#    result = unittest.TextTestRunner(verbosity=2).run(suite(multi_node, qserv_server=qservServer))
#>>>>>>> qserv on czar1.localdomain is now getting the query, but it doesn't find the database schema.
#    testRunner = unittest.TextTestRunner(verbosity=2).run(suite(multi_node, qservServer))
#=======
#    _LOG.info("qservServer=%s", qservServer)
#    result = unittest.TextTestRunner(verbosity=2).run(suite(multi_node, qserv_server=qservServer))
#>>>>>>> Removed some log messages.
#=======
#    if not multi_node:
#        multi_czar = False
#
#    _LOG.info("qservServer=%s", qservServer)
#    result = unittest.TextTestRunner(verbosity=2).run(suite(multi_node, 
#                                                            qserv_server=qservServer,
#                                                            multi_czar=multi_czar))
#>>>>>>> Changed to create databases on all czars.

    if not multi_node:
        multi_czar = False


    testRunner = unittest.TextTestRunner(verbosity=2).run(suite(multi_node, 
                                                                qserv_server=qservServer,
                                                                multi_czar=multi_czar))

    for run_test in args.run_tests:
        verify(testRunner.run(all_tests[run_test]()))

    sys.exit(0)
