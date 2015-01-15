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
Launch one integration tests for Qserv

@author  Fabrice Jammes, IN2P3/SLAC
"""

import argparse
import logging
import os
import sys

from lsst.qserv.tests import benchmark
from lsst.qserv.admin import commons
from lsst.qserv.admin import logger


def parseArgs():

    parser = argparse.ArgumentParser(
        description="Launch one Qserv integration test with fine-grained " +
        "parameters, usefull for developers in order to debug/test " +
        "manually a specific part of Qserv. Configuration values " +
        "are read from ~/.lsst/qserv.conf.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser = logger.add_logfile_opt(parser)

    group = parser.add_argument_group('General options',
                                      'Options related to data loading and querying')

    group.add_argument("-i", "--case-no", dest="case_no",
                       default="01",
                       help="test case number")
    mode_option_values = ['mysql', 'qserv', 'all']
    group.add_argument(
        "-m", "--mode", dest="mode", choices=mode_option_values,
        default='all',
        help="Qserv test modes (direct mysql connection, or via qserv)")

    group = parser.add_argument_group('Load options',
                                      'Options related to data loading')
    group.add_argument("-l", "--load", action="store_true", dest="load_data",
                       default=False,
                       help="Load test dataset prior to query execution")

    default_testdata_dir = None
    if os.environ.get('QSERV_TESTDATA_DIR') is not None:
        default_testdata_dir = os.path.join(
            os.environ.get('QSERV_TESTDATA_DIR'), "datasets"
        )

    group.add_argument("-t", "--testdata-dir", dest="testdata_dir",
                       default=default_testdata_dir,
                       help="Absolute path to directory containing test " +
                       "datasets. This value is set, by precedence, by this" +
                       " option, and then by QSERV_TESTDATA_DIR/datasets/ " +
                       "if QSERV_TESTDATA_DIR environment variable is not " +
                       "empty"
                       )

    group = parser.add_argument_group('Query options',
                                      'Options related to query execution')
    config = commons.read_user_config()
    group.add_argument("-o", "--out-dir", dest="out_dirname",
                       default=config['qserv']['tmp_dir'],
                       help=("Full path to directory for storing query results."
                             "The results will be stored in "
                             "<OUT_DIRNAME>/qservTest_case<CASE_NO>/"))
    group.add_argument("-s", "--stop-at-query", type=int, dest="stop_at_query",
                       default=9999,
                       help="Stop at query with given number")

    args = parser.parse_args()

    if args.mode == 'all':
        args.mode_list = ['mysql', 'qserv']
    else:
        args.mode_list = [args.mode]

    return args


def main():

    args = parseArgs()

    logger.setup_logging(args.log_conf)
    log = logging.getLogger()

    commons.read_user_config()

    bench = benchmark.Benchmark(args.case_no, args.testdata_dir,
                                args.out_dirname)
    bench.run(args.mode_list, args.load_data, args.stop_at_query)

    returnCode = 1
    if len(args.mode_list) > 1:
        failed_queries = bench.analyzeQueryResults()

        if len(failed_queries) == 0:
            log.info("Test case #%s succeed", args.case_no)
            returnCode = 0
        else:
            log.fatal("Test case #%s failed", args.case_no)
            if args.load_data == False:
                log.warn("Please check that case%s data are loaded, " +
                         "otherwise run %s with --load option.",
                         args.case_no,
                         os.path.basename(__file__))

    else:
        log.info("No result comparison")
        returnCode = 0

    sys.exit(returnCode)

if __name__ == '__main__':
    main()
