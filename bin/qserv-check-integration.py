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

from lsst.qserv.admin import commons
from lsst.qserv.admin import logger
from lsst.qserv.tests import benchmark
from lsst.qserv.tests import dataCustomizer

LOG = logging.getLogger()


def parseArgs():

    # used to get default values
    config = commons.read_user_config()

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

    group.add_argument("-i", "--case-id", dest="case_id",
                       default="01",
                       help="Test case number")
    mode_option_values = ['mysql', 'qserv', 'all']
    group.add_argument("-m", "--mode", dest="mode", choices=mode_option_values,
                       default='all',
                       help="Qserv test modes (direct mysql connection, or via qserv)")
    group.add_argument("-mn", "--multi", action="store_true", dest="multi_node",
                       default=False,
                       help="Run test in multi-node (must specify --mode=qserv")

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

    group.add_argument("-o", "--out-dir", dest="out_dir",
                       default=config['qserv']['tmp_dir'],
                       help=("Absolute path to directory for storing query results."
                             "The results will be stored in "
                             "<OUT_DIR>/qservTest_case<CASE_ID>/"))
    group.add_argument("-s", "--stop-at-query", type=int, dest="stop_at_query",
                       default=benchmark.MAX_QUERY,
                       help="Stop at query with given number")

    group = parser.add_argument_group('Input dataset customization options',
                                      ('Options related to input data set customization'
                                       ))
    group.add_argument("-T", "--work-dir", dest="work_dir",
                       default=config['qserv']['tmp_dir'],
                       help="Absolute path to parent directory where source test " +
                       "datasets will be copied, and big datasets will be " +
                       "eventually downloaded"
                       )
    group.add_argument("-C", "--custom", action="store_true", dest="do_custom",
                       default=False,
                       help="If <WORK_DIR>/case<CASE_ID> doesn't exists"
                       ", copy it from <TESTDATA_DIR>"
                       ", disable load and query operations"
                       ", and had to be performed before them")
    group.add_argument("-D", "--download", action="store_true", dest="do_download",
                       default=False,
                       help=("Download big datasets using rsync over ssh"
                             ", implies --custom, enable batch mode with "
                             "~/.ssh/config and ssh-agent"))
    group.add_argument("-I", "--custom-case-id", dest="custom_case_id",
                       default=None,
                       help="Rename custom test to case/CUSTOM_CASE_ID")
    group.add_argument("-U", "--username", dest="username",
                       default=None,
                       help="rsync username")

    args = parser.parse_args()

    if args.do_download:
        args.do_custom = True

    if args.mode == 'all':
        args.mode_list = ['mysql', 'qserv']
    else:
        args.mode_list = [args.mode]

    if args.multi_node == True and args.mode != 'qserv':
        print "Select --mode=qserv for multi-node test"
        sys.exit()

    return args


def run_integration_test(case_id, testdata_dir, out_dir, mode_list,
                         multi_node,load_data, stop_at_query):
    ''' Run integration tests, eventually perform data-loading and query results
    comparison
    @param case_id: test case number
    @param testdata_dir: directory containing test datasets
    @param out_dir: directory containing query results
    @param mode_list: run test for Qserv, MySQL or both
    @param multi_node: run test in multi-node setup
    @param load_data: load data before running queries
    @param stop_at_query: run queries between 0 and it
    '''
    bench = benchmark.Benchmark(case_id, multi_node, testdata_dir, out_dir)
    bench.run(mode_list, load_data, stop_at_query)

    returnCode = 1
    if len(mode_list) > 1:
        failed_queries = bench.analyzeQueryResults()

        if len(failed_queries) == 0:
            LOG.info("Test case #%s succeed", case_id)
            returnCode = 0
        else:
            LOG.fatal("Test case #%s failed", case_id)
            if load_data == False:
                log.warn("Please check that case%s data are loaded, " +
                         "otherwise run %s with --load option.",
                         case_id,
                         os.path.basename(__file__))

    else:
        LOG.info("No result comparison")
        returnCode = 0
    return returnCode


def main():

    args = parseArgs()

    logger.setup_logging(args.log_conf)

    returnCode = 1
    if args.do_custom:
        customizer = dataCustomizer.DataCustomizer(args.case_id,
                                                   args.testdata_dir,
                                                   args.work_dir,
                                                   args.do_download,
                                                   args.custom_case_id,
                                                   args.username)

        customizer.run()

    else:
        returnCode = run_integration_test(args.case_id, args.testdata_dir,
                                          args.out_dir, args.mode_list,
                                          args.multi_node,
                                          args.load_data, args.stop_at_query)

    sys.exit(returnCode)

if __name__ == '__main__':
    main()
