#!/usr/bin/env python
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
import tarfile

from lsst.qserv.tests import benchmark
from lsst.qserv.admin import commons, logger


def parseArgs():

    parser = argparse.ArgumentParser(
            description="Launch one Qserv integration test with fine-grained " +
            "parameters, usefull for developers in order to debug/test " +
            "manually a specific part of Qserv. Configuration values "+
            "are read from ~/.lsst/qserv.conf.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )

    parser = logger.add_logfile_opt(parser)
    parser = benchmark.add_testdatadir_opt(parser)

    parser.add_argument("-i", "--case-no", dest="case_no",
              default="01",
              help="test case number")
    mode_option_values = ['mysql','qserv','all']
    parser.add_argument("-m", "--mode", dest="mode", choices=mode_option_values,
              default='all',
              help= "Qserv test modes (direct mysql connection, or via qserv)")
    parser.add_argument("-s", "--stop-at-query", type=int, dest="stop_at_query",
              default = 7999,
              help="Stop at query with given number")
    parser.add_argument("-l", "--load", action="store_true", dest="load_data", default=False,
              help="Load test dataset prior to query execution")
    parser.add_argument("-o", "--out-dir", dest="out_dirname",
              help="Full path to directory for storing temporary results. The results will be stored in <OUTDIR>/qservTest_case<CASENO>/")
    args = parser.parse_args()

    if args.mode=='all':
        args.mode_list = ['mysql','qserv']
    else:
        args.mode_list = [args.mode]

    return args

def main():

    args = parseArgs()

    logger.setup_logging(args.log_conf)
    log = logging.getLogger(__name__)

    benchmark.init(args)
    bench = benchmark.Benchmark(args.case_no, args.out_dirname)
    bench.run(args.mode_list, args.load_data, args.stop_at_query)

    return_code=0
    if (len(args.mode_list) > 1):
        failed_queries = bench.analyzeQueryResults()

        if len(failed_queries) == 0:
            log.info("Test case #%s succeed", args.case_no)
            return_code=0
        else:
            if args.load_data == False:
                log.warn("Please check that case%s data are loaded, " +
                            "otherwise run %s with --load option.",
                            args.case_no,
                            os.path.basename(__file__))
            log.fatal("Test case #%s failed", args.case_no)
    else:
        log.info("No result comparison")

    sys.exit(return_code)

if __name__ == '__main__':
    main()
