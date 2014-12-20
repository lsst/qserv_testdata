#!/usr/bin/env python

from lsst.qserv.admin import commons, logger
import logging
from lsst.qserv.tests import benchmark

import argparse
import os
import sys
import tarfile

def parseArgs():

    parser = argparse.ArgumentParser(
            description='''Launch one Qserv integration test with fine-grained parameter, usefull for developers in order to debug/test manually a specific part of Qserv. Configuration values
are read from ~/.lsst/qserv.conf.''',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )

    parser = benchmark.add_generic_arguments(parser)

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

    args.verbose_level = logger.verbose_dict[args.verbose_str]

    if args.mode=='all':
        args.mode_list = ['mysql','qserv']
    else:
        args.mode_list = [args.mode]

    return args

def main():

    args = parseArgs()

    logger = logging.getLogger()

    benchmark.init(args, logfile="qserv-check-integration-dataset{0}".format(args.case_no))

    bench = benchmark.Benchmark(args.case_no, args.out_dirname)
    bench.run(args.mode_list, args.load_data, args.stop_at_query)

    return_code=0
    if (len(args.mode_list) > 1):
        failed_queries = bench.analyzeQueryResults()

        if len(failed_queries) == 0:
            logger.info("Test case #%s succeed", args.case_no)
            return_code=0
        else:
            if args.load_data == False:
                logger.warn("Please check that case%s data are loaded, " +
                            "otherwise run %s with --load option.",
                            args.case_no,
                            os.path.basename(__file__))
            logger.fatal("Test case #%s failed", args.case_no)
    else:
        logger.info("No result comparison")

    sys.exit(return_code)

if __name__ == '__main__':
    main()
