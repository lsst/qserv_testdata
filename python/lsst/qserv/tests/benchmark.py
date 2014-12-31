#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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
#

"""
Integration test tool :
- loads multiple datasets in Qserv and MySQL,
- launches queries against them
- checks if results between both databases are identical

@author = "Jacek Becla, Fabrice Jammes"
"""

import logging
import shutil
import errno
import os
import re
import stat
import sys
from filecmp import dircmp

from lsst.qserv.admin import commons
from lsst.qserv.tests import dataconfig, qservloader, mysqlloader
from lsst.qserv.tests.sql import cmd, const


class Benchmark(object):

    def __init__(self, case_id, out_dirname_prefix):

        self.logger = logging.getLogger(__name__)
        self.dataLoader = dict()
        self._sqlInterface = dict()
        self._mode = None
        self._dbName = None

        self.config = commons.getConfig()

        self.noQservLine = re.compile('[\w\-\."%% ]*-- noQserv')

        self._case_id = case_id

        if not out_dirname_prefix:
            out_dirname_prefix = self.config['qserv']['tmp_dir']
        self._out_dirname = os.path.join(out_dirname_prefix,
                                         "qservTest_case%s" % case_id)

        self.testdata_dir = self.config['qserv']['testdata_dir']

        qserv_tests_dirname = os.path.join(
            self.testdata_dir,
            "case%s" % self._case_id
        )

        self._in_dirname = os.path.join(qserv_tests_dirname, 'data')

        self.dataReader = dataconfig.DataConfig(self._in_dirname,
                                                "case%s" % self._case_id)

        self._queries_dirname = os.path.join(qserv_tests_dirname, "queries")

    def runQueries(self, stopAt):
        self.logger.debug("Running queries : (stop-at : %s)" % stopAt)
        if self._mode == 'qserv':
            withQserv = True
            self._sqlInterface['query'] = cmd.Cmd(config=self.config,
                                                  mode=const.MYSQL_PROXY,
                                                  database=self._dbName
                                                  )
        else:
            withQserv = False
            self._sqlInterface['query'] = cmd.Cmd(config=self.config,
                                                  mode=const.MYSQL_SOCK,
                                                  database=self._dbName
                                                  )

        myOutDir = os.path.join(self._out_dirname, "outputs", self._mode)
        if not os.access(myOutDir, os.F_OK):
            os.makedirs(myOutDir)
            # because mysqld will write there
            os.chmod(myOutDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        qDir = self._queries_dirname
        self.logger.debug("Testing queries from %s" % qDir)
        queries = sorted(os.listdir(qDir))
        queryCount = 0
        queryRunCount = 0
        for qFN in queries:
            queryCount += 1
            if qFN.endswith(".sql"):
                queryRunCount += 1
                if int(qFN[:4]) <= stopAt:
                    query_filename = os.path.join(qDir, qFN)

                    qF = open(query_filename, 'r')
                    qText, pragmas = self._parseFile(qF, withQserv)

                    outFile = os.path.join(
                        myOutDir, qFN.replace('.sql', '.txt'))
                    #qText += " INTO OUTFILE '%s'" % outFile
                    self.logger.info("Launch: {1} against: {0}"
                                     .format(self._mode, qFN))
                    self.logger.debug("SQL: {0} pragmas: {1}\n"
                                      .format(self._mode, qFN, qText, pragmas))
                    column_names = 'noheader' not in pragmas
                    self._sqlInterface['query'].execute(qText,
                                                        outFile,
                                                        column_names)

        self.logger.info("Test case #{1}: {0} queries launched on a total of {2}"
                         .format(queryRunCount, self._case_id, queryCount))

    def _parseFile(self, qF, withQserv):
        '''
        Reads a file with SQL query, filters it based on qserv/mysql mode
        and finds additional pragmas. Returns query text and set of pragmas
        as a dictionary.
        '''

        qText = []
        pragmas = {}
        for line in qF:

            # squeeze/strip spaces
            line = line.strip()
            line = re.sub(' +', ' ', line)

            if not line:
                # empty
                pass
            elif withQserv and line.startswith("-- withQserv"):
                # strip the "-- withQserv" text
                qText.append(line[13:])
            elif line.startswith("--"):
                # check for pragma, format is:
                #    '-- pragma keyval [keyval...]'
                #    where keyval is 'key=value' or 'key'
                words = line.split()
                if len(words) > 1 and words[1] == 'pragma':
                    for keyval in words[2:]:
                        kv = keyval.split('=', 1) + [None]
                        pragmas[kv[0]] = kv[1]
            else:
                qData = self.noQservLine.search(line)
                if not withQserv:
                    if qData:
                        qText.append(qData.group(0)[:-10])
                    else:
                        qText.append(line)
                elif not qData:
                    qText.append(line)

        return ' '.join(qText), pragmas

    def loadData(self):
        """
        Creates orderedTables and load data for input file located in caseXX/data/
        """
        self.logger.info("Loading data from %s (%s mode)" % (self._in_dirname,
                                                             self._mode))
        for table in self.dataReader.orderedTables:
            self.dataLoader[self._mode].createLoadTable(table)

    def cleanup(self):
        """
        Cleanup of previous tests temporary ant output files
        """
        if os.path.exists(self._out_dirname):
            shutil.rmtree(self._out_dirname)
        os.makedirs(self._out_dirname)

    def connectAndInitDatabases(self):
        self.logger.debug("Creation of data loader for %s mode" % self._mode)
        if (self._mode == 'mysql'):
            self.dataLoader[self._mode] = mysqlloader.MysqlLoader(
                self.config,
                self.dataReader,
                self._dbName,
                self._out_dirname
            )
        elif (self._mode == 'qserv'):
            self.dataLoader[self._mode] = qservloader.QservLoader(
                self.config,
                self.dataReader,
                self._dbName,
                self._out_dirname
            )
        self.logger.debug("Initializing database for %s mode" % self._mode)
        self.dataLoader[self._mode].prepareDatabase()

    def finalize(self):
        if (self._mode == 'qserv'):
            self.dataLoader['qserv'].workerInsertXrootdExportPath()

            # Reload xroot export paths w.r.t loaded chunks
            commons.restart('xrootd')

            # Reload Qserv meta-data
            commons.restart('qserv-czar')

        # Close socket connections
        del(self.dataLoader[self._mode])

    def run(self, mode_list, load_data, stop_at_query=7999):

        self.cleanup()
        self.dataReader.analyzeInputData()

        for mode in mode_list:
            self._mode = mode

            self._dbName = "qservTest_case%s_%s" % (self._case_id, self._mode)

            if load_data:
                self.connectAndInitDatabases()
                self.loadData()
                self.finalize()

            self.runQueries(stop_at_query)

    def analyzeQueryResults(self):

        outputs_dir = os.path.join(self._out_dirname, "outputs")

        failing_queries = []

        mysql_out_dir = os.path.join(outputs_dir, "mysql")
        qserv_out_dir = os.path.join(outputs_dir, "qserv")

        dcmp = dircmp(mysql_out_dir, qserv_out_dir)

        if self.dataReader.notLoadedTables:
            self.logger.info("Tables/Views not loaded: %s",
                             self.dataReader.notLoadedTables)

        if not dcmp.diff_files:
            self.logger.info("MySQL/Qserv results are identical")
        else:
            for query_name in dcmp.diff_files:
                failing_queries.append(query_name)
            self.logger.error("MySQL/Qserv differs for {0} queries:"
                              .format(len(failing_queries)))
            self.logger.error("Broken queries list in {0}: {1}"
                              .format(qserv_out_dir, failing_queries))

        return failing_queries


def add_arguments(parser):

    default_log_conf = "{0}/.lsst/logging.yaml".format(os.path.expanduser('~'))
    parser.add_argument("-V", "--log-cfg", dest="log_conf",
                    default=default_log_conf,
                    help="Absolute path to yaml file containing python" +
                    " standard logger configuration file"
                        )

    default_testdata_dir = None
    if os.environ.get('QSERV_TESTDATA_DIR') is not None:
        default_testdata_dir = os.path.join(
            os.environ.get('QSERV_TESTDATA_DIR'), "datasets"
        )

    parser.add_argument("-t", "--testdata-dir", dest="testdata_dir",
                        default=default_testdata_dir,
                        help="Absolute path to directory containing test " +
                        "datasets. This value is set, by precedence, by this" +
                        " option, and then by QSERV_TESTDATA_DIR/datasets/ " +
                        "if QSERV_TESTDATA_DIR environment variable is not "+
                        "empty"
                        )
    return parser


def init(args):

    config = commons.read_user_config()

    log = logging.getLogger(__name__)

    if args.testdata_dir is not None and os.path.isdir(args.testdata_dir):
        log.debug("Setting testdata_dir value to %s",
                  args.testdata_dir
                  )
        config['qserv']['testdata_dir'] = args.testdata_dir
    else:
        log.fatal(
            "Unable to find tests datasets. (testdata_dir value is %s)",
            args.testdata_dir
        )
        sys.exit(errno.EIO)
