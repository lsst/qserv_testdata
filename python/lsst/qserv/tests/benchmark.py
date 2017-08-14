#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008-2015 LSST Corporation.
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

@author  Jacek Becla SLAC
@author  Fabrice Jammes IN2P3
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
from lsst.qserv.admin import dataDuplicator
from . import dataConfig
from . import mysqlDbLoader
from . import qservDbLoader
from .sql import cmd, const

MAX_QUERY = 10000


class Benchmark(object):

    def __init__(self, case_id, multi_node, testdata_dir, out_dirname_prefix=None):

        self.logger = logging.getLogger(__name__)
        self.dataLoader = dict()
        self._mode = None
        self._dbName = None

        self.config = commons.getConfig()

        self._case_id = case_id
        self._multi_node = multi_node

        if not out_dirname_prefix:
            out_dirname_prefix = self.config['qserv']['tmp_dir']
        self._out_dirname = os.path.join(out_dirname_prefix,
                                         "qservTest_case%s" % case_id)

        dataset_dir = Benchmark.getDatasetDir(testdata_dir, case_id)
        self._in_dirname = os.path.join(dataset_dir, 'data')

        self.dataReader = dataConfig.DataConfig(self._in_dirname)

        self._queries_dirname = os.path.join(dataset_dir, "queries")

        self.dataDuplicator = dataDuplicator.DataDuplicator(self.dataReader,
                                                            self._in_dirname,
                                                            self._out_dirname)

    @staticmethod
    def getDatasetDir(testdata_dir, case_id):
        LOG = logging.getLogger(__name__)
        if testdata_dir is not None and os.path.isdir(testdata_dir):
            LOG.debug("Setting testdata_dir value to %s", testdata_dir)
        else:
            LOG.fatal(
                "Datasets directory (%s) doesn't exists or isn't a directory",
                testdata_dir
            )
            sys.exit(errno.EIO)

        dataset_dir = os.path.join(testdata_dir, "case{0}".format(case_id))
        return dataset_dir

    def runQueries(self, stopAt=MAX_QUERY):
        self.logger.debug("Running queries : (stop-at: %s)", stopAt)
        if self._mode == 'qserv':
            withQserv = True
            sqlInterface = cmd.Cmd(config=self.config,
                                   mode=const.MYSQL_PROXY,
                                   database=self._dbName)
        else:
            withQserv = False
            sqlInterface = cmd.Cmd(config=self.config,
                                   mode=const.MYSQL_SOCK,
                                   database=self._dbName)

        myOutDir = os.path.join(self._out_dirname, "outputs", self._mode)
        if not os.access(myOutDir, os.F_OK):
            os.makedirs(myOutDir)
            # because mysqld will write there
            os.chmod(myOutDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        qDir = self._queries_dirname
        self.logger.debug("Testing queries from %s", qDir)
        queries = sorted(os.listdir(qDir))
        queryCount = 0
        queryRunCount = 0
        for qFN in queries:
            queryCount += 1
            if qFN.endswith(".sql"):
                queryRunCount += 1
                if int(qFN[:4]) <= stopAt:
                    self.logger.info("Launch %s against %s",
                                     qFN,
                                     self._mode)
                    query_filename = os.path.join(qDir, qFN)

                    qF = open(query_filename, 'r')
                    qText, pragmas = self._parseFile(qF, withQserv)

                    outFile = os.path.join(
                        myOutDir, qFN.replace('.sql', '.txt'))
                    #qText += " INTO OUTFILE '%s'" % outFile

                    self.logger.debug("SQL: %s pragmas: %s\n",
                                      qText,
                                      str(pragmas))
                    column_names = 'noheader' not in pragmas
                    sqlInterface.execute(qText, outFile, column_names)
                    if 'sortresult' in pragmas:
                        with open(outFile, "r+") as f:
                            sortedLines = sorted(f.readlines())
                            f.seek(0)
                            f.writelines(sortedLines)

        self.logger.info("Test case #%s: %s queries launched on a total of %s",
                         self._case_id,
                         queryRunCount,
                         queryCount)

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
            elif line.endswith("-- noQserv"):
                if withQserv:
                    # skip this line
                    pass
                else:
                    # strip the "-- noQserv" text
                    qText.append(line[:-10])
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
                # append all non-annotated lines
                qText.append(line)

        return ' '.join(qText), pragmas

    def loadData(self):
        """
        Creates orderedTables and load data for input file located in caseXX/data/
        """
        self.logger.info("Loading data from %s (%s mode)", self._in_dirname,
                         self._mode)
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
        self.logger.debug("Creation of data loader for %s mode", self._mode)
        if (self._mode == 'mysql'):
            self.dataLoader[self._mode] = mysqlDbLoader.MysqlLoader(
                self.config,
                self.dataReader,
                self._dbName,
                self._multi_node,
                self._out_dirname
            )
        elif (self._mode == 'qserv'):
            self.dataLoader[self._mode] = qservDbLoader.QservLoader(
                self.config,
                self.dataReader,
                self._dbName,
                self._multi_node,
                self._out_dirname
            )
        self.logger.debug("Initializing database for %s mode", self._mode)
        self.dataLoader[self._mode].prepareDatabase()

    def finalize(self):
        if (self._mode == 'qserv'):
            self.dataLoader['qserv'].workerInsertXrootdExportPath()

            # xrootd is restarted by wmgr

            # Reload Qserv (empty) chunk cache
            self.dataLoader['qserv'].resetChunksCache()

        # Close socket connections
        del(self.dataLoader[self._mode])

    def run(self, mode_list, load_data, stop_at_query=MAX_QUERY):

        self.cleanup()

        if load_data:
            if self.dataReader.duplicatedTables:
                self.logger.info("Tables to Duplicate %s", self.dataReader.duplicatedTables)
                self.dataDuplicator.run()

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
            self.logger.error("MySQL/Qserv differs for %s queries:",
                              len(failing_queries))
            self.logger.error("Broken queries list in %s: %s",
                              qserv_out_dir, failing_queries)

        return failing_queries
