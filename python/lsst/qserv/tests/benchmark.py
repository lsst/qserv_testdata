#
# LSST Data Management System
# Copyright 2008-2017 LSST Corporation.
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

from __future__ import absolute_import, division, print_function

import errno
from filecmp import dircmp
import logging
import os
import re
import shutil
import stat
import sys

from lsst.qserv.admin import commons
from lsst.qserv.admin import dataDuplicator
from . import dataConfig
from . import mysqlDbLoader
from . import qservDbLoader
from .sql import cmd, const

MAX_QUERY = 10000

_LOG = logging.getLogger(__name__)

class Benchmark(object):
    """Class implementing query running and result comparison for single test.

    Parameters
    ----------
    case_id : str
        Test case identifier, e.g. "01", "02", corresponds to directory name
        in datasets directory (after stripping initial "case")
    multi_node : boolean
        `True` for multi-node qserv setup.
    testdata_dir : str
        Location the directory containing test datasets
    out_dirname_prefix : str, optional
        Top-level directory for test outputs.
    """

    def __init__(self, case_id, multi_node, testdata_dir, out_dirname_prefix=None):

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
        """Returns directory name containg data for a test case.

        Parameters
        ----------
        testdata_dir : str
            Location the directory containing test datasets
        case_id : str
            Test case identifier, e.g. "01", "02", corresponds to directory name
            in datasets directory (after stripping initial "case")
        """
        if testdata_dir is not None and os.path.isdir(testdata_dir):
            _LOG.debug("Setting testdata_dir value to %s", testdata_dir)
        else:
            _LOG.fatal(
                "Datasets directory (%s) doesn't exists or isn't a directory",
                testdata_dir
            )
            sys.exit(errno.EIO)

        dataset_dir = os.path.join(testdata_dir, "case{0}".format(case_id))
        return dataset_dir

    def runQueries(self, mode, dbName, stopAt=MAX_QUERY):
        """Run all queries agains loaded data.

        Parameters
        ----------
        mode : str
            One of "mysql" or "qserv"
        dbName : str
            Database name
        stopAt : int, optional
            Max query number.
        """
        _LOG.debug("Running queries : (stop-at: %s)", stopAt)
        if mode == 'qserv':
            withQserv = True
            sqlInterface = cmd.Cmd(config=self.config,
                                   mode=const.MYSQL_PROXY,
                                   database=dbName)
        else:
            withQserv = False
            sqlInterface = cmd.Cmd(config=self.config,
                                   mode=const.MYSQL_SOCK,
                                   database=dbName)

        myOutDir = os.path.join(self._out_dirname, "outputs", mode)
        if not os.access(myOutDir, os.F_OK):
            os.makedirs(myOutDir)
            # because mysqld will write there
            os.chmod(myOutDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        qDir = self._queries_dirname
        _LOG.debug("Testing queries from %s", qDir)
        queries = sorted(os.listdir(qDir))
        queryCount = 0
        queryRunCount = 0
        for qFN in queries:
            queryCount += 1
            if qFN.endswith(".sql"):
                queryRunCount += 1
                if int(qFN[:4]) <= stopAt:
                    _LOG.info("Launch %s against %s", qFN, mode)
                    query_filename = os.path.join(qDir, qFN)

                    qF = open(query_filename, 'r')
                    qText, pragmas = self._parseFile(qF, withQserv)

                    outFile = os.path.join(
                        myOutDir, qFN.replace('.sql', '.txt'))
                    #qText += " INTO OUTFILE '%s'" % outFile

                    _LOG.debug("SQL: %s pragmas: %s\n", qText, pragmas)
                    column_names = 'noheader' not in pragmas
                    sqlInterface.execute(qText, outFile, column_names)
                    if 'sortresult' in pragmas:
                        with open(outFile, "r+b") as f:
                            sortedLines = sorted(f.readlines())
                            f.seek(0)
                            f.writelines(sortedLines)

        _LOG.info("Test case #%s: %s queries launched on a total of %s",
                  self._case_id, queryRunCount, queryCount)

    def _parseFile(self, qF, withQserv):
        """Reads a file with SQL query, filters it based on qserv/mysql mode
        and finds additional pragmas.

        Parameters
        ----------
        qF : file object
            open file with query text and extra stuff.
        withQserv : bool
            if `True` then prepare query for QServ, otherwise for mysql.

        Returns
        -------
        2-tuple of query text and set of pragmas as a dictionary.
        """

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

    def loadData(self, mode, dbName):
        """Loads data from input files located in caseXX/data/

        Parameters
        ----------
        mode : str
            One of "mysql" or "qserv"
        dbName : str
            Database name
        """
        dataLoader = self.connectAndInitDatabases(mode, dbName)
        _LOG.info("Loading data from %s (%s mode)", self._in_dirname, mode)
        for table in self.dataReader.orderedTables:
            dataLoader.createLoadTable(table)
        dataLoader.finalize()

    def cleanup(self):
        """Cleanup of previous tests output files
        """
        if os.path.exists(self._out_dirname):
            shutil.rmtree(self._out_dirname)
        os.makedirs(self._out_dirname)

    def connectAndInitDatabases(self, mode, dbName):
        """Establish database server connection and create database.

        Parameters
        ----------
        mode : str
            One of "mysql" or "qserv"
        dbName : str
            Database name

        Returns
        -------
        `DbLoader` instance to be used for data loading
        """
        _LOG.debug("Creation of data loader for %s mode", mode)
        if mode == 'mysql':
            dataLoader = mysqlDbLoader.MysqlLoader(
                self.config,
                self.dataReader,
                dbName,
                self._multi_node,
                self._out_dirname
            )
        elif mode == 'qserv':
            dataLoader = qservDbLoader.QservLoader(
                self.config,
                self.dataReader,
                dbName,
                self._multi_node,
                self._out_dirname
            )
        _LOG.debug("Initializing database for %s mode", mode)
        dataLoader.prepareDatabase()
        return dataLoader

    def run(self, mode_list, load_data, stop_at_query=MAX_QUERY):
        """Execute all tests in a test case.

        Parameters
        ----------
        mode_list : list
            List of strings like "mysql", "qserv".
        load_data : boolean
            If True the n load test data.
        """

        self.cleanup()

        if load_data:
            if self.dataReader.duplicatedTables:
                _LOG.info("Tables to Duplicate %s", self.dataReader.duplicatedTables)
                self.dataDuplicator.run()

        for mode in mode_list:

            dbName = "qservTest_case%s_%s" % (self._case_id, mode)

            if load_data:
                self.loadData(mode, dbName)

            self.runQueries(mode, dbName, stop_at_query)

    def analyzeQueryResults(self):
        """Compare results from runs with different modes.
        """

        outputs_dir = os.path.join(self._out_dirname, "outputs")

        failing_queries = []

        mysql_out_dir = os.path.join(outputs_dir, "mysql")
        qserv_out_dir = os.path.join(outputs_dir, "qserv")

        dcmp = dircmp(mysql_out_dir, qserv_out_dir)

        if self.dataReader.notLoadedTables:
            _LOG.info("Tables/Views not loaded: %s",
                      self.dataReader.notLoadedTables)

        if not dcmp.diff_files:
            _LOG.info("MySQL/Qserv results are identical")
        else:
            for query_name in dcmp.diff_files:
                failing_queries.append(query_name)
            _LOG.error("MySQL/Qserv differs for %s queries:",
                       len(failing_queries))
            _LOG.error("Broken queries list in %s: %s",
                       qserv_out_dir, failing_queries)

        return failing_queries
