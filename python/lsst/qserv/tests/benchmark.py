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

try:
    import configparser
except ImportError:
    import ConfigParser as configparser  # python2
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

# list of possible modes accepted by run() metho
MODES = ['mysql', 'qserv', 'qserv_async']

MAX_QUERY = 10000

_LOG = logging.getLogger(__name__)

def is_multi_node():
    """ Check is Qserv install is multi node

        it assumes integration tests are launched
        on master for mono-node instance

        Returns
        -------

        true if Qserv install is multi-node
    """
    multi_node = True
    # FIXME code below is specific to mono-node setup
    # and might be removed
    config = commons.read_user_config()
    run_dir = config['qserv']['qserv_run_dir']
    config_file = os.path.join(run_dir, "qserv-meta.conf")
    if os.path.isfile(config_file):
        parser = configparser.SafeConfigParser()
        parser.read(config_file)
        if parser.get('qserv', 'node_type') in ['mono']:
            _LOG.info("Running Integration test in mono-node setup")
            multi_node = False
    return multi_node

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
    czar_list: list
        list of czar addresses (czar1.localdomain) that should be updated.
    """

    def __init__(self, case_id, multi_node, testdata_dir,
                 out_dirname_prefix=None, czar_list=None):

        self.config = commons.read_user_config()

        self._case_id = case_id
        self._multi_node = multi_node
        if czar_list is None:
            czar_list = []
        self._czar_list = czar_list

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

    def runQueries(self, mode, dbName, stopAt=MAX_QUERY, qservServer=""):
        """Run all queries agains loaded data.

        Parameters
        ----------
        mode : str
            One of MODES values
        dbName : str
            Database name
        stopAt : int, optional
            Max query number.
        qservServer: str 
            address of the effective qserv master (master.localdomain)
        """
        _LOG.debug("Running queries : (stop-at: %s)", stopAt)
        if mode in ('qserv', 'qserv_async'):
            withQserv = True
            if (not qservServer):
                sqlInterface = cmd.Cmd(config=self.config,
                                       mode=const.MYSQL_PROXY,
                                       database=dbName)
            else:
                conf = self.config
                conf['qserv']['master'] = qservServer
                _LOG.debug(" conf=%s", conf)
                sqlInterface = cmd.Cmd(conf,
                                       mode=const.MYSQL_PROXY,
                                       database=dbName)
        elif mode == 'mysql':
            withQserv = False
            sqlInterface = cmd.Cmd(config=self.config,
                                   mode=const.MYSQL_SOCK,
                                   database=dbName)
        else:
            raise ValueError("unexpected mode: " + str(mode))

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
        dbNameDot = dbName + '.'
        for qFN in queries:
            queryCount += 1
            if qFN.endswith(".sql"):
                queryRunCount += 1
                if int(qFN[:4]) <= stopAt:
                    _LOG.info("Launch %s mode=%s db=%s", qFN, mode, dbNameDot)
                    query_filename = os.path.join(qDir, qFN)

                    qF = open(query_filename, 'r')
                    qText, pragmas = self._parseFile(qF, withQserv)
                    # qText needs correct database name inserted.
                    qText = qText.replace('{DBTAG_A}', dbNameDot)
                    _LOG.debug("qText=%s", qText)

                    outFile = os.path.join(
                        myOutDir, qFN.replace('.sql', '.txt'))
                    #qText += " INTO OUTFILE '%s'" % outFile

                    _LOG.debug("SQL: %s pragmas: %s\n", qText, pragmas)
                    column_names = 'noheader' not in pragmas

                    async_timeout = 0
                    if mode == 'qserv_async':
                        # no_async pragma disables async behaviour
                        if "no_async" not in pragmas:
                            # default timeout for async queries is 10 minutes, allow to
                            # override it via "pragma async_timeout=NNN"
                            async_timeout = int(pragmas.get('async_timeout', 600))
                    sqlInterface.execute(qText, outFile, column_names, async_timeout)
                    if 'sortresult' in pragmas:
                        try:
                            with open(outFile, "r+b") as f:
                                sortedLines = sorted(f.readlines())
                                f.seek(0)
                                f.writelines(sortedLines)
                        except OSError as exc:
                            # file probably does not exist
                            _LOG.error("Failed to sort output: %s", exc)

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
            One of MODES values.
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
            One of MODES values.
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
                self._out_dirname,
                self._czar_list
            )
        else:
            raise ValueError("unexpected mode: " + str(mode))

        _LOG.debug("Initializing database for %s mode", mode)
        dataLoader.prepareDatabase()
        return dataLoader

    def run(self, mode_list, load_data, stop_at_query=MAX_QUERY, qservServer=""):
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

        if load_data:
            # when loading qserv_async is the same as qserv (do not load twice)
            load_modes = set('qserv' if mode == 'qserv_async' else mode for mode in mode_list)
            for mode in load_modes:
                dbName = "qservTest_case%s_%s" % (self._case_id, mode)
                self.loadData(mode, dbName)

        for mode in mode_list:

            dbName = "qservTest_case%s_%s" % (self._case_id,
                                              'qserv' if mode == 'qserv_async' else mode)
            self.runQueries(mode, dbName, stop_at_query, qservServer)

    def analyzeQueryResults(self, mode_list):
        """Compare results from runs with different modes.

        If "mysql" is in the mode_list compare all other modes against "mysql",
        otherwise compare all others against first.

        Parameters
        ----------
        mode_list : list
            List of strings like "mysql", "qserv", length of list should be at least 2.
        """

        outputs_dir = os.path.join(self._out_dirname, "outputs")

        failing_queries = []

        baseline = 'mysql' if 'mysql' in mode_list else mode_list[0]
        other_modes = [mode for mode in mode_list if mode != baseline]

        baseline_out_dir = os.path.join(outputs_dir, baseline)
        for mode in other_modes:

            other_out_dir = os.path.join(outputs_dir, mode)

            dcmp = dircmp(baseline_out_dir, other_out_dir)

            if self.dataReader.notLoadedTables:
                _LOG.info("%s/%s: Tables/Views not loaded: %s",
                          baseline, mode, self.dataReader.notLoadedTables)

            diffs = dcmp.left_only + dcmp.right_only + dcmp.diff_files
            if not diffs:
                _LOG.info("%s/%s results are identical", baseline, mode)
            else:
                _LOG.error("%s/%s differs for %s queries:",
                           baseline, mode, len(diffs))
                _LOG.error("Broken queries list in %s: %s",
                           other_out_dir, diffs)

                failing_queries += diffs

        return failing_queries
