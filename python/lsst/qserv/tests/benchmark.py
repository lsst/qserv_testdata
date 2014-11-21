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

""" This test tool :
- loads multiple datasets in Qserv and MySQL,
- launches queries against them
- checks if results between both DB are the same
"""

__author__ = "Jacek Becla, Fabrice Jammes"

import logging
import optparse
import shutil
import tarfile

from lsst.qserv.tests import qservdataloader, mysqldataloader, datareader
from lsst.qserv.admin import commons, download, logger
from lsst.qserv.tests.sql import cmd, connection, const
import errno
import os
import re
import stat
import sys

from filecmp import dircmp


class Benchmark():

    def __init__(self, case_id, out_dirname_prefix,
                 log_file_prefix='qserv-tests',
                 logging_level=logging.INFO ):

        self.logger = logging.getLogger()
        self.dataLoader = dict()
        self._sqlInterface = dict()
        self._mode=None
        self._dbName=None

        self.config = commons.getConfig()

        self.noQservLine = re.compile('[\w\-\."%% ]*-- noQserv')

        self._case_id = case_id
        self._logFilePrefix = log_file_prefix

        if out_dirname_prefix == None :
            out_dirname_prefix = self.config['qserv']['tmp_dir']
        self._out_dirname = os.path.join(out_dirname_prefix, "qservTest_case%s" % case_id)

        self.testdata_dir = self.config['qserv']['testdata_dir']

        qserv_tests_dirname = os.path.join(
            self.testdata_dir,
            "case%s" % self._case_id
        )

        self._in_dirname = os.path.join(qserv_tests_dirname,'data')

        self.dataReader = datareader.DataReader(self._in_dirname, "case%s" % self._case_id)

        self._queries_dirname = os.path.join(qserv_tests_dirname,"queries")

    def runQueries(self, stopAt):
        self.logger.debug("Running queries : (stop-at : %s)" % stopAt)
        if self._mode == 'qserv':
            withQserv = True
            self._sqlInterface['query'] = cmd.Cmd(config = self.config,
                                                  mode = const.MYSQL_PROXY,
                                                  database = self._dbName
                                                  )
        else:
            withQserv = False
            self._sqlInterface['query'] = cmd.Cmd(config = self.config,
                                                mode = const.MYSQL_SOCK,
                                                database = self._dbName
                                                )

        myOutDir = os.path.join(self._out_dirname, "outputs",self._mode)
        if not os.access(myOutDir, os.F_OK):
            os.makedirs(myOutDir)
            # because mysqld will write there
            os.chmod(myOutDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        qDir = self._queries_dirname
        self.logger.info("Testing queries from %s" % qDir)
        queries = sorted(os.listdir(qDir))
        for qFN in queries:
            if qFN.endswith(".sql"):
                if int(qFN[:4]) <= stopAt:
                    query_filename = os.path.join(qDir,qFN)

                    qF = open(query_filename, 'r')
                    qText, pragmas = self._parseFile(qF, withQserv)

                    outFile = os.path.join(myOutDir, qFN.replace('.sql', '.txt'))
                    #qText += " INTO OUTFILE '%s'" % outFile
                    self.logger.info("LAUNCHING QUERY : {1} against {0}, SQL : {2} pragmas : {3}\n".format(self._mode, qFN, qText, pragmas))

                    column_names = 'noheader' not in pragmas
                    self._sqlInterface['query'].execute(qText, outFile, column_names)


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


    def gunzip(self, table_name, zipped_data_filename):
        # check if the corresponding data file exists
        if not os.path.exists(zipped_data_filename):
            raise Exception, "File: '%s' not found" %  zipped_data_filename

        tmp_suffix = ("%s%s" % (table_name,self.dataReader.dataConfig['data-extension']))
        tmp_data_file = os.path.join(self._out_dirname,tmp_suffix)

        self.logger.info("Unzipping: %s into %s" %  (zipped_data_filename, tmp_data_file))
        commons.run_command(["gunzip", "-c", zipped_data_filename], stdout_file=tmp_data_file)
        return tmp_data_file

    def loadData(self):
        """
        Creates tables and load data for input file located in caseXX/data/
        """
        self.logger.info("Loading data from %s (%s mode)" % (self._in_dirname, self._mode))

        for table_name in  self.dataReader.tables:
            self.logger.debug("Using data of %s" % table_name)
            (schema_filename, data_filename, zipped_data_filename) =  self.dataReader.getSchemaAndDataFilenames(table_name)

            if zipped_data_filename is not None :
                tmp_data_file = self.gunzip(table_name, zipped_data_filename)
                input_filename = tmp_data_file
            else:
                input_filename = data_filename

            self.dataLoader[self._mode].createAndLoadTable(table_name, schema_filename, input_filename)

            if zipped_data_filename is not None :
                os.unlink(tmp_data_file)

    def cleanup(self):
        # cleanup of previous tests
        if os.path.exists(self._out_dirname):
            shutil.rmtree(self._out_dirname)
        os.makedirs(self._out_dirname)


    def connectAndInitDatabases(self):
        self.logger.debug("Creation of data loader for %s mode" % self._mode)
        if (self._mode=='mysql'):
            self.dataLoader[self._mode] = mysqldataloader.MysqlDataLoader(
                self.config,
                self.dataReader.dataConfig,
                self._dbName,
                self._out_dirname,
                self._logFilePrefix
                )
        elif (self._mode=='qserv'):
            self.dataLoader[self._mode] = qservdataloader.QservDataLoader(
                self.config,
                self.dataReader.dataConfig,
                self._dbName,
                self._in_dirname,
                self._out_dirname,
                self._logFilePrefix
            )
        self.logger.debug("Initializing database for %s mode" % self._mode)
        self.dataLoader[self._mode].connectAndInitDatabase()

    def finalize(self):
        if (self._mode=='qserv'):
            self.dataLoader['qserv'].createCssDatabase()
            self.dataLoader['qserv'].configureQservMetaEmptyChunk()

            # restart xrootd in order to reload  export paths w.r.t loaded chunks, cf. #2478
            commons.restart('xrootd')

            # Qserv fails to start if CSS db is empty, so starting it again may be required
            commons.restart('qserv-czar')

        # in order to close socket connections
        del(self.dataLoader[self._mode])

    def run(self, mode_list, load_data, stop_at_query=7999):

        self.cleanup()
        self.dataReader.readInputData()

        for mode in mode_list:
            self._mode = mode

            if self._mode == 'qserv':
                self._dbName = "LSST"
            else:
                self._dbName = "qservTest_case%s_%s" % (self._case_id, self._mode)

            if load_data:
                self.connectAndInitDatabases()
                self.loadData()
                self.finalize()

            self.runQueries(stop_at_query)

    def analyzeQueryResults(self):

        outputs_dir = os.path.join(self._out_dirname, "outputs")

        failing_queries=[]

        mysql_out_dir = os.path.join(outputs_dir,"mysql")
        qserv_out_dir = os.path.join(outputs_dir,"qserv")

        dcmp = dircmp( mysql_out_dir, qserv_out_dir)

        if len(dcmp.diff_files)!=0:
            for query_name in dcmp.diff_files:
                message="{0} and {1} differ".format(os.path.join(mysql_out_dir,query_name), os.path.join(qserv_out_dir,query_name))
                self.logger.error(message)
                failing_queries.append(query_name)

        return failing_queries

def add_generic_arguments(parser):

    verbose_arg_values = logger.verbose_dict.keys()
    parser.add_argument("-v", "--verbose-level", dest="verbose_str", choices=verbose_arg_values,
        default='INFO',
        help="verbosity level"
        )

    # directory containing test dataset
    default_testdata_dir=None
    if os.environ.get('QSERV_TESTDATA_DIR') is not None:
        default_testdata_dir = os.path.join(
            os.environ.get('QSERV_TESTDATA_DIR'), "datasets"
        ) 

    parser.add_argument("-t", "--testdata-dir", dest="testdata_dir",
            default=default_testdata_dir,
            help="""absolute path to directory containing test datasets. This value is set, by precedence,
by this option, and then by QSERV_TESTDATA_DIR/datasets/ if
QSERV_TESTDATA_DIR environment variable is not empty"""
            )

    return parser

def init(args, logfile):

    config = commons.read_user_config()
    logger.init_default_logger(
            logfile,
            args.verbose_level,
            log_path=config['qserv']['log_dir']
            )
    log = logging.getLogger()

    if args.testdata_dir is not None and os.path.isdir(args.testdata_dir):
       log.debug(
            "Setting testdata_dir value to %s",
            args.testdata_dir
        )
       config['qserv']['testdata_dir'] = args.testdata_dir
    else:
        log.fatal(
            "Unable to find tests datasets. (testdata_dir value is %s)",
            args.testdata_dir
        )
        sys.exit(errno.EIO)
