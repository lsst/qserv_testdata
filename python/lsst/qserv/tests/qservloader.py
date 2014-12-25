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
Module defining Qserv loader class for integration test.

Public functions are used in benchmark module, using duck-typing interface.
Wrap Qserv user-friendly loader.

@author  Fabrice Jammes, IN2P3/SLAC
"""

import logging
import os
import sys
import tempfile

from lsst.qserv.admin import commons
from lsst.qserv.tests.sql import const, cmd, connection

class QservLoader(object):

    def __init__(self, config,
                 data_reader,
                 db_name,
                 out_dirname,
                 log_file_prefix='qserv-loader',
                 logging_level=logging.DEBUG):

        self.config = config
        self.dataConfig = data_reader
        self._dbName = db_name


        run_dir = self.config['qserv']['run_base_dir']
        self._emptyChunksFile = os.path.join(run_dir, "var", "lib",
                                         "qserv", "empty_" +
                                         self._dbName +
                                         ".txt")

        self._out_dirname = out_dirname

        self.logger = logging.getLogger()
        self.sock_params = {
            'config': self.config,
            'mode': const.MYSQL_SOCK
            }

        self._sqlInterface = dict()

    def createLoadTable(self, table):
        """
        Create and load a table in Qserv
        """
        self._callLoader(table)
        # Create emptyChunks file in cas it not exist
        open(self._emptyChunksFile, 'a').close()

    def _callLoader(self, table):
        """
        Call Qserv loader
        """
        self.logger.info("Partition data, create and load table %s", table)

        tmp_dir = self.config['qserv']['tmp_dir']
        loader_cmd = [
            'qserv-data-loader.py',
            '--verbose-all',
            '-vvv',
            '--config={0}'.format(os.path.join(self.dataConfig.dataDir,
                                               "common.cfg")),
            '--css-remove',
            '--user={0}'.format(self.config['mysqld']['user']),
            '--password={0}'.format(self.config['mysqld']['pass']),
            '--socket={0}'.format(self.config['mysqld']['sock']),
            # TODO: load emptyChunk only for director table
            '--delete-tables',
            # WARN: required to unzip input data file
            '--chunks-dir={0}'.format(os.path.join(tmp_dir,
                                                   "loader_chunks",
                                                   table))]

        if table in self.dataConfig.partitionedTables:
            loader_cmd += ['--config={0}'
                           .format(os.path.join(self.dataConfig.dataDir,
                                                table + ".cfg"))]

            # WARN emptyChunks.txt might also be intersection of
            # all empltyChunkk file: seel with D. Wang and A. Salnikov
            if table in self.dataConfig.directors:
                loader_cmd += ['--empty-chunks={0}'
                               .format(self._emptyChunksFile)]
            else:
                loader_cmd += ["--index-db={0}".format("")]

        else:
            loader_cmd += ['--skip-partition', '--one-table']

        loader_cmd += [
            self._dbName,
            table,
            self.dataConfig.getSchemaFile(table),
            self.dataConfig.getInputDataFile(table)]

        out = commons.run_command(loader_cmd)
        self.logger.info("Partitioned %s data loaded (stdout : %s)", table, out)

    def prepareDatabase(self):
        """
        Connect to MySQL via sock
        Drop and create MySQL database
        Drop CSS database
        Assume that other meta-data will be removed by the user-friendly
        loader (qservMeta, emptyChunks file)
        Create MySQL command-line client
        """

        self._sqlInterface['sock'] = connection.Connection(**self.sock_params)

        self.logger.info("Drop and create MySQL database for Qserv: %s",
                         self._dbName)
        sql_instructions = [
            "DROP DATABASE IF EXISTS %s" % self._dbName,
            "CREATE DATABASE %s" % self._dbName,
            ("GRANT ALL ON {0}.* TO '{1}'@'localhost'"
             .format(self._dbName, self.config['qserv']['user'])),
            "USE {0}".format(self._dbName)
            ]

        for sql in sql_instructions:
            self._sqlInterface['sock'].execute(sql)

        cmd_connection_params = self.sock_params
        cmd_connection_params['database'] = self._dbName
        self._sqlInterface['cmd'] = cmd.Cmd(**cmd_connection_params)

        self.logger.info("Drop CSS database for Qserv")
        self.dropCssDatabase()

    def dropCssDatabase(self):
        script = "qserv-admin.py"
        cmd = [script,
               "-c",
               "localhost:%s" % self.config['zookeeper']['port'],
               "-v",
               str(self.logger.getEffectiveLevel()),
               "-f",
               os.path.join(self.config['qserv']['log_dir'],
                            "qadm-%s.log" % self.dataConfig.dataName)]

        with tempfile.NamedTemporaryFile('w+t') as f:
            f.write('DROP DATABASE {0};'.format(self._dbName))
            f.flush()
            out = commons.run_command(cmd, f.name)
            self.logger.info("Drop CSS database: %s",
                             self._dbName)


    def workerInsertXrootdExportPath(self):
        sql = ("SELECT * FROM qservw_worker.Dbs WHERE db='{0}';"
               .format(self._dbName))
        rows = self._sqlInterface['sock'].execute(sql)

        if len(rows) == 0:
            sql = ("INSERT INTO qservw_worker.Dbs VALUES('{0}');"
                   .format(self._dbName))
            self._sqlInterface['sock'].execute(sql)
        elif len(rows) > 1:
            self.logger.fatal("Duplicated value '%s' in qservw_worker.Dbs",
                              self._dbName)
            sys.exit(1)
