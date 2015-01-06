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

import os
import sys
import tempfile

from lsst.qserv.admin import commons
from lsst.qserv.tests.dbloader import DbLoader
from lsst.qserv.tests.sql import cmd, connection


class QservLoader(DbLoader):

    def __init__(self, config,
                 data_reader,
                 db_name,
                 out_dirname):

        super(self.__class__, self).__init__(config,
                                             data_reader,
                                             db_name,
                                             out_dirname)

        run_dir = self.config['qserv']['run_base_dir']
        self._emptyChunksFile = os.path.join(run_dir, "var", "lib",
                                             "qserv", "empty_" +
                                             self._dbName +
                                             ".txt")

    def createLoadTable(self, table):
        """
        Create and load a table in Qserv
        """
        self._callLoader(table)
        # Create emptyChunks file in case it doesn't exist
        open(self._emptyChunksFile, 'a').close()

    def _callLoader(self, table):
        """
        Call Qserv loader
        """
        self.logger.info("Partition data, create and load table %s", table)

        loaderCmd = self.loaderCmdCommonOpts(table)

        loaderCmd += ['--css-remove']

        if table in self.dataConfig.partitionedTables:
            loaderCmd += ['--config={0}'
                           .format(os.path.join(self.dataConfig.dataDir,
                                                table + ".cfg"))]
        else:
            loaderCmd += ['--skip-partition', '--one-table']

        # WARN emptyChunks.txt might also be intersection of
        # all empltyChunkk file: seel with D. Wang and A. Salnikov
        if table in self.dataConfig.directors:
            loaderCmd += ['--empty-chunks={0}'.format(self._emptyChunksFile)]
        else:
            loaderCmd += ["--index-db={0}".format("")]

        loaderCmd += self.loaderCmdCommonArgs(table)

        # Use same logging configuration for loader and integration test
        # command line, this allow to redirect loader to sys.stdout, sys.stderr
        commons.run_command(loaderCmd,
                                  stdout=sys.stdout,
                                  stderr=sys.stderr)
        self.logger.info(
            "Partitioned %s data loaded (stdout : %s)", table)

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
