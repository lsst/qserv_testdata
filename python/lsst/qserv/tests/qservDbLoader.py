# LSST Data Management System
# Copyright 2014-2015 AURA/LSST.
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

from lsst.db.db import Db
from lsst.db.db import DbException
from lsst.qserv.admin import commons
from lsst.qserv.admin import qservAdmin
from lsst.qserv.admin import workerAdmin
from lsst.qserv.tests.dbLoader import DbLoader
from lsst.qserv.tests.sql import cmd, connection


class QservLoader(DbLoader):

    def __init__(self, config,
                 data_reader,
                 db_name,
                 out_dirname,
                 multi_node):

        super(self.__class__, self).__init__(config,
                                             data_reader,
                                             db_name,
                                             out_dirname,
                                             multi_node)
        self.logger = logging.getLogger(__name__)

        run_dir = self.config['qserv']['run_base_dir']
        self._emptyChunksFile = os.path.join(run_dir, "var", "lib",
                                             "qserv", "empty_" +
                                             self._dbName +
                                             ".txt")
        self.dataConfig = data_reader
        self.tmpDir = self.config['qserv']['tmp_dir']
        self.multi_node = multi_node

        # Adding Qserv Admin
        if self.multi_node == True:
            self.qAdmin = qservAdmin.QservAdmin('localhost:12181')
            self.wAdmin = workerAdmin.WorkerAdmin('worker-dbdev3b',self.qAdmin)
            self.db = self.wAdmin.mysqlConn(user='qsmaster')

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

        if self.multi_node == True:
            # Hard coded worker list
            loaderCmd += ['--worker', 'worker-dbdev3b']

        if self.dataConfig.duplicatedTables:
            loaderCmd += ['--skip-partition']
            loaderCmd += ['--chunks-dir={0}'.format(os.path.join(self.tmpDir,
                                                                 self._out_dirname,
                                                                 "chunks/",table))]
        # include table-specific config if it exists
        tableCfg = os.path.join(self.dataConfig.dataDir, table + ".cfg")
        if os.path.exists(tableCfg):
            loaderCmd += ['--config={0}'.format(tableCfg)]

        # WARN emptyChunks.txt might also be intersection of
        # all empltyChunkk file: seel with D. Wang and A. Salnikov
        if table in self.dataConfig.directors:
            loaderCmd += ['--empty-chunks={0}'.format(self._emptyChunksFile)]

        loaderCmd += self.loaderCmdCommonArgs(table)

        # Use same logging configuration for loader and integration test
        # command line, this allow to redirect loader to sys.stdout, sys.stderr
        commons.run_command(loaderCmd,
                                  stdout=sys.stdout,
                                  stderr=sys.stderr)
        self.logger.info("Partitioned data loaded for table %s", table)

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

        sql_instructions_multi = [
            "DROP DATABASE %s" % self._dbName,
            "CREATE DATABASE %s" % self._dbName,
            "USE {0}".format(self._dbName)
        ]

        for sql in sql_instructions:
            self._sqlInterface['sock'].execute(sql)

        if self.multi_node == True:
            for sql in sql_instructions_multi:
                try:
                    self.db.execCommandN(sql)
                except DbException as exc:
                    if exc.errCode() == DbException.DB_DOES_NOT_EXIST:
                        self.logger.info("Drop Db failed")
                    else:
                        raise

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
                            "qadm-%s.log" % self._dbName)]

        with tempfile.NamedTemporaryFile('w+t') as f:
            f.write('DROP DATABASE {0};'.format(self._dbName))
            f.flush()
            commons.run_command(cmd, f.name)
            self.logger.info("Drop CSS database: %s",
                             self._dbName)

    def workerInsertXrootdExportPath(self):
        sql = ("SELECT * FROM qservw_worker.Dbs WHERE db='{0}';"
               .format(self._dbName))

        if self.multi_node == True:
            rows = self.db.execCommandN(sql)
        else:
            rows = self._sqlInterface['sock'].execute(sql)

        if len(rows) == 0:
            sql = ("INSERT INTO qservw_worker.Dbs VALUES('{0}');"
                   .format(self._dbName))
            if self.multi_node == True:
                self.db.execCommandN(sql)
            else:
                self._sqlInterface['sock'].execute(sql)

        elif len(rows) > 1:
            self.logger.fatal("Duplicated value '%s' in qservw_worker.Dbs",
                              self._dbName)
            sys.exit(1)
