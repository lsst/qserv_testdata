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
Module defining MySQL loader class for integration test and related methods.

Public functions are used in benchmark module, using duck-typing interface.
Wrap Qserv user-friendly loader.

@author  Fabrice Jammes, IN2P3/SLAC
"""

from  lsst.qserv.admin import commons
from  lsst.qserv.tests.sql import const, cmd, connection
import logging
import os

class MysqlLoader(object):

    def __init__(self, config, data_reader, db_name, out_dirname,
                 log_file_prefix='qserv-loader', logging_level=logging.DEBUG):
        self.config = config
        self.dataConfig = data_reader
        self._dbName = db_name

        self._out_dirname = out_dirname

        self.logger = logging.getLogger()
        self.sock_connection_params = {
            'config' : self.config,
            'mode' : const.MYSQL_SOCK
            }

        self._sqlInterface = dict()

    def createLoadTable(self, table):
        self._callLoader(table)

    def _callLoader(self, table):
        '''
        Call Qserv loader to load plain-MySQL table
         '''

        self.logger.info("Create, load table %s", table)

        loader_cmd = [
            'qserv-data-loader.py',
            '--verbose-all',
            '-vvv',
            '--config={0}'.format(os.path.join(self.dataConfig.dataDir,
                                               "common.cfg")),
            '--no-css',
            '--user={0}'.format(self.config['mysqld']['user']),
            '--password={0}'.format(self.config['mysqld']['pass']),
            '--socket={0}'.format(self.config['mysqld']['sock']),
            '--delete-tables',
            '--skip-partition',
            '--one-table',
            self._dbName,
            table,
            self.dataConfig.getSchemaFile(table)]

        data = self.dataConfig.getInputDataFile(table)
        if data is not None:
            loader_cmd.append(self.dataConfig.getInputDataFile(table))

        out = commons.run_command(loader_cmd)
        self.logger.info("Partitioned %s data loaded (stdout : %s)", table, out)


    def prepareDatabase(self):
        """
        Connect to MySQL via sock
        Create MySQL database
        Create MySQL command-line client
        """

        self._sqlInterface['sock'] = connection.Connection(**self.sock_connection_params)

        self._sqlInterface['sock'].dropAndCreateDb(self._dbName)
        self._sqlInterface['sock'].setDb(self._dbName)

        cmd_connection_params =   self.sock_connection_params
        cmd_connection_params['database'] = self._dbName
        self._sqlInterface['cmd'] = cmd.Cmd(**cmd_connection_params)

