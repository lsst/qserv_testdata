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
import sys
import logging

from lsst.qserv.admin import commons
from lsst.qserv.tests.dbLoader import DbLoader
from lsst.qserv.tests.sql import cmd, connection


class MysqlLoader(DbLoader):

    def __init__(self, config,
                 data_reader,
                 db_name,
                 out_dirname):

        super(self.__class__, self).__init__(config,
                                             data_reader,
                                             db_name,
                                             out_dirname)
        self.logger = logging.getLogger(__name__)

        self.dataConfig = data_reader

    def createLoadTable(self, table):
        self._callLoader(table)

    def _callLoader(self, table):
        """
        Call Qserv loader to load plain-MySQL table
        """

        self.logger.info("Create, load table %s", table)

        loaderCmd = self.loaderCmdCommonOpts(table)

        loaderCmd += ['--no-css',
                      '--skip-partition',
                      '--one-table']

        loaderCmd += self.loaderCmdCommonArgs(table)

        commons.run_command(loaderCmd,
                                  stdout=sys.stdout,
                                  stderr=sys.stderr)
        self.logger.info("Partitioned data loaded for table %s", table)

    def prepareDatabase(self):
        """
        Connect to MySQL via sock
        Create MySQL database
        Create MySQL command-line client
        """

        self._sqlInterface['sock'] = connection.Connection(**self.sock_params)

        self._sqlInterface['sock'].dropAndCreateDb(self._dbName)
        self._sqlInterface['sock'].setDb(self._dbName)

        cmd_connection_params = self.sock_params
        cmd_connection_params['database'] = self._dbName
        self._sqlInterface['cmd'] = cmd.Cmd(**cmd_connection_params)
