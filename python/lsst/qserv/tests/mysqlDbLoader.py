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
Module defining MySQL loader class for integration test and related methods.

Public functions are used in benchmark module, using duck-typing interface.
Wrap Qserv user-friendly loader.

@author  Fabrice Jammes, IN2P3/SLAC
"""
import logging
import os
import sys

from lsst.qserv.admin import commons
from .dbLoader import DbLoader


class MysqlLoader(DbLoader):

    def __init__(self, config,
                 data_reader,
                 db_name,
                 multi_node,
                 out_dirname):

        super(self.__class__, self).__init__(config,
                                             data_reader,
                                             db_name,
                                             multi_node,
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

        # include table-specific config if it exists
        tableCfg = os.path.join(self.dataConfig.dataDir, table + ".cfg")
        if os.path.exists(tableCfg):
            loaderCmd += ['--config={0}'.format(tableCfg)]

        loaderCmd += self.loaderCmdCommonArgs(table)

        commons.run_command(loaderCmd,
                            stdout=sys.stdout,
                            stderr=sys.stderr)
        self.logger.info("Partitioned data loaded for table %s", table)

    def prepareDatabase(self):
        """
        Connect to MySQL via socket
        Create MySQL database
        Create MySQL command-line client
        """

        self.czar_wmgr.dropDb(self._dbName, mustExist=False)
        self.czar_wmgr.createDb(self._dbName)
