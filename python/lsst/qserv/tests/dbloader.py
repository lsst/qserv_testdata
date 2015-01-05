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
Module defining generic database loader class for integration test and
related methods.

Wrap Qserv user-friendly loader.

@author  Fabrice Jammes, IN2P3/SLAC
"""

import logging
import os

from lsst.qserv.tests.sql import const


class DbLoader(object):

    def __init__(self, config, data_reader, db_name, out_dirname):

        self.config = config
        self.dataConfig = data_reader
        self._dbName = db_name

        self._out_dirname = out_dirname

        self.logger = logging.getLogger(__name__)
        self.sock_params = {
            'config': self.config,
            'mode': const.MYSQL_SOCK
        }

        self._sqlInterface = dict()

    def loaderCmdCommonOpts(self, table):
        """
        Return user-friendly loader command-line options wich are common
        to both Qserv and MySQL
        """
        tmp_dir = self.config['qserv']['tmp_dir']
        cmd = ['qserv-data-loader.py']

        logLevel = self.logger.getEffectiveLevel()
        if logLevel is logging.DEBUG:
            cmd += ['--verbose-all',
                    '-vvv']
        elif logLevel is logging.INFO:
            cmd += ['-vv']

        cmd += ['--config={0}'.format(os.path.join(self.dataConfig.dataDir,
                                                   "common.cfg")),
                '--user={0}'.format(self.config['mysqld']['user']),
                '--password={0}'.format(self.config['mysqld']['pass']),
                '--socket={0}'.format(self.config['mysqld']['sock']),
                '--delete-tables',
                # WARN: required to unzip input data file
                '--chunks-dir={0}'.format(os.path.join(tmp_dir,
                                                       "qserv_data_loader",
                                                       table))]

        return cmd

    def loaderCmdCommonArgs(self, table):
        """
        Return user-friendly loader command-line arguments wich are common
        to both Qserv and MySQL
        """
        cmd = [self._dbName,
               table,
               self.dataConfig.getSchemaFile(table)]

        dataFile = self.dataConfig.getInputDataFile(table)
        if dataFile:
            cmd += [dataFile]
        return cmd
