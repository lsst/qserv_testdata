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
Module defining generic database loader class for integration test and
related methods.

Wrap Qserv user-friendly loader.

@author  Fabrice Jammes, IN2P3/SLAC
"""

import glob
import logging
import os

from lsst.qserv.tests.sql import const


class DbLoader(object):

    def __init__(self, config, data_reader, db_name, out_dirname, multi_node):

        self.config = config
        self.dataConfig = data_reader
        self._dbName = db_name

        self._out_dirname = out_dirname
        self._multi_node = multi_node

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
            cmd += ['-v']

        if self._multi_node == True:
            cmd += ['--config={0}'.format(os.path.join(self.dataConfig.dataDir,
                                                       "common.cfg")),
                    '--user=qsmaster',
                    '--socket={0}'.format(self.config['mysqld']['sock']),
                    '--delete-tables']
        else:
            cmd += ['--config={0}'.format(os.path.join(self.dataConfig.dataDir,
                                                       "common.cfg")),
                    '--user={0}'.format(self.config['mysqld']['user']),
                    '--password={0}'.format(self.config['mysqld']['pass']),
                    '--socket={0}'.format(self.config['mysqld']['sock']),
                    '--delete-tables']

        if self.dataConfig.duplicatedTables:
            # Other parameters if using duplicated data
            cmd += ['--config={0}'.format(os.path.join(self.dataConfig.dataDir,
                                                       table+".cfg"))]
        else:
            # WARN: required to unzip input data file
            cmd += ['--chunks-dir={0}'.format(os.path.join(tmp_dir,
                                                           "qserv_data_loader",
                                                           table))]

        return cmd

    def loaderCmdCommonArgs(self, table):
        """
        Return user-friendly loader command-line arguments which are common
        to both Qserv and MySQL
        """
        tmp_dir = self.config['qserv']['tmp_dir']
        cmd = [self._dbName,
               table,
               self.dataConfig.getSchemaFile(table)]

        if self.dataConfig.duplicatedTables:
            for filename in glob.glob(os.path.join(tmp_dir,self._out_dirname,
                                                   "chunks/",table,'chunk_[0-9][0-9][0-9][0-9].txt')):
                os.system("cat "+filename+" >> "+os.path.join(tmp_dir,self._out_dirname,"chunks/",table,table+".txt"))
            dataFile = os.path.join(tmp_dir,self._out_dirname,"chunks/",table,table+".txt")
        else:
            dataFile = self.dataConfig.getInputDataFile(table)

        if dataFile:
            cmd += [dataFile]
        return cmd
