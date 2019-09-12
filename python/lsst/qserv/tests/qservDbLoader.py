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

from __future__ import absolute_import, division, print_function

import logging
import os
import sys

from lsst.qserv import css
from lsst.qserv.admin import commons
from .dbLoader import DbLoader


class QservLoader(DbLoader):

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

        data_dir = self.config['qserv']['qserv_data_dir']
        self._emptyChunksFile = os.path.join(data_dir, "qserv",
                                             "empty_" + self._dbName +
                                             ".txt")
        self.dataConfig = data_reader
        self.tmpDir = self.config['qserv']['tmp_dir']
        self.multi_node = multi_node

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

        if self.multi_node:
            for node in self.nWmgrs:
                loaderCmd += ['--worker', node]

        if self.dataConfig.duplicatedTables:
            loaderCmd += ['--skip-partition']
            loaderCmd += ['--chunks-dir={0}'.format(os.path.join(self.tmpDir,
                                                                 self._out_dirname,
                                                                 "chunks/", table))]
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
        Drop and create MySQL database
        Drop CSS database
        Assume that other meta-data will be removed by the user-friendly
        loader (qservMeta, emptyChunks file)
        """

        if self.multi_node:
            for node in self.nWmgrs:
                self.logger.info("Running on node: %s", node)

        self.logger.info("Drop and create MySQL database for Qserv: %s",
                         self._dbName)

        self.czar_wmgr.dropDb(self._dbName, mustExist=False)
        self.czar_wmgr.createDb(self._dbName)

        if self.multi_node:
            for wmgr in self.nWmgrs.values():
                wmgr.dropDb(self._dbName, mustExist=False)
                wmgr.createDb(self._dbName)

        self.logger.info("Drop CSS database for Qserv")
        self.dropCssDatabase()

    def dropCssDatabase(self):
        cssAccess = css.CssAccess.createFromConfig(self.config['css'], '')
        if cssAccess.containsDb(self._dbName):
            cssAccess.dropDb(self._dbName)
        self.logger.info("Drop CSS database: %s", self._dbName)

    def workerInsertXrootdExportPath(self):
        if self.multi_node:
            for wmgr in self.nWmgrs.values():
                wmgr.xrootdRegisterDb(self._dbName, allowDuplicate=True)
        else:
            self.czar_wmgr.xrootdRegisterDb(self._dbName, allowDuplicate=True)

    def finalize(self):
        """Finalize data loading process
        """
        self.workerInsertXrootdExportPath()

