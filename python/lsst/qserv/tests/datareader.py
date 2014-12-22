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
Module defining DataReader class and related methods.

DataReader class reads configuration file containing meta-data
and set all parameters related to test dataset.

@author  Fabrice Jammes, IN2P3/SLAC
"""

import logging
import os
import UserDict

class DataReader(object):
    """
    Class which holds all test data meta-configuration.
    Implemented as a dictionary with some extra methods.
    """

    def __init__(self, data_dir_name, data_name):
        self.log = logging.getLogger()
        self._dataDir = data_dir_name
        self.dataName = data_name
        self.dataConfig = dict()
        self.dataConfig['data-name'] = data_name

        self.dataConfig['tables'] = dict()

        self.orderedTables = []
        self.notLoadedTables = []

    def analyzeInputData(self):
        """
        Read meta-data for test data
        and set table to load based on a given order or on schema files in
        input data
        """
        self._readMetaData()
        fromFileTables = self._tableFromSchemaFile()
        if not self.orderedTables:
            self.orderedTables = fromFileTables
        else:
            self.notLoadedTables = list(set(fromFileTables) -
                                        set(self.orderedTables))
        self.log.debug("Tables to load : %s", self.orderedTables)

    def _readMetaData(self):

        self.dataConfig['sql-views'] = []
        self.dataConfig['input-dir']=self._dataDir
        self.dataConfig['data-name']=self.dataName

        if self.dataName=="case01":

            self.dataConfig['tables']['director'] = ["Object"]
            self.dataConfig['partitioned-tables'] = ["Object", "Source"]

            self.dataConfig['schema-extension']='.schema'
            self.dataConfig['data-extension']='.tsv'
            self.dataConfig['zip-extension']='.gz'

        # for PT1.1
        elif self.dataName=="case02":

            self.dataConfig['tables']['director'] = ["Object"]
            self.dataConfig['partitioned-tables'] = ["Object", "Source"]

            self.dataConfig['schema-extension']='.sql'
            self.dataConfig['data-extension']='.txt'
            self.dataConfig['zip-extension']='.gz'

        # for W13
        elif self.dataName=="case03":

            # Force specific order: view must be loaded after table
            self.orderedTables=['Science_Ccd_Exposure_Metadata_coadd_r',
                         'AvgForcedPhotYearly', 'Science_Ccd_Exposure_Metadata',
                         'ZZZ_Db_Description', 'RefObject', 'RefDeepSrcMatch',
                         'Science_Ccd_Exposure_coadd_r', 'Science_Ccd_Exposure',
                         'AvgForcedPhot', 'DeepCoadd_To_Htm10',
                         'Science_Ccd_Exposure_To_Htm10_coadd_r', 'LeapSeconds',
                         'DeepCoadd', 'DeepCoadd_Metadata',
                         'Filter']

            self.dataConfig['sql-views'] = ['DeepForcedSource','DeepSource']

            self.dataConfig['partitioned-tables'] = ['RefObject',
                                                     'RunDeepSource',
                                                     'RunDeepForcedSource']

            self.dataConfig['partitioned-sql-views'] = ['DeepSource',
                                                        'DeepForcedSource']

            self.dataConfig['schema-extension']='.sql'
            self.dataConfig['data-extension']='.txt'
            self.dataConfig['zip-extension']='.gz'

        self.log.debug("Data configuration : %s" % self.dataConfig)

    def _tableFromSchemaFile(self):
        """
        Return a list of orderedTables names deduced from the input data
        schema-file names
        """
        files = os.listdir(self._dataDir)
        tables = []
        for f in files:
            filename, fileext = os.path.splitext(f)
            if fileext == self.dataConfig['schema-extension']:
                tables.append(filename)
        return tables

    def getSchemaFile(self, table_name):
        if table_name not in self.orderedTables:
            raise
        else:
            prefix = os.path.join(self._dataDir, table_name)
            schema_filename = prefix + self.dataConfig['schema-extension']
            return schema_filename

    def getInputDataFile(self, table_name):
        if table_name not in self.orderedTables:
            raise
        data_filename = None
        if table_name not in self.dataConfig['sql-views']:
            prefix = os.path.join(self._dataDir, table_name)
            data_filename = prefix + self.dataConfig['data-extension']
            if self.dataConfig['zip-extension'] is not None:
                data_filename += self.dataConfig['zip-extension']
        return data_filename

