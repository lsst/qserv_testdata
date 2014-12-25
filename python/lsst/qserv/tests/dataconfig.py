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
Module defining DataConfig class and related methods.

DataConfig class reads configuration file containing meta-data
and set all parameters related to test dataset.

@author  Fabrice Jammes, IN2P3/SLAC
"""

import io
import logging
import os
import sys
import yaml

class DataConfig(dict):
    """
    Class which holds all test data meta-configuration.
    Implemented as a dictionary with some extra methods.
    """

    def __init__(self, data_dir_name, data_name):
        self.log = logging.getLogger()
        self.dataDir = data_dir_name
        self.dataName = data_name

    def analyzeInputData(self):
        """
        Read meta-data for test data
        and set table to load based on a given order or on schema files in
        input data
        """
        _topLevelConfigFile=os.path.join(self.dataDir, "description.yaml")
        with io.open(_topLevelConfigFile, 'r') as file:
            self.update(yaml.load(file))

        self.log.debug("Data configuration : %s" % self)

        fromFileTables = self._tableFromSchemaFile()
        # a specific load order on a restricted number of tables
        # ca be specified in yaml
        if not self['tables']['load-order']:
            self['tables']['load-order'] = fromFileTables
            self.notLoadedTables = []
        else:
            self.notLoadedTables = list(set(fromFileTables) -
                                        set(self.orderedTables))
        self.log.debug("Tables to load : %s", self.orderedTables)

    @property
    def _schemaExt(self):
        return self['extensions']['schema']

    @property
    def _dataExt(self):
        return self['extensions']['data']

    @property
    def _zipExt(self):
        return self['extensions']['zip']

    @property
    def _views(self):
        v = self['tables']['views']
        return v if v else []

    @property
    def directors(self):
        v = self['tables']['directors']
        return v if v else []

    @property
    def partitionedTables(self):
        v = self['tables']['partitioned-tables']
        return v if v else []

    @property
    def orderedTables(self):
        return self['tables']['load-order']

    def _tableFromSchemaFile(self):
        """
        Return a list of orderedTables names deduced from the input data
        schema-file names
        """
        files = os.listdir(self.dataDir)
        tables = []
        for f in files:
            filename, fileext = os.path.splitext(f)
            if fileext == self._schemaExt:
                tables.append(filename)
        return tables

    def getSchemaFile(self, table_name):
        if table_name not in self.orderedTables:
            raise
        else:
            prefix = os.path.join(self.dataDir, table_name)
            schema_filename = prefix + self._schemaExt
            return schema_filename

    def getInputDataFile(self, table_name):
        if table_name not in self.orderedTables:
            raise
        data_filename = None
        if table_name not in self._views:
            prefix = os.path.join(self.dataDir, table_name)
            data_filename = prefix + self._dataExt
            if self._zipExt:
                data_filename += self._zipExt
        return data_filename

