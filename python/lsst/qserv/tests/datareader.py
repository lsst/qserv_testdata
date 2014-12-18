import logging
import os
import tempfile
import UserDict

from lsst.qserv.admin import commons, const

class DataReader(UserDict.UserDict):
    """
    Class which holds all test data meta-configuration.
    Implemented as a dictionary with some extra methods.
    """

    def __init__(self, data_dir_name, data_name=None):
        self.log = logging.getLogger()
        self._dataDirName = data_dir_name
        self.dataName = data_name
        self.dataConfig = dict()
        self.dataConfig['data-name'] = data_name

        self.tables = []

    def readInputData(self):
        self._analyze()
        self._readTableList()

    def _analyze(self):

        self.dataConfig['sql-views'] = []
        self.dataConfig['partitioned-sql-views'] = []
        self.dataConfig['input-dir']=self._dataDirName
        self.dataConfig['data-name']=self.dataName

        if self.dataName=="case01":
            self.dataConfig['partitioned-tables'] = ["Object", "Source"]

            self.dataConfig['schema-extension']='.schema'
            self.dataConfig['data-extension']='.tsv'
            self.dataConfig['zip-extension']='.gz'

            self.log.debug("Data configuration : %s" % self.dataConfig)

        # for PT1.1
        elif self.dataName=="case02":

            self.dataConfig['partitioned-tables'] = ["Object", "Source"]

            self.dataConfig['schema-extension']='.sql'
            self.dataConfig['data-extension']='.txt'
            self.dataConfig['zip-extension']='.gz'

            self.log.debug("Data configuration : %s" % self.dataConfig)


        # for W13
        elif self.dataName=="case03":

            # TODO next params should be deduced from meta files
            self.tables=['Science_Ccd_Exposure_Metadata_coadd_r', 'AvgForcedPhotYearly', 'Science_Ccd_Exposure_Metadata', 'RunDeepSource',  'RunDeepForcedSource', 'DeepForcedSource', 'ZZZ_Db_Description', 'RefObject', 'RefDeepSrcMatch', 'Science_Ccd_Exposure_coadd_r', 'Science_Ccd_Exposure', 'AvgForcedPhot', 'DeepCoadd_To_Htm10', 'Science_Ccd_Exposure_To_Htm10_coadd_r', 'LeapSeconds', 'DeepCoadd', 'DeepCoadd_Metadata', 'DeepSource', 'Filter']

            self.dataConfig['sql-views'] = ['DeepForcedSource','DeepSource']

            self.dataConfig['partitioned-tables'] = ["AvgForcedPhot",
                                                "AvgForcedPhotYearly",
                                                "RefObject",
                                                "RunDeepSource",
                                                "RunDeepForcedSource"]

            self.dataConfig['partitioned-sql-views'] = ['DeepForcedSource','DeepSource']

            for table in self.dataConfig['partitioned-tables']:
                self.dataConfig[table]=dict()
                # chunkId and subChunkId will be added
                self.dataConfig[table]['chunk-column-id'] = None

            self.dataConfig['schema-extension']='.sql'
            self.dataConfig['data-extension']='.txt'
            self.dataConfig['zip-extension']='.gz'
            self.dataConfig['delimiter']=','

            # TODO : read from CSS db.params file
            self.dataConfig['num-stripes'] = 85
            self.dataConfig['num-substripes'] = 12

            self.dataConfig['AvgForcedPhot']['ra-column'] = 1
            self.dataConfig['AvgForcedPhot']['decl-column'] = 2

            self.dataConfig['AvgForcedPhotYearly']['ra-column'] = 2
            self.dataConfig['AvgForcedPhotYearly']['decl-column'] = 3

            self.dataConfig['RefObject']['ra-column'] = 12
            self.dataConfig['RefObject']['decl-column'] = 13

            self.dataConfig['RunDeepSource']['ra-column'] = 1
            self.dataConfig['RunDeepSource']['decl-column'] = 2

            self.dataConfig['RunDeepForcedSource']['ra-column'] = 1
            self.dataConfig['RunDeepForcedSource']['decl-column'] = 2

    def _readTableList(self):
        files = os.listdir(self._dataDirName)
        for f in files:
            filename, fileext = os.path.splitext(f)
            if fileext == self.dataConfig['schema-extension']:
                self.tables.append(filename)
        self.log.debug("%s.readTableList() found : %s" %  (self.__class__.__name__, self.tables))

    def getSchemaFile(self, table_name):
        if table_name not in self.tables:
            raise
        else:
            prefix = os.path.join(self._dataDirName, table_name)
            schema_filename = prefix + self.dataConfig['schema-extension']
            return schema_filename

    def getInputDataFile(self, table_name):
        if table_name not in self.tables:
            raise
        data_filename = None
        if table_name not in self.dataConfig['sql-views']:
            prefix = os.path.join(self._dataDirName, table_name)
            data_filename = prefix + self.dataConfig['data-extension']
            if self.dataConfig['zip-extension'] is not None:
                data_filename += self.dataConfig['zip-extension']
        return data_filename

