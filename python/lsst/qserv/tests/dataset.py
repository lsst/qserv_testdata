#!/usr/bin/env python
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
Customize integration test datasets
- download remote big data file

@author  Fabrice Jammes, IN2P3/SLAC
"""

import logging
import os
import shutil
import urllib2

from lsst.qserv.tests import dataconfig
from lsst.qserv.tests import benchmark

LOG = logging.getLogger(__name__)

class Customizer(object):

    def __init__(self, source_case_no, testdata_dir, target_testdata_dir,
                         download_action=True, username=None, password=None):
        ''' Contain informations allowing to customize a dataset
        :param source_case_no: dataset to duplicate
        :param testdata_dir: directory containing test dataset to duplicate
        :param target_testdata_dir: destination directory
        :param download_action: use data configuration to eventually override data with
        remote big data files
        :return True if success, else False
        '''

        self._username = username
        self._password = password
        self._downloadAction = download_action
        self._src_dataset_dir = benchmark.Benchmark.getDatasetDir(testdata_dir, source_case_no)
        self._dest_dataset_dir = benchmark.Benchmark.getDatasetDir(target_testdata_dir,
                                                           source_case_no)
        

    def run(self):
        ''' Customize  one integration test dataset
        :param source_case_no: dataset to duplicate
        :param testdata_dir: directory containing test dataset to duplicate
        :param target_testdata_dir: destination directory
        :param download: use data configuration to eventually override data with
        remote big data files
        :return True if success, else False
        '''
        
        Customizer._duplicate_data_dir(self._src_dataset_dir,
                                       self._dest_dataset_dir)

        self._data_dir = os.path.join(self._dest_dataset_dir, "data")
        self._dataConfig = dataconfig.DataConfig(self._data_dir)

        if self._downloadAction :
            if self._username:
                self._addHTTPBasicAuthHandler()
            urls = self._dataConfig.urls
            LOG.debug("Big data files to download %s", urls)
            for url in urls:
                self._download(url) 
                
        return True

    @staticmethod
    def _duplicate_data_dir(src, dest):
        try:
            shutil.rmtree(dest)    
        except OSError:
            pass
        
        try:
            shutil.copytree(src, dest)
        except OSError:
            LOG.exception("Unable to copy input data set from %s to %s",
                      src, dest)
            raise
        
    def _addHTTPBasicAuthHandler(self):
        
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        passman.add_password(None, self._dataConfig.baseurl,
                             self._username,
                             self._password)
        
        authhandler = urllib2.HTTPBasicAuthHandler(passman)
        
        opener = urllib2.build_opener(authhandler)
        
        urllib2.install_opener(opener)
    
    def _download(self, url):
        try:
            f = urllib2.urlopen(url)
    
            # Open local file for writing
            dest_file = os.path.join(self._data_dir, os.path.basename(url)) 
            LOG.info("Downloading: %s to %s", url, dest_file)
            with open(dest_file, "wb") as local_file:
                local_file.write(f.read())
    
        #handle errors
        except urllib2.HTTPError, e:
            LOG.error("HTTP error %s on url %s", e.code, url)
            raise
        except urllib2.URLError, e:
            LOG.exception("URL error on url %s", url)
            raise
 


