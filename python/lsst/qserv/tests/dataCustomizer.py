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

from __future__ import absolute_import, division, print_function

import logging
import os
import shutil

from lsst.qserv.admin import commons
from lsst.qserv.tests import dataConfig
from lsst.qserv.tests import benchmark

LOG = logging.getLogger(__name__)


class DataCustomizer(object):

    def __init__(self, source_case_id, testdata_dir, target_testdata_dir,
                 do_download=True, custom_case_id=None, username=None):
        ''' Contain informations allowing to customize a dataset
        @param source_case_id: dataset to duplicate
        @param testdata_dir: directory containing test dataset to duplicate
        @param target_testdata_dir: destination directory
        @param download_action: use data configuration to eventually override data with
        remote big data files
        @return True if success, else False
        '''

        self._username = username
        self._doDownload = do_download
        self._src_dataset_dir = benchmark.Benchmark.getDatasetDir(
            testdata_dir, source_case_id)
        self._custom_case_id = custom_case_id if custom_case_id else source_case_id
        self._dest_dataset_dir = benchmark.Benchmark.getDatasetDir(target_testdata_dir,
                                                                   self._custom_case_id)

    def run(self):
        ''' Customize one integration test dataset
            using current DataCustomizer object information
        '''

        LOG.info("Customizing integration tests datasets")
        DataCustomizer._duplicate_data_dir(self._src_dataset_dir,
                                           self._dest_dataset_dir)

        self._data_dir = os.path.join(self._dest_dataset_dir, "data")
        self._dataConfig = dataConfig.DataConfig(self._data_dir)

        if self._doDownload:
            urls = self._dataConfig.rsyncUrls
            for url in urls:
                dest_file = os.path.join(self._data_dir, os.path.basename(url))
                DataCustomizer._rsync(url, dest_file, self._username)

        LOG.info("Customization successful")
        return True

    @staticmethod
    def _duplicate_data_dir(src, dest):
        LOG.info("Customized dataset location: %s", dest)
        try:
            if not os.path.exists(dest):
                if not os.path.exists(src):
                    raise IOError("Can't access source dataset location: %s",
                                  src)
                else:
                    LOG.info("Copy source dataset %s to %s", src, dest)
                    shutil.copytree(src, dest)
        except OSError:
            LOG.exception("Unable to copy input data set from %s to %s",
                          src, dest)
            raise

    @staticmethod
    def _rsync(url, dest_file, username=None):
        full_url = "{0}@{1}".format(username, url) if username else url
        cmd = ["rsync",
               "-avzhe",
               "ssh",
               full_url,
               dest_file]
        commons.run_command(cmd, loglevel=logging.WARN)
