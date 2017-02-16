# -*- coding: utf-8 -*-

import sys
import os
from os import path
import shutil
import tempfile
import zipfile
import logging

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel("INFO")


class SparkImportHelper(object):
    """Helper class for loading python dependecy files into Spark's distributed
    cache.

    The only interface method is add_deps(). When used with path (/my/path for
     example) string, it first uploads all .py files in this directory to Spark
    by using SparkContext.addPyFile().
    Then, for each top-level sub-directory under /my/path, it creates a zipped
    file with all .py files in and under this sub-directory and uploads into
    Spark. Zipping a sub-module is necessary; otherwise, all files will be
    located in the root directory of Spark's distributed cache. This causes
    "Import Error: No module named..." exception.

    Usage:
        myhelper = SparkImportHelper(sparkSession)
        myhelper.add_deps("/path/to/some/custom/lib")
    """

    def __init__(self, spark):
        self.spark = spark
        self.tmpdir = None

    def __find_files(self, basedir, exts, recursive=True):
        """Find all files having one of given file extensions in a given
        basedir. If recursive is True, it also searches in sub-directories.
        """
        if not path.isdir(basedir):
            raise ValueError("basedir is not a directory: %s", basedir)

        target_files = []
        if recursive:
            # get all files
            all_files = []
            for root, dirs, files in os.walk(basedir):
                for onefile in files:
                    all_files.append(path.join(root, onefile))

            # filtering with file extension
            for onefile in all_files:
                if path.splitext(onefile)[1] in exts:
                    target_files.append(onefile)
        else:
            # get all files
            all_files = []
            for onepath in os.listdir(basedir):
                onepath = path.join(basedir, onepath)
                if path.isfile(onepath):
                    all_files.append(onepath)

            # filtering with file extension
            for onefile in all_files:
                if path.splitext(onefile)[1] in exts:
                    target_files.append(onefile)

        LOGGER.info("Files found in %s: %s", basedir, str(target_files))
        return target_files

    def __get_sub_module(self, basedir, subdir, exts):
        """Create a zipped sub-module.
        """
        matched_files = self.__find_files(subdir, exts, True)
        if not matched_files:
            return ""

        zipmod_path = path.join(
            self.tmpdir,
            os.path.basename(subdir)+".zip")
        with zipfile.ZipFile(zipmod_path, "w") as zfp:
            for abs_filepath in matched_files:
                rel_filepath = path.relpath(abs_filepath, basedir)
                zfp.write(abs_filepath, rel_filepath)
        LOGGER.info("Zipped a module: %s", zipmod_path)

        return zipmod_path

    def __add_module(self, filepath):
        if filepath:
            LOGGER.info("Adding a file: %s", filepath)
            self.spark.sparkContext.addPyFile(filepath)
        else:
            LOGGER.warn("Module name for __add_module is an empty string")


    def add_deps(self, basedir, exts=[".py"]):
        """PySpark dependency file uploader.

        Params:
            basedir: directory in which main modules reside
            exts   : file extensions for uploading
        """
        basedir = path.normpath(basedir)
        LOGGER.info("Target base dir: %s", basedir)

        # 1. add main modules
        #     Main modules are going to be imported with their own file
        #   names (e.g. import my_main_module). To make it work, we have
        #   to upload them into the root directory of Spark's distributed
        #   cache.
        for filepath in self.__find_files(basedir, exts, False):
            self.__add_module(filepath)

        # 2. add sub modules
        #     Sub-modules, uploaded into Spark's distributed cache, must
        #   have the directory structure same to the one in local machine.
        #   Otherwise, it will cause "ImportError: No module named..."
        #   exception.
        #     To make it work, we create a zip file for each top-level
        #   sub-module (e.g. module1.zip for myprogram/module1) and
        #   add it to Spark's distributed cache.
        LOGGER.info("Creating a tempdir")
        self.tmpdir = tempfile.mkdtemp()

        # absolute paths of top-level sub-directories
        subdirs = []
        for item in os.listdir(basedir):
            onepath = path.join(basedir, item)
            if path.isdir(onepath):
                subdirs.append(onepath)

        # make a zipped sub-module and add it to Spark
        for subdir in subdirs:
            filepath = self.__get_sub_module(basedir, subdir, exts)
            self.__add_module(filepath)

        LOGGER.info("Removing the tempdir: %s", self.tmpdir)
        shutil.rmtree(self.tmpdir)


if __name__ == "__main__":
    print "This is not a stand-alone program!"
    sys.exit(0)


