# 
# LSST Data Management System
# Copyright 2008, 2009, 2010, 2011 LSST Corporation.
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
#
import sys
import traceback

from .task import Task, TaskError
from .struct import Struct
from .argumentParser import ArgumentParser

__all__ = ["CmdLineTask"]

class CmdLineTask(Task):
    """A task that can be executed from the command line
    
    Subclasses must specify the following attribute:
    _DefaultName: default name used for this task
    """
    @classmethod
    def parseAndRun(cls, args=None, config=None, log=None):
        """Parse an argument list and run the command

        @param args: list of command-line arguments; if None use sys.argv
        @param config: config for task (instance of pex_config Config); if None use cls.ConfigClass()
        @param log: log (instance of pex_logging Log); if None use the default log
        
        @return a Struct containing:
        - argumentParser: the argument parser
        - parsedCmd: the parsed command returned by argumentParser.parse_args
        - task: the instantiated task
        The return values are primarily for testing and debugging
        
        The parsedCmd object is also attached as an instance variable of the task.
        """
        argumentParser = cls._makeArgumentParser()
        if config is None:
            config = cls.ConfigClass()
        parsedCmd = argumentParser.parse_args(config=config, args=args, log=log)
        task = cls(name = cls._DefaultName, config = parsedCmd.config, log = parsedCmd.log)
        task.parsedCmd = parsedCmd
        task._runParsedCmd(parsedCmd)
        return Struct(
            argumentParser = argumentParser,
            parsedCmd = parsedCmd,
            task = task,
        )

    @classmethod
    def _makeArgumentParser(cls):
        """Create an argument parser

        Subclasses may wish to override, e.g. to change the dataset type or data ref level
        """
        return ArgumentParser(name=cls._DefaultName)
    
    def _runParsedCmd(self, parsedCmd):
        """Execute the parsed command
        """
        name = self._DefaultName
        for dataRef in parsedCmd.dataRefList:
            try:
                configName = self._getConfigName()
                if configName is not None:
                    dataRef.put(parsedCmd.config, configName)
            except Exception, e:
                self.log.log(self.log.WARN, "Could not persist config for dataId=%s: %s" % \
                    (dataRef.dataId, e,))
            if parsedCmd.doraise:
                self.run(dataRef)
            else:
                try:
                    self.run(dataRef)
                except Exception, e:
                    self.log.log(self.log.FATAL, "Failed on dataId=%s: %s" % (dataRef.dataId, e))
                    if not isinstance(e, TaskError):
                        traceback.print_exc(file=sys.stderr)
            try:
                metadataName = self._getMetadataName()
                if metadataName is not None:
                    dataRef.put(self.getFullMetadata(), metadataName)
            except Exception, e:
                self.log.log(self.log.WARN, "Could not persist metadata for dataId=%s: %s" % \
                    (dataRef.dataId, e,))
    
    def _getConfigName(self):
        """Return the name of the config dataset, or None if config is not persisted
        """
        return self._DefaultName + "_config"
    
    def _getMetadataName(self):
        """Return the name of the metadata dataset, or None if metadata is not persisted
        """
        return self._DefaultName + "_metadata"
