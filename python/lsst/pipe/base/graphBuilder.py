# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Module defining GraphBuilder class and related methods.
"""

__all__ = ['GraphBuilder']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import copy
from collections import namedtuple
from itertools import chain
import logging

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .graph import QuantumGraphTaskNodes, QuantumGraph
from lsst.daf.butler import Quantum, DatasetRef
from lsst.daf.butler.exprParser import ParserYacc, ParserYaccError

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])

# Tuple containing TaskDef, its input dataset types and output dataset types
#
# Attributes
# ----------
# taskDef : `TaskDef`
# inputs : `list` of `DatasetType`
# outputs : `list` of `DatasetType`
_TaskDatasetTypes = namedtuple("_TaskDatasetTypes", "taskDef inputs outputs initInputs initOutputs")


class GraphBuilderError(Exception):
    """Base class for exceptions generated by graph builder.
    """
    pass


class UserExpressionError(GraphBuilderError):
    """Exception generated by graph builder for error in user expression.
    """

    def __init__(self, expr, exc):
        msg = "Failed to parse user expression `{}' ({})".format(expr, exc)
        GraphBuilderError.__init__(self, msg)


class OutputExistsError(GraphBuilderError):
    """Exception generated when output datasets already exist.
    """

    def __init__(self, taskName, refs):
        refs = ', '.join(str(ref) for ref in refs)
        msg = "Output datasets already exist for task {}: {}".format(taskName, refs)
        GraphBuilderError.__init__(self, msg)


# ------------------------
#  Exported definitions --
# ------------------------


class GraphBuilder(object):
    """
    GraphBuilder class is responsible for building task execution graph from
    a Pipeline.

    Parameters
    ----------
    taskFactory : `TaskFactory`
        Factory object used to load/instantiate PipelineTasks
    registry : `~lsst.daf.butler.Registry`
        Data butler instance.
    skipExisting : `bool`, optional
        If ``True`` (default) then Quantum is not created if all its outputs
        already exist, otherwise exception is raised.
    """

    def __init__(self, taskFactory, registry, skipExisting=True):
        self.taskFactory = taskFactory
        self.registry = registry
        self.dimensions = registry.dimensions
        self.skipExisting = skipExisting

    @staticmethod
    def _parseUserQuery(userQuery):
        """Parse user query.

        Parameters
        ----------
        userQuery : `str`
            User expression string specifying data selecton.

        Returns
        -------
        `exprTree.Node` instance representing parsed expression tree.
        """
        parser = ParserYacc()
        # do parsing, this will raise exception
        try:
            tree = parser.parse(userQuery)
            _LOG.debug("parsed expression: %s", tree)
        except ParserYaccError as exc:
            raise UserExpressionError(userQuery, exc)
        return tree

    def _loadTaskClass(self, taskDef):
        """Make sure task class is loaded.

        Load task class, update task name to make sure it is fully-qualified,
        do not update original taskDef in a Pipeline though.

        Parameters
        ----------
        taskDef : `TaskDef`

        Returns
        -------
        `TaskDef` instance, may be the same as parameter if task class is
        already loaded.
        """
        if taskDef.taskClass is None:
            tClass, tName = self.taskFactory.loadTaskClass(taskDef.taskName)
            taskDef = copy.copy(taskDef)
            taskDef.taskClass = tClass
            taskDef.taskName = tName
        return taskDef

    def makeGraph(self, pipeline, originInfo, userQuery):
        """Create execution graph for a pipeline.

        Parameters
        ----------
        pipeline : `Pipeline`
            Pipeline definition, task names/classes and their configs.
        originInfo : `~lsst.daf.butler.DatasetOriginInfo`
            Object which provides names of the input/output collections.
        userQuery : `str`
            String which defunes user-defined selection for registry, should be
            empty or `None` if there is no restrictions on data selection.

        Returns
        -------
        graph : `QuantumGraph`

        Raises
        ------
        UserExpressionError
            Raised when user expression cannot be parsed.
        OutputExistsError
            Raised when output datasets already exist.
        Exception
            Other exceptions types may be raised by underlying registry
            classes.
        """

        # make sure all task classes are loaded
        taskList = [self._loadTaskClass(taskDef) for taskDef in pipeline]

        # collect inputs/outputs from each task
        taskDatasets = []
        for taskDef in taskList:
            taskClass = taskDef.taskClass
            taskIo = []
            for attr in ("Input", "Output", "InitInput", "InitOutput"):
                getter = getattr(taskClass, f"get{attr}DatasetTypes")
                ioObject = getter(taskDef.config) or {}
                taskIo.append([dsTypeDescr.datasetType for dsTypeDescr in ioObject.values()])
            taskDatasets.append(_TaskDatasetTypes(taskDef, *taskIo))

        # build initial dataset graph
        inputs, outputs, initInputs, initOutputs = self._makeFullIODatasetTypes(taskDatasets)

        # make a graph
        return self._makeGraph(taskDatasets, inputs, outputs, initInputs, initOutputs,
                               originInfo, userQuery)

    def _makeFullIODatasetTypes(self, taskDatasets):
        """Returns full set of input and output dataset types for all tasks.

        Parameters
        ----------
        taskDatasets : sequence of `_TaskDatasetTypes`
            Tasks with their inputs, outputs, initInputs and initOutputs.

        Returns
        -------
        inputs : `set` of `butler.DatasetType`
            Datasets used as inputs by the pipeline.
        outputs : `set` of `butler.DatasetType`
            Datasets produced by the pipeline.
        initInputs : `set` of `butler.DatasetType`
            Datasets used as init method inputs by the pipeline.
        initOutputs : `set` of `butler.DatasetType`
            Datasets used as init method outputs by the pipeline.
        """
        # to build initial dataset graph we have to collect info about all
        # datasets to be used by this pipeline
        allDatasetTypes = {}
        inputs = set()
        outputs = set()
        initInputs = set()
        initOutputs = set()
        for taskDs in taskDatasets:
            for ioType, ioSet in zip(("inputs", "outputs", "initInputs", "initOutputs"),
                                     (inputs, outputs, initInputs, initOutputs)):
                for dsType in getattr(taskDs, ioType):
                    ioSet.add(dsType.name)
                    allDatasetTypes[dsType.name] = dsType
        # remove outputs from inputs
        inputs -= outputs

        # remove initOutputs from initInputs
        initInputs -= initOutputs

        inputs = set(allDatasetTypes[name] for name in inputs)
        outputs = set(allDatasetTypes[name] for name in outputs)
        initInputs = set(allDatasetTypes[name] for name in initInputs)
        initOutputs = set(allDatasetTypes[name] for name in initOutputs)
        return inputs, outputs, initInputs, initOutputs

    def _makeGraph(self, taskDatasets, inputs, outputs, initInputs, initOutputs, originInfo, userQuery):
        """Make QuantumGraph instance.

        Parameters
        ----------
        taskDatasets : sequence of `_TaskDatasetTypes`
            Tasks with their inputs and outputs.
        inputs : `set` of `DatasetType`
            Datasets which should already exist in input repository
        outputs : `set` of `DatasetType`
            Datasets which will be created by tasks
        initInputs : `set` of `DatasetType`
            Datasets which should exist in input repository, and will be used
            in task initialization
        initOutputs : `set` of `DatasetType`
            Datasets which which will be created in task initialization
        originInfo : `DatasetOriginInfo`
            Object which provides names of the input/output collections.
        userQuery : `str`
            String which defines user-defined selection for registry, should be
            empty or `None` if there is no restrictions on data selection.

        Returns
        -------
        `QuantumGraph` instance.
        """
        parsedQuery = self._parseUserQuery(userQuery or "")
        expr = None if parsedQuery is None else str(parsedQuery)
        rows = self.registry.selectDimensions(originInfo, expr, inputs, outputs)

        # store result locally for multi-pass algorithm below
        # TODO: change it to single pass
        dimensionVerse = []
        for row in rows:
            _LOG.debug("row: %s", row)
            dimensionVerse.append(row)

        # Next step is to group by task quantum dimensions
        qgraph = QuantumGraph()
        qgraph._inputDatasetTypes = inputs
        qgraph._outputDatasetTypes = outputs
        for dsType in initInputs:
            for collection in originInfo.getInputCollections(dsType.name):
                result = self.registry.find(collection, dsType)
                if result is not None:
                    qgraph.initInputs.append(result)
                    break
            else:
                raise GraphBuilderError(f"Could not find initInput {dsType.name} in any input"
                                        " collection")
        for dsType in initOutputs:
            qgraph.initOutputs.append(DatasetRef(dsType, {}))

        for taskDss in taskDatasets:
            taskQuantaInputs = {}    # key is the quantum dataId (as tuple)
            taskQuantaOutputs = {}   # key is the quantum dataId (as tuple)
            qlinks = []
            for dimensionName in taskDss.taskDef.config.quantum.dimensions:
                dimension = self.dimensions[dimensionName]
                qlinks += dimension.links()
            _LOG.debug("task %s qdimensions: %s", taskDss.taskDef.label, qlinks)

            # some rows will be non-unique for subset of dimensions, create
            # temporary structure to remove duplicates
            for row in dimensionVerse:
                qkey = tuple((col, row.dataId[col]) for col in qlinks)
                _LOG.debug("qkey: %s", qkey)

                def _dataRefKey(dataRef):
                    return tuple(sorted(dataRef.dataId.items()))

                qinputs = taskQuantaInputs.setdefault(qkey, {})
                for dsType in taskDss.inputs:
                    dataRefs = qinputs.setdefault(dsType, {})
                    dataRef = row.datasetRefs[dsType]
                    dataRefs[_dataRefKey(dataRef)] = dataRef
                    _LOG.debug("add input dataRef: %s %s", dsType.name, dataRef)

                qoutputs = taskQuantaOutputs.setdefault(qkey, {})
                for dsType in taskDss.outputs:
                    dataRefs = qoutputs.setdefault(dsType, {})
                    dataRef = row.datasetRefs[dsType]
                    dataRefs[_dataRefKey(dataRef)] = dataRef
                    _LOG.debug("add output dataRef: %s %s", dsType.name, dataRef)

            # all nodes for this task
            quanta = []
            for qkey in taskQuantaInputs:
                # taskQuantaInputs and taskQuantaOutputs have the same keys
                _LOG.debug("make quantum for qkey: %s", qkey)
                quantum = Quantum(run=None, task=None)

                # add all outputs, but check first that outputs don't exist
                outputs = list(chain.from_iterable(dataRefs.values()
                                                   for dataRefs in taskQuantaOutputs[qkey].values()))
                for ref in outputs:
                    _LOG.debug("add output: %s", ref)
                if self.skipExisting and all(ref.id is not None for ref in outputs):
                    _LOG.debug("all output dataRefs already exist, skip quantum")
                    continue
                if any(ref.id is not None for ref in outputs):
                    # some outputs exist, can't override them
                    raise OutputExistsError(taskDss.taskDef.taskName, outputs)
                for ref in outputs:
                    quantum.addOutput(ref)

                # add all inputs
                for dataRefs in taskQuantaInputs[qkey].values():
                    for ref in dataRefs.values():
                        quantum.addPredictedInput(ref)
                        _LOG.debug("add input: %s", ref)

                quanta.append(quantum)

            qgraph.append(QuantumGraphTaskNodes(taskDss.taskDef, quanta))

        return qgraph
