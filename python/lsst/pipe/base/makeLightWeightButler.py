from __future__ import annotations

__all__ = ("LightWeightButlerBuilder", )

import io

from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, DefaultDict, Mapping, Optional, Set, Tuple, Iterable, List, Union
import os
import shutil

from lsst.daf.butler import (DatasetRef, DatasetType, Butler, ButlerConfig, Registry, DataCoordinate,
                             RegistryConfig)
from lsst.daf.butler.core.utils import getClassOf
from lsst.daf.butler.transfers import RepoExportContext


from . import QuantumGraph, QuantumNode

DataSetTypeMap = Mapping[DatasetType, Set[DataCoordinate]]


@dataclass
class LightWeightButlerBuilder:
    r"""LightWeightButlerBuilder is an object that is responsible for exporting
    input `QuantumGraphs` into a new minimal `~lsst.daf.butler.Butler` which
    only contains datasets specified by the `QuantumGraph`. These datasets are
    both those that already exist in the input `~lsst.daf.butler.Butler`, and
    those that are expected to be produced during the execution of the
    `QuantumGraph`.

    Parameters
    ----------
    butler : `lsst.daf.butler.Bulter`
        This is the existing `~lsst.daf.butler.Butler` instance from which
        existing datasets will be exported. This should be the
        `~lsst.daf.butler.Butler` which was used to create any `QuantumGraphs`
        that will be converted with this object.
    collections : `~typing.Iterable` of `str`, Optional
        An iterable of collection names that will be exported from the input
        `~lsst.daf.butler.Butler` when creating the lightweight butler. If not
        supplied the `~lsst.daf.butler.Butler`\ 's `~lsst.daf.butler.Registry`
        default collections will be used.
    """
    butler: Butler
    collections: Optional[Iterable[str]] = None

    def _accumulate(self, graph: QuantumGraph) -> Tuple[Set[DatasetRef], DataSetTypeMap]:
        # accumulate the dataIds that will be transferred to the lightweight
        # registry

        # exports holds all the existing data that will be migrated to the
        # lightweight butler
        exports = set()

        # inserts is the mapping of DatasetType to dataIds for what is to be
        # inserted into the registry. These are the products that are expected
        # to be produced during processing of the QuantumGraph
        inserts: DefaultDict[DatasetType, Set[DataCoordinate]] = defaultdict(set)

        n: QuantumNode
        for quantum in (n.quantum for n in graph):
            for attrName in ("initInputs", "inputs", "outputs"):
                attr: Mapping[DatasetType, Union[DatasetRef, List[DatasetRef]]] = getattr(quantum, attrName)

                for type, refs in attr.items():
                    # This if block is because init inputs has a different
                    # signature for its items
                    if not isinstance(refs, list):
                        refs = [refs]
                    # iterate over all the references, if it has an id, it
                    # means it exists and should be exported, if not it should
                    # be inserted into the new registry
                    for ref in refs:
                        if ref.isComponent():
                            # We can't insert a component, and a component will
                            # be part of some other upstream dataset, so it
                            # should be safe to skip them here
                            continue

                        if ref.id:
                            exports.add(ref)
                        else:
                            inserts[type].add(ref.dataId)
        return exports, inserts

    def _discoverCollections(self, collections: Iterable[str]) -> set[str]:
        # Recuse though any discovered collections to make sure all collections
        # are exported. This exists because I ran into a situation where some
        # collections were not properly being discovered and exported. This
        # method may be able to be removed in the future if collection export
        # logic changes
        collections = set(collections)
        while True:
            discoveredCollections = set(self.butler.registry.queryCollections(collections, flattenChains=True,
                                                                              includeChains=True))
            if discoveredCollections - collections:
                collections = discoveredCollections
            else:
                break
        return collections

    def _export(self, exports: Set[DatasetRef]) -> io.StringIO:
        # This exports the datasets that exist in the input butler using
        # daf butler objects, however it reaches in deep and does not use the
        # public methods so that it can export it to a string buffer and skip
        # disk access.
        yamlBuffer = io.StringIO()
        # Yaml is hard coded, since the class controlls both ends of the
        # export/import
        BackendClass = getClassOf(self.butler._config["repo_transfer_formats"]["yaml"]["export"])
        backend = BackendClass(yamlBuffer)
        exporter = RepoExportContext(self.butler.registry, self.butler.datastore, backend, directory=None,
                                     transfer=None)
        exporter.saveDatasets(exports)

        # Look for any defined collection, if not get the defaults
        if (collections := self.collections) is None:
            collections = self.butler.registry.defaults.collections

        # look up all collections associated with those inputs, this follows
        # all chains to make sure everything is properly exported
        for c in self._discoverCollections(collections):
            exporter.saveCollection(c)
        exporter._finish()

        # reset the string buffer to the beginning so the read operation will
        # actually *see* the data that was exported
        yamlBuffer.seek(0)
        return yamlBuffer

    def _setupNewButler(self, outputLocation: str, dirExists: bool) -> Butler:
        # Set up the new butler object at the specified location
        if dirExists:
            if os.path.isfile(outputLocation):
                os.remove(outputLocation)
            else:
                shutil.rmtree(outputLocation)
        os.mkdir(outputLocation)

        # Copy the existing butler config, modifying the location of the
        # registry to the specified location.
        # Preserve the root path from the existing butler so things like
        # file data stores continue to look at the old location.
        config = ButlerConfig(self.butler._config)
        config["registry", "db"] = f"sqlite:///{outputLocation}/gen3.sqlite3"
        config["root"] = self.butler._config.configDir.ospath

        # Create the new registry which will create and populate the sqlite
        # file.
        Registry.createFromConfig(RegistryConfig(config))

        # Return a newly created butler
        return Butler(config, writeable=True)

    def _import(self,
                yamlBuffer: io.StringIO,
                newButler: Butler,
                inserts: DataSetTypeMap,
                run: str,
                butlerModifier: Optional[Callable[[ButlerConfig], Butler]]
                ) -> Butler:
        # This method takes the exports from the existing butler, imports
        # them into the newly created butler, and then inserts the datasets
        # that are expected to be produced.

        # import the existing datasets
        newButler.import_(filename=yamlBuffer, format='yaml', reuseIds=True)

        # If there is modifier callable, run it to make necessary updates
        # to the new butler.
        if butlerModifier is not None:
            newButler = butlerModifier(newButler._config)

        # Register datasets to be produced and insert them into the registry
        for dsType, refs in inserts.items():
            newButler.registry.registerDatasetType(dsType)
            newButler.registry.insertDatasets(dsType, refs, run)

        return newButler

    def build(self,
              graph: QuantumGraph,
              outputLocation: str,
              run: str,
              *,
              clobber: bool = False,
              butlerModifier: Optional[Callable[[ButlerConfig], Butler]] = None
              ) -> None:
        """Builds a butler and lightweight registry only containing entries
        that are contained in the input `QuantumGraph`.

        Parameters
        ----------
        graph : `QuantumGraph`
            Graph containing nodes that are to be exported into a lightweight
            butler
        outputLocation : `str`
            Location at which the lightweight butler is to be exported
        run : `str`
            The run collection that the exported datasets are to be placed in.
        clobber : `bool`, Optional
            By default a butler will not be created if a file or directory
            already exists at the output location. If this is set to `True`
            what is at the location will be deleted prior to running the
            export. Defaults to `False`
        butlerModifier : `~typing.Callable`, Optional
            If supplied this should be a callable that accepts a
            `~lsst.daf.butler.ButlerConfig`, and returns an instantiated
            `~lsst.daf.butler.Butler`. This callable may be used to make any
            modifications to the `~lsst.daf.butler.Butler` desired. This
            will be called after importing all datasets that exist in the input
            `~lsst.daf.butler.Butler` but prior to inserting Datasets expected
            to be produced. Examples of what this method could do include
            things such as creating collections/runs/ etc.

        Raises
        ------
        FileExistsError
            Raise if something exists in the filesystem at the specified output
            location and clobber is `False`
        """
        # Do this first to Fail Fast if the output exists
        if (dirExists := os.path.exists(outputLocation)) and not clobber:
            raise FileExistsError("Cannot create a butler at specified location, location exists")

        exports, inserts = self._accumulate(graph)
        yamlBuffer = self._export(exports)

        newButler = self._setupNewButler(outputLocation, dirExists)

        newButler = self._import(yamlBuffer, newButler, inserts, run, butlerModifier)
        newButler._config.dumpToUri(f"{outputLocation}/butler.yaml")
