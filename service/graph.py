# -*- coding: utf-8 -*-
# Copyright (C) Bouvet ASA - All Rights Reserved.
# Unauthorized copying of this file, via any medium is strictly prohibited.

import sys
import threading
import sesamclient
from collections import OrderedDict
import logging
from datetime import datetime
from pprint import pformat
import time

logger = logging.getLogger('bootstrapper-scheduler.graph')


class GraphNode:
    _config = None

    def __init__(self, component, node_id=None):
        self._component = component
        self._node_id = node_id or self.config.get("_id")
        self._graph_id = self.node_id.replace("-", "_").replace(" ", "_")
        self._sinks = []
        self._sources = []
        self._dependees = []
        self._dependencies = []
        self._visited = False
        self._rank = -1
        self._tag = None
        self._is_endpoint = False

    def __str__(self):
        return 'GraphNode %s' % self.id

    def __repr__(self):
        return str(self)

    def setIsEndpoint(self, is_endpoint=True):
        self._is_endpoint = is_endpoint

    @property
    def is_endpoint(self):
        return self._is_endpoint

    @property
    def node_id(self):
        return self._node_id

    @property
    def graph_id(self):
        return self._graph_id

    @property
    def id(self):
        return self.config.get("_id", self._node_id)

    @property
    def component(self):
        return self._component

    @property
    def visited(self):
        return self._visited

    def setVisited(self, _visited):
        self._visited = _visited

    def setTag(self, tag):
        self._tag = tag

    @property
    def tag(self):
        return self._tag

    @property
    def config(self):
        if self._config:
            return self._config

        return self.component.config.get("effective", self.component.config)

    @property
    def sinks(self):
        return self._sinks

    @property
    def rank(self):
        return self._rank

    def setRank(self, rank):
        self._rank = rank

    @property
    def sources(self):
        return self._sources

    def add_sink(self, _node):
        if _node not in self._sinks:
            self._sinks.append(_node)

    def add_source(self, _node):
        if _node not in self._sources:
            self._sources.append(_node)

    def floodFill(self, tag):
        # Recursively visit all nodes reachable from this one
        # and mark then with the given "tag"
        self.setTag(tag)
        self.setVisited(True)

        for source in self.sources:
            if not source.visited:
                source.floodFill(tag)

        for sink in self.sinks:
            if not sink.visited:
                sink.floodFill(tag)

        for dep in self.dependencies:
            if not dep.visited:
                dep.floodFill(tag)

        for dep in self.dependees:
            if not dep.visited:
                dep.floodFill(tag)

    def get_upstream_nodes(self, include_deps=True, resultset=[]):
        if not include_deps and hasattr(self, "_upstream_nodes"):
            return self._upstream_nodes
        elif include_deps and hasattr(self, "_upstream_nodes_with_deps"):
            return self._upstream_nodes_with_deps

        result = [s for s in self._sources]
        if include_deps:
            for dep in self.dependencies:
                if dep not in result:
                    result.append(dep)

        result_copy = result[:]
        history = resultset + result_copy
        for _source_node in result_copy:

            if _source_node in resultset:
                logger.warning("Found circular reference in %s -> %s" % (self.node_id, _source_node.node_id))
                result.remove(_source_node)
                continue
                #raise AssertionError("Circular reference")

            for upstream_node in _source_node.get_upstream_nodes(resultset=history):
                if upstream_node not in result:
                    result.append(upstream_node)

        if not include_deps:
            self._upstream_nodes = result
        else:
            self._upstream_nodes_with_deps = result

        return result

    def getMaxRank(self):
        max_rank = self.rank
        try:
            for node in self.get_upstream_nodes():
                if node.rank > max_rank:
                    max_rank = node.rank
        except:
            pass

        return max_rank

    def get_downstream_nodes(self, include_deps=False, only_non_visited=False):
        #if not include_deps and hasattr(self, "_downstream_nodes"):
        #    return self._downstream_nodes
        #elif include_deps and hasattr(self, "_downstream_nodes_with_deps"):
        #    return self._downstream_nodes_with_deps

        if only_non_visited:
            self.setVisited(True)

        result = [s for s in self._sinks]

        if include_deps:
            for dep in self.dependees:
                if dep not in result:
                    result.append(dep)

        result_copy = result[:]
        for _sink_node in result_copy:
            if only_non_visited and _sink_node.visited:
                logger.warning("Found circular reference in %s <- %s" % (self.node_id, _sink_node.node_id))
                result.remove(_sink_node)
                continue

            for downstream_node in _sink_node.get_downstream_nodes(include_deps=include_deps,
                                                                   only_non_visited=only_non_visited):
                if downstream_node not in result:
                    result.append(downstream_node)

        #if not include_deps:
        #    self._downstream_nodes = result
        #else:
        #    self._downstream_nodes_with_deps = result

        return result

    @property
    def dependencies(self):
        return self._dependencies

    def add_dependencies(self, _node):
        if _node not in self._dependencies:
            self._dependencies.append(_node)

    def add_dependees(self, _node):
        if _node not in self._dependees:
            self._dependees.append(_node)

    @property
    def dependees(self):
        return self._dependees

    @property
    def pipe_sinks(self):
        # Get the first pipes downstream (i.e. skip any intermediate dataset sink nodes)
        pipe_sinks = []
        for sink in self.sinks:
            if isinstance(sink, PipeNode):
                pipe_sinks.append(sink)
            else:
                for node in sink.sinks:
                    if isinstance(node, PipeNode):
                        pipe_sinks.append(node)

        return pipe_sinks


class DatasetNode(GraphNode):

    def __init__(self, dataset):
        if isinstance(dataset, dict):
            self._config = dataset
        else:
            self._config = {"_id": dataset.id}

        super().__init__(self, node_id="dataset_" + self.config["_id"])

    @property
    def config(self):
        return self._config

    @property
    def rank(self):
        rank = self._rank
        for source in self.sources:
            if source.rank > rank:
                rank = source.rank + 1
        return rank


class PipeNode(GraphNode):
    def __init__(self, component):
        if isinstance(component, dict):
            self._config = component
            super().__init__(self, node_id="pipe_" + component["_id"])
        else:
            super().__init__(component, node_id="pipe_" + component.id)


class Graph:
    _lock_graph = threading.RLock()

    _edges = []
    _dotted_edges = []
    _nodes = {}

    _datasets = []
    _source_endpoints = []
    _sink_endpoints = []
    _pipes = []

    def __init__(self, node):
        self._node = node

        # Get datasets from node and add them as nodes

        with self._lock_graph:
            self._edges = []
            self._dotted_edges = []
            self._nodes = {}

            self._datasets = []
            self._source_endpoints = []
            self._sink_endpoints = []
            self._pipes = []

            for dataset in node.get_datasets():
                if dataset.id.startswith("system:"):
                    # Skip the execution log datasets and any other internal datasets
                    continue

                self.addNode(DatasetNode(dataset))

            for pipe in node.get_pipes():
                #if not pipe.is_valid_config:
                #    # Skip invalid pipes
                #    continue

                pipe_node = self.addNode(PipeNode(pipe))

                # Add a dotted edge from any hops-derived dependent datasets to this pipe
                for dep in pipe.runtime["dependencies"]:
                    did = "dataset_" + dep
                    if did not in self._nodes:
                        self.addNode(DatasetNode({"_id": dep}))

                    dep_node = self._nodes[did]

                    self.addDottedEdge(dep_node, pipe_node)

                source = pipe_node.config.get("source", {})
                sink = pipe_node.config.get("sink", {})

                source_datasets = source.get("datasets", source.get("dataset"))
                if source_datasets and not isinstance(source_datasets, list):
                    source_datasets = [source_datasets]

                # Clean datasets, in case there are "merge" ones
                if source.get("type") == "merge":
                    source_datasets = [ds.rpartition(" ")[0] for ds in source_datasets if ds]

                sink_datasets = sink.get("datasets", sink.get("dataset"))
                if sink_datasets and not isinstance(sink_datasets, list):
                    sink_datasets = [sink_datasets]

                if source_datasets is not None:
                    # Add all sources
                    for source_dataset in source_datasets:
                        sdid = "dataset_" + source_dataset
                        if sdid not in self._nodes:
                            self.addNode(DatasetNode({"_id": source_dataset}))

                        source_dataset_node = self._nodes[sdid]
                        self.addEdge(source_dataset_node, pipe_node)
                else:
                    # If there are no source datasets, it must be either a external system or a http_endpoint source
                    pipe_node.setIsEndpoint()

                if sink_datasets is not None:
                    # Add all sinks
                    for sink_dataset in sink_datasets:
                        sdid = "dataset_" + sink_dataset
                        if sdid not in self._nodes:
                            self.addNode(DatasetNode({"_id": sink_dataset}))

                        sink_dataset_node = self._nodes[sdid]
                        self.addEdge(pipe_node, sink_dataset_node)
                else:
                    # If there are no sink datasets, it must be either a external system or a http_endpoint sink
                    pipe_node.setIsEndpoint()

            self._datasets = [node for node in self._nodes.values() if isinstance(node, DatasetNode)]
            self._source_endpoints = [node for node in self._nodes.values() if node.is_endpoint and len(node.sources) == 0]
            self._sink_endpoints = [node for node in self._nodes.values() if node.is_endpoint and len(node.sinks) == 0]
            self._pipes = [node for node in self._nodes.values() if isinstance(node, PipeNode)]

    @property
    def node(self):
        return self._node

    @property
    def nodes(self):
        with self._lock_graph:
            return self._nodes

    @property
    def edges(self):
        with self._lock_graph:
            return self._edges

    @property
    def datasets(self):
        with self._lock_graph:
            return self._datasets

    @property
    def source_endpoints(self):
        with self._lock_graph:
            return self._source_endpoints

    @property
    def sink_endpoints(self):
        with self._lock_graph:
            return self._sink_endpoints

    def unvisitNodes(self):
        with self._lock_graph:
            for node in self._nodes.values():
                node.setVisited(False)

    def untagNodes(self):
        with self._lock_graph:
            for node in self._nodes.values():
                node.setTag(None)
    @property
    def pipes(self):
        with self._lock_graph:
            return self._pipes

    @property
    def dotted_edges(self):
        with self._lock_graph:
            return self._dotted_edges

    def addEdge(self, node1, node2):
        with self._lock_graph:
            self._edges.append((node1, node2))
            node1.add_sink(node2)
            node2.add_source(node1)

    def addDottedEdge(self, node1, node2):
        with self._lock_graph:
            self._dotted_edges.append((node1, node2))
            node1.add_dependees(node2)
            node2.add_dependencies(node1)

    def addNode(self, node):
        with self._lock_graph:
            if not node in self._nodes:
                self._nodes[node.node_id] = node

            return node

    def visitNodesBreadthFirst(self, current_rank, nodes, ranked_graph):
        with self._lock_graph:
            new_rank = set()

            for node in nodes:
                node.setVisited(True)

                for sink in node.sinks:
                    sink.setVisited(True)

                    if sink.is_endpoint:
                        continue

                    elif isinstance(sink, DatasetNode):
                        # Jump to next pipe
                        for pipe in sink.sinks:
                            if isinstance(pipe, PipeNode) and not pipe.visited and not pipe.is_endpoint:
                                new_rank.add(pipe)

                    elif isinstance(sink, PipeNode) and not sink.visited:
                        new_rank.add(sink)

            if len(new_rank) > 0:
                ranked_graph[current_rank] = new_rank
                self.visitNodesBreadthFirst(current_rank + 1, new_rank, ranked_graph)

    def computeRanks(self, graph, input_nodes, iterations=5):
        seen_nodes = set()

        for node in input_nodes:
            node.setRank(0)

        seen_nodes.update(input_nodes)

        converged = False
        runs = 0
        while not converged and runs < iterations:
            converged = True
            for node in input_nodes:
                graph.unvisitNodes()
                downstream_nodes = node.get_downstream_nodes(include_deps=True, only_non_visited=True)
                seen_nodes.update(downstream_nodes)
                for subnode in downstream_nodes:
                    # Set the subnode's rank to the max of its source/deps ranks
                    rank_nodes = set(subnode.sources + subnode.dependencies)

                    graph.unvisitNodes()
                    subnode.setVisited(True)

                    subnode_downstream = subnode.get_downstream_nodes(include_deps=True, only_non_visited=True)

                    rank_nodes = rank_nodes - set(subnode_downstream)

                    max_rank = -1
                    for rank_node in rank_nodes:
                        if rank_node._rank > max_rank:
                            max_rank = rank_node._rank

                    max_rank += 1

                    if max_rank > subnode._rank:
                        print("Iteration %s: %s -> %s" % (runs, subnode.id, max_rank))
                        subnode.setRank(max_rank)
                        converged = False

                        # Update all subnodes too
            runs += 1

        # Build new ranked_graph
        new_ranked_graph = OrderedDict()
        for node in seen_nodes:
            if not node._rank in new_ranked_graph:
                new_ranked_graph[node._rank] = set()

            new_ranked_graph[node._rank].add(node)

        return new_ranked_graph

    def updateRanks(self, ranked_graph, iterations=100):
        with self._lock_graph:
            # First note current rank
            pipes = []
            for rank in ranked_graph:
                for pipe in ranked_graph[rank]:
                    pipes.append(pipe)
                    pipe.setRank(rank)

            # Iterate over pipes and update ranks until converged or max iterations exceeded
            i = 0
            converged = False
            while not converged:
                converged = True
                for pipe in pipes:
                    max_rank = pipe.getMaxRank()
                    if max_rank > pipe.rank:
                        pipe.setRank(max_rank + 1)
                        converged = False

                i += 1
                if i > iterations:
                    print("Maximum iterations exceeded, terminating update")
                    break

                if converged:
                    print("Optimal ranking achieved after %d iterations" % i)

            # Build new ranked_graph
            new_ranked_graph = OrderedDict()
            for pipe in pipes:
                if not pipe.rank in new_ranked_graph:
                    new_ranked_graph[pipe.rank] = set()

                new_ranked_graph[pipe.rank].add(pipe)

            return new_ranked_graph


class TestGraph(Graph):

    def __init__(self):

        pipes = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "S1", "S2"]

        nodes = {}

        for id in pipes:
            nodes[id] = self.addNode(PipeNode({"_id": id}))

        self.addEdge(nodes["A"], nodes["E"])
        self.addEdge(nodes["E"], nodes["F"])
        self.addEdge(nodes["F"], nodes["K"])

        self.addEdge(nodes["B"], nodes["F"])
        self.addEdge(nodes["B"], nodes["G"])
        self.addEdge(nodes["G"], nodes["K"])
        self.addEdge(nodes["G"], nodes["J"])
        self.addEdge(nodes["J"], nodes["M"])

        self.addEdge(nodes["J"], nodes["S1"])
        self.addEdge(nodes["S1"], nodes["S2"])
        self.addEdge(nodes["S2"], nodes["M"])

        self.addEdge(nodes["C"], nodes["H"])
        self.addEdge(nodes["H"], nodes["I"])
        self.addEdge(nodes["H"], nodes["G"])

        self.addEdge(nodes["D"], nodes["I"])

        self.addEdge(nodes["A"], nodes["E"])

        self.addEdge(nodes["I"], nodes["L"])
        self.addEdge(nodes["L"], nodes["M"])

        self.addDottedEdge(nodes["S2"], nodes["G"])

        nodes["A"].setIsEndpoint()
        nodes["B"].setIsEndpoint()
        nodes["C"].setIsEndpoint()
        nodes["D"].setIsEndpoint()

        self._datasets = [node for node in self._nodes.values() if isinstance(node, DatasetNode)]
        self._source_endpoints = [node for node in self._nodes.values() if node.is_endpoint and len(node.sources) == 0]
        self._sink_endpoints = [node for node in self._nodes.values() if node.is_endpoint and len(node.sinks) == 0]
        self._pipes = [node for node in self._nodes.values() if isinstance(node, PipeNode)]
