# -*- coding: utf-8 -*-
# Copyright (C) Bouvet ASA - All Rights Reserved.
# Unauthorized copying of this file, via any medium is strictly prohibited.

import sys
import argparse
import threading
import sesamclient
from collections import OrderedDict
import logging
from datetime import datetime
from pprint import pformat
from tarjan import tarjan
import time

from .graph import Graph
from .runner import Runner

logger = logging.getLogger('bootstrapper-scheduler.main')

class SchedulerThread:

    def __init__(self, subgraph_runners):
        self._status = "init"
        self._runners = subgraph_runners

    def run(self):
        self._status = "running"

        finished = True
        runs = 0
        while not finished and runs < 100:
            finished = False
            total_processed = 0
            for runner in runners:
                logger.info("Running subgraph #%s..." % runner.subgraph)

                total_processed += runner.run_pipes_no_deps()

            if total_processed == 0:
                finished = True

            runs += 1

        if not finished:
            logger.error("Could not finish after %s runs :(" % runs)
        else:
            logger.info("Finished after %s runs :)" % runs)

        self.done()

    def stop(self):
        self._status = "stopped"

    def done(self):
        self._status = "finished"

    @property
    def status(self):
        return self._status


if __name__ == '__main__':
    # TODO implement scheduler

    global headers

    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='Service for running a bootstrap scheduler')

    parser.add_argument('-n', dest='node_url', required=False,
                        help="URL to node", default="http://localhost:9042/")

    parser.add_argument('-t', dest='jwt_token', required=False,
                        help="JWT token for node")

    args = parser.parse_args()

    node = sesamclient.Connection(sesamapi_base_url=args.node_url + "api", jwt_auth_token=args.jwt_token, timeout=60*10)

    graph = Graph(node)
    #graph = TestGraph()

    # Compute optimal rank
    graph.unvisitNodes()

    # Gather all connected graphs
    connected_components = 0
    for node in graph.source_endpoints[:]:
        if not node.visited:
            node.floodFill(connected_components)
            connected_components += 1

    logger.info("Found %s connected components in the graph!" % connected_components)

    # iterate over the connected components in the graph (the flows)

    graph.unvisitNodes()

    all_runner = Runner(api_connection=node, pipes=[p.id for p in graph.pipes])
    all_runner.stop_and_disable_pipes()

    sub_graph_runners = []

    # Compute optimal ranking for all subgraphs using Tarjan's algorithm

    for subgraph in range(0, connected_components):
        logger.info("Processing subgraph #%s..." % subgraph)

        g = {}
        for node in [node for node in graph.nodes.values() if node.tag == subgraph]:
            edges = [n.node_id for n in set(node.sinks + node.dependees)]
            g[node.node_id] = edges

        dg = tarjan(g)
        dg.reverse()
        i = 0
        for s in dg:
            s.reverse()
            dg[i] = [n[5:] for n in s if n.startswith("pipe_")]
            i += 1

        pipes = []
        for s in dg:
            if len(s) == 1:
                pipes.append(s[0])
                continue
            elif len(s) > 1:
                pipes.extend(s)

        logger.info("Optimal sequence for subgraph #%s:\n%s" % (subgraph, pformat(pipes)))

        runner = Runner(api_connection=node, pipes=pipes)
        runner.subgraph = subgraph
        sub_graph_runners.append(runner)

    # TODO: start scheduler thread
