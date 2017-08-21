# -*- coding: utf-8 -*-
# Copyright (C) Bouvet ASA - All Rights Reserved.
# Unauthorized copying of this file, via any medium is strictly prohibited.

import sys
import argparse
import threading
import sesamclient
import logging
import logging.handlers
import threading
from datetime import datetime
from pprint import pformat
from tarjan import tarjan
import time
import os

from graph import Graph
from runner import Runner

logger = logging.getLogger('bootstrapper-scheduler')

class SchedulerThread:

    def __init__(self, subgraph_runners):
        self._status = "init"
        self._runners = subgraph_runners

    def run(self):
        self._status = "running"

        finished = False
        runs = 0
        while self.keep_running and not finished and runs < 100:
            finished = False
            total_processed = 0
            for runner in self._runners:
                logger.info("Running subgraph #%s (run #%s)...", runner.subgraph, runs)

                if runs == 0:
                    # First run; delete datasets, reset and run all pipes
                    total_processed += runner.run_pipes_no_deps(reset_pipes_and_delete_datasets=True)
                else:
                    # All other runs, only run internal pipes
                    total_processed += runner.run_pipes_no_deps(skip_input_sources=True)

            if total_processed == 0:
                # No entities was processed, we might be done - check queues to be sure
                logger.info("No entities processed after run #%s, checking queues...", runs)

                total_dataset_queue = 0
                total_pipe_queue = 0

                for queue in self._runners[0].get_dataset_queues():
                    total_dataset_queue += queue.get("size", 0)

                if total_dataset_queue > 0:
                    logger.info("Dataset queues are not empty (was %s) - doing another run..", total_dataset_queue)
                    continue

                for runner in self._runners:
                    for queue in runner.get_pipe_queues(runner.pipes.values()):
                        source_queue = queue.get("source", 0)
                        if isinstance(source_queue, int):
                            total_pipe_queue += source_queue
                        elif isinstance(source_queue, dict):
                            for key in source_queue:
                                value = source_queue[key]
                                if isinstance(value, int):
                                    total_pipe_queue += source_queue[key]
                                else:
                                    raise AssertionError("Don't know what '%s' is" % source_queue)
                        else:
                            raise AssertionError("Don't know what '%s' is" % source_queue)

                if total_pipe_queue > 0:
                    logger.info("Pipe queues are not empty (was %s) - doing another run..", total_pipe_queue)
                    continue

                # No more entities and the queues are empty - this means we're done
                finished = True
            else:
                logger.info("Processed %s entities in run #%s. Not done yet!", total_processed, runs)

            runs += 1

        if not self.keep_running:
            logger.info("Task was interrupted!")
            self._status = "stopped"
        else:
            if not finished:
                logger.error("Could not finish after %s runs :(" % runs)
            else:
                logger.info("Finished after %s runs :)" % runs)

            self._status = "finished"

    def start(self):
        self.keep_running = True
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

    def stop(self):
        self.keep_running = False
        self._thread.join()

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

    rotating_logfile_handler = logging.handlers.RotatingFileHandler(
        os.path.join(os.getcwd(), "bootstrap-scheduler.log"),
        mode='a',
        maxBytes=1024*1024*100,
        backupCount=9,
        encoding='utf-8', delay=0)
    rotating_logfile_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(rotating_logfile_handler)

    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='Service for running a bootstrap scheduler')

    parser.add_argument('-n', dest='node_url', required=False,
                        help="URL to node", default="http://localhost:9042/")

    parser.add_argument('-t', dest='jwt_token', required=False,
                        help="JWT token for node")

    args = parser.parse_args()

    api_connection = sesamclient.Connection(sesamapi_base_url=args.node_url + "api", jwt_auth_token=args.jwt_token, timeout=60*10)

    graph = Graph(api_connection)
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

    all_runner = Runner(api_connection=api_connection, pipes=[p.id for p in graph.pipes])
    all_runner.stop_and_disable_pipes(all_runner.pipes.values())

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

        runner = Runner(api_connection=api_connection, pipes=pipes, skip_output_pipes=True)
        runner.subgraph = subgraph
        sub_graph_runners.append(runner)

    # TODO: start scheduler thread
    scheduler = SchedulerThread(sub_graph_runners)
    scheduler.start()

    while scheduler.status not in ["done", "finished"]:
        time.sleep(10)
