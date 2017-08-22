# -*- coding: utf-8 -*-
# Copyright (C) Bouvet ASA - All Rights Reserved.
# Unauthorized copying of this file, via any medium is strictly prohibited.

from flask import Flask, request, Response
import json
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

from pprint import pprint

scheduler = None
headers = None

app = Flask(__name__)

logger = logging.getLogger('bootstrapper-scheduler')


class SchedulerThread:

    def __init__(self, subgraph_runners):
        self._status = "init"
        self._runners = subgraph_runners
        self._thread = None
        self._reset_pipes = False
        self._delete_datasets = False
        self._skip_input_pipes = False

    def run(self):
        self._status = "running"

        finished = False
        runs = 0
        while self.keep_running and not finished and runs < 100:
            finished = False
            total_processed = 0
            for runner in self._runners:
                logger.info("Running subgraph #%s of %s - pass #%s...", runner.subgraph, len(self._runners), runs)

                if runs == 0:
                    # First run; delete datasets, reset and run all pipes
                    total_processed += runner.run_pipes_no_deps(reset_pipes=self._reset_pipes,
                                                                delete_datasets=self._delete_datasets,
                                                                skip_input_sources=self._skip_input_pipes)
                else:
                    # All other runs, only run internal pipes
                    total_processed += runner.run_pipes_no_deps(skip_input_sources=True)

            if total_processed == 0:
                # No entities was processed, we might be done - check queues to be sure
                logger.info("No entities processed after pass #%s, checking queues...", runs)

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
                    logger.info("Pipe queues are not empty (was %s) - doing another pass..", total_pipe_queue)
                    continue

                # No more entities and the queues are empty - this means we're done
                finished = True
            else:
                logger.info("Processed a total of %s entities for in pass #%s. Not done yet!", total_processed, runs)

            runs += 1

        if not self.keep_running:
            logger.info("Task was interrupted!")
            self._status = "stopped"
        else:
            if not finished:
                logger.error("Could not finish after %s passes :(" % runs)
            else:
                logger.info("Finished after %s passes! :)" % runs)

            self._status = "finished"

    def start(self, reset_pipes=None, delete_datasets=None, skip_input_pipes=None):
        self.keep_running = True
        self._reset_pipes = reset_pipes is not None
        self._delete_datasets = delete_datasets is not None
        self._skip_input_pipes = skip_input_pipes is not None
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

    def stop(self):
        if self._thread is not None:
            self.keep_running = False

            self._thread.join()
            self._thread = None

    @property
    def status(self):
        return self._status


@app.route('/start', methods=['POST'])
def start():

    reset_pipes = request.args.get('reset_pipes')
    delete_datasets = request.args.get('delete_datasets')
    skip_input_pipes = request.args.get('skip_input_pipes')

    log_level = {"INFO": logging.INFO, "DEBUG": logging.DEBUG, "WARN": logging.WARNING,
                 "ERROR": logging.ERROR}.get(request.args.get('log_level', 'INFO'), logging.INFO)

    logger.setLevel(log_level)

    if reset_pipes is None and delete_datasets is not None:
        logger.warning("delete_datasets flag ignored because reset_pipes parameter not set")

    global scheduler

    if scheduler is None:
        return Response(status=403, response="Bootstrap scheduler is not ready yet")

    if scheduler.status not in ["running"]:
        scheduler.stop()
        scheduler.start(reset_pipes=reset_pipes, delete_datasets=delete_datasets, skip_input_pipes=skip_input_pipes)
        return Response(status=200, response="Bootstrap scheduler started")
    else:
        return Response(status=403, response="Bootstrap scheduler is already running")


@app.route('/stop', methods=['POST'])
def stop():

    global scheduler

    if scheduler is None:
        return Response(status=403, response="Bootstrap scheduler is not ready yet")

    sleep_time = 5
    timeout = 900

    cumul_time = 0
    if scheduler.status in ["running"]:
        scheduler.stop()

        while scheduler.status not in ["stopped", "finished"] and cumul_time > timeout:
            cumul_time += sleep_time
            time.sleep(sleep_time)

        if scheduler.status not in ["stopped", "finished"]:
            return Response(status=500, response="Error! Bootstrap scheduler did not stop within timeout (%ss)" % timeout)

        return Response(status=200, response="Bootstrap scheduler stopped")
    else:
        return Response(status=200, response="Bootstrap scheduler is not running")


@app.route('/', methods=['GET'])
def status():
    global scheduler

    if scheduler is None:
        return Response(status=200, response=json.dumps({"state": "init"}), mimetype='application/json')

    return Response(status=200, response=json.dumps({"state": scheduler.status}), mimetype='application/json')


if __name__ == '__main__':
    # TODO implement scheduler
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    logger.propagate = False

    parser = argparse.ArgumentParser(description='Service for running a bootstrap scheduler')

    parser.add_argument('-n', dest='node_url', required=False,
                        help="URL to node", default="http://localhost:9042/")

    parser.add_argument('-t', dest='jwt_token', required=False,
                        help="JWT token for node")

    parser.add_argument('-d', dest='log_dir', required=False,
                        help="logs directory")

    parser.add_argument('-l', dest='log_file', required=False,
                        help="log file")

    parser.add_argument('-L', dest='log_level', required=False, default="INFO",
                        help="log level")

    args = parser.parse_args()

    log_level = {"INFO": logging.INFO, "DEBUG": logging.DEBUG, "WARN": logging.WARNING,
                 "ERROR": logging.ERROR}.get(args.log_level, logging.INFO)

    if args.log_file:
        log_dir = args.log_dir

        if not log_dir:
            log_dir = os.getcwd()

        rotating_logfile_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, args.log_file),
            mode='a',
            maxBytes=1024*1024*100,
            backupCount=9,
            encoding='utf-8', delay=0)
        rotating_logfile_handler.setFormatter(logging.Formatter(format_string))
        logger.addHandler(rotating_logfile_handler)

    logger.setLevel(log_level)

    node_url = os.environ.get("MASTER_NODE")
    if not node_url:
        node_url = args.node_url

    jwt_token = os.environ.get("JWT_TOKEN")
    if not jwt_token:
        jwt_token = args.jwt_token

    if jwt_token is None:
        logger.error("JWT token not set in command line args or environment. Can't run. Exiting.")
        sys.exit(1)

    logger.info("Master API endpoint is: %s" % node_url + "api")

    api_connection = sesamclient.Connection(sesamapi_base_url=node_url + "api", jwt_auth_token=jwt_token, timeout=60*10)

    graph = Graph(api_connection)

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
        logger.debug("Processing subgraph #%s..." % subgraph)

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

        logger.debug("Optimal sequence for subgraph #%s:\n%s" % (subgraph, pformat(pipes)))

        runner = Runner(api_connection=api_connection, pipes=pipes, skip_output_pipes=True)
        runner.subgraph = subgraph
        sub_graph_runners.append(runner)

    scheduler = SchedulerThread(sub_graph_runners)
    #scheduler.start()

    app.run(debug=False, host='0.0.0.0', port=5000)
