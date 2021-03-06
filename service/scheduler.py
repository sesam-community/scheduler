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
import requests

from graph import Graph
from runner import Runner

from pprint import pprint

scheduler = None
headers = {}
node_url = None
jwt_token = None

DEFAULT_ZERO_RUNS = 3
DEFAULT_SCHEDULER_PORT = 5000

app = Flask(__name__)

logger = logging.getLogger('bootstrapper-scheduler')

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


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
        failed = False
        runs = 0

        if not self._runners:
            logger.info("No pipes to run!")
            self._status = "success"
        else:
            try:
                zero_runs = 0
                while self.keep_running and not finished and runs < 100:
                    finished = False
                    total_changed = 0
                    runs += 1
                    logger.info("Running pass #%s (%d subgraphs)...", runs, len(self._runners))

                    for runner in self._runners:
                        logger.debug("Running subgraph #%s of %s - pass #%s...", runner.subgraph,
                                     len(self._runners), runs)

                        if runs == 1:
                            # First run; delete datasets, reset and run all pipes (so all datasets are created)
                            total_changed += runner.run_pipes_no_deps(
                                disable_pipes=self._disable_pipes,
                                reset_pipes=self._reset_pipes,
                                delete_datasets=self._delete_datasets,
                                skip_input_sources=self._skip_input_pipes,
                                compact_execution_datasets=self._compact_execution_datasets)
                        else:
                            # All other runs, only run internal pipes and skip any pipes with empty queues
                            total_changed += runner.run_pipes_no_deps(skip_input_sources=True, skip_empty_queues=True)

                    if total_changed == 0:
                        zero_runs += 1

                        if zero_runs < self._zero_runs:
                            # Run a couple of times more to be completely sure that the queues are updated
                            logger.info("Rerunning pass just in case (%s of %s)" % (zero_runs, self._zero_runs))
                            continue

                        # No entities was processed after a while, we might be done - check queues to be sure
                        logger.info("No entities changed after pass #%s, checking queues...", runs)

                        total_dataset_queue = 0
                        total_pipe_queue = 0

                        for queue in self._runners[0].get_dataset_queues():
                            total_dataset_queue += queue.get("size", 0)

                        if total_dataset_queue > 0:
                            logger.info("Dataset queues are not empty (was %s) - doing another run..", total_dataset_queue)
                            continue

                        for runner in self._runners:
                            for pipe in runner.pipes.values():
                                pipe_queue_size = runner.get_pipe_queue_size(pipe)
                                if isinstance(pipe_queue_size, int):
                                    # Non-internal sources return None
                                    total_pipe_queue += pipe_queue_size

                        if total_pipe_queue > 0:
                            logger.info("Pipe queues are not empty (was %s) - doing another pass..", total_pipe_queue)
                            continue

                        logger.info("Pipe queues are empty, nothing more do do")

                        # No more entities and the queues are empty - this means we're done
                        finished = True
                    else:
                        logger.info("A total of %s entities changed in pass #%s. We're not done yet!", total_changed, runs)
                        zero_runs = 0

            except BaseException as e:
                failed = True
                logger.exception("Scheduler failed!")

        if failed:
            self._status = "failed"
        elif not self.keep_running:
            logger.info("Task was interrupted!")
            self._status = "stopped"
        else:
            if not finished:
                logger.error("Could not finish after %s passes :(" % runs)
                self._status = "failed"
            else:
                logger.info("Finished after %s passes! :)" % runs)

                self._status = "success"

    def start(self, reset_pipes=None, delete_datasets=None, skip_input_pipes=None, compact_execution_datasets=None,
              zero_runs=DEFAULT_ZERO_RUNS, disable_pipes=None):
        self.keep_running = True
        self._zero_runs = zero_runs
        self._disable_pipes = disable_pipes is True
        self._reset_pipes = reset_pipes is not None
        self._delete_datasets = delete_datasets is not None
        self._skip_input_pipes = skip_input_pipes is not None
        self._compact_execution_datasets = compact_execution_datasets is not None
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


def reload_and_wipe_microservices(api_connection):
    for system in api_connection.get_systems():
        if system.config["effective"].get("type") == "system:microservice":
            logger.info("Processing microservice '%s'..", system.id)

            response = requests.get(node_url + "/systems/%s/status" % system.id, headers=headers)

            try:
                response.raise_for_status()
            except:
                logger.error("Failed to get status for microservice '%s': %s %s", system.id,
                             response.text, response.reason)
                raise

            restart = False
            old_pid = None
            status_data = response.json()
            if status_data:
                old_pid = status_data.get("pid")
                restart = status_data.get("running", False)

            if old_pid == os.getpid():
                logger.info("Skipping reload of microservice '%s' as it matches our own PID (%s)..", system.id, old_pid)
                continue

            if restart:
                logger.info("Reloading and wiping running microservice '%s'..", system.id)

                response = requests.post(node_url + "/systems/%s/reload" % system.id,
                                         headers=headers, params={"delete_data": True})

                try:
                    response.raise_for_status()
                except:
                    logger.error("Failed to reload microservice '%s': %s %s", system.id,
                                 response.text, response.reason)
                    raise

                # Wait for the MS to stop
                stopped = False
                timeout_delay = 0.1
                timeout = 30

                while not stopped and timeout > 0:
                    response = requests.get(node_url + "/systems/%s/status" % system.id, headers=headers)

                    try:
                        response.raise_for_status()
                    except:
                        logger.error("Failed to get status for microservice '%s': %s %s", system.id,
                                     response.text, response.reason)
                        raise

                    status_data = response.json()
                    if status_data:
                        if not status_data.get("running"):
                            stopped = True

                    time.sleep(timeout_delay)
                    timeout -= timeout_delay

                if not stopped:
                    logger.warning("Timed out waiting for MS '%s' to stop!", system.id)

                reloaded = False
                timeout = 30
                while not reloaded and timeout > 0:
                    response = requests.get(node_url + "/systems/%s/status" % system.id, headers=headers)

                    try:
                        response.raise_for_status()
                    except:
                        logger.error("Failed to get status for microservice '%s': %s %s", system.id,
                                     response.text, response.reason)
                        raise

                    status_data = response.json()
                    if status_data:
                        new_pid = status_data.get("pid")
                        reloaded = status_data.get("running", False)

                        if reloaded:
                            if old_pid == new_pid:
                                logger.warning("Microservice restarted, but the PID was the same!")

                    time.sleep(timeout_delay)
                    timeout -= timeout_delay

                if not reloaded:
                    logger.warning("Timed out waiting for MS '%s' to reload!", system.id)

            else:
                logger.info("Microservice '%s' isn't running, skipping it...", system.id)


@app.route('/start', methods=['POST'])
def start():
    reset_pipes = request.args.get('reset_pipes')
    disable_pipes = request.args.get('disable_pipes', "true") == "true"
    zero_runs = int(request.args.get("zero_runs", DEFAULT_ZERO_RUNS))
    delete_datasets = request.args.get('delete_datasets')
    skip_input_pipes = request.args.get('skip_input_pipes')
    reload_and_wipe_ms = request.args.get('reload_and_wipe_microservices')
    compact_execution_datasets = request.args.get('compact_execution_datasets')

    log_level = {"INFO": logging.INFO, "DEBUG": logging.DEBUG, "WARN": logging.WARNING,
                 "ERROR": logging.ERROR}.get(request.args.get('log_level', 'INFO'), logging.INFO)

    logger.setLevel(log_level)

    logger.info("Scheduler start() called! Arguments: %s" % repr(request.args))

    global scheduler
    global node_url
    global jwt_token

    if scheduler is None or scheduler.status not in ["running"]:
        if scheduler is not None:
            scheduler.stop()

        if reset_pipes is None and delete_datasets is not None:
            logger.warning("delete_datasets flag ignored because reset_pipes parameter not set")

        api_connection = sesamclient.Connection(sesamapi_base_url=node_url, jwt_auth_token=jwt_token, timeout=60*10,
                                                verify_ssl=False)

        if reload_and_wipe_ms is not None:
            logger.info("Wiping microservices...")
            reload_and_wipe_microservices(api_connection)

        logger.info("Building graph over pipes and datasets...")
        graph = Graph(api_connection)
        logger.info("Graph built: %s nodes" % len(graph.nodes))

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
                # Sort the connected components so we're sure the run order is deterministic
                dg[i] = sorted([n[5:] for n in s if n.startswith("pipe_")])
                i += 1

            pipes = []
            for s in dg:
                if len(s) == 1:
                    pipes.append(s[0])
                    continue
                elif len(s) > 1:
                    pipes.extend(s)

            logger.debug("Optimal sequence for subgraph #%s:\n%s" % (subgraph, pformat(pipes)))

            try:
                runner = Runner(api_connection=api_connection, pipes=pipes, skip_output_pipes=True)

                if disable_pipes is True:
                    # Make sure the endpoint pipes are disabled as well, as the runner doesn't include them
                    endpoint_pipes = []
                    for pipe_id in pipes:
                        pipe = api_connection.get_pipe(pipe_id)
                        if not pipe:
                            logger.error("Couldn't find pipe '%s'!" % pipe_id)
                            sys.exit(1)

                        effective_config = pipe._raw_jsondata["config"]["effective"]
                        source = effective_config.get("source")
                        sink = effective_config.get("sink")

                        source_system = source.get("system")
                        sink_system = sink.get("system")

                        if source["type"] in ["http_endpoint", "embedded"]:
                            continue
                        else:
                            if source_system.startswith("system:sesam-node"):
                                if not sink_system.startswith("system:sesam-node"):
                                    # Disable endpoint pipe
                                    endpoint_pipes.append(pipe)

                    if len(endpoint_pipes) > 0:
                        logger.info("Disabling %s endpoint pipes..." % len(endpoint_pipes))
                        runner.stop_and_disable_pipes(endpoint_pipes)
            except BaseException as e:
                return Response(status=403, response="Failed to read config from the node, can't start scheduler - "
                                                     "check if the config is valid\n\nThe exception was:\n%s" % repr(e))

            runner.subgraph = subgraph
            sub_graph_runners.append(runner)

        scheduler = SchedulerThread(sub_graph_runners)
        scheduler.start(reset_pipes=reset_pipes, delete_datasets=delete_datasets, skip_input_pipes=skip_input_pipes,
                        compact_execution_datasets=compact_execution_datasets, disable_pipes=disable_pipes,
                        zero_runs=zero_runs)
        return Response(status=200, response="Bootstrap scheduler started")
    else:
        return Response(status=403, response="Bootstrap scheduler is already running")


@app.route('/stop', methods=['POST'])
def stop():

    logger.info("Scheduler stop() called!")

    global scheduler

    if scheduler is None:
        return Response(status=403, response="Bootstrap scheduler is not ready yet")

    sleep_time = 5
    timeout = 900

    cumul_time = 0
    if scheduler.status in ["running"]:
        scheduler.stop()

        while scheduler.status in ["running"] and cumul_time > timeout:
            cumul_time += sleep_time
            time.sleep(sleep_time)

        if scheduler.status not in ["stopped", "success", "failed"]:
            return Response(status=500, response="Error! Bootstrap scheduler did not stop within timeout (%ss)" % timeout)

        return Response(status=200, response="Bootstrap scheduler stopped")
    else:
        return Response(status=200, response="Bootstrap scheduler is not running")


@app.route('/', methods=['GET'])
def status():
    global scheduler

    if scheduler is None:
        logger.info("Scheduler status() called! Result = 'init'")
        return Response(status=200, response=json.dumps({"state": "init"}), mimetype='application/json')

    logger.info("Scheduler status() called! Result = '%s'" % scheduler.status)
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
                        help="URL to node", default="http://localhost:9042/api")

    parser.add_argument('-t', dest='jwt_token', required=False,
                        help="JWT token for node")

    parser.add_argument('-d', dest='log_dir', required=False,
                        help="logs directory")

    parser.add_argument('-l', dest='log_file', required=False,
                        help="log file")

    parser.add_argument('-L', dest='log_level', required=False, default="INFO",
                        help="log level")

    args = parser.parse_args()

    log_level = os.environ.get("LOGLEVEL")
    if log_level is None:
        log_level = args.log_level

    log_level = {"INFO": logging.INFO, "DEBUG": logging.DEBUG, "WARN": logging.WARNING,
                 "ERROR": logging.ERROR}.get(log_level, logging.INFO)

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

    if os.environ.get("SESAM_NOT_IN_DOCKER") is not None:
        node_url = os.environ.get("URL")
        if not node_url:
            node_url = args.node_url
    else:
        node_url = "http://sesam-node:8042/api"

    http_port = int(os.environ.get("SCHEDULER_PORT", DEFAULT_SCHEDULER_PORT))

    jwt_token = os.environ.get("JWT")
    if not jwt_token:
        jwt_token = args.jwt_token

    if jwt_token is None:
        logger.error("JWT token not set in command line args or environment. Can't run. Exiting.")
        sys.exit(1)

    headers["Authorization"] = "Bearer " + jwt_token

    logger.info("Master API endpoint is: %s" % node_url)

    werkzeug_log = logging.getLogger('werkzeug')
    werkzeug_log.disabled = True
    app.logger.disabled = True

    app.run(debug=False, host='0.0.0.0', port=http_port)
