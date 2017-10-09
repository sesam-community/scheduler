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
        runs = 0

        if not self._runners:
            logger.info("No pipes to run!")
            self._status = "success"
        else:
            while self.keep_running and not finished and runs < 100:
                finished = False
                total_processed = 0
                for runner in self._runners:
                    logger.info("Running subgraph #%s of %s - pass #%s...", runner.subgraph, len(self._runners), runs)

                    if runs == 0:
                        # First run; delete datasets, reset and run all pipes (so all datasets are created)
                        total_processed += runner.run_pipes_no_deps(
                            reset_pipes=self._reset_pipes,
                            delete_datasets=self._delete_datasets,
                            skip_input_sources=self._skip_input_pipes,
                            compact_execution_datasets=self._compact_execution_datasets)
                    else:
                        # All other runs, only run internal pipes and skip any pipes with empty queues
                        total_processed += runner.run_pipes_no_deps(skip_input_sources=True, skip_empty_queues=True)

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
                        for pipe in runner.pipes.values():
                            pipe_queue_size = runner.get_pipe_queue_size(pipe)
                            if isinstance(pipe_queue_size, int):
                                # Non-internal sources return None
                                total_pipe_queue += pipe_queue_size

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
                self._status = "failed"
            else:
                logger.info("Finished after %s passes! :)" % runs)

                self._status = "success"

    def start(self, reset_pipes=None, delete_datasets=None, skip_input_pipes=None, compact_execution_datasets=None):
        self.keep_running = True
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
    delete_datasets = request.args.get('delete_datasets')
    skip_input_pipes = request.args.get('skip_input_pipes')
    reload_and_wipe_ms = request.args.get('reload_and_wipe_microservices')
    compact_execution_datasets = request.args.get('compact_execution_datasets')

    log_level = {"INFO": logging.INFO, "DEBUG": logging.DEBUG, "WARN": logging.WARNING,
                 "ERROR": logging.ERROR}.get(request.args.get('log_level', 'INFO'), logging.INFO)

    logger.setLevel(log_level)

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
            reload_and_wipe_microservices(api_connection)

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

            runner = Runner(api_connection=api_connection, pipes=pipes, skip_output_pipes=True)
            runner.subgraph = subgraph
            sub_graph_runners.append(runner)

        scheduler = SchedulerThread(sub_graph_runners)
        scheduler.start(reset_pipes=reset_pipes, delete_datasets=delete_datasets, skip_input_pipes=skip_input_pipes,
                        compact_execution_datasets=compact_execution_datasets)
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

    node_url = os.environ.get("URL")
    if not node_url:
        node_url = args.node_url

    jwt_token = os.environ.get("JWT")
    if not jwt_token:
        jwt_token = args.jwt_token

    if jwt_token is None:
        logger.error("JWT token not set in command line args or environment. Can't run. Exiting.")
        sys.exit(1)

    headers["Authorization"] = "Bearer " + jwt_token

    logger.info("Master API endpoint is: %s" % node_url)

    app.run(debug=False, host='0.0.0.0', port=5000)
