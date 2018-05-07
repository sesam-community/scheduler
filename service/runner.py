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

logger = logging.getLogger('bootstrapper-scheduler.runner')


class Runner:

    def __init__(self, api_connection, pipes, profiling=False,
                 skip_input_pipes=False, skip_internal_pipes=False, skip_output_pipes=False):
        self.api_connection = api_connection
        self.api_connection.session.verify = False
        self.execution_datasets = {}
        self.stats = {}
        self.input_pipes = {}
        self.internal_pipes = {}

        if profiling:
            self.pump_params = {
                "profile": "true"
            }
        else:
            self.pump_params = {}

        _pipes = []
        for pipe_id in pipes:
            pipe = self.api_connection.get_pipe(pipe_id)
            if not pipe:
                logger.error("Couldn't find pipe '%s'!" % pipe_id)
                sys.exit(1)

            effective_config = pipe._raw_jsondata["config"]["effective"]

            source = effective_config.get("source")
            sink = effective_config.get("sink")

            source_system = source.get("system")
            sink_system = sink.get("system")

            if source["type"] == "http_endpoint":
                # No point in running http_endpoint sources!
                logging.info("Skipping 'http_endpoint' input pipe '%s'", pipe_id)
                continue
            elif source["type"] == "embedded":
                # Embedded sources are input pipes, even if system is the node
                self.input_pipes[pipe.id] = pipe
                if skip_input_pipes:
                    logging.info("Skipping input pipe '%s'", pipe_id)
                    continue
            else:
                if source_system.startswith("system:sesam-node"):
                    if sink_system.startswith("system:sesam-node") and \
                                             sink["type"] not in ["http_endpoint", "xml_endpoint",
                                                                  "csv_endpoint", "null"]:
                        self.internal_pipes[pipe.id] = pipe
                        if skip_internal_pipes:
                            logging.info("Skipping internal pipe '%s'", pipe_id)
                            continue
                    else:
                        if skip_output_pipes:
                            logging.info("Skipping output pipe '%s'", pipe_id)
                            continue
                else:
                    self.input_pipes[pipe.id] = pipe
                    if skip_input_pipes:
                        logging.info("Skipping input pipe '%s'", pipe_id)
                        continue

            _pipes.append(pipe)

        self.pipes = OrderedDict()
        for pipe in _pipes:
            self.pipes[pipe.id] = pipe

    def stop_and_disable_pipes(self, pipes):
        for pipe in pipes:
            pump = pipe.get_pump()
            # Stop the pipe
            if "stop" in pump.supported_operations:
                pump.stop()

            if "disable" in pump.supported_operations:
                pump.disable()
            else:
                logger.warning("Could not disable pump for pipe '%s'! Is it set to manual?" % pipe.id)

    def enable_pipes(self, pipes):
        for pipe in pipes:
            pump = pipe.get_pump()
            # Stop the pipe
            if "enable" in pump.supported_operations:
                pump.enable()
            else:
                logger.error("Could not enable pump for pipe '%s'!" % pipe.id)

    def start_pipes(self, pipes):
        for pipe in pipes:
            pump = pipe.get_pump()
            # Stop the pipe
            if "start" in pump.supported_operations:
                pump.start(operation_parameters=self.pump_params)
            else:
                logger.error("Could not start pump for pipe '%s'!" % pipe.id)

    def enable_and_run_pipes(self, pipes):
        for pipe in pipes:
            pump = pipe.get_pump()
            if "enable" in pump.supported_operations:
                logger.debug("Enabling pipe %s.." % pipe.id)
                pump.enable()

            if "start" in pump.supported_operations:
                logger.debug("Starting pipe %s.." % pipe.id)
                pump.start(operation_parameters=self.pump_params)

    def reset_pipes(self, pipes):
        for pipe in pipes:
            logger.debug("Resetting pipe '%s'.." % pipe.id)
            pump = pipe.get_pump()

            if "update-last-seen" in pump.supported_operations:
                pump.unset_last_seen()

    def delete_datasets(self, pipes):
        for pipe in pipes:
            effective_config = pipe.config.get("effective")
            if effective_config:
                sink = effective_config.get("sink")
                if sink:
                    sink_datasets = sink.get("datasets", sink.get("dataset"))
                    if sink_datasets and not isinstance(sink_datasets, list):
                        sink_datasets = [sink_datasets]

                    if sink_datasets:
                        logger.debug("Deleting datasets: %s" % sink_datasets)
                        for dataset_id in sink_datasets:
                            dataset = self.api_connection.get_dataset(dataset_id)
                            if dataset:
                                logger.debug("Deleting dataset '%s' in in node.." % dataset_id)
                                dataset.delete()
                            else:
                                logger.warning("Failed to delete dataset '%s' in in node "
                                               "- could not find dataset" % dataset_id)


    def compact_execution_logs(self, pipes):
        # curl -s -X POST -d ‘operation=schedule-compaction&time_threshold_date=2016-09-17T06:42:50.265403Z&keep_versions=2’
        # ‘http://localhost:6547/api/datasets/system:pump:workentry-currenttime-with-jira-keys-step8b-has-no-errors-filter'

        for pipe in pipes:
            execution_dataset_id = "system:pump:" + pipe.id
            dataset = self.api_connection.get_dataset(execution_dataset_id)

            compaction_params = {
                "time_threshold_date": datetime.utcnow().isoformat() + "Z",
                "keep_versions": 2
            }

            if dataset is not None:
                logger.info("Scheduling compaction for pump execution dataset for pipe '%s'", pipe.id)

                retries = 3
                failed = True
                while retries > 0:
                    try:
                        dataset.run_operation("schedule-compaction", compaction_params)
                        failed = False
                        break
                    except BaseException as e:
                        logger.warning("Failed to execute compaction for dataset '%s' - retrying..",
                                       execution_dataset_id)
                        logger.debug("Reason was '%s'" % repr(e))
                        time.sleep(2)
                        retries -= 1

                if failed:
                    logger.warning("Failed to execute compaction for dataset '%s' even after 3 retries - skipping it",
                                   execution_dataset_id)
                    return True
            else:
                logger.warning("Could not find pump execution dataset for pipe '%s' - ignoring", pipe.id)

        return True


    def _get_latest_done_event_from_execution_log(self):
        return self._get_latest_one_of_two_events_from_execution_log("pump-completed",
                                                                     "pump-failed")

    def _get_latest_one_of_two_events_from_execution_log(self, dataset, one, two):
        first_event = dataset.get_entity(one)
        other_event = dataset.get_entity(two)

        if (first_event is not None) and (other_event is not None):
            # We have both events, so we must figure out which one occured last.
            if first_event["_updated"] > other_event["_updated"]:
                latest_event = first_event
            else:
                latest_event = other_event

        elif first_event is not None:
            latest_event = first_event
        else:
            latest_event = other_event

        return latest_event

    def get_last_run_entities(self, pipes):
        entities = {}

        for pipe in pipes:
            dataset_id = "system:pump:" + pipe.id

            dataset = self.execution_datasets.get(dataset_id)
            if dataset is None:
                dataset = self.api_connection.get_dataset(dataset_id)
                self.execution_datasets[dataset_id] = dataset

            if dataset is not None:
                entities[pipe.id] = self._get_latest_one_of_two_events_from_execution_log(dataset, "pump-completed",
                                                                                          "pump-failed")
            else:
                # Never run before
                entities[pipe.id] = None

        return entities

    def _run_pipes_until_finished(self, pipes, skip_empty_queues=False):
        _pipes = [p for p in pipes]

        if skip_empty_queues:
            # We're given permission to check the queue first to see if there is anything to do
            for pipe in _pipes[:]:
                pipe_queue_size = self.get_pipe_queue_size(pipe)
                if isinstance(pipe_queue_size, int) and pipe_queue_size == 0:
                    # Remove any pipe that has no work to do
                    logger.info("Nothing to do for pipe '%s', skipping it", pipe.id)
                    _pipes.remove(pipe)

            if len(_pipes) == 0:
                return 0

        previous_entities = self.get_last_run_entities(pipes)
        total_processed = 0
        self.enable_and_run_pipes(pipes)

        retries = {}

        finished = False
        sleep_time = 0.001
        num_retries = 0

        while not finished:
            # Run until all execution datasets have been updated with either a pump-completed or a pump-failed entity
            new_entities = self.get_last_run_entities(_pipes)

            finished = True
            for pipe in _pipes[:]:
                if new_entities[pipe.id] is None or new_entities[pipe.id] == previous_entities[pipe.id]:
                    finished = False
                else:
                    if new_entities[pipe.id]["event_type"] == "pump-failed":
                        reason = new_entities[pipe.id].get("reason_why_stopped", "")
                        if reason.find("all dependent datasets must exist and be indexed and the source and sink must be valid") > -1:
                            #print("new_entities[pipe.id]=", new_entities[pipe.id])
                            #print("previous_entities[pipe.id]=", previous_entities[pipe.id])
                            pump = pipe.get_pump()
                            if "start" in pump.supported_operations:
                                # We need to retry this one a couple of times - the indexing might not be finished yet
                                retries_so_far = retries.get(pipe.id, 1)
                                if retries_so_far <= num_retries:
                                    previous_entities = new_entities
                                    logger.warning("Pipe %s failed, retrying (%s).." % (pipe.id, retries_so_far))
                                    retries[pipe.id] = retries_so_far + 1
                                    sleep_time += 1.0 # Wait a little longer each retry
                                    finished = False
                                    pump.start(operation_parameters=self.pump_params)
                                else:
                                    logger.error("Pipe %s failed to run after %s retries, "
                                                 "giving up and disabling it..." % (pipe.id, retries_so_far))
                                    self.stop_and_disable_pipes([pipe])
                                    _pipes.remove(pipe)
                            else:
                                logger.error("Pipe %s failed to run and we're not allowed to start it again! "
                                             "Giving up and disabling it..." % pipe.id)
                                self.stop_and_disable_pipes([pipe])
                                _pipes.remove(pipe)
                        else:
                            logger.error("Pipe %s failed to run for some reason! "
                                         "Giving up and disabling it... Reason was:\n%s" % (pipe.id, reason))
                            self.stop_and_disable_pipes([pipe])
                            _pipes.remove(pipe)
                    else:
                        processed = new_entities[pipe.id].get("processed_last_run", 0)
                        total_processed += processed
                        logger.debug("Pipe %s is finished (%s processed), disabling it..." % (pipe.id, processed))
                        self.stop_and_disable_pipes([pipe])
                        _pipes.remove(pipe)

            if not finished:
                time.sleep(sleep_time)

        # Disable pipes before retuning
        self.stop_and_disable_pipes(pipes)

        return total_processed

    def _run_pipes_until_finished_api(self, pipes):
        # Use the new sync pipe runs api

        total_processed = 0

        result = self.api_connection.session.post(self.api_connection.sesamapi_base_url + "/pipes/run-pipes?timeout=20",
                                                  json=[p.id for p in pipes])

        if result:
            for pipe_id, pipe_result in result.json().items():
                if pipe_result["result"] == "completed":
                    total_processed += pipe_result["status"]["processed_last_run"]
                else:
                    logger.warning("Pipe '%s' did not complete successfully. "
                                   "The reason was: '%s'" % (pipe_id, pipe_result["reason"]))
        else:
            logger.error("Failed to run pipes!")

        return total_processed

    def run_pipes_until_finished(self, title, pipes, sequential=False, skip_empty_queues=False, use_new_api=True):
        starttime = time.monotonic()
        processed_entities = 0
        if use_new_api:
            logger.debug("Running pipes using the run-pipes API")

            processed_entities = self._run_pipes_until_finished_api(pipes)

            run_time = time.monotonic() - starttime
            entities_per_second = int(processed_entities / run_time)
            logger.debug("Sequential '%s' run done (%s entities, %s entities/s)" % (title.lower(),
                                                                                   processed_entities,
                                                                                   entities_per_second))
            self.stats["Sequential API %s" % title.lower()] = {
                "run_time": run_time,
                "processed_entities": processed_entities,
                "entities_per_second": entities_per_second
            }
            return processed_entities

        else:
            if sequential:
                logger.debug("Running sequential %s" % title.lower())
                for pipe in pipes:
                    logger.info("Running pipe '%s'...", pipe.id)
                    entities_processed = self._run_pipes_until_finished([pipe], skip_empty_queues=skip_empty_queues)
                    if entities_processed > 0:
                        logger.info("%s entities processed by pipe '%s'", entities_processed, pipe.id)

                    processed_entities += entities_processed
                run_time = time.monotonic() - starttime
                entities_per_second = int(processed_entities / run_time)
                logger.debug("Sequential '%s' run done (%s entities, %s entities/s)" % (title.lower(),
                                                                                       processed_entities,
                                                                                       entities_per_second))
                self.stats["Sequential %s" % title.lower()] = {
                    "run_time": run_time,
                    "processed_entities": processed_entities,
                    "entities_per_second": entities_per_second
                }
                return processed_entities
            else:
                logger.debug("Running parallel %s" % title.lower())
                processed_entities += self._run_pipes_until_finished(pipes, skip_empty_queues=skip_empty_queues)
                run_time = time.monotonic() - starttime
                entities_per_second = int(processed_entities / run_time)
                logger.debug("Parallel %s test done (%s entities/s)" % (title.lower(), entities_per_second))
                self.stats["Parallel %s" % title.lower()] = {
                    "run_time": run_time,
                    "processed_entities": processed_entities,
                    "entities_per_second": entities_per_second
                }
                return processed_entities

    def get_pipe_queues(self, pipes):
        queues = []
        for pipe in self.pipes.values():
            updated_pipe = self.api_connection.get_pipe(pipe.id)
            queues.append(updated_pipe.runtime["queues"])

        return queues

    def get_pipe_queue_size(self, pipe):
        effective_config = pipe._raw_jsondata["config"]["effective"]

        source = effective_config.get("source")
        source_system = source.get("system")

        total_pipe_queue = None

        if source_system.startswith("system:sesam-node") and source["type"] not in ["http_endpoint", "embedded"]:
            updated_pipe = self.api_connection.get_pipe(pipe.id)
            source_queue = updated_pipe.runtime["queues"].get("source")

            total_pipe_queue = 0

            if isinstance(source_queue, int):
                total_pipe_queue = source_queue
            elif isinstance(source_queue, dict):
                for key in source_queue:
                    value = source_queue[key]
                    if isinstance(value, int):
                        total_pipe_queue += source_queue[key]

            # Also check the dependency queues
            dep_queue = updated_pipe.runtime["queues"].get("dependencies", {})
            for key in dep_queue:
                value = dep_queue[key]
                if isinstance(value, int):
                    total_pipe_queue += dep_queue[key]

        return total_pipe_queue

    def get_dataset_queues(self):
        queues = []
        for dataset in self.api_connection.get_datasets():
            if dataset.id.startswith("system:"):
                continue

            queues.append(dataset._raw_jsondata["runtime"]["queues"])

        return queues

    def run_pipes_no_deps(self, reset_pipes=False, delete_datasets=False, skip_input_sources=False,
                          skip_empty_queues=False, compact_execution_datasets=False):
        """ New style runner with sync deps tracker """
        self.stop_and_disable_pipes(self.pipes.values())
        if reset_pipes:
            logger.info("Resetting %s pipes... ", len(self.pipes.values()))
            self.reset_pipes(self.pipes.values())

            if delete_datasets:
                logger.info("Deleting datasets for pipes... ")
                self.delete_datasets(self.pipes.values())
            else:
                logger.info("Not deleting datasets in this run...")

        else:
            logger.info("Not resetting pipes or deleting datasets in this run...")

        if skip_input_sources:
            logger.info("Skipping input sources for this run..")

            # Note that we can't skip input pipes which has dependencies (hops to other datasets) as the output might
            # change at some point when the dependent dataset has been populated
            pipes = [pipe for pipe in self.pipes.values()
                     if pipe.id not in self.input_pipes or len(pipe.runtime.get("dependencies", [])) > 0]
        else:
            pipes = self.pipes.values()

        if compact_execution_datasets:
            logger.info("Scheduling pump execution dataset compaction for pipes... ")
            if not self.compact_execution_logs(self.pipes.values()):
                raise RuntimeError("Execution log compaction failed")
        else:
            logger.info("Not compacting pump execution datasets in this run...")

        total_processed = self.run_pipes_until_finished("All pipes", pipes, sequential=True,
                                                        skip_empty_queues=skip_empty_queues)

        return total_processed
