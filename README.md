# Bootstrap scheduler
Scheduler that when triggered runs a project until it stabilizes, then stops. The service runs on port 5000.

[![Build Status](https://travis-ci.org/sesam-community/scheduler.svg?branch=master)](https://travis-ci.org/sesam-community/scheduler)

## API

GET / returns scheduler state (se example-state.json)
```
{
  "state" : "init|running|stopped|failed|success"
}
```


POST /start starts the scheduler when not in state ``running``.

The /start endpoint takes three request parameters:

* reset_pipes=true - will reset all pipes before starting
* delete_datasets=true - will delete all internal datasets associated with the pipes before running (only if
  reset_pipes is set
* skip_input_sources=true - will skip all input sources when running the first pass
* reload_and_wipe_microservices=true - will attempt to reload and wipe the data for all running microservices (except
  ourselves)
* compact_execution_datasets=true - will schedule compaction jobs for the pipe execution datasets to keep them from
  growing too large

All of these are off by default. Note that this means that the mere inclusion of a parameter means that the corresponding
flag is "on", the actual value of the parameter is ignored.

In addition you can change the amount of logging using:
* log_level=INFO - sets the log level when running

## Microservice configuration

```
    {
        "_id": "bootstrap-scheduler-microservice",
        "name": "Name of microservice",
        "type": "system:microservice",
        "docker": {
            "image": "sesamcommunity/scheduler:latest",
            "port": 5000,
            "memory": 128,
            "environment": {
                "MASTER_NODE": "https://m1.sesam.cloud",
                "JWT_TOKEN" : "fklrl464nimsnfskfklrl464nimskfklrl464nimsnfskfkfklrl464nimsnfskf4nimsnfskfklrl464n",
                "LOGLEVEL": "INFO"
            }
        }
    }
```

You can reach the running microservice API using the built-in proxy function. See details here:
https://docs.sesam.io/api.html#get--systems-system_id-proxy--path-relative_path-
