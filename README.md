# Bootstrap scheduler
Scheduler that when triggered runs a project until it stabilizes, then stops. 

[![Build Status](https://travis-ci.org/sesam-community/scheduler.svg?branch=master)](https://travis-ci.org/sesam-community/scheduler)

## API

GET / returns scheduler state (se example-state.json)
```
{
  "state" : "init|running|stopped|finished"
}
```


POST /start starts the scheduler when not in state ``running``.

The /start endpoint takes three request parameters:

* reset_pipes=true - will reset all pipes before starting
* delete_datasets=true - will delete all internal datasets associated with the pipes before running (only if
  reset_pipes is set to 'true'
* skip_input_sources=true - will skip all input sources when running the first pass

All of these are off by default.

In addition you can change the amount of logging using:
* log_level=INFO - sets the log level when running

## Microservice configuration

```
    {
        "_id": "bootstrap-scheduler-microservice",
        "name": "Name of microservice",
        "type": "system:microservice",
        "docker": {
            "image": "sesam-community/scheduler:latest",
            "port": 5001,
            "memory": 128,
            "environment": {
                "MASTER_NODE": "https://m1.sesam.cloud",
                "JWT_TOKEN" : "fklrl464nimsnfskfklrl464nimskfklrl464nimsnfskfkfklrl464nimsnfskf4nimsnfskfklrl464n"
            }
        }
    }
```

You can reach the running microservice API using the built-in proxy function. See details here:
https://docs.sesam.io/api.html#get--systems-system_id-proxy--path-relative_path-
