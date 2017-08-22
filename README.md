# Alternative scheduler
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
  reset_pupes is set to 'true'
* skip_input_sources=true - will skip all input sources when running the first pass

All of these are off by default.
