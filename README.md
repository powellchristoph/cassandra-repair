# cassandra-repair

`cassandra-repair` will iterate through all keyspaces and repair each column_family individually on each cassandra node.

### Requirements
This requires that this process reach each cassandra node to execute `nodetool` and `cqlsh` commands. You must include **ALL** Cassandra nodes. Failure to do so will result in a portion of the ring not being repaired and data inconsistencies.

**_NOTE:_** This script leverages 'abstract sockets' to ensure that only a single instance of the script is running at a time. This feature is Linux-specific and not POSIX in general.

Fill in config.yaml with your cassandra nodes.
```
# Default values included. The only required setting is "hosts"
cassandra_bin: /usr/bin         # No trailing slash
connect: <First host in hosts>
timeout: 3600
retries: 3
redis: 'localhost:6379'
hosts:
    - list
    - all
    - cassandra
    - ips
    - here
recoverable: True

blacklist:
    - <do not repair this keyspace>
```

```
usage: cassandra-repair.py [-h] [--config CONFIG_FILE]

Cassandra Repair Utility

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG_FILE
```

* `hosts` - yaml list of cassandra nodes. You must include all nodes. This is a required configuration setting.
* `cassandra_bin` - path to cassandra's bin dir where nodetool/cqlsh are located. Default: /usr/bin
* `connect` - The cassandra node that will be queried for keyspace/columnfamily information. Default is the first host from the hosts list.
* `timeout` - Repairs are notorious for hanging. This will kill the repair job if it extents past this value in seconds. Currently, timed out jobs are not retried. Default: 3600 sec
* `retries` - The number of times the job will retry a failure.
* `redis` - The redis server to connect too.
* `blacklist` - List of keyspaces not to repair. They will be skipped.
* `recoverable` - Enable recoverable repairs Default: True. NOTE: See below

### Recoverable Repairs
Recoverable repairs are enabled by default. This was added to allow the script to continue if the repair previously failed or was interrupted. If enabled, the RepairManager will skip any previously completed repair jobs and continue with any remaining repairs or potential failures.

Completed repairs are cleared on a successful repair run. If disabled, all repairs will be run even if previously completed.

### Redis
Redis support has been added to maintain state between runs. This state can then be used externally to monitor your Repair.
Currently supported values:
* `REPAIR_STATUS` - Current status of the repair job. `completed`, `running`, `error: some error`
* `REPAIR_START_TIME` - The time the repair started in epoch.
* `REPAIR_CURRENT_JOB` - The current `host/keyspace.columnfamily` that is being repaired.
* `REPAIR_FAILED_JOBS` - Json encoded list of failed jobs. `["host/keyspace.columnfamily"]`
* `REPAIR_TOTAL_TIME` - The total time of the repair in epoch.
* `REPAIR_LAST_SUCCESSFUL_RUN` - The epoch timestamp of the last successful repair.
* `REPAIR_COMPLETED_JOBS` - The completed repair jobs
