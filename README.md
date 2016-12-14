# cassandra-repair

`cassandra-repair` will iterate through all keyspaces and repair each column_family individually on each cassandra node.

REQUIRED: This requires that this process reach each cassandra node to execute `nodetool` and `cqlsh` commands.

Fill in config.yaml with your cassandra nodes.
```
ip: <any cassandra node>
hosts:
    - list
    - all
    - cassandra
    - ips
    - here

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
