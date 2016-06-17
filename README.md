# graphite_riakts


An OTP application to be plugged in Riak TS to make it behave like a Graphite storage node.

# Quick Start (ugly for now)

## Install and configure Riak TS

Download and install Riak TS from this url: http://docs.basho.com/riak/ts/latest/downloads/ . Test version: 1.3.0.

Create a Riak TS table to store the graphite metrics:
```
CREATE TABLE metrics (
	family		varchar   not null,
	series      varchar   not null,
	time        timestamp not null,
	metric      double,
	PRIMARY KEY (
		(family, series, quantum(time, 1, 'd')
	),family, series, time))	
```	

The family `field` will always have the value `graphite`. The `series` field
will contain the metric names, and the `metric` field will contain the value

## Build the code

    $ rebar3 compile

## Configuration

Enable Riak Search (see [https://docs.basho.com/riak/kv/2.1.4/configuring/search/#enabling-riak-search](the
documentation)). In `riak.conf`: `search = on`.
Then restart Riak TS, now you should have search enabled, and be able to add and setup an index:

Create search index:
`PUT /search/index/metric_names_index`

Associate index with bucket:
`PUT /buckets/bucket/props -d '{"props":{"search_index":"metric_names_index"}}'`

We also need set CRDTs, so let's create a bucket type for that:
`riak-admin bucket-type create sets '{"props":{"datatype":"set"}}'`

Change advanced.config to add the path where the source were built, and start the application:
```
[
 {riak_kv, [
            {add_paths, [ "/path/to/ebin"
                        ]}
           ]},
 {vm_args, [
            {'-s graphite_riakts_app', ""}
           ]}
].
```

create and edit `/etc/riak/graphite_riakts.conf` (currently hardcoded), below
are all the configuration fields and default values:

```
{ ranch_port, 2003}.
{ ranch_backlog_nb, 100}.
{ ranch_max_connections_nb, 100}.
{ ranch_acceptors_nb, 100}.
{ ranch_idle_timeout_ms, 5000}.
{ table_name, "metrics"}.
{ bucket_name, "metric_names"}.
{ riakts_ip, "127.0.0.1"}.
{ riakts_port, 8087}.
{ riakts_write_batch_size, 1000}.
{ riakkv_ip, "127.0.0.1"}.
{ riakkv_port, 8087}.
{ riaksearch_ip, "127.0.0.1"}.
{ riaksearch_port, 8087}.
{ riaksearch_indexing_batch_size, 100}.
{ riaksearch_batch_indexing_timeout_ms, 2000}.
```

Then restart Riak TS

# Overview

The application uses [https://github.com/ninenines/ranch](ranch) to maintain a
pool of TCP acceptors, to receive graphite line protocol data. Currently only
the line protocol is supported. Graphite data is stored in the Riak TS table.

Indexing metric names is done by Riak Search. To avoid overloading Riak Search,
indexing is done in batches. New Metric names are stored in a non-indexed Riak
KV bucket, whose keys are added to a CRDT Set. A genserver is polling this set
for new keys, and consume them, fetch the list of metric names, and store them
in a Riak KV bucket that is indexed by Riak Search. The daemon then waits for
the last metric to be indexed, before moving to the next batch.

When new graphite metrics arrive, the graphite_riakts application check if the
metric names are known, or new (they need to be indexed). Instead of querying
Riak KV bucket for each metric names, the application maintains an in memory
local cache, using fogfish's [git://github.com/fogfish/cache.git](caching
library).

# Caveat

If you use the default configuration, the Riak TS, Riak KV and Riak Search IP
is the same 127.0.0.1, which means that everything will run inside the Riak TS
instance. However, Riak Search is not officially supported in Riak TS,
especially AAE issues can happen. It is possible to have one instance of Riak
TS and one for Riak KV + Riak Search and have then either running on the same
node (you need to use a different token and port), or on different nodes.

Further testing may dismiss this issue, for instance if it's proven that Riak
Search works fine with standard (non Riak TS) buckets, including AAE and index
cleanup.

# TODO

- expose memory cache configuration
- memory cache warmup at startup
- HTTP APIs needs to be implemented
- Pickle protocl support
- implement retention configuration
- implement expiration (deletion of data in Riak TS table, cleanup of KV/Search, and memory cache)
- implement downsampling
- better documentation
