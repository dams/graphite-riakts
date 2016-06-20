
-module(graphite_riakts_config).
-export([ init_context/0 ]).
-include_lib("graphite_riakts_config.hrl").

-define(get_from_config(X), X = proplists:get_value(X, Config, Default#context.X)).

init_context() ->
    {ok, Config} = file:consult("/etc/riak/graphite_riakts.conf"),
    Default = #context{},
    #context{
       ?get_from_config(ranch_port),
       ?get_from_config(ranch_backlog_nb),
       ?get_from_config(ranch_max_connections_nb),
       ?get_from_config(ranch_acceptors_nb),
       ?get_from_config(ranch_idle_timeout_ms),
       ?get_from_config(table_name),
       ?get_from_config(bucket_name),
       ?get_from_config(riakts_ip),
       ?get_from_config(riakts_port),
       ?get_from_config(riakts_write_batch_size),
       ?get_from_config(riakkv_ip),
       ?get_from_config(riakkv_port),
       ?get_from_config(riaksearch_ip),
       ?get_from_config(riaksearch_port),
       ?get_from_config(riaksearch_indexing_batch_size),
       ?get_from_config(riaksearch_batch_indexing_timeout_ms),
       ?get_from_config(warmup_memcache)
      }.
