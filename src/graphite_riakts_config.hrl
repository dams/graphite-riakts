-record(context, {
          % to be overriden by configuragion
          ranch_port = 2003                           :: non_neg_integer(),
          ranch_backlog_nb = 100                      :: non_neg_integer(),
          ranch_max_connections_nb = 100              :: non_neg_integer(),
          ranch_acceptors_nb = 100                    :: non_neg_integer(),
          ranch_idle_timeout_ms = 5000                :: non_neg_integer(),
          table_name = "metrics"                      :: string(),
          bucket_name = "metric_names"                :: string(),
          riakts_ip = "127.0.0.1"                     :: string(),
          riakts_port  = 8087                         :: non_neg_integer(),
          riakts_write_batch_size = 1000              :: non_neg_integer(),
          riakkv_ip  = "127.0.0.1"                    :: string(),
          riakkv_port = 8087                          :: non_neg_integer(),
          riaksearch_ip  = "127.0.0.1"                :: string(),
          riaksearch_port = 8087                      :: non_neg_integer(),
          riaksearch_indexing_batch_size = 1000       :: non_neg_integer(),
          riaksearch_batch_indexing_timeout_ms = 2000 :: non_neg_integer(),
          warmup_memcache = true                      :: 'true' | 'false',

          % private fields
          riakts_pid = undefined,
          riakkv_pid = undefined,
          riaksearch_pid = undefined
         }).

