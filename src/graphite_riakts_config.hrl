-record(context, {
          % to be overriden by configuragion
          ranch_port = 2003,
          ranch_backlog_nb = 100,
          ranch_max_connections_nb = 100,
          ranch_acceptors_nb = 100,
          ranch_idle_timeout_ms = 5000,
          table_name = "metrics",
          bucket_name = "metric_names",
          riakts_ip = "127.0.0.1",
          riakts_port = 8087,
          riakts_write_batch_size = 1000,
          riakkv_ip = "127.0.0.1",
          riakkv_port = 8087,
          riaksearch_ip = "127.0.0.1",
          riaksearch_port = 8087,
          riaksearch_indexing_batch_size = 100,
          riaksearch_batch_indexing_timeout_ms = 2000,
          warmup_memcache = true,

          % private fields
          riakts_pid = undefined,
          riakkv_pid = undefined,
          riaksearch_pid = undefined
         }).

