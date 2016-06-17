%%%-------------------------------------------------------------------
%% @doc graphite_riakts public API
%% @end
%%%-------------------------------------------------------------------

-module(graphite_riakts_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).
-export([start/0]).

-include_lib("graphite_riakts_config.hrl").

% This is called when erlang is started with -s
start() ->
    application:ensure_all_started(cache, transient),
    % allocate in-memory cache, 1 day exp, 48 slices, 512MB per slices
    % so that's 24GB max mem usage, (512MB per 30 min)
    % check cache size/expiration every 10 min
    % the documentation wrongly says "quota" instead of "check"
    {ok, _} = cache:start_link(metric_names_cache, Opts = [{n, 48}, {memory, 512*1024*1024}, {ttl, 3600 * 24}, {check, 600} ]),
    error_logger:info_msg("~p: memory cache started, opts: ~p ~n", [ ?MODULE, Opts ]),
    application:ensure_all_started(graphite_riakts, transient).

start(_StartType, _StartArgs) ->
    error_logger:info_msg("~p: waiting for service riak_kv...~n", [ ?MODULE ]),
    riak_core:wait_for_service(riak_kv),
    error_logger:info_msg("~p: waiting for service yokozuna...~n", [ ?MODULE ]),
    riak_core:wait_for_service(yokozuna),
    error_logger:info_msg("~p: application started and activated~n", [ ?MODULE ]),
    C = graphite_riakts_config:init_context(),
    Port             = C#context.ranch_port,
    BacklogNb        = C#context.ranch_backlog_nb,
    MaxConnectionsNb = C#context.ranch_max_connections_nb,
    AcceptorsNb      = C#context.ranch_acceptors_nb,
    % this starts the TCP listener, supervised by the ranch listener
    {ok, _} = ranch:start_listener(graphite_riakts_listener, AcceptorsNb,
				   ranch_tcp, [{port, Port}, {backlog, BacklogNb}, {max_connections, MaxConnectionsNb}],
				   graphite_riakts_protocol, []),
    error_logger:info_msg("~p: ranch listeners started port ~p, backlog ~p, maxconn ~p, acceptors ~p~n",
			  [?MODULE, Port, BacklogNb, MaxConnectionsNb, AcceptorsNb]),
    graphite_riakts_sup:start_link().

stop(_State) ->
    ok.

