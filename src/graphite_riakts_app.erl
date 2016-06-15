%%%-------------------------------------------------------------------
%% @doc graphite_riakts public API
%% @end
%%%-------------------------------------------------------------------

-module(graphite_riakts_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-export([start/0]).

%%====================================================================
%% API
%%====================================================================

% This is called when erlang is started with -s
start() ->
    application:ensure_all_started(cache, transient),
    % allocate in-memory cache, 1 day exp, 48 slices, 512MB per slices
    % so that's 24GB max mem usage, (512MB per 30 min)
    % check cache size/expiration every 10 min
    % the documentation wrongly says "quota" instead of "check"
    {ok, _} = cache:start_link(nodes_cache, Opts = [{n, 48}, {memory, 512*1024*1024}, {ttl, 3600 * 24}, {check, 600} ]),
    error_logger:info_msg("memory cache started, opts: ~p ~n", [ Opts ]),
    application:ensure_all_started(graphite_riakts, transient).

start(_StartType, _StartArgs) ->
    riak_core:wait_for_service(riak_kv),


    error_logger:info_msg(atom_to_list(?MODULE) ++ " application started and activated~n",[]),
    % this starts the listener, supervised by the ranch listener
    {ok, Config} = file:consult("/etc/riak/graphite_riakts.conf"),
    Port             = proplists:get_value(ranch_port,               Config),
    BacklogNb        = proplists:get_value(ranch_backlog_nb,         Config),
    MaxConnectionsNb = proplists:get_value(ranch_max_connections_nb, Config),
    AcceptorsNb      = proplists:get_value(ranch_acceptors_nb,       Config),
    {ok, _} = ranch:start_listener(graphite_riakts_listener, AcceptorsNb,
				   ranch_tcp, [{port, Port}, {backlog, BacklogNb}, {max_connections, MaxConnectionsNb}],
				   graphite_riakts_protocol, []),
    error_logger:info_msg(atom_to_list(?MODULE) ++ " ranch listeners started port ~p, backlog ~p, maxconn ~p, acceptors ~p~n",
			  [Port, BacklogNb, MaxConnectionsNb, AcceptorsNb]),
    graphite_riakts_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================


