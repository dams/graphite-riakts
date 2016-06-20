-module(graphite_riakts_cache_warmup).
-behaviour(gen_server).

-include_lib("graphite_riakts_config.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, warmup/0, get_percent_done/0, get_metrics_count/0 ]).

-record(search_results, {
          docs,      %% Result documents
          max_score, %% Maximum score
          num_found  %% Number of results
         }).

-define(ROWS_PER_BATCH, 1000).

%% public API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

warmup() ->
    gen_server:cast(?MODULE, {warmup}).

get_percent_done() ->
    gen_server:call(?MODULE, {get_percent_done}).

get_metrics_count() ->
    gen_server:call(?MODULE, {get_metrics_count}).

%% behaviour implementation

init([]) ->
    InitC = graphite_riakts_config:init_context(),
    { ok, RiakSearchPid } = riakc_pb_socket:start_link(InitC#context.riaksearch_ip, InitC#context.riaksearch_port),
    C = InitC#context{ riaksearch_pid = RiakSearchPid },
    { ok, _State = { C, _Start = 0, _TotalRows = -1} }.

% calls
handle_call({ get_percent_done }, _From, State = { _C, _Start = 0, _TotalRows = 0 }) -> {reply, 100, State};
handle_call({ get_percent_done }, _From, State = { _C, _Start,     _TotalRows = -1}) -> {reply, 0, State};
handle_call({ get_percent_done }, _From, State = { _C, _Start,     _TotalRows = 0 }) -> {reply, 0, State};
handle_call({ get_percent_done }, _From, State = { _C,  Start,      TotalRows     }) -> {reply, Start / TotalRows * 100, State};

handle_call({ get_metrics_count }, _From, State = { _C, _Start, TotalRows }) -> {reply, TotalRows, State};

handle_call(_Message, _From, State) -> {reply, error, State}.

% casts
handle_cast({ warmup }, _State = { C, _Start, _TotalRows = -1 } ) ->
    {ok, Results} = riakc_pb_socket:search(C#context.riaksearch_pid, <<"metric_names_index">>,
                                           <<"metric_name_s:*">>, [ {rows, 0}, { start, 0}]),
    NewTotalRows = Results#search_results.num_found,

    % call ourselves again to continue warmup
    warmup(),
    { noreply, { C, _NewStart = 0, NewTotalRows} };

handle_cast({ warmup }, _State = { C, Start, TotalRows } ) when Start >= TotalRows ->
    { noreply, { C, _NewStart = TotalRows, TotalRows} };

handle_cast({ warmup }, _State = { C, Start, TotalRows } ) ->
    {ok, Results} = riakc_pb_socket:search(C#context.riaksearch_pid, <<"metric_names_index">>,
                                           <<"metric_name_s:*">>, [ {rows, ?ROWS_PER_BATCH}, { start, Start}]),
    Documents = Results#search_results.docs,
    ok = add_to_cache(Documents),
    % cast ourselves again to continue warmup
    warmup(),
    { noreply, { C, Start + ?ROWS_PER_BATCH, TotalRows} };

handle_cast(_Message, State) -> {noreply, State}.
handle_info(_Message, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVersion, State, _Extra) -> {ok, State}.

%% private functions

add_to_cache([]) -> ok;
add_to_cache([Document | Tail]) ->
    { <<"metric_names_index">>, Value } = Document,
    case proplists:get_value(<<"metric_name_s">>, Value) of
        undefined ->
            { error, no_such_field };
        MetricName ->
            ok = cache:put(metric_names_cache, MetricName, <<"">>),
            add_to_cache(Tail)
    end.
