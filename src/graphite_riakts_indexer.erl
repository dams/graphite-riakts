-module(graphite_riakts_indexer).
-behaviour(gen_server).

-include_lib("graphite_riakts_config.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, maybe_index/0]).

-record(search_results, {
          docs,      %% Result documents
          max_score, %% Maximum score
          num_found  %% Number of results
         }).

%% public API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
maybe_index() ->
    gen_server:call(?MODULE, {maybe_index}).    

%% behaviour implementation

init([]) ->
    InitC = graphite_riakts_config:init_context(),
    { ok, RiakSearchPid } = riakc_pb_socket:start_link(InitC#context.riaksearch_ip, InitC#context.riaksearch_port),
    C = InitC#context{ riaksearch_pid = RiakSearchPid },
    {ok, _Timer} = timer:apply_after(1000, ?MODULE, maybe_index, []),
    error_logger:info_msg("~p: indexer daemon initialized ~n",[ ?MODULE ]),
    {ok, _State = { C }}.

handle_call({maybe_index}, _From, _State = { C }) ->
    Set = riakc_pb_socket:fetch_type(C#context.riaksearch_pid, {<<"sets">>,<<"graphite_riakts_sets">>},
                                     <<"new_metrics_keys">>),
    SomethingWasIndexed = index_from_set(Set, C),
    SleepTime = case SomethingWasIndexed of
                    false -> 10000; % there were nothing to index, call us in 10 sec
                    _ -> 1         % we indexed something, call us in 1 ms 
                end,
    {ok, _Timer} = timer:apply_after(SleepTime, ?MODULE, maybe_index, []),    
    { noreply, _NewState = { C } };

handle_call(_Message, _From, State) ->
        {reply, error, State}.

handle_cast(_Message, State) -> {noreply, State}.
handle_info(_Message, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVersion, State, _Extra) -> {ok, State}.

%% private functions

wait_for_indexing(MetricName, GroupKey, C) -> wait_for_indexing(MetricName, GroupKey, 0, C).
wait_for_indexing(undefined,  _GroupKey, _WaitTime, _C) -> ok;
wait_for_indexing(MetricName, _GroupKey,  WaitTime,  C) when WaitTime > C#context.riaksearch_batch_indexing_timeout_ms ->
    error_logger:error_msg("~p: timeout error waiting for indexing of metric ~p (timeout is ~p) ~n",
                           [ ?MODULE, MetricName, C#context.riaksearch_batch_indexing_timeout_ms ]),
    {error, timeout};
    % TODO: if wait failed, readd the metrics to be indexed in the set
wait_for_indexing(MetricName,  GroupKey,  WaitTime,  C) ->
    T1 = get_timestamp_ms(),
    {ok, Results} = riakc_pb_socket:search(C#context.riaksearch_pid, <<"metric_names_index">>,
                                           <<"name_s:", MetricName/binary >>),
    case Results#search_results.num_found of
        0 -> T2 = get_timestamp_ms(),
             wait_for_indexing(MetricName, GroupKey, WaitTime + T2 - T1, C);
        Num -> error_logger:info_msg("~p: found ~p result for metric ~p ~n", [ ?MODULE, Num, MetricName ]),
               ok = riakc_pb_socket:delete(C#context.riaksearch_pid, <<"to_be_indexed">>, GroupKey),
               ok
    end.

index_from_set({error, {notfound, set}}, _C) ->
    false;
index_from_set({ok, Set}, C) ->
    Groups = riakc_set:value(Set),
    { MetricNameToCheck, GroupKey } = index_first_group(Groups, Set, C),
    % we're going to wait for the last stored metric name to be indexed
    wait_for_indexing(MetricNameToCheck, GroupKey, C),
    true;
index_from_set(Reason, _C) ->
    error_logger:info_msg("~p: couldn't get Set to index. Reason: ~p ~n",[ ?MODULE, Reason ]),    
    false.

index_first_group([], _Set, _C) -> { undefined, undefined };
index_first_group([GroupKey|_Tail], Set, C) ->
    % we are taking care of the first metric names group. We remove it from the set, and process it
    ok = riakc_pb_socket:update_type(C#context.riaksearch_pid,
                                     {<<"sets">>, <<"graphite_riakts_sets">>}, <<"new_metrics_keys">>,
                                     riakc_set:to_op(riakc_set:del_element(GroupKey, Set))),
    error_logger:info_msg("~p: indexing metric names group ~p (~p groups left)~n",[ ?MODULE, GroupKey, riakc_set:size(Set) - 1 ]),

    % we fetch the corresponding key from the riakkv bucket, we get the list of metric names
    { ok, Obj } = riakc_pb_socket:get(C#context.riaksearch_pid, <<"to_be_indexed">>, GroupKey),
    MetricNames = binary_to_term(riakc_obj:get_value(Obj)),

    % we store them as individual keys, in a bucket that has riak search indexing enabled
    MetricNameToCheck = store_metric_names(MetricNames, undefined, C),
    { MetricNameToCheck, GroupKey }.

store_metric_names([], LastMetricNameIndexed, _C) -> LastMetricNameIndexed;
store_metric_names([MetricName|Tail], _LastMetricNameIndexed, C) -> 
    error_logger:info_msg("~p: METRIC ~p ~n",[ ?MODULE, MetricName ]),
    store_nodes(MetricName, C),
    store_metric_names(Tail, MetricName, C).

store_nodes(MetricName, C) ->
    [FirstBranch|Nodes] = binary:split(MetricName, <<".">>, [global, trim]),
    case Nodes of
        [] -> store_leaf(FirstBranch, 1,C);
        NonEmptyNodes ->
            store_branch(FirstBranch, 1, C),
            store_nodes(FirstBranch, NonEmptyNodes, 2, C)
    end.

store_nodes( Branch, [Node | Tail], Level, C) ->
    NewBranch = <<Branch/binary, ".", Node/binary>>,
    case Tail of 
        [] ->
            store_leaf(NewBranch, Level, C);
        _Otherwise ->
            store_branch(NewBranch, Level, C),
            store_nodes(NewBranch, Tail, Level + 1, C)
    end.

store_branch(Name, Level, C) ->
    % we use only one memcache for both branches and leaves
    case cache:get(metric_names_cache, Name) of
        undefined ->
            ok = cache:put(metric_names_cache, Name, <<"branch">>),
            case riakc_pb_socket:get(C#context.riaksearch_pid, C#context.bucket_name, Name) of
                { ok, _Obj } -> ok; % already stored in KV (was missing from the cache)
                _Otherwise ->
                    LevelBin = integer_to_binary(Level),
                    % JSON crafted by hand, what could possibly go wrong ?
                    Value = <<"{\"name_s\": \"", Name/binary, "\", \"level_i\": ",
                              LevelBin/binary, ", \"type_s\": \"branch\"}">>,
                    Obj = riakc_obj:new(list_to_binary(C#context.bucket_name), _Key = Name, Value ),
                    Obj2 = riakc_obj:update_content_type(Obj, <<"application/json">>),
                    ok = riakc_pb_socket:put(C#context.riaksearch_pid, Obj2, [ {w, 3}, {dw, 3}, {pw, 0} ])
            end;
        _val -> ok % already in memory cache 
    end.

% store_leaf doesn't check the memory cache because graphite_riakts_protocol
% immediately adds metric names (so leaves) to the memory cache already
store_leaf(Name, Level, C) ->
    % we use only one memcache for both branches and leaves
    case riakc_pb_socket:get(C#context.riaksearch_pid, C#context.bucket_name, Name) of
        { ok, _Obj } -> ok; % already stored in KV (was missing from the cache)
        _Otherwise ->
            LevelBin = integer_to_binary(Level),
            % JSON crafted by hand, what could possibly go wrong ?
            Value = <<"{\"name_s\": \"", Name/binary, "\", \"level_i\": ",
                      LevelBin/binary, ", \"type_s\": \"leaf\"}">>,
            Obj = riakc_obj:new(list_to_binary(C#context.bucket_name), _Key = Name, Value ),
            Obj2 = riakc_obj:update_content_type(Obj, <<"application/json">>),
            ok = riakc_pb_socket:put(C#context.riaksearch_pid, Obj2, [ {w, 3}, {dw, 3}, {pw, 0} ])
    end.

get_timestamp_ms() ->
  {Mega, Sec, Micro} = os:timestamp(),
  (Mega*1000000 + Sec)*1000 + round(Micro/1000).
