%% Feel free to use, reuse and abuse the code in this file.

-module(graphite_riakts_protocol).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

-include_lib("graphite_riakts_config.hrl").

-type ref() :: any().
-type socket() :: any().
-type transport() :: any().

-type family() :: bitstring().
-type metric_name() :: bitstring().
-type timestamp() :: non_neg_integer().
-type datapoint() :: { family(), metric_name(), timestamp(), number() }.
-type datapoints() :: list(datapoint()).

-spec start_link(ref(), socket(), transport(), ranch:opts()) -> {ok, pid()}.
start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    { ok, Pid }.

-spec init(ref(), socket(), transport(), ranch:opts()) -> ok.
init(Ref, Socket, Transport, _Opts = []) ->
    InitC = graphite_riakts_config:init_context(),
    { ok, RiakTsPid }     = riakc_pb_socket:start_link(InitC#context.riakts_ip, InitC#context.riakts_port),
    { ok, RiakKvPid }     = riakc_pb_socket:start_link(InitC#context.riakkv_ip, InitC#context.riakkv_port),
    { ok, RiakSearchPid } = riakc_pb_socket:start_link(InitC#context.riaksearch_ip, InitC#context.riaksearch_port),
    C = InitC#context{ riakts_pid = RiakTsPid,
                       riakkv_pid = RiakKvPid,
                       riaksearch_pid = RiakSearchPid },
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, _PartialLine = <<"">>, _Points = [], _NbPoints = 0, _NbProcessed = 0, C).

-spec loop(socket(), transport(), binary(), datapoints(), non_neg_integer(), non_neg_integer(), #context{} ) -> ok.
% We've accumulated enough data points, send to riak as batch
loop(Socket, Transport, PartialLine, Points, NbPoints, NbProcessed, C) when NbPoints > C#context.riakts_write_batch_size ->
    { ok, N } = send_to_riakts(Points, C),
    loop(Socket, Transport, PartialLine, _Points = [], _NbPoints = 0, NbProcessed + N, C );
% We have not yet accumulated enough data points
loop(Socket, Transport, PartialLine, Points, NbPoints, NbProcessed, C) ->
    case Transport:recv(Socket, 0, C#context.ranch_idle_timeout_ms) of
        {ok, Data} ->
            % we support the graphite line protocol, so split on \n
            Lines = binary:split(Data, <<$\n>>,[global]),
            % prepend the first line with the data left over from previous tcp packet
            [ FirstLine | Rest ] = Lines,
            Lines2 = [ <<PartialLine/binary, FirstLine/binary>> | Rest],
            % Then check and process each lines, get datapoints and new leftover data
            { NewPoints, NewNbPoints, NewPartialLine } = process_lines(Lines2, Points, NbPoints),
            % loop again, accumulating data points
            loop(Socket, Transport, NewPartialLine, NewPoints, NewNbPoints, NbProcessed, C );
        _Anything ->
            % no more network data, process final line of data.
            NewPoints = process_line(PartialLine, Points),
            % then send the points we have as a last batch to riak ts
            { ok, N } = send_to_riakts(NewPoints, C),
            error_logger:info_msg("~p: processed ~p Points ~n",[ ?MODULE, NbProcessed + N ]),    
            ok = Transport:close(Socket)
    end.

-spec process_lines([bitstring()], datapoints(), non_neg_integer())
                   -> {datapoints(), non_neg_integer(), bitstring()}.
% we should never reach that code
process_lines(_Lines = [], Acc, Count) ->
    { Acc, Count, <<"">> };
% only one line left, don't process it, because it's probably incomplete
process_lines(_Lines = [LastLine], Acc, Count) ->
    { Acc, Count, _PartialLine = LastLine };
% general case
process_lines([Line|Rest], Acc, Count) ->
    NewAcc = process_line(Line, Acc),
    process_lines(Rest, NewAcc, Count + 1).

-spec process_line(bitstring(), datapoints()) -> datapoints().
process_line(_Line = <<>>, Acc) -> Acc;
process_line(Line, Acc) ->
    Fields = binary:split(Line, <<" ">>, [global, trim]),
    NewAcc = case length(Fields) of
        3 ->
            [Metric,ValueBinary,TimestampBinary] = Fields,
            Timestamp = round(number_to_float(TimestampBinary) * 1000),
            Value = binary_to_float(ValueBinary),
            % Family = graphite
            Point = { _Family = <<"graphite">>, Metric, Timestamp, Value },
            [ Point | Acc];
        _ ->
            error_logger:error_msg("~p: Graphite data wrong format: ~p~nOriginal Line:~p~n", [?MODULE, Fields, Line]),
            Acc
    end,
    NewAcc.

-spec send_to_riakts(datapoints(), #context{}) -> { ok, non_neg_integer() }.
send_to_riakts([], _C) -> { ok, 0 };
send_to_riakts(Points, C) ->
    TableName = C#context.table_name,
    riakc_ts:put(C#context.riakts_pid, TableName, Points),
    Metrics = [ Metric || { _Family, Metric, _Timestamp, _Value} <- Points ],
    UniqueMetrics = sets:to_list(sets:from_list(Metrics)),
    { CacheMissCount, NewMetrics } = extract_new_metrics(UniqueMetrics, C),
    case CacheMissCount of 0 -> ok; _ -> error_logger:warning_msg("~p: ~p memory cache miss ~n", [?MODULE, CacheMissCount]) end,

    PartSize = C#context.riaksearch_indexing_batch_size,
    NewMetricsGroups = part_list(NewMetrics, PartSize),
    send_to_be_indexed(NewMetricsGroups, C),
    { ok, length(Points) }.

    
% list partitioning
-spec part_list(list(), non_neg_integer()) -> list(list()).
part_list(_List, PartSize) when PartSize =< 0 -> [];
part_list( List, PartSize) -> part_list(List, PartSize, _CurrentCount = 0, _Acc = [], _Res = []).

-spec part_list(list(), non_neg_integer(), non_neg_integer(), list(), list(list())) -> list(list()).
part_list([],  _PartSize, _CurrentCount, _Acc = [], Res) -> Res;
part_list([],  _PartSize, _CurrentCount,  Acc,      Res) -> [Acc|Res];
part_list(List, PartSize,  CurrentCount,  Acc,      Res) when CurrentCount >= PartSize ->
    part_list(List, PartSize, 0, [], [Acc|Res]);
part_list([H|T], PartSize, CurrentCount, Acc, Res) ->
    part_list(T, PartSize, CurrentCount + 1, [H|Acc], Res).

-spec extract_new_metrics(list(metric_name()), #context{}) -> {non_neg_integer(), list(metric_name())}.
extract_new_metrics(UniqueMetrics, C) ->
    extract_new_metrics(UniqueMetrics, _CacheMissCount = 0, _Acc = [], C).

-spec extract_new_metrics(list(metric_name()), non_neg_integer(), list(), #context{})
                         -> {non_neg_integer(), list(metric_name())}.
extract_new_metrics([], CacheMissCount, Acc, _C) -> {CacheMissCount, Acc};
extract_new_metrics([ Metric | Tail ], CacheMissCount, Acc, C) ->
    % lookup metrics from local memory cache, or riak kv.
    { NewCacheMissCount, NewAcc } =
        case cache:get(metric_names_cache, Metric) of
            undefined ->
                ok = cache:put(metric_names_cache, Metric, <<"">>),
                case riakc_pb_socket:get(C#context.riakkv_pid, C#context.bucket_name , Metric) of
                    { ok, _Obj } -> { CacheMissCount + 1, Acc };
                    _Otherwise -> { CacheMissCount, [Metric | Acc] }
                end;
            _val -> { CacheMissCount, Acc }
        end,
    extract_new_metrics(Tail, NewCacheMissCount, NewAcc, C).


% stores in Riak KV the groups of new metrics to be indexed
-spec send_to_be_indexed(list(metric_name()), #context{}) -> ok.
send_to_be_indexed([], _C) -> ok;
send_to_be_indexed([NewMetricsGroup| Tail], C) ->
    % we store the new metrics with no key, Riak will generate one for us
    Obj = riakc_obj:new(_Bucket = <<"to_be_indexed">>, _Key = undefined, _Value = term_to_binary(NewMetricsGroup) ),
    RiakKvPid = C#context.riakkv_pid,
    { ok, ResObj} = riakc_pb_socket:put(RiakKvPid, Obj, [ {w, 1}, {dw, 0}, {pw, 0} ]),
    GroupKey = riakc_obj:key(ResObj),
    error_logger:info_msg("~p: added metric names group ~p to set ~n",[ ?MODULE, GroupKey ]),
    % we keep track of the group key in a CRDT set
    ok = riakc_pb_socket:update_type(RiakKvPid,
                                     {<<"sets">>, <<"graphite_riakts_sets">>}, <<"new_metrics_keys">>,
                                     riakc_set:to_op(riakc_set:add_element(GroupKey, riakc_set:new()))),
    send_to_be_indexed(Tail, C).

-spec number_to_float(bitstring()) -> number().
number_to_float(Number) ->
    try binary_to_float(Number)
    catch error:badarg ->
            binary_to_integer(Number)
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
part_list_test_() ->
    [ ?_assertEqual(part_list([], 0), []),
      ?_assertEqual(part_list([], 2), []),
      ?_assertEqual(part_list([1, 2, 3, 4], 5), [[4,3,2,1]]),
      ?_assertEqual(part_list([1, 2, 3, 4], 2), [[4,3],[2,1]]),
      ?_assertEqual(part_list([1, 2, 3], 2),    [[3],[2,1]])
    ].
-endif.
