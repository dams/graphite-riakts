%% Feel free to use, reuse and abuse the code in this file.

-module(graphite_riakts_protocol).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

-define(IDLE_TIMEOUT, 5000). % milliseconds
-define(BATCH_SIZE, 1000).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    { ok, Pid }.

%    Res = riakc_pb_socket:start_link(Ip, 8087),
%    case Res of
%    	{ ok, RiakClientPid } ->
%    	    error_logger:info_msg(" ** connected to riak_ts ip ~p ~n",[ Ip ]),
%    	    {ok, Pid};
%    	Otherwise ->
%    	    error_logger:info_msg(" ******* CONNECTING TO RIAK_TS --- FAILED with ~p~n",[ Otherwise ]),
%    	    false
%    end.

init(Ref, Socket, Transport, _Opts = []) ->
    {ok, Config} = file:consult("/etc/riak/graphite_riakts.conf"),
    Ip = proplists:get_value(riakts_ip, Config),
    { ok, RiakClientPid } = riakc_pb_socket:start_link(Ip, 8087),
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, _PartialLine = <<"">>,
	 _Points = [], _NbPoints = 0,
	 _NbProcessed = 0, RiakClientPid, Config).


% We've accumulated enough data points, send to riak as batch
loop(Socket, Transport, PartialLine, Points, NbPoints, NbProcessed, RiakClientPid, Config) when NbPoints > ?BATCH_SIZE ->
    { ok, N } = send_to_riakts(Points, RiakClientPid, Config),
    loop(Socket, Transport, PartialLine,
	 _Points = [], _NbPoints = 0,
	 NbProcessed + N, RiakClientPid, Config );
% We have not yet accumulated enough data points
loop(Socket, Transport, PartialLine, Points, NbPoints, NbProcessed, RiakClientPid, Config) ->
    case Transport:recv(Socket, 0, ?IDLE_TIMEOUT) of
	{ok, Data} ->
	    % we support the graphite line protocol, so split on \n
	    Lines = binary:split(Data, <<$\n>>,[global]),
	    % prepend the first line with the data left over from previous tcp packet
	    [ FirstLine | Rest ] = Lines,
	    Lines2 = [ <<PartialLine/binary, FirstLine/binary>> | Rest],
	    % Then check and process each lines, get datapoints and new leftover data
	    { NewPoints, NewNbPoints, NewPartialLine } = process_lines(Lines2, Points, NbPoints),
	    % loop again, accumulating data points
	    loop(Socket, Transport, NewPartialLine,
		 NewPoints, NewNbPoints,
		 NbProcessed, RiakClientPid, Config );
	_Anything ->
	    % no more network data, process final line of data.
	    NewPoints = process_line(PartialLine, Points),
	    % then send the points we have as a last batch to riak ts
	    { ok, N } = send_to_riakts(NewPoints, RiakClientPid, Config),
	    error_logger:info_msg(" ---------- Processed ~p Points ~n",[ NbProcessed + N ]),    
	    ok = Transport:close(Socket)
    end.

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
	    error_logger:info_msg("Graphite data Wrong format: ~p~nOriginal Line:~p~n", [Fields, Line]),
	    Acc
    end,
    NewAcc.

send_to_riakts([], _RiakClientPid, _Config) -> { ok, 0 };
send_to_riakts(Points, RiakClientPid, Config) ->
    TableName = proplists:get_value(table_name, Config),
    {T, _} = timer:tc(fun() -> riakc_ts:put(RiakClientPid, TableName, Points) end),
    T2 = T/1000000,
%    ok = riakc_ts:put(RiakClientPid, "test2", Points),
    error_logger:info_msg(" ****************** sent ~p points to Riak table ~p in ~p sec~n",[length(Points), TableName, T2]),

    % UNCOMMENT BELOW TO SEND TO RIAK KV AND SEARCH
    %% Metrics = [ Metric || { _Family, Metric, _Timestamp, _Value} <- Points ],
    %% UniqueMetrics = sets:to_list(sets:from_list(Metrics)),

    %% {T5, _} = timer:tc(fun() -> send_to_riakkv(UniqueMetrics, RiakClientPid) end),
    %% T6 = T5/1000000,
    %% error_logger:info_msg(" ***** sent ~p keys to riakkv in ~p sec~n",[ length(UniqueMetrics), T6]),
    %% [ Example | _ ]= UniqueMetrics,
    %% error_logger:info_msg(" *********** example: ~p ~n",[ Example ]),
    { ok, length(Points) }.

send_to_riakkv(Metrics, _RiakClientPid) ->
    send_to_riakkv(Metrics, _RiakClientPid, _CacheHitCount = 0).


send_to_riakkv([ ], _RiakClientPid, CacheHitCount) ->
    error_logger:info_msg(" ***** CACHEHIT : ~p~n",[ CacheHitCount ]),
    ok;
send_to_riakkv([ Metric | T], RiakClientPid, CacheHitCount) ->
    Key = <<"node-", Metric/binary>>,
    NewCacheHitCount = case cache:get(nodes_cache, Key) of
	undefined ->
	    %error_logger:info_msg(" ***** DIDN'T FIND metric ~p~n",[ Key ]),
	    Value = <<"{\"node_s\": \"", Metric/binary, "\", \"type_s\": \"node\"}">>,
	    Obj = riakc_obj:new(_Bucket = <<"metric_nodes">>, _Key = Key, Value ),
	    Obj2 = riakc_obj:update_content_type(Obj, <<"application/json">>),
	    ok = riakc_pb_socket:put(RiakClientPid, Obj2, [ {w, 1}, {dw, 0}, {pw, 0} ]),
	    ok = cache:put(nodes_cache, Key, <<"1">>),
	    CacheHitCount;
	    %error_logger:info_msg(" ***** ADDED metric ~p~n",[ Key ]);
	_CachedValue ->
	    %error_logger:info_msg(" ***** FOUND metric ~p~n",[ Key ]),
	    CacheHitCount + 1
    end,
    send_to_riakkv(T, RiakClientPid, NewCacheHitCount).

number_to_float(Number) ->
    try binary_to_float(Number)
    catch error:badarg ->
	    binary_to_integer(Number)
    end.
