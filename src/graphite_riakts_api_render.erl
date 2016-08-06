-module(graphite_riakts_api_render).

%% webmachine resource exports
-export([ init/1,
          content_types_provided/2,
          service_available/2,
          malformed_request/2,
          to_textplain/2
         ]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("graphite_riakts_config.hrl").


-record(wm_context, {
          format = "json" :: string(),
          riakts_pid      :: pid(),
          table_name      :: string(),
          target          :: string(),
          from            :: integer(),
          until           :: integer()
         }).

-spec init(any()) -> {ok, #wm_context{}}.
init(_) ->
    {ok, #wm_context{}}.

-spec content_types_provided(#wm_reqdata{}, #wm_context{}) -> {[any()], #wm_reqdata{}, #wm_context{}}.
content_types_provided(ReqData, C) ->
    {[{"plain/text", to_textplain}],
     ReqData, C}.

-spec malformed_request(#wm_reqdata{}, #wm_context{}) -> {true | false, #wm_reqdata{}, #wm_context{}}.
malformed_request(ReqData, _C) ->
    InitC = graphite_riakts_config:init_context(),
    { ok, RiakTsPid } = riakc_pb_socket:start_link(InitC#context.riakts_ip, InitC#context.riakts_port),
    NewC = #wm_context{
              target = wrq:get_qs_value("target", ReqData),
              from = to_int(wrq:get_qs_value("from", ReqData)),
              until = to_int(wrq:get_qs_value("until", ReqData)),
              riakts_pid = RiakTsPid,
              table_name = InitC#context.table_name
             },
    {false, ReqData, NewC}.

% main function called when receiving a request
-spec to_textplain(#wm_reqdata{}, #wm_context{}) -> {binary(), #wm_reqdata{}, #wm_context{}}.
to_textplain(ReqData, C) ->
    Target = C#wm_context.target,
    Series = list_to_binary(Target),
    From = C#wm_context.from,
    Until = C#wm_context.until,
    TableName = C#wm_context.table_name,
    Family = <<"graphite">>,

    TimeSpan = [From * 1000, Until * 1000],
    Key = [Family, Series, TimeSpan],
    error_logger:info_msg("~p: got render request target: ~p from ~p until ~p ~n", [ ?MODULE, Target, From, Until ]),
    error_logger:info_msg("~p: key: ~p ~n", [ ?MODULE, Key ]),
    
    Query = [ <<"select * from ">>, TableName, <<" ">>,
              <<"where series='">>, Target, <<"' ">>,
              <<"AND family='graphite' AND time > ">>, integer_to_binary(From*1000), <<" ">>,
              <<"AND time < ">>, integer_to_binary(Until*1000)
            ],
    error_logger:info_msg("~p: query: ~p ~n", [ ?MODULE, Query ]),

    Result = riakc_ts:query(C#wm_context.riakts_pid, Query),
    %% Res = riakc_ts:get(C#wm_context.riakts_pid, TableName, Key, []),

    error_logger:info_msg("~p: res: ~p ~n", [ ?MODULE, Result ]),

    {ok,{[<<"family">>,<<"series">>,<<"time">>,<<"metric">>],DataPoints}} = Result,

    DataPoints2 = lists:map(fun({_Family, _Metric, _Time, Value }) -> Value end, DataPoints),
    
%% [{<<"graphite">>,<<"one_min.perf_test.test0.metric0">>,1468958262000,0.6046603},
%% {<<"graphite">>,<<"one_min.perf_test.test0.metric0">>,1468958357000,0.6046603},
%% {<<"graphite">>,<<"one_min.perf_test.test0.metric0">>,1468958358000,0.6046603},
%% {<<"graphite">>,<<"one_min.perf_test.test0.metric0">>,1468958359000,0.6046603}]}}

%% {"metrics":[{"name":"carbon.agents.graphite-1130_ams4_prod_booking_com-store00.avgUpdateTime","startTime":1466515800,"stopTime":1466515800,"stepTime":1800,"values":[8.098284403483073e-05,8.018261347061549e-05],
%% "isAbsent":[] }]}

    Envelop = {[ {"metrics", [ {[ {"name", list_to_binary(Target)},
                              {"startTime", From},
                              {"stopTime", Until},
                              {"stepTime", 1},
                              {"values", DataPoints2 }
                           ]}
                         ]
             }
          ]},


    Json = mochijson2:encode(Envelop),
    ReqData2 = wrq:set_resp_header("Content-Type", "application/json", ReqData),
    {Json, ReqData2, C}.
            

-spec service_available(#wm_reqdata{}, #wm_context{}) -> { boolean(), #wm_reqdata{}, #wm_context{} }.
service_available(ReqData, Ctx) ->
    {true, ReqData, Ctx}.

-spec to_int(string()) -> integer().
to_int(String) ->
    { Integer, _Rest = [] } = string:to_integer(String),
    Integer.
