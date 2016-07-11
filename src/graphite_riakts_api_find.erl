-module(graphite_riakts_api_find).

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
	  query           :: string(),
          riaksearch_pid  :: pid()
	 }).

-type search_doc() :: {bitstring(), [proplists:property()] }.
-record(search_results, {
          docs :: [search_doc()],         %% Result documents
          max_score :: float(),           %% Maximum score
          num_found :: non_neg_integer()  %% Number of results
         }).

-spec init(any()) -> {ok, #wm_context{}}.
init(_) ->
    {ok, #wm_context{}}.

-spec content_types_provided(#wm_reqdata{}, #wm_context{}) -> {[any()], #wm_reqdata{}, #wm_context{}}.
content_types_provided(ReqData, C) ->
    {[{"plain/text", to_textplain}],
     ReqData, C}.

-spec malformed_request(#wm_reqdata{}, #wm_context{}) -> {true | false, #wm_reqdata{}, #wm_context{}}.
malformed_request(ReqData, C) ->
    InitC = graphite_riakts_config:init_context(),
    { ok, RiakSearchPid } = riakc_pb_socket:start_link(InitC#context.riaksearch_ip, InitC#context.riaksearch_port),
    NewC = #wm_context{
	      query = case wrq:get_qs_value("query", ReqData) of undefined -> C#wm_context.query; V -> V end,
	      riaksearch_pid = RiakSearchPid
	     },
    case NewC of
        #wm_context{ query = undefined } -> {true, ReqData, C};
	_Otherwise                       -> {false, ReqData, NewC}
    end.
    
% main function called when receiving a request
-spec to_textplain(#wm_reqdata{}, #wm_context{}) -> {binary(), #wm_reqdata{}, #wm_context{}}.
to_textplain(ReqData, C) ->
    Query = C#wm_context.query,
    error_logger:info_msg("~p: got find request query: ~p ~n", [ ?MODULE, Query ]),

    {ok, RegexDotStarEnd} = re:compile("\\.\\*$"),
    QueryKind = case re:run(Query, RegexDotStarEnd) of
	      {match, _ } ->
		  Query2 = re:replace(Query, RegexDotStarEnd, ""),
		  case looks_like_regex(Query2) of
		      false ->
		        % query is not a regex, it justs ends with .*
		        % so we are doing a simple tree walking
			  { tree_walking, Query2 };
		      true ->
			  regex
		         % query is a regex, ask Riak Search
		  end;
	      nomatch ->
		  case looks_like_regex(Query) of
		      false -> simple_match
	          % query is not a regex, doesn't end with .*, it's a simple
	          % metricname match, ask memory cache or KV
		      ;
		      true ->
			  regex
		  end
	  end,

    {Result, NewReqData} = perform_search(QueryKind, ReqData, C),
    {Result, NewReqData, C}.
	    

-spec looks_like_regex(string()) -> boolean().
looks_like_regex(Query) ->
    looks_like_regex(Query, _Offset = 0).
-spec looks_like_regex(string(), non_neg_integer()) -> boolean().
looks_like_regex(Query, Offset) ->
    {ok, ReRegexChars} = re:compile("(\\\\*)[{[*]"),
    case re:run(Query, ReRegexChars, [ {offset, Offset} ]) of
	nomatch -> false;
	{match, [ { WholeMatchOffset, WholeMatchLength },
		  { _BackslashMatchOffset, BackslashMatchLength } ]} ->
            % first match is the whole regex matching, the second one is the (\\\\*) part
	    case  BackslashMatchLength rem 2 of
		0 -> true; % there were an even number of (or zero) backslash.
		1 -> looks_like_regex(Query, WholeMatchOffset+WholeMatchLength)
	    end
    end.


  %% % if simple metric eq
  %%   % remove potential trailing dot
  %%   {ok, Regex} = re:compile("\\.$"),
  %%   MetricName = re:replace(Query, Regex, ""),
  %%   case cache:get(metric_names_cache, Query) of

-spec perform_search( {tree_walking, string()} | regex | simple_match,
                      #wm_reqdata{}, #wm_context{}) -> { iodata(), #wm_reqdata{} }.

-define(MAX_RESULTS, 1000).
perform_search({tree_walking, _ParentMetricName}, ReqData, C) ->
    error_logger:info_msg("~p: query looks like a simple tree walking ~n", [ ?MODULE ]),

    % TODO : replace by tree walking from CRDTs represented tree
    Query = C#wm_context.query,
    QueryParts = string:tokens(Query, "."),
    Level = length(QueryParts),
    LevelBin = integer_to_binary(Level),
    {ok, Regex} = re:compile("\\*"),
    SearchQuery0 = re:replace(string:join(QueryParts, "\\."), Regex, ".*", [global]),
    SearchQuery1 = list_to_binary(SearchQuery0),
    SearchQuery = <<"name_s:/", SearchQuery1/binary, "/ AND level_i:", LevelBin/binary>>,
    error_logger:info_msg("~p: SearchQuery :  ~p ~n", [ ?MODULE, SearchQuery ]),

    case riakc_pb_socket:search(C#wm_context.riaksearch_pid, <<"metric_names_index">>,
				SearchQuery, [ {fl, [<<"name_s">>, <<"type_s">>]}, {rows, ?MAX_RESULTS}, { start, 0}]) of
	{ok, Results} ->
	    NumFound = Results#search_results.num_found,
	    error_logger:info_msg("~p: found ~p results ~n", [ ?MODULE, NumFound ]),
	    Documents = Results#search_results.docs,

	    Matches = build_matches(Documents),
	    Result = {[ {<<"name">>, list_to_binary(Query)},
			{<<"matches">>, Matches} ]},
	    Json = iolist_to_binary(mochijson2:encode(Result)),

	    error_logger:info_msg("~p: JSON  ~p ~n", [ ?MODULE, Json ]),
	    ReqData2 = wrq:set_resp_header("Content-Type", "application/json", ReqData),
	    {Json, ReqData2};
	{error, _} = Error ->
	    {Error, ReqData}
    end;
perform_search(regex, ReqData, C) ->
    error_logger:info_msg("~p: query looks like a regex ~n", [ ?MODULE ]),
    
    % query looks like a regexp, let's split the parts between dots, compute
    % the level we need, replace * by .*, and original dots by \. so that we
    % have a PCRE regex we can pass to solr (riak search). Run the search, and
    % format the output
    Query = C#wm_context.query,
    QueryParts = string:tokens(Query, "."),
    Level = length(QueryParts),
    LevelBin = integer_to_binary(Level),
    {ok, Regex} = re:compile("\\*"),
    SearchQuery0 = re:replace(string:join(QueryParts, "\\."), Regex, ".*", [global]),
    SearchQuery1 = list_to_binary(SearchQuery0),
    SearchQuery = <<"name_s:/", SearchQuery1/binary, "/ AND level_i:", LevelBin/binary>>,
    error_logger:info_msg("~p: SearchQuery :  ~p ~n", [ ?MODULE, SearchQuery ]),

    %% todo : if the original query has no star (*), do a get on a key
    %% <level>-<node_name>, to check for exact match first. We need an
    %% additional bucket for that. Also a memory cache

    %% % try exact query from memory cache
    %% case cache:get(metric_names_cache, Query) of
    %% 	undefined ->
    %% 	    % try exact query from Riak KV
    %% 	    case riakc_pb_socket:get(C#context.riakkv_pid, C#context.bucket_name, Query) of
    %% 		{ ok, Obj } -> Value = binary_to_term(riakc_obj:get_value(Obj)),
    %% 			       error_logger:info_msg("~p: exact query in Riak KV: ~p ~n", [ ?MODULE, Value ]);
    %% 		_Otherwise -> 
    %% 		    {ok, Results} = riakc_pb_socket:search(C#wm_context.riaksearch_pid, <<"metric_names_index">>,
    %% 							   <<"metric_name_s:*">>, [ {rows, ?MAX_RESULTS}, { start, 0}]),
    %% 	    end;
    %% 	<<"">> -> error_logger:info_msg("~p: exact match metric in memory cache ~n", [ ?MODULE ]);
    %% 	<<"node">> -> error_logger:info_msg("~p: exact match node in memory cache ~n", [ ?MODULE ])
    %% end

    case riakc_pb_socket:search(C#wm_context.riaksearch_pid, <<"metric_names_index">>,
				SearchQuery, [ {fl, [<<"name_s">>, <<"type_s">>]}, {rows, ?MAX_RESULTS}, { start, 0}]) of
	{ok, Results} ->
	    NumFound = Results#search_results.num_found,
	    error_logger:info_msg("~p: found ~p results ~n", [ ?MODULE, NumFound ]),
	    Documents = Results#search_results.docs,

	    Matches = build_matches(Documents),
	    Result = {[ {<<"name">>, list_to_binary(Query)},
			{<<"matches">>, Matches} ]},
	    Json = iolist_to_binary(mochijson2:encode(Result)),

	    error_logger:info_msg("~p: JSON  ~p ~n", [ ?MODULE, Json ]),
	    ReqData2 = wrq:set_resp_header("Content-Type", "application/json", ReqData),
	    {Json, ReqData2};
	{error, _} = Error ->
	    {Error, ReqData}
    end.

-spec service_available(#wm_reqdata{}, #wm_context{}) -> { boolean(), #wm_reqdata{}, #wm_context{} }.
service_available(ReqData, Ctx) ->
    {true, ReqData, Ctx}.

-type match() :: {[proplists:property()]}.
-spec build_matches( [search_doc()] ) -> [match()].
-spec build_matches( [search_doc()], [match()] ) -> [match()].
build_matches(Documents) ->
    build_matches(Documents, []).
build_matches([], Acc) -> Acc;
build_matches([Document|Tail], Acc) ->
    { <<"metric_names_index">>, Value } = Document,
    Path = proplists:get_value(<<"name_s">>, Value),
    IsLeaf = case proplists:get_value(<<"type_s">>, Value) of
		 <<"branch">> -> false;
		 <<"leaf">> -> true
	     end,
    Element = {[ {path, Path}, {isLeaf, IsLeaf} ]},
    build_matches(Tail, [Element| Acc]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

looks_like_regex_test_() ->
    [ ?_assertEqual(looks_like_regex(""), false),
      ?_assertEqual(looks_like_regex("foo.bar"), false),
      ?_assertEqual(looks_like_regex("foo.[b]ar"), true),
      ?_assertEqual(looks_like_regex("foo.[bar"), true),
      ?_assertEqual(looks_like_regex("foo.{bar"), true),
      ?_assertEqual(looks_like_regex("foo.bar*"), true),
      ?_assertEqual(looks_like_regex("foo.ba*r"), true),
      ?_assertEqual(looks_like_regex("foo.ba\*r"), true), % remember, backslash needs double escaping in Erlang strings, so "\*" is *
      ?_assertEqual(looks_like_regex("foo.ba\\*r"), false),
      ?_assertEqual(looks_like_regex("foo.ba\\\\*r"), true),
      ?_assertEqual(looks_like_regex("foo.ba\\\\\\*r"), false),
      ?_assertEqual(looks_like_regex("foo.ba\\\\\\\\*r"), true)
    ].
-endif.
