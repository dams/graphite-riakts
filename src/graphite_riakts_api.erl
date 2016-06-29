%%%-------------------------------------------------------------------
%% @doc graphite_riakts API implementation
%% @end
%%%-------------------------------------------------------------------

-module(graphite_riakts_api).

-export([init/0]).

init() ->
    [webmachine_router:add_route(R) 
     || R <- [ {["metrics", "find"],
		graphite_riakts_api_find, []}
	     ]],
    ok.
