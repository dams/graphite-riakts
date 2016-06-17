%%%-------------------------------------------------------------------
%% @doc graphite_riakts top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(graphite_riakts_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%% Helper macro for declaring children of supervisor
-define(CHILD(I),
        {I, {I, start_link, []}, 
         permanent, 5000, worker, [I]}).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {ok, { {one_for_one, 0, 1},
	   [
            ?CHILD(graphite_riakts_indexer)
	   ]} }.
