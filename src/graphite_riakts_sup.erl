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
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
-define(CHILD(I, J),
        {I, {I, start_link, []}, J, 5000, worker, [I]}).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    %% If more than 0 restart  in one second, sup terminates children and itself
    {ok, { {one_for_one, 0, 1},
           [
            ?CHILD(graphite_riakts_indexer, permanent),
            ?CHILD(graphite_riakts_cache_warmup, transient)
           ]
         }
    }.
