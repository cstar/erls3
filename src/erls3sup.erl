%%%-------------------------------------------------------------------
%% @doc supervisor for gen_server to access S3
%% 
%% == Contents ==
%%
%% {@section Introduction}<br/>
%% == Introduction ==
%%  This library provides access to the web service using REST based interface.
%%  Mostly lifted and adapted from ejabberd_odbc_sup code.
%%  APIs:
%%  
%%
%% @copyright Eric Cestari 2009
%%  
%% For license information see LICENSE.txt
%% 
%% @end
%%%-------------------------------------------------------------------
-module(erls3sup).
-author("Eric Cestari <ecestari@mac.com> [http://www.cestari.info]").
%% API
-export([start_link/2,
	 init/1,
	 get_pids/0,
	 get_random_pid/0
	]).

-define(DEFAULT_START_INTERVAL, 30). % 30 seconds

start_link(Params, N) ->
    supervisor:start_link({local, ?MODULE},
			  ?MODULE, [Params, N]).

init([Params, N]) ->
    {ok, {{one_for_one, N+1, ?DEFAULT_START_INTERVAL},
	  lists:map(
	    fun(I) ->
		    {I,
		     {erls3server, start_link, [Params]},
		     transient,
		     brutal_kill,
		     worker,
		     [erls3server]}
	    end, lists:seq(1, N))}}.

get_pids() ->
    [Child ||
	{_Id, Child, _Type, _Modules} <- supervisor:which_children(?MODULE),
	Child /= undefined].

get_random_pid() ->
    Pids = get_pids(),
    lists:nth(erlang:phash(now(), length(Pids)), Pids).