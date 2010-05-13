-module (console_events).

-behaviour(gen_event).
-export([init/1, handle_event/2, terminate/2]).
init(_Args) ->
    {ok, []}.
handle_event(ErrorMsg, State) ->
    io:format("***Info*** ~p~n", [ErrorMsg]),
    {ok, State}.
terminate(_Args, _State) ->
    ok.