-module(s3util).
-export([collapse/1, string_join/2, join/1,filter_keyset/2, string_value/1, unix_time/1, url_encode/1, sleep/1]).
-include_lib("xmerl/include/xmerl.hrl").

%% Collapse equal keys into one list
consume ({K,V}, [{K,L}|T]) -> [{K,[V|L]}|T];
consume ({K,V}, L) -> [{K,[V]}|L].

collapse (L) ->
    Raw = lists:foldl( fun consume/2, [], L ),
    Ordered = lists:reverse( Raw ),
    lists:keymap( fun lists:sort/1, 2, Ordered ).

unix_time(Expires)->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()) + Expires - 62167219200.

%%collapse_empty_test() -> ?assertMatch( [], collapse( [] ) ).
%%collapse_single_test() -> ?assertMatch( [{a,[1]}], collapse( [{a,1}] ) ).
%%collapse_many_test() -> ?assertMatch( [{a,[1,2]},{b,[3]}], collapse( [ {a,1}, {a,2}, {b,3} ] ) ).
%%collapse_order_test() -> ?assertMatch( [{a,[1,2]},{b,[3]}], collapse( [ {a,1}, {a,2}, {b,3} ] ) ).

string_join(Items, Sep) ->
    lists:flatten(lists:reverse(string_join1(Items, Sep, []))).

string_join1([], _Sep, Acc) -> Acc;
string_join1([Head | []], _Sep, Acc) ->
    [Head | Acc];
string_join1([Head | Tail], Sep, Acc) ->
    string_join1(Tail, Sep, [Sep, Head | Acc]).

join ({Key,Values}) ->
    Key ++ ":" ++ string_join(Values,",").

%%join_one_test () -> ?assertMatch( "key:one", join({"key",["one"]} ) ).
%%join_two_test () -> ?assertMatch( "key:one,two", join({"key",["one","two"]} ) ).

filter_keyset (L,KeySet) -> [ {K,V} || {K,V} <- L, lists:member(K,KeySet) ].
     

% All the text nodes in an xml doc
string_value( #xmlDocument{ content=Content } ) -> lists:flatten(lists:map( fun string_value/1, Content ));
string_value( #xmlElement{ content=Content } ) -> lists:flatten(lists:map( fun string_value/1, Content ));
string_value( #xmlText{value=Value} ) -> Value;
string_value( [Nodes]) -> lists:flatten(lists:map( fun string_value/1, Nodes ));
string_value( _ ) ->  "".
     
create_timestamp(Minutes) -> create_timestamp(calendar:now_to_universal_time(now()), Minutes).
create_timestamp({{Y, M, D}, {H, Mn, S}}, Minutes) ->
	to_str(Y) ++ "-" ++ to_str(M) ++ "-" ++ to_str(D) ++ "T" ++
	to_str(H) ++ ":" ++ to_str(Mn + Minutes)++ ":" ++ to_str(S) ++ "Z".
add_zeros(L) -> if length(L) == 1 -> [$0|L]; true -> L end.
to_str(L) -> add_zeros(integer_to_list(L)).
%%--------------------------------------------------------------------
%% @doc url_encode - lifted from the ever precious yaws_utils.erl    
%% <pre>
%% Types:
%%  String
%% </pre>
%% @spec url_encode(String) -> String
%% @end
%%--------------------------------------------------------------------
url_encode([H|T]) ->
    if
        H >= $a, $z >= H ->
            [H|url_encode(T)];
        H >= $A, $Z >= H ->
            [H|url_encode(T)];
        H >= $0, $9 >= H ->
            [H|url_encode(T)];
        H == $_; H == $.;H == $~; H == $- -> % FIXME: more..
            [H|url_encode(T)];
        true ->
            case integer_to_hex(H) of
                [X, Y] ->
                    [$%, X, Y | url_encode(T)];
                [X] ->
                    [$%, $0, X | url_encode(T)]
            end
     end;

url_encode([]) ->
    [].
integer_to_hex(I) ->
    case catch erlang:integer_to_list(I, 16) of
        {'EXIT', _} ->
            old_integer_to_hex(I);
        Int ->
            Int
    end.

old_integer_to_hex(I) when I<10 ->
    integer_to_list(I);
old_integer_to_hex(I) when I<16 ->
    [I-10+$A];
old_integer_to_hex(I) when I>=16 ->
    N = trunc(I/16),
    old_integer_to_hex(N) ++ old_integer_to_hex(I rem 16).
    
sleep(T) ->
    receive
    after T ->
       true
    end.