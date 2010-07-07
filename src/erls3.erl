%%%-------------------------------------------------------------------
%%% File    : erls3.erl
%%% Author  : Andrew Birkett <andy@nobugs.org>
%%% Description : 
%%%
%%% Created : 14 Nov 2007 by Andrew Birkett <andy@nobugs.org>
%%%-------------------------------------------------------------------
-module(erls3).

-behaviour(application).
-define(TIMEOUT, 15000).
-define(MAX_RETRIES, 6).
%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
     start/0,
	 start/2,
	 shutdown/0,
	 stop/1
	 ]).
%% API

-compile(export_all).
-export([
    read_term/2, write_term/3,
	  list_buckets/0, create_bucket/1, 
	  delete_bucket/1, link_to/3, 
	  head/2, policy/1, get_objects/2, get_objects/3,
	  list_objects/2, list_objects/1, 
	  write_object/4, write_object/5, 
	  read_object/2, read_object/3, 
	  delete_object/2,
	  write_from_file/5,
	  read_to_file/3,
	  set_versioning/2,
	  get_versioning/1,
	  copy/4 ]).



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("xmerl/include/xmerl.hrl").

start()->
    application:start(sasl),
    application:start(crypto),
    application:start(xmerl),
    application:start(ibrowse),
    application:start(erls3).
    

start(_Type, _StartArgs) ->
    ID = get(access, "AMAZON_ACCESS_KEY_ID"),
    Secret = get(secret, "AMAZON_SECRET_ACCESS_KEY"),
    MaxSessions = param(max_sessions, 100),
    MaxPipeline = param(max_pipeline_size, 20),
    SSL = param(ssl, false),
    N = param(workers, 1),
    EventHandler = param(event_handler, none),
    random:seed(),
    Timeout = param(timeout, ?TIMEOUT),
    Port = if SSL == true -> 
            ssl:start(),
            443;
        true -> 80
    end,
    ibrowse:set_max_sessions("s3.amazonaws.com", Port,MaxSessions),
    ibrowse:set_max_pipeline_size("s3.amazonaws.com", Port, MaxPipeline),
    if ID == error orelse Secret == error ->
            {error, "AWS credentials not set. Pass as application parameters or as env variables."};
        true ->
            R = erls3sup:start_link([ID, Secret, SSL, Timeout], N),
            case EventHandler of
              none -> ok;
              _H -> gen_event:add_handler(erls3_events, EventHandler, [])
            end,
            R
	end.
	
shutdown() ->
    application:stop(erls3).

link_to(Bucket, Key, Expires)->
    call({link_to, Bucket, Key, Expires} ).

set_versioning(Bucket, Enable)->
  Switch = case Enable of 
    Ok when Ok =:= enabled 
        orelse Ok =:= on 
        orelse Ok =:= true ->
      <<"Enabled">>;
    Ko when Ko =:= disabled 
        orelse Ko =:= off 
        orelse Ko =:= false ->
      <<"Suspended">>
  end,
  Body = iolist_to_binary([<<"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"> 
  <Status>">>,Switch ,<<"</Status> 
</VersioningConfiguration>">>]),
  call({put, Bucket, "?versioning", Body, fun(_B, _Header)->
    %error_logger:info_report([{body, B}, {header, Header}])
    ok
  end}).
get_versioning(Bucket)->
  case read_object(Bucket, "?versioning") of 
    {ok, {Body, _Header}}->
      {Xml, _Rest} = xmerl_scan:string(binary_to_list(Body)),
      TextNodes  = xmerl_xpath:string("//VersioningConfiguration/Status/text()", Xml),
      RawStatus = lists:map( fun (#xmlText{value=T}) -> T end, TextNodes),
      Status = case RawStatus of 
        ["Enabled"] ->
          enabled;
        [] ->
          disabled;
        ["Suspended"] ->
          disabled
      end,
      {ok, Status};
    Error ->
      Error
  end.
  
create_bucket (Name) -> 
    call({put, Name} ).
delete_bucket (Name) -> 
    call({delete, Name} ).
list_buckets ()      -> 
    call({listbuckets}).

write_from_file(Bucket, Key, Filename, ContentType, Metadata)->
    call({from_file, Bucket, Key,Filename, ContentType, Metadata}).
    
read_to_file(Bucket, Key, Filename)->
    call({to_file, Bucket, Key, Filename}).
    
write_term(Bucket, Key, Term)->
    write_object (Bucket, Key,term_to_binary(Term), "application/poet", []).

write_object(Bucket, Key, Data, ContentType)->
    write_object (Bucket, Key, Data, ContentType, []).

write_object (Bucket, Key, Data, ContentType, Metadata) -> 
    call({put, Bucket, Key, Data, ContentType, Metadata}).

read_term(Bucket, Key)->
    case read_object (Bucket, Key) of
        {ok, {B, H}} -> {ok, {binary_to_term(B), H}};
        E -> E
    end.

copy(SrcBucket, SrcKey, DestBucket, DestKey)->
  call({copy, DestBucket, DestKey,[{"x-amz-copy-source", "/"++SrcBucket++"/" ++ SrcKey}]}).

head(Bucket, Key)->
    call({head, Bucket, Key}).

read_object (Bucket, Key, Etag) -> 
    call({get, Bucket, Key, Etag}).
    
read_object (Bucket, Key) -> 
    call({get, Bucket, Key}).
    
delete_object (Bucket, Key) -> 
    call({delete, Bucket, Key}).

    
% Gets objects in // from S3.
%% option example: [{delimiter, "/"},{maxkeys,10},{prefix,"/foo"}]
get_objects(Bucket, Options)->
  get_objects(Bucket, Options, fun(_B, Obj)-> Obj end).

% Fun = fun(Bucket, {Key, Content, Headers})
get_objects(Bucket, Options, Fun)->
    {ok, {Objects, _}} = list_objects(Bucket, Options),
    pmap(fun get_object/3,Objects, Bucket, Fun).
      
get_object({object_info, {"Key", Key}, _, _, _}, Bucket, Fun)->
  case call({get_with_key, Bucket, Key}) of
    {ok, Obj} -> Fun(Bucket, Obj);
    Error -> Error
  end.
%% option example: [{delimiter, "/"},{maxkeys,10},{prefix,"/foo"}]
list_objects (Bucket, Options ) -> 
    call({list, Bucket, Options }).
list_objects (Bucket) -> 
    list_objects( Bucket, [] ).


%  Sample policy file, 
% See : http://docs.amazonwebservices.com/AmazonS3/latest/index.html?HTTPPOSTForms.html
%{struct,  [{"expiration", <<"2007-04-01T12:00:00.000Z">>}, 
%  {"conditions",  [
%      {struct,  [{"acl", <<"public-read">>}]}, 
%      {struct, [{"bucket", <<"mybucket">>}]}, 
%      {struct, [{"x-amz-meta-user", <<"cstar">>}]}, 
%      [<<"starts-with">>, <<"$Content-Type">>, <<"image/">>],
%      [<<"starts-with">>, <<"$key">>, <<"/user/cstar">>]
%  ]}]}.
% erls3:policy will return : (helpful for building the form)
% [{"AWSAccessKeyId",<<"ACCESS">>},
% {"Policy",
%  <<"eyJleHBpcmF0aW9uIjoiMjAwNy0wNC0wMVQxMjowMDowMC4wMDBaIiwiY29uZGl0aW9ucyI6W3siYWNsIjoicHVibGljLXJlYWQi"...>>},
% {"Signature",<<"dNTpGLbdlz5KI+iQC6re8w5RnYc=">>},
% {"key",<<"/user/cstar">>},
% {"Content-Type",<<"image/">>},
% {"x-amz-meta-user",<<"cstar">>},
% {"bucket",<<"mybucket">>},
% {"acl",<<"public-read">>},
% {"file",<<>>}]
policy(Policy)->
    Pid = erls3sup:get_random_pid(),
    gen_server:call(Pid, {policy,Policy}).  
    
stop(_State) ->
    ok.


call(M)->
    call(M, 0).
call(_M, ?MAX_RETRIES)->
  {error, max_retries_reached};
  
call(M, Retries)->
    Pid = erls3sup:get_random_pid(),
    case gen_server:call(Pid, M, infinity) of
      retry -> 
          Sleep = random:uniform(trunc(math:pow(4, Retries)*100)),
          timer:sleep(Sleep),
          call(M, Retries + 1);   
     {timeout, _} ->
         Sleep = random:uniform(trunc(math:pow(4, Retries)*100)),
          timer:sleep(Sleep),
          call(M, Retries + 1);
      R -> R
  end.

%%%%% Internal API stuff %%%%%%%%%
get(Atom, Env)->
    case application:get_env(Atom) of
     {ok, Value} ->
         Value;
     undefined ->
         case os:getenv(Env) of
     	false ->
     	    error;
     	Value ->
     	    Value
         end
    end.
    
param(Name, Default)->
	case application:get_env(?MODULE, Name) of
		{ok, Value} -> Value;
		_-> Default
	end.

%% Lifted from http://lukego.livejournal.com/6753.html	
pmap(_F,[], _Bucket, _Fun) -> [];
pmap(F,List, Bucket, Fun) ->
    Pid = self(),
    spawn(fun()->
        [spawn_worker(Pid,F,E, Bucket, Fun) || E <- List] 
    end),
        lists:map(fun(_N)->
            wait_result()
    end, lists:seq(1, length(List))).
spawn_worker(Parent, F, E, Bucket, Fun) ->
    spawn_link(fun() -> Parent ! {pmap, self(), F(E, Bucket, Fun)} end).

wait_result() ->
    receive
        {'EXIT', Reason} -> exit(Reason);
	    {pmap, _Pid,Result} -> Result
    end.

notify(Event)->
  gen_event:notify(erls3_events, Event).

-ifdef(EUNIT).

erls3_test()->
  Name = test_setup(),
  {ok, Buckets} = list_buckets(),
  ?assertEqual(true, lists:member(Name, Buckets)),
  Term = {toto, [Name, <<"term">>]},
  ?assertMatch({ok, _Etag}, write_term(Name, "test", Term)),
  ?assertMatch({ok, {Term, _H}}, read_term(Name, "test")),

  ?assertEqual({error, not_found, "Not found"}, read_object(Name, "nonexistent")),

  %% Etag
  {ok, Etag} = write_object(Name, "key1", <<"data">>, "text/plain", [{"x-amz-meta-test-value", "sacha"}]),
  ?assertMatch({ok,not_modified}, read_object(Name, "key1", Etag)),
  
  {ok, {_B, H}} = read_object(Name, "key1"),
  ?assertEqual(true,lists:member({"x-amz-meta-test-value", "sacha"}, H)),
  
  %Signed link :
  ?assertMatch({ok, "200", _H, "data"}, 
      ibrowse:send_req(link_to(Name, "key1", 1000), [], get)),
  
  %% File
  file:write_file("/tmp/input", <<"foo">>),
  write_from_file(Name, "bar", "/tmp/input", "text/plain", []),
  ?assertMatch({ok, {<<"foo">>, _H}}, read_object(Name, "bar")),
  
  
  read_to_file(Name, "bar", "/tmp/result"),
  ?assertEqual({ok,<<"foo">>}, file:read_file("/tmp/result")),
  
  %copy
  copy(Name, "bar", Name, "foo"),
  ?assertMatch({ok, {<<"foo">>, _H}}, read_object(Name, "foo")),
  
  test_teardown(Name).

%listing_test()->
%  Name = test_setup(),
%  lists:map(fun(Int)->
%    ItemName = integer_to_list(Int),
%    write_object(Name, ItemName, <<"data">>, "text/plain")
%  end, lists:seq(1, 30)),
%  
%  ?assertEqual(30, lists:size(list_objects(Name,[]))),
%  
%  test_teardown(Name).

versioning_test()->
  Name = test_setup(),
  set_versioning(Name, false),
  ?assertEqual({ok, disabled}, get_versioning(Name)),
  set_versioning(Name, true),
  ?assertEqual({ok, enabled}, get_versioning(Name)),
  test_teardown(Name).

test_setup()->
  erls3:start(),
  Name = generate_bucket_name(),
  {ok, ok} = erls3:create_bucket(Name),
  Name.
  
generate_bucket_name()->
  "erls3_test_" ++ lists:flatten(lists:foldl(fun(_X,AccIn) ->
    [random:uniform(25) + 97|AccIn] end,
    [], lists:seq(1,12))).
    
test_teardown(Name)->
  erls3:delete_bucket(Name).
  
-endif.  
