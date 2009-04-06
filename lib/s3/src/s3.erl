%%%-------------------------------------------------------------------
%%% File    : s3.erl
%%% Author  : Andrew Birkett <andy@nobugs.org>
%%% Description : 
%%%
%%% Created : 14 Nov 2007 by Andrew Birkett <andy@nobugs.org>
%%%-------------------------------------------------------------------
-module(s3).

-behaviour(application).
-define(TIMEOUT, 15000).
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
-export([
      read_term/2, write_term/3,
	  list_buckets/0, create_bucket/1, delete_bucket/1, link_to/3, head/2, policy/1, get_objects/2,
	  list_objects/2, list_objects/1, write_object/4, write_object/5, read_object/2, read_object/3, delete_object/2 ]).
	  

start()->
    application:start(crypto),
    application:start(xmerl),
    application:start(ibrowse),
    application:start(s3).
    

start(_Type, _StartArgs) ->
    ID = get(access, "AMAZON_ACCESS_KEY_ID"),
    Secret = get(secret, "AMAZON_SECRET_ACCESS_KEY"),
    SSL = param(ssl, false),
    N = param(workers, 1),
    Timeout = param(timeout, ?TIMEOUT),
    Port = if SSL == true -> 
            ssl:start(),
            443;
        true -> 80
    end,
    ibrowse:set_dest("s3.amazonaws.com", Port, [{max_sessions, 100},{max_pipeline_size, 10}]),
    if ID == error orelse Secret == error ->
            {error, "AWS credentials not set. Pass as application parameters or as env variables."};
        true ->
            s3sup:start_link([ID, Secret, SSL, Timeout], N)
	end.
	
shutdown() ->
    application:stop(s3).
    

link_to(Bucket, Key, Expires)->
    call({link_to, Bucket, Key, Expires} ).

create_bucket (Name) -> 
    call({put, Name} ).
delete_bucket (Name) -> 
    call({delete, Name} ).
list_buckets ()      -> 
    call({listbuckets}).

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
get_object({object_info, {"Key", Key}, _, _, _}, Bucket)->
  {ok, Obj} = call({get_with_key, Bucket, Key}),
  Obj.
    
get_objects(Bucket, Options)->
    {ok, Objects} = list_objects(Bucket, Options),
    pmap(fun get_object/2,Objects, Bucket).
    
%% option example: [{delimiter, "/"},{maxkeys,10},{prefix,"/foo"}]
list_objects (Bucket, Options ) -> 
    call({list, Bucket, Options }).
list_objects (Bucket) -> 
    list_objects( Bucket, [] ).


%  Sample policy file, 
% See : http://docs.amazonwebservices.com/AmazonS3/latest/index.html?HTTPPOSTForms.html
%{obj, [{"expiration", <<"2007-04-01T12:00:00.000Z">>}, 
%  {"conditions",  [
%      {obj, [{"acl", <<"public-read">>}]}, 
%      {obj,[{"bucket", <<"mybucket">>}]}, 
%      {obj,[{"x-amz-meta-user", <<"cstar">>}]}, 
%      [<<"starts-with">>, <<"$Content-Type">>, <<"image/">>],
%      [<<"starts-with">>, <<"$key">>, <<"/user/cstar">>]
%  ]}]}.
% s3:policy will return : (helpful for building the form)
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
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {policy,Policy}).  
    
stop(_State) ->
    ok.

call(M)->
    Pid = s3sup:get_random_pid(),
    case gen_server:call(Pid, M, infinity) of
      retry -> 
          s3util:sleep(10),
          call(M);
      {timeout, _} ->
           s3util:sleep(10),
          call(M);
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
%s3:get_objects("drupal.ohmforce.com", []).

%% Lifted from http://lukego.livejournal.com/6753.html	
pmap(_F,[], _Bucket) -> [];
pmap(F,List, Bucket) ->
    Pid = self(),
    spawn(fun()->
        [spawn_worker(Pid,F,E, Bucket) || E <- List] 
    end),
        lists:map(fun(_N)->
            wait_result()
    end, lists:seq(1, length(List))).
spawn_worker(Parent, F, E, Bucket) ->
    spawn_link(fun() -> Parent ! {self(), F(E, Bucket)} end).

wait_result() ->
    receive
        {'EXIT', Reason} -> exit(Reason);
	    {_Pid,Result} -> Result
	    
    end.