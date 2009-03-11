%%%-------------------------------------------------------------------
%%% File    : s3.erl
%%% Author  : Andrew Birkett <andy@nobugs.org>
%%% Description : 
%%%
%%% Created : 14 Nov 2007 by Andrew Birkett <andy@nobugs.org>
%%%-------------------------------------------------------------------
-module(s3).

-behaviour(application).

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
	  list_buckets/0, create_bucket/1, delete_bucket/1, link_to/3, head/2, policy/1, get_object/3, get_objects/2,
	  list_objects/2, list_objects/1, write_object/4, write_object/5, read_object/2, read_object/3, delete_object/2 ]).
	  
%timer:tc(s3,get_objects,["ejabberd.conf", [{maxkeys, 20}]]).
start()->
    crypto:start(),
    application:start(xmerl),
    ibrowse:start(),
    application:start(s3).
    

start(_Type, _StartArgs) ->
    
    ID = get(access, "AMAZON_ACCESS_KEY_ID"),
    Secret = get(secret, "AMAZON_SECRET_ACCESS_KEY"),
    SSL = param(ssl, true),
    N = param(workers, 5),
    Timeout = param(timeout, nil),
    Port = if SSL == true -> 
            ssl:start(),
            443;
        true -> 80
    end,
    ibrowse:set_dest("s3.amazonaws.com", Port, [{max_sessions, N*20},{max_pipeline_size, N*20}]),
    if ID == error orelse Secret == error ->
            {error, "AWS credentials not set. Pass as application parameters or as env variables."};
        true ->
          
            s3sup:start_link([ID, Secret, SSL, Timeout], N)
	end.
	
shutdown() ->
    application:stop(s3).
    

link_to(Bucket, Key, Expires)->
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {link_to, Bucket, Key, Expires} ).

create_bucket (Name) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {put, Name} ).
delete_bucket (Name) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {delete, Name} ).
list_buckets ()      -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {listbuckets}).

write_term(Bucket, Key, Term)->
    write_object (Bucket, Key,term_to_binary(Term), "application/poet", []).

write_object(Bucket, Key, Data, ContentType)->
    write_object (Bucket, Key, Data, ContentType, []).

write_object (Bucket, Key, Data, ContentType, Metadata) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {put, Bucket, Key, Data, ContentType, Metadata}).

read_term(Bucket, Key)->
    case read_object (Bucket, Key) of
        {ok, {B, H}} -> {ok, {binary_to_term(B), H}};
        E -> E
    end.

head(Bucket, Key)->
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {head, Bucket, Key}).

read_object (Bucket, Key, Etag) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {get, Bucket, Key, Etag}).
    
read_object (Bucket, Key) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {get, Bucket, Key}).

delete_object (Bucket, Key) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {delete, Bucket, Key}).

get_object (From, Bucket, Key) -> 
    Pid = s3sup:get_random_pid(),
    {ok, {Content, Headers}} = gen_server:call(Pid, {get, Bucket, Key}),
    From ! {s3object, Content}. 
    
% Gets objects in // from S3.
%% option example: [{delimiter, "/"},{maxkeys,10},{prefix,"/foo"}]
get_objects(Bucket, Options)->
    {ok, Objects} = list_objects (Bucket, Options ),
    lists:foreach(fun({object_info, {"Key", Key}, _, _, _})->
        spawn(?MODULE, get_object,[self(), Bucket, Key ])
    end, Objects),
    R = lists:foldl(fun(_N, Acc)->
        receive
            {s3object, R} -> 
                [R|Acc]
        end
    end,[],  Objects),
    {ok, R}.
    
%% option example: [{delimiter, "/"},{maxkeys,10},{prefix,"/foo"}]
list_objects (Bucket, Options ) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {list, Bucket, Options }).
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