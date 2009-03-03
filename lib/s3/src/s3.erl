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
	  list_buckets/0, create_bucket/1, delete_bucket/1, link_to/3, head/2, policy/1,
	  list_objects/2, list_objects/1, write_object/4, write_object/5, read_object/2, delete_object/2 ]).
	  

start()->
    crypto:start(),
    application:start(xmerl),
    inets:start(),
    application:start(s3).
    

start(_Type, _StartArgs) ->
    ID = get(access, "AMAZON_ACCESS_KEY_ID"),
    Secret = get(secret, "AMAZON_SECRET_ACCESS_KEY"),
    SSL = param(ssl, true),
    Timeout = param(timeout, nil),
    if SSL == true -> ssl:start();
        true -> ok
    end,
    if ID == error orelse Secret == error ->
            {error, "AWS credentials not set. Pass as application parameters or as env variables."};
        true ->
            N = param(workers, 5),
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

write_object(Bucket, Key, Data, ContentType)->
    write_object (Bucket, Key, Data, ContentType, []).

write_object (Bucket, Key, Data, ContentType, Metadata) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {put, Bucket, Key, Data, ContentType, Metadata}).
    
head(Bucket, Key)->
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {head, Bucket, Key}).
    
read_object (Bucket, Key) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {get, Bucket, Key}).
delete_object (Bucket, Key) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {delete, Bucket, Key}).

%% option example: [{delimiter, "/"},{maxresults,10},{prefix,"/foo"}]
list_objects (Bucket, Options ) -> 
    Pid = s3sup:get_random_pid(),
    gen_server:call(Pid, {list, Bucket, Options }).
list_objects (Bucket) -> 
    list_objects( Bucket, [] ).

% Sample policy file, 
% See : http://docs.amazonwebservices.com/AmazonS3/latest/index.html?HTTPPOSTForms.html
%{obj, [{"expiration", <<"2007-04-01T12:00:00.000Z">>}, 
%  {"conditions",  [
%      {obj, [{"acl", <<"public-read">>}]}, 
%      {obj,[{"bucket", <<"mybucket">>}]}, 
%      {obj,[{"x-amz-meta-user", <<"cstar">>}]}, 
%      [<<"starts-with">>, <<"$Content-Type">>, <<"image/">>],
%      [<<"starts-with">>, <<"$key">>, <<"/user/cstar">>]
%  ]}]}.
%% returns {AWSAccessKey, EncodedPolicy, Signature, Attributes to set in the form}
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