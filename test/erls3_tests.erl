-module(erls3_tests).

-include_lib("eunit/include/eunit.hrl").

-include("erls3.hrl").
-define(BUCKET, "test.s3.erl").

start() ->
	erls3:start(),
	erls3:clean_bucket(?BUCKET),
	{ok, ok} = erls3:create_bucket(?BUCKET).

%connection_test() ->
%	erls3:head(?BUCKET, "").

write_test() ->
	%{error, not_found, _} = erls3:write_object("this is not a bucket", "test.txt", "hello world", "plain/text", [{"x-amz-beuha", "aussi"}]),
	{ok, Hash} = erls3:write_object(?BUCKET, "test.txt", "hello world", "plain/text", [{"x-amz-beuha", "aussi"}]).
	
stop() -> erls3:clean_bucket(?BUCKET).
	