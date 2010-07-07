-module(erls3_tests).

-include_lib("eunit/include/eunit.hrl").

-include("erls3.hrl").
-define(BUCKET, "test.s3.erl").

start() ->
	erls3:start(),
	{ok, ok} = erls3:create_bucket(?BUCKET).

connection_test() ->
	erls3:head(?BUCKET, "").

write_test() ->
	erls3:write_object(?BUCKET, "test.txt", "hello world", "plain/text", [{"x-amz-beuha", "aussi"}]).