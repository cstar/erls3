-module(erls3_tests).

-include_lib("eunit/include/eunit.hrl").

-include("erls3.hrl").
-define(BUCKET, 'test.s3.erl');

connection_test() ->
	erls3:head(BUCKET, '').