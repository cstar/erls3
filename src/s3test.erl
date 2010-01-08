-module(s3test).
-export([drupal/0]).

drupal()->
    application:set_env(s3, memcached,"../../erlang/merle/ketama.servers"),
    s3:start(),
    %catch merle:flushall(),
    statistics(wall_clock),
    Bucket="drupal.ohmforce.com",
    {ok, List} = s3:list_objects(Bucket, [{maxkeys, 40}]),
    s3:get_objects(Bucket, [{maxkeys, 40}]),
    {_, Time} = statistics(wall_clock),
    statistics(wall_clock),
    s3:get_objects(Bucket, [{maxkeys, 40}]),
    {_, Time2} = statistics(wall_clock),
    io:format("Fetched : ~p elements from ~s~n", [length(List), Bucket]),
    io:format("Runtime : no cache ~p ms~n", [Time]),
    io:format("Runtime : memcached ~p ms~n", [Time2]).