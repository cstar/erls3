%%%-------------------------------------------------------------------
%%% File    : erls3.erl
%%% Author  : Andrew Birkett <andy@nobugs.org>
%%% Description : 
%%%
%%% Created : 14 Nov 2007 by Andrew Birkett <andy@nobugs.org>
%%%-------------------------------------------------------------------
-module(erls3server).

-behaviour(gen_server).
-define(TIMEOUT, 400000 ).
%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
	start_link/1,
	stop/0
	]).

-define(DEBUG(T, P), error_logger:info_msg(T, P)).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
	 terminate/2, code_change/3]).
	 
-include_lib("kernel/include/file.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include("../include/erls3.hrl").

-record(state, {ssl,access_key, secret_key, pending, timeout=?TIMEOUT}).
-record(request, {pid, callback, started, opts, code, to_file=false, headers=[], content=[]}).
%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% @doc Starts the server.
%% @spec start_link() -> {ok, pid()} | {error, Reason}
%% @end
%%--------------------------------------------------------------------
start_link([Access, Secret, SSL, Timeout]) ->
    gen_server:start_link(?MODULE, [Access, Secret, SSL, Timeout], []).

%%--------------------------------------------------------------------
%% @doc Stops the server.
%% @spec stop() -> ok
%% @end
%%--------------------------------------------------------------------
stop() ->
    gen_server:cast(?MODULE, stop).

init([Access, Secret, SSL, nil]) ->
    {ok, #state{ssl = SSL,access_key=Access, secret_key=Secret,pending=gb_trees:empty()}};
init([Access, Secret, SSL, Timeout]) ->
    {ok, #state{ssl = SSL,access_key=Access, secret_key=Secret, timeout=Timeout, pending=gb_trees:empty()}}.



%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------

% Bucket operations
handle_call({listbuckets}, From, State) ->
    genericRequest(From, State, get, "", "", [],[], <<>>, "", fun xmlToBuckets/2 );

handle_call({ put, Bucket }, From, State) ->
    genericRequest(From, State, put, Bucket, "", [], [], <<>>, "", fun(_,_) -> ok end);

handle_call({ put, Bucket, Key, Command,  Fun}, From, State)  when is_function(Fun) ->
    genericRequest(From, State, put, Bucket, Key, [], [], Command, "", Fun);

handle_call({delete, Bucket }, From, State) ->
    genericRequest(From, State, delete, Bucket, "", [], [],<<>>, "", fun(_,_) -> ok end);
        
%object operations no cache
handle_call({put, Bucket, Key, Content, ContentType, AdditionalHeaders}, From, State) ->
    genericRequest(From, State, put, Bucket, Key, [], AdditionalHeaders, Content, ContentType, fun(_X, Headers) -> 
            {value,{"ETag",ETag}} = lists:keysearch( "ETag", 1, Headers ),
            ETag
        end);

handle_call({copy, Bucket, Key, AdditionalHeaders}, From, State) ->
    genericRequest(From, State, put, Bucket, Key, [], AdditionalHeaders, <<>>, nil,  fun parseCopy/2 );

handle_call({ list, Bucket, Options }, From, State) ->
    Headers = lists:map( fun option_to_param/1, Options ),
    genericRequest(From, State, get, Bucket, "", Headers, [], <<>>, "",  fun parseBucketListXml/2 );

handle_call({to_file, Bucket, Key, Filename}, From, State)->
    genericRequest(From, State, get,  Bucket, Key, [], [], <<>>, "", 
            fun(_B, H) -> {Key,Filename, H} 
    end, Filename);

handle_call({from_file, Bucket, Key,Filename, ContentType, AdditionalHeaders}, From, State)->
    case get_fd(Filename, [read, binary, read_ahead]) of
        {error, E} ->
            {reply,{error, E}, State};
        Fd ->
            Fun = fun(read)->
                    case file:read(Fd, 65536) of
                        {ok, R} ->
                            {ok, R, read};
                        O->O
                    end;
                (size)->
                   {ok, #file_info{size=Size}} = file:read_file_info(Filename),
                   Size
            end,
            genericRequest(From, State, put,  Bucket, Key, [], AdditionalHeaders, {Fun, read}, ContentType, 
                    fun(_X, Headers) -> 
                        {value,{"ETag",ETag}} = lists:keysearch( "ETag", 1, Headers ),
                        ETag
            end)
    end;
        

handle_call({ get, Bucket, Key, Etag}, From, State) ->
    genericRequest(From, State, get,  Bucket, Key, [], [{"If-None-Match", Etag}], <<>>, "", fun(B, H) -> {B,H} end);
    
handle_call({ get, Bucket, Key}, From, State) ->
    genericRequest(From, State, get,  Bucket, Key, [], [], <<>>, "", fun(B, H) -> {B,H} end);
    
handle_call({ get_with_key, Bucket, Key}, From, State) ->
    genericRequest(From, State, get,  Bucket, Key, [], [], <<>>, "", fun(B, H) -> {Key, B,H} end);
handle_call({ head, Bucket, Key }, From, State) ->
    genericRequest(From, State, head,  Bucket, Key, [], [], <<>>, "", fun(_, H) -> H end);

handle_call({delete, Bucket, Key }, From, State) ->
    genericRequest(From, State, delete, Bucket, Key, [], [], <<>>, "", fun(_,_) -> ok end);

handle_call({link_to, Bucket, Key, Expires}, _From, #state{access_key=Access, secret_key=Secret, ssl=SSL}=State)->
    Exp = integer_to_list(erls3util:unix_time(Expires)),
    QueryParams = [{"AWSAccessKeyId", Access},{"Expires", Exp}],
    Url = buildUrl(Bucket,Key,QueryParams, SSL),
    Signature = erls3util:url_encode(
                sign( Secret,
		        stringToSign( "GET", "", 
				    Exp, Bucket, Key, "" ))),
    {reply, Url++"&Signature="++Signature, State};
    
handle_call({policy, {struct, Attrs}=Policy}, _From, #state{access_key=Access, secret_key=Secret}=State)->
  Conditions = proplists:get_value(conditions, Attrs, []),
  Attributes = 
    lists:foldl(fun([<<"content-length-range">>, Min,Max], Acc) when is_integer(Min) andalso is_integer(Max) ->
                        Acc; %% ignore not used for building the form 
                  ([_, DolName, V], Acc) ->
                    [$$|Name] = binary_to_list(DolName),
                    [{Name, V}|Acc];
                   ({struct,[{Name, V}]}, Acc) ->
                     [{Name, V}| Acc]
      end, [], Conditions),
      %?DEBUG("Policy = ~p",[mochijson2:encode(Policy)]),
  Enc =base64:encode(
        list_to_binary(mochijson2:encode(Policy))),
  Signature = base64:encode(crypto:sha_mac(Secret, Enc)),
  {reply, [{"AWSAccessKeyId",list_to_binary(Access)},
           {"Policy", Enc}, 
           {"Signature", Signature}
          |Attributes] ++
          [{"file", <<"">>}], 
          State}.
  
%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({ibrowse_async_headers,RequestId,Code,Headers },State = #state{pending=P}) ->
    %?DEBUG("******* Response :  ~p~n", [Response]),
	case gb_trees:lookup(RequestId,P) of
		{value,#request{pid = Pid }=R} -> 
		    {ICode, []} = string:to_integer(Code),
		    if ICode >= 500 ->
		        gen_server:reply(Pid,retry),
		        {noreply,State#state{pending=gb_trees:delete(RequestId, P)}};
		    true ->
			    {noreply,State#state{pending=gb_trees:enter(RequestId,R#request{code = Code, headers=Headers},P)}}
			end;
		none -> 
		    {noreply,State}
			%% the requestid isn't here, probably the request was deleted after a timeout
	end;
handle_info({ibrowse_async_response,_RequestId,{chunk_start, _N} },State) ->
    {noreply, State};
handle_info({ibrowse_async_response,_RequestId,chunk_end },State) ->
    {noreply, State};	
    
handle_info({ibrowse_async_response,RequestId,Body },State = #state{pending=P}) when is_binary(Body)->
	case gb_trees:lookup(RequestId,P) of
		{value,#request{content=Content, to_file=false}=R} -> 
			{noreply,State#state{pending=gb_trees:enter(RequestId,R#request{content=[Content,Body]}, P)}};
		{value,#request{to_file=Fd}} ->
		    case file:write(Fd,Body) of
	            ok ->
	                ok;
	            {error, _Reason} ->
	                error
	                %{error, {file_write_error, Reason}} %%TODO use error and cancel transfer.
            end,
		    {noreply,State};
		none -> {noreply,State}
			%% the requestid isn't here, probably the request was deleted after a timeout
	end;
handle_info({ibrowse_async_response_end,RequestId}, State = #state{pending=P})->
    case gb_trees:lookup(RequestId,P) of
		{value,#request{started=Started, to_file=Fd, opts=Opts}=R} -> 
		    if Fd /= false ->
		        file:close(Fd);
		    true ->
		        ok
		    end,
		    handle_http_response(R),
		    erls3:notify([{time, timer:now_diff(now(), Started)/1000 }| Opts]),
			  {noreply,State#state{pending=gb_trees:delete(RequestId, P)}};
		none -> {noreply,State}
			%% the requestid isn't here, probably the request was deleted after a timeout
	end;
handle_info({ibrowse_async_response,RequestId,{error,Error}}, State = #state{pending=P}) ->
    case gb_trees:lookup(RequestId,P) of
		{value,#request{pid=Pid}} -> 
		    error_logger:warning_msg("Warning query failed (retrying) : ~p~n", [Error]),
		    gen_server:reply(Pid, retry),
		    {noreply,State#state{pending=gb_trees:delete(RequestId, P)}};
		none -> {noreply,State}
			%% the requestid isn't here, probably the request was deleted after a timeout
	end;	

handle_info(_Info, State) ->
    %io:format("Unkown Info : ~p~n", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
option_to_param( { prefix, X } ) -> 
    { "prefix", X };
option_to_param( { maxkeys, X } ) -> 
    { "max-keys", integer_to_list(X) };
option_to_param( { delimiter, X } ) -> 
    { "delimiter", X }.

handle_http_response(#request{pid=From, code="304"})->
    gen_server:reply(From, {ok, not_modified});
handle_http_response(#request{pid=From, code="404"})-> 
    gen_server:reply(From, {error, not_found, "Not found"});
handle_http_response(#request{pid=From, callback=CallBack, code=Code, headers=Headers, content=Content})
                    when Code =:= "200" orelse Code =:= "204"->
    gen_server:reply(From,{ok, CallBack(iolist_to_binary(Content), Headers)});
    
handle_http_response(#request{pid=From, content=Content})->
    {Xml, _Rest} = xmerl_scan:string(
            binary_to_list(iolist_to_binary(Content))),
    [#xmlText{value=ErrorCode}]    = xmerl_xpath:string("//Error/Code/text()", Xml),
    [#xmlText{value=ErrorMessage}] = xmerl_xpath:string("//Error/Message/text()", Xml),
    gen_server:reply(From,{error, ErrorCode, ErrorMessage}).
    
    
isAmzHeader( Header ) -> lists:prefix("x-amz-", Header).

canonicalizedAmzHeaders( AllHeaders ) ->
    AmzHeaders = [ {string:to_lower(K),V} || {K,V} <- AllHeaders, isAmzHeader(K) ],
    Strings = lists:map( 
		fun erls3util:join/1, 
		erls3util:collapse( 
		  lists:keysort(1, AmzHeaders) ) ),
    erls3util:string_join( lists:map( fun (S) -> S ++ "\n" end, Strings), "").
    
canonicalizedResource ( "", "" ) -> "/";
canonicalizedResource ( Bucket, "" ) -> "/" ++ Bucket ++ "/";
canonicalizedResource ( Bucket, Path ) -> "/" ++ Bucket ++ "/" ++ Path.

stringToSign ( Verb, nil, Date, Bucket, Path, OriginalHeaders ) ->
  stringToSign ( Verb, "", Date, Bucket, Path, OriginalHeaders );
stringToSign ( Verb, ContentType, Date, Bucket, Path, OriginalHeaders ) ->
    Parts = [ Verb, proplists:get_value("Content-MD5", OriginalHeaders, ""), ContentType, Date, canonicalizedAmzHeaders(OriginalHeaders)],
    erls3util:string_join( Parts, "\n") ++ canonicalizedResource(Bucket, Path).
    
sign (Key,Data) ->
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

queryParams( [] ) -> "";
queryParams( L ) -> 
    Stringify = fun ({K,V}) -> K ++ "=" ++ V end,
    "?" ++ erls3util:string_join( lists:sort(lists:map( Stringify, L )), "&" ).
    
buildUrl(Bucket,Path,QueryParams, false) -> 
    "http://s3.amazonaws.com" ++ canonicalizedResource(Bucket,Path) ++ queryParams(QueryParams);

buildUrl(Bucket,Path,QueryParams, true) -> 
    "https://s3.amazonaws.com"++ canonicalizedResource(Bucket,Path) ++ queryParams(QueryParams).

buildContentHeaders( <<>>, _ContentType, AdditionalHeaders ) -> AdditionalHeaders;
buildContentHeaders( {_F, read} = C, ContentType, AdditionalHeaders ) -> 
    [ {"Content-Type", ContentType}, %%TODO no md5 when upstreaming for the moment.
      {"Content-Length", content_length(C)}
     | AdditionalHeaders];
buildContentHeaders( Contents, ContentType, AdditionalHeaders ) -> 
    ContentMD5 = crypto:md5(Contents),
    [{"Content-MD5", binary_to_list(base64:encode(ContentMD5))},
     {"Content-Type", ContentType}
     | AdditionalHeaders].


buildOptions(<<>>, nil,SSL)->
    [{is_ssl, SSL}, {ssl_options, []}, {response_format, binary}];
buildOptions(<<>>, _ContentType, SSL)->
     [{stream_to, self()},{is_ssl, SSL}, 
      {ssl_options, []},{response_format, binary}];
buildOptions(Content, ContentType,SSL)->
    [{content_length, content_length(Content)},
    {content_type, ContentType}, {response_format, binary},
    {is_ssl, SSL},{ssl_options, []},{stream_to, self()}].

content_length({F, read})->
    integer_to_list(F(size)); 
content_length(Content) when is_binary(Content)->
    integer_to_list(size(Content));
content_length(Content) when is_list(Content)->
    integer_to_list(length(Content)).
    
genericRequest(From, State, 
            Method, Bucket, Path, QueryParams, AdditionalHeaders,Contents, ContentType, Callback) ->
    genericRequest(From, State, 
            Method, Bucket, Path, QueryParams, AdditionalHeaders,Contents, ContentType, Callback, false).
            
genericRequest(From, #state{ssl=SSL, access_key=AKI, secret_key=SAK, timeout=Timeout, pending=P }=State, 
            Method, Bucket, Path, QueryParams, AdditionalHeaders,Contents, ContentType, Callback, ToFile ) ->
    Date = httpd_util:rfc1123_date(),
    MethodString = string:to_upper( atom_to_list(Method) ),
    Url = buildUrl(Bucket,Path,QueryParams, SSL),
    OriginalHeaders = buildContentHeaders( Contents, ContentType, AdditionalHeaders ),
    Signature = sign( SAK,
		      stringToSign( MethodString,  ContentType, 
				    Date, Bucket, Path, OriginalHeaders )),
   
    Headers = [ {"Authorization","AWS " ++ AKI ++ ":" ++ Signature },
		        {"Host", "s3.amazonaws.com" },
		        {"Date", Date } 
	            | OriginalHeaders ],
    Options = buildOptions(Contents, ContentType, SSL), 
    Params = [{method, Method}, {bucket, Bucket}, {key, Path}, {options, Options}],
    %error_logger:info_report([{method, Method}, {bucket, Bucket}, {key, Path}, {options, Options}]),
    case get_fd(ToFile, [write, delayed_write, raw]) of
        {error, R} ->
            {reply, {error, R, "Error Occured"}, State};
        Fd ->
            case ibrowse:send_req(Url, Headers,  Method, Contents,Options, Timeout) of
                {ibrowse_req_id,RequestId} ->
                    Pendings = gb_trees:insert(RequestId,#request{pid=From, to_file=Fd, opts=Params, started=now(),  callback=Callback},P),
                    {noreply, State#state{pending=Pendings}};
                {ok, "200", H, B}->
                   {reply, {ok, H, Callback(B, H)}, State};
                {ok, Code, _H, _B} ->
                   {reply, {error, return_code, Code}, State};
                {error,E} when E =:= retry_later orelse E =:= conn_failed ->
                    {reply, retry, State};
                {error, E} ->
                    {reply, {error, E, "Error Occured"}, State}
            end
    end.

get_fd(false, _Opts)->false;
get_fd(FileName, Opts)->
    case file:open(FileName, Opts) of
	{ok, Fd} ->
	    Fd;
	{error, Reason} ->
	    {error,  Reason}
	end.

parseCopy(XmlDoc, _H)->
  {Xml, _Rest} = xmerl_scan:string(binary_to_list(XmlDoc)),
  [ EtagS|_] = xmerl_xpath:string("/CopyObjectResult/ETag/text()", Xml),
  [LastModified|_] = xmerl_xpath:string("/CopyObjectResult/LastModified/text()", Xml),
   "\"" ++ Etag = EtagS#xmlText.value,
  [{etag,Etag}, {lastmodified,LastModified#xmlText.value }].
  
parseBucketListXml (XmlDoc, _H) ->
    %?DEBUG("******* Response :  ~p~n", [XmlDoc]),
    {Xml, _Rest} = xmerl_scan:string(binary_to_list(XmlDoc)),
    ContentNodes = xmerl_xpath:string("/ListBucketResult/Contents", Xml),

    GetObjectAttribute = fun (Node,Attribute) -> 
		      [Child] = xmerl_xpath:string( Attribute, Node ),
		      {Attribute, erls3util:string_value( Child )}
	      end,
	      
    NodeToRecord = fun (Node) ->
      {_, Key } = GetObjectAttribute(Node,"Key"),
      {_, LastModified } = GetObjectAttribute(Node,"LastModified"),
      {_, ETag } = GetObjectAttribute(Node,"ETag"),
      {_, Size } = GetObjectAttribute(Node,"Size"),
			#object_info{ 
			 key =          Key,
			 lastmodified = LastModified,
			 etag =         ETag,
			 size =         Size
			 }
		   end,
    Prefixes       = xmerl_xpath:string("//CommonPrefixes/Prefix/text()", Xml),
    {lists:map( NodeToRecord, ContentNodes ), lists:map(fun (#xmlText{value=T}) -> T end, Prefixes)}.


xmlToBuckets( Body, _H) ->
    {Xml, _Rest} = xmerl_scan:string(binary_to_list(Body)),
    TextNodes       = xmerl_xpath:string("//Bucket/Name/text()", Xml),
    lists:map( fun (#xmlText{value=T}) -> T end, TextNodes).

