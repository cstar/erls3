%%%-------------------------------------------------------------------
%%% File    : s3.erl
%%% Author  : Andrew Birkett <andy@nobugs.org>
%%% Description : 
%%%
%%% Created : 14 Nov 2007 by Andrew Birkett <andy@nobugs.org>
%%%-------------------------------------------------------------------
-module(s3server).

-behaviour(gen_server).
-define(TIMEOUT, 20000).
%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
	start_link/1,
	stop/0
	]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
	 terminate/2, code_change/3]).

-include_lib("xmerl/include/xmerl.hrl").
-include("../include/s3.hrl").

-record(state, {ssl,access_key, secret_key, pending, timeout=?TIMEOUT}).

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
    {ok, #state{ssl = SSL,access_key=Access, secret_key=Secret, pending=gb_trees:empty()}};
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
    genericRequest(From, State, get, "", "", [], <<>>, "", fun xmlToBuckets/2 );

handle_call({ put, Bucket }, From, State) ->
    genericRequest(From, State, put, Bucket, "", [],  <<>>, "", fun(_,_) -> ok end);

handle_call({delete, Bucket }, From, State) ->
    genericRequest(From, State, delete, Bucket, "", [], <<>>, "", fun(_,_) -> ok end);

% Object operations
handle_call({put, Bucket, Key, Content, ContentType }, From, State) ->
    genericRequest(From, State, put, Bucket, Key, [], Content, ContentType, fun(_X, Headers) -> 
            {value,{"etag",ETag}} = lists:keysearch( "etag", 1, Headers ),
            {ok, ETag}
        end);
    

handle_call({ list, Bucket, Options }, From, State) ->
    Headers = lists:map( fun option_to_param/1, Options ),
    genericRequest(From, State, get, Bucket, "", Headers,<<>>, "",  fun parseBucketListXml/2 );

handle_call({ get, Bucket, Key }, From, State) ->
    genericRequest(From, State, get,  Bucket, Key, [], <<>>, "", fun(B, H) -> {B,H} end);

handle_call({delete, Bucket, Key }, From, State) ->
    genericRequest(From, State, delete, Bucket, Key, [], <<>>, "", fun(_,_) -> ok end).


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
handle_info({http,{RequestId,Response}},State = #state{pending=P}) ->
    %?DEBUG("******* Response :  ~p~n", [Response]),
	case gb_trees:lookup(RequestId,P) of
		{value,Request} -> handle_http_response(Response,Request, State),
						 {noreply,State#state{pending=gb_trees:delete(RequestId,P)}};
		none -> {noreply,State}
				%% the requestid isn't here, probably the request was deleted after a timeout
	end;

handle_info(_Info, State) ->
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

s3Host () ->
    "s3.amazonaws.com".

option_to_param( { prefix, X } ) -> 
    { "prefix", X };
option_to_param( { maxkeys, X } ) -> 
    { "max-keys", integer_to_list(X) };
option_to_param( { delimiter, X } ) -> 
    { "delimiter", X }.



handle_http_response(HttpResponse,{From,Callback}, _State)->
    case HttpResponse of 
        {{_HttpVersion, StatusCode, _ErrorMessage}, Headers, Body } ->
    	    %?DEBUG("******* Status ~p ~n", [StatusCode]),
            
            case StatusCode of
    	        200 ->
	            gen_server:reply(From,{ok, Callback(Body, Headers)});
	        _ ->
	            {Xml, _Rest} = xmerl_scan:string(binary_to_list(Body)),
    		    [#xmlText{value=ErrorCode}]    = xmerl_xpath:string("//Error/Code/text()", Xml),
    		    [#xmlText{value=ErrorMessage}] = xmerl_xpath:string("//Error/Message/text()", Xml),
    	        gen_server:reply(From,{error, ErrorCode, ErrorMessage})
            end;
        {error, ErrorMessage} ->  gen_server:reply(From,{error, http_error, ErrorMessage})
	    %case ErrorMessage of 
		%    Error when  Error == timeout ->
    	%    	    %?DEBUG("URL Timedout, retrying~n", []),
    	%    	    erlsdb_util:sleep(1000),
		%            rest_request(From,Action, Params, RequestOp, State);
		%_ ->
    	%           gen_server:reply(From,{error, http_error, ErrorMessage})
	    %end
    end.


isAmzHeader( Header ) -> lists:prefix("x-amz-", Header).

canonicalizedAmzHeaders( AllHeaders ) ->
    AmzHeaders = [ {string:to_lower(K),V} || {K,V} <- AllHeaders, isAmzHeader(K) ],
    Strings = lists:map( 
		fun s3util:join/1, 
		s3util:collapse( 
		  lists:keysort(1, AmzHeaders) ) ),
    s3util:string_join( lists:map( fun (S) -> S ++ "\n" end, Strings), "").
    
canonicalizedResource ( "", "" ) -> "/";
canonicalizedResource ( Bucket, "" ) -> "/" ++ Bucket ++ "/";
canonicalizedResource ( Bucket, Path ) -> "/" ++ Bucket ++ "/" ++ Path.

stringToSign ( Verb, ContentMD5, ContentType, Date, Bucket, Path, OriginalHeaders ) ->
    Parts = [ Verb, ContentMD5, ContentType, Date, canonicalizedAmzHeaders(OriginalHeaders)],
    s3util:string_join( Parts, "\n") ++ canonicalizedResource(Bucket, Path).
    
sign (Key,Data) ->
%    io:format("Data being signed is ~p~n", [Data]),
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

queryParams( [] ) -> "";
queryParams( L ) -> 
    Stringify = fun ({K,V}) -> K ++ "=" ++ V end,
    "?" ++ s3util:string_join( lists:map( Stringify, L ), "&" ).

buildHost("") -> s3Host();
buildHost(Bucket) -> Bucket ++ "." ++ s3Host().
    
buildUrl(Bucket,Path,QueryParams, false) -> 
    "http://" ++ buildHost(Bucket) ++ "/" ++ Path ++ queryParams(QueryParams);

buildUrl(Bucket,Path,QueryParams, true) -> 
    "https://" ++ buildHost(Bucket) ++ "/" ++ Path ++ queryParams(QueryParams).

buildContentHeaders( <<>>, _ ) -> [];
buildContentHeaders( Contents, ContentType ) -> 
    [{"Content-Length", integer_to_list(size(Contents))},
     {"Content-Type", ContentType}].

genericRequest(From, #state{ssl=SSL, access_key=AKI, secret_key=SAK, timeout=Timeout, pending=P }=State, Method, Bucket, Path, QueryParams, Contents, ContentType, Callback ) ->
    Date = httpd_util:rfc1123_date(),
    MethodString = string:to_upper( atom_to_list(Method) ),
    Url = buildUrl(Bucket,Path,QueryParams, SSL),

    OriginalHeaders = buildContentHeaders( Contents, ContentType ),
    ContentMD5 = "",
    Body = Contents,


    Signature = sign( SAK,
		      stringToSign( MethodString, ContentMD5, ContentType, 
				    Date, Bucket, Path, OriginalHeaders )),

    Headers = [ {"Authorization","AWS " ++ AKI ++ ":" ++ Signature },
		{"Host", buildHost(Bucket) },
		{"Date", Date } 
	       | OriginalHeaders ],
    
    Request = case Method of
		  get -> { Url, Headers };
		  put -> { Url, Headers, ContentType, Body };
		  delete -> { Url, Headers }
	      end,
    HttpOptions = [{timeout, Timeout}],
    Options = [ {sync,false}, {headers_as_is,true} ],

%    io:format("Sending request ~p~n", [Request]),
    {ok,RequestId} = http:request( Method, Request, HttpOptions, Options ),
    Pendings = gb_trees:insert(RequestId,{From,Callback},P),
    
    {noreply, State#state{pending=Pendings}}.
%    io:format("HTTP reply was ~p~n", [Reply]),
    


parseBucketListXml (XmlDoc, _H) ->
    {Xml, _Rest} = xmerl_scan:string(binary_to_list(XmlDoc)),
    ContentNodes = xmerl_xpath:string("/ListBucketResult/Contents", Xml),

    GetObjectAttribute = fun (Node,Attribute) -> 
		      [Child] = xmerl_xpath:string( Attribute, Node ),
		      {Attribute, s3util:string_value( Child )}
	      end,

    NodeToRecord = fun (Node) ->
			   #object_info{ 
			 key =          GetObjectAttribute(Node,"Key"),
			 lastmodified = GetObjectAttribute(Node,"LastModified"),
			 etag =         GetObjectAttribute(Node,"ETag"),
			 size =         GetObjectAttribute(Node,"Size")}
		   end,
    lists:map( NodeToRecord, ContentNodes ).


xmlToBuckets( Body, _H) ->
    {Xml, _Rest} = xmerl_scan:string(binary_to_list(Body)),
    TextNodes       = xmerl_xpath:string("//Bucket/Name/text()", Xml),
    lists:map( fun (#xmlText{value=T}) -> T end, TextNodes).

