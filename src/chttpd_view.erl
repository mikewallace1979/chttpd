% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(chttpd_view).
-include("chttpd.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").

-export([handle_view_req/3, handle_temp_view_req/2, get_reduce_type/1,
    view_group_etag/2, view_group_etag/3, parse_bool_param/1,
    extract_view_type/3]).

-export([view_cb/2]).

multi_query_view(Req, Db, DDoc, ViewName, Queries) ->
    % TODO proper calculation of etag
    % Etag = view_group_etag(ViewGroup, Db, Queries),
    Etag = couch_uuids:new(),
    {ok, #mrst{views=Views}} = couch_mrview_util:ddoc_to_mrst(Db, DDoc),
    DefaultParams = lists:flatmap(fun({K,V}) -> parse_view_param(K,V) end,
        chttpd:qs(Req)),
    [couch_stats_collector:increment({httpd, view_reads}) || _I <- Queries],
    chttpd:etag_respond(Req, Etag, fun() ->
        FirstChunk = "{\"results\":[",
        {ok, Resp} = chttpd:start_delayed_json_response(Req, 200, [{"Etag",Etag}], FirstChunk),
        VAcc0 = #vacc{db=Db, req=Req, resp=Resp},
        {_, VAcc} = lists:foldl(fun({QueryProps}, {Chunk, RespAcc}) ->
            if Chunk =/= nil -> chttpd:send_delayed_chunk(RespAcc#vacc.resp, Chunk); true -> ok end,
            ThisQuery = lists:flatmap(fun parse_json_view_param/1, QueryProps),
            FullParams0 = lists:ukeymerge(1, ThisQuery, DefaultParams),
            FullParams = couch_mrview:to_mrargs(FullParams0),
            {ok, VAcc1} = fabric:query_view(
                Db,
                DDoc,
                ViewName,
                fun view_cb/2,
                RespAcc,
                couch_mrview_util:set_view_type(FullParams, ViewName, Views)),
            {",\n", VAcc1}
        end, {nil, VAcc0}, Queries),
        {ok, Resp1} = chttpd:send_delayed_chunk(VAcc#vacc.resp, "]}"),
        chttpd:end_delayed_json_response(Resp1)
    end).

design_doc_view(Req, Db, DDoc, ViewName, Keys0) ->
    Keys = case Keys0 of % Keys should be undefined, not nil
        nil -> undefined;
        _ -> Keys0
    end,
    {ok, #mrst{views=Views}} = couch_mrview_util:ddoc_to_mrst(Db, DDoc),
    QueryArgs0 = couch_mrview_http:parse_qs(Req, Keys),
    QueryArgs = couch_mrview_util:set_view_type(QueryArgs0, ViewName, Views),
    % TODO proper calculation of etag
    % Etag = view_group_etag(ViewGroup, Db, Keys),
    Etag = couch_uuids:new(),
    couch_stats_collector:increment({httpd, view_reads}),
    chttpd:etag_respond(Req, Etag, fun() ->
        VAcc0 = #vacc{db=Db, req=Req},
        CB = fun view_cb/2,
        {ok, VAcc} = fabric:query_view(Db, DDoc, ViewName, CB, VAcc0, QueryArgs),
        chttpd:end_delayed_json_response(VAcc#vacc.resp)
    end).

view_cb({meta, Meta}, Acc) ->
    Headers = [{"ETag", Acc#vacc.etag}],
    {ok, Resp} = case Acc#vacc.resp of
        undefined ->
            chttpd:start_delayed_json_response(Acc#vacc.req, 200, Headers);
        _ ->            {ok, Acc#vacc.resp}
    end,
    % Map function starting
    Parts = case couch_util:get_value(total, Meta) of
        undefined -> [];
        Total -> [io_lib:format("\"total_rows\":~p", [Total])]
    end ++ case couch_util:get_value(offset, Meta) of
        undefined -> [];
        Offset -> [io_lib:format("\"offset\":~p", [Offset])]
    end ++ case couch_util:get_value(update_seq, Meta) of
        undefined -> [];
        UpdateSeq -> [io_lib:format("\"update_seq\":~p", [UpdateSeq])]
    end ++ ["\"rows\":["],
    Chunk = lists:flatten("{" ++ string:join(Parts, ",") ++ "\r\n"),
    {ok, Resp1} = chttpd:send_delayed_chunk(Resp, Chunk),
    {ok, Acc#vacc{resp=Resp1, prepend=""}};
view_cb({row, Row}, #vacc{resp=undefined}=Acc) ->
    % Reduce function starting
    Headers = [{"ETag", Acc#vacc.etag}],
    {ok, Resp} = chttpd:start_delayed_json_response(Acc#vacc.req, 200, Headers),
    {ok, Resp1} = chttpd:send_delayed_chunk(Resp,
            ["{\"rows\":[\r\n", row_to_json(Row)]),
    {ok, #vacc{resp=Resp1, prepend=",\r\n"}};
view_cb({row, Row}, Acc) ->
    % Adding another row
    {ok, Resp} = chttpd:send_delayed_chunk(Acc#vacc.resp, [Acc#vacc.prepend, row_to_json(Row)]),
    {ok, Acc#vacc{resp=Resp, prepend=",\r\n"}};
view_cb(complete, #vacc{resp=undefined}=Acc) ->
    % Nothing in view
    {ok, Resp} = chttpd:send_json(Acc#vacc.req, 200, {[{rows, []}]}),
    {ok, Acc#vacc{resp=Resp}};
view_cb(complete, Acc) ->
    % Finish view output
    {ok, Resp} = chttpd:send_delayed_chunk(Acc#vacc.resp, ["\r\n]}"]),
    {ok, Acc#vacc{resp=Resp}}.


row_to_json(Row) ->
    Id = couch_util:get_value(id, Row),
    row_to_json(Id, Row).

row_to_json(error, Row) ->
    % Special case for _all_docs request with KEYS to
    % match prior behavior.
    Key = couch_util:get_value(key, Row),
    Val = couch_util:get_value(value, Row),
    Obj = {[{key, Key}, {error, Val}]},
    ?JSON_ENCODE(Obj);
row_to_json(Id0, Row) ->
    Id = case Id0 of
        undefined -> [];
        Id0 -> [{id, Id0}]
    end,
    Key = couch_util:get_value(key, Row, null),
    Val = couch_util:get_value(value, Row),
    Doc = case couch_util:get_value(doc, Row) of
        undefined -> [];
        Doc0 -> [{doc, Doc0}]
    end,
    Obj = {Id ++ [{key, Key}, {value, Val}] ++ Doc},
    ?JSON_ENCODE(Obj).


extract_view_type(_ViewName, [], _IsReduce) ->
    throw({not_found, missing_named_view});
extract_view_type(ViewName, [View|Rest], IsReduce) ->
    case lists:member(ViewName, [Name || {Name, _} <- View#view.reduce_funs]) of
    true ->
        if IsReduce -> reduce; true -> red_map end;
    false ->
        case lists:member(ViewName, View#view.map_names) of
        true -> map;
        false -> extract_view_type(ViewName, Rest, IsReduce)
        end
    end.

handle_view_req(#httpd{method='GET',
        path_parts=[_, _, _, _, ViewName]}=Req, Db, DDoc) ->
    design_doc_view(Req, Db, DDoc, ViewName, nil);

handle_view_req(#httpd{method='POST',
        path_parts=[_, _, _, _, ViewName]}=Req, Db, DDoc) ->
    {Fields} = chttpd:json_body_obj(Req),
    Queries = couch_util:get_value(<<"queries">>, Fields),
    Keys = couch_util:get_value(<<"keys">>, Fields),
    case {Queries, Keys} of
    {Queries, undefined} when is_list(Queries) ->
        multi_query_view(Req, Db, DDoc, ViewName, Queries);
    {undefined, Keys} when is_list(Keys) ->
        design_doc_view(Req, Db, DDoc, ViewName, Keys);
    {undefined, undefined} ->
        throw({bad_request, "POST body must contain `keys` or `queries` field"});
    {undefined, _} ->
        throw({bad_request, "`keys` body member must be an array"});
    {_, undefined} ->
        throw({bad_request, "`queries` body member must be an array"});
    {_, _} ->
        throw({bad_request, "`keys` and `queries` are mutually exclusive"})
    end;

handle_view_req(Req, _Db, _DDoc) ->
    chttpd:send_method_not_allowed(Req, "GET,POST,HEAD").

handle_temp_view_req(Req, _Db) ->
    Msg = <<"Temporary views are not supported in BigCouch">>,
    chttpd:send_error(Req, 403, forbidden, Msg).

get_reduce_type(Req) ->
    list_to_existing_atom(chttpd:qs_value(Req, "reduce", "true")).


parse_json_view_param({<<"key">>, V}) ->
    [{start_key, V}, {end_key, V}];
parse_json_view_param({<<"startkey_docid">>, V}) ->
    [{start_docid, V}];
parse_json_view_param({<<"endkey_docid">>, V}) ->
    [{end_docid, V}];
parse_json_view_param({<<"startkey">>, V}) ->
    [{start_key, V}];
parse_json_view_param({<<"endkey">>, V}) ->
    [{end_key, V}];
parse_json_view_param({<<"limit">>, V}) when is_integer(V), V > 0 ->
    [{limit, V}];
parse_json_view_param({<<"stale">>, <<"ok">>}) ->
    [{stale, ok}];
parse_json_view_param({<<"stale">>, <<"update_after">>}) ->
    [{stale, update_after}];
parse_json_view_param({<<"descending">>, V}) when is_boolean(V) ->
    [{descending, V}];
parse_json_view_param({<<"skip">>, V}) when is_integer(V) ->
    [{skip, V}];
parse_json_view_param({<<"group">>, true}) ->
    [{group_level, exact}];
parse_json_view_param({<<"group">>, false}) ->
    [{group_level, 0}];
parse_json_view_param({<<"group_level">>, V}) when is_integer(V), V > 0 ->
    [{group_level, V}];
parse_json_view_param({<<"inclusive_end">>, V}) when is_boolean(V) ->
    [{inclusive_end, V}];
parse_json_view_param({<<"reduce">>, V}) when is_boolean(V) ->
    [{reduce, V}];
parse_json_view_param({<<"include_docs">>, V}) when is_boolean(V) ->
    [{include_docs, V}];
parse_json_view_param({<<"conflicts">>, V}) when is_boolean(V) ->
    [{conflicts, V}];
parse_json_view_param({<<"list">>, V}) ->
    [{list, couch_util:to_binary(V)}];
parse_json_view_param({<<"sorted">>, V}) when is_boolean(V) ->
    [{sorted, V}];
parse_json_view_param({K, V}) ->
    [{extra, {K, V}}].

parse_view_param("", _) ->
    [];
parse_view_param("key", Value) ->
    JsonKey = ?JSON_DECODE(Value),
    [{start_key, JsonKey}, {end_key, JsonKey}];
parse_view_param("startkey_docid", Value) ->
    [{start_docid, ?l2b(Value)}];
parse_view_param("endkey_docid", Value) ->
    [{end_docid, ?l2b(Value)}];
parse_view_param("startkey", Value) ->
    [{start_key, ?JSON_DECODE(Value)}];
parse_view_param("endkey", Value) ->
    [{end_key, ?JSON_DECODE(Value)}];
parse_view_param("limit", Value) ->
    [{limit, parse_positive_int_param(Value)}];
parse_view_param("count", _Value) ->
    throw({query_parse_error, <<"Query parameter 'count' is now 'limit'.">>});
parse_view_param("stale", "ok") ->
    [{stale, ok}];
parse_view_param("stale", "update_after") ->
    [{stale, update_after}];
parse_view_param("stale", _Value) ->
    throw({query_parse_error,
            <<"stale only available as stale=ok or as stale=update_after">>});
parse_view_param("update", _Value) ->
    throw({query_parse_error, <<"update=false is now stale=ok">>});
parse_view_param("descending", Value) ->
    [{descending, parse_bool_param(Value)}];
parse_view_param("skip", Value) ->
    [{skip, parse_int_param(Value)}];
parse_view_param("group", Value) ->
    case parse_bool_param(Value) of
        true -> [{group_level, exact}];
        false -> [{group_level, 0}]
    end;
parse_view_param("group_level", Value) ->
    [{group_level, parse_positive_int_param(Value)}];
parse_view_param("inclusive_end", Value) ->
    [{inclusive_end, parse_bool_param(Value)}];
parse_view_param("reduce", Value) ->
    [{reduce, parse_bool_param(Value)}];
parse_view_param("include_docs", Value) ->
    [{include_docs, parse_bool_param(Value)}];
parse_view_param("conflicts", Value) ->
    [{conflicts, parse_bool_param(Value)}];
parse_view_param("list", Value) ->
    [{list, ?l2b(Value)}];
parse_view_param("callback", _) ->
    []; % Verified in the JSON response functions
parse_view_param("sorted", Value) ->
    [{sorted, parse_bool_param(Value)}];
parse_view_param(Key, Value) ->
    [{extra, {Key, Value}}].

view_group_etag(Group, Db) ->
    view_group_etag(Group, Db, nil).

view_group_etag(#group{sig=Sig,current_seq=CurrentSeq}, _Db, Extra) ->
    % ?LOG_ERROR("Group ~p",[Group]),
    % This is not as granular as it could be.
    % If there are updates to the db that do not effect the view index,
    % they will change the Etag. For more granular Etags we'd need to keep
    % track of the last Db seq that caused an index change.
    chttpd:make_etag({Sig, CurrentSeq, Extra}).

parse_bool_param("true") -> true;
parse_bool_param("false") -> false;
parse_bool_param(Val) ->
    Msg = io_lib:format("Invalid value for boolean paramter: ~p", [Val]),
    throw({query_parse_error, ?l2b(Msg)}).

parse_int_param(Val) ->
    case (catch list_to_integer(Val)) of
    IntVal when is_integer(IntVal) ->
        IntVal;
    _ ->
        Msg = io_lib:format("Invalid value for integer parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

parse_positive_int_param(Val) ->
    case parse_int_param(Val) of
    IntVal when IntVal >= 0 ->
        IntVal;
    _ ->
        Fmt = "Invalid value for positive integer parameter: ~p",
        Msg = io_lib:format(Fmt, [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.
