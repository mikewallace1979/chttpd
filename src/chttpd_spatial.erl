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

% MW Port of GeoCouch couch_httpd_spatial.erl, modified to work with BigCouch
% by proxying queries through fabric

-module(chttpd_spatial).
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch/include/couch_spatial.hrl").

-export([handle_spatial_req/3]).

-import(couch_httpd,
        [send_json/2, send_json/3, send_method_not_allowed/2, send_chunk/2,
         start_json_response/2, start_json_response/3, end_json_response/1]).

% Either answer a normal spatial query, or keep dispatching if the path part
% after _spatial starts with an underscore.
handle_spatial_req(#httpd{
        path_parts=[_, _, _Dname, _, SpatialName|_]}=Req, Db, DDoc) ->
    case SpatialName of
    % TODO MW put this back in when _spatial requests fully working
    % the path after _spatial starts with an underscore => dispatch
    %<<$_,_/binary>> ->
    %    dispatch_sub_spatial_req(Req, Db, DDoc);
    _ ->
        handle_spatial(Req, Db, DDoc)
    end.

% TODO MW put this back in once simplest case working
% the dispatching of endpoints below _spatial needs to be done manually
%dispatch_sub_spatial_req(#httpd{
%        path_parts=[_, _, _DName, Spatial, SpatialDisp|_]}=Req,
%        Db, DDoc) ->
%    Conf = couch_config:get("httpd_design_handlers",
%        ?b2l(<<Spatial/binary, "/", SpatialDisp/binary>>)),
%    Fun = geocouch_duplicates:make_arity_3_fun(Conf),
%    apply(Fun, [Req, Db, DDoc]).

design_doc_spatial(Req, Db, DDoc, SpatialName) ->
    Group = couch_spatial_group:design_doc_to_spatial_group(DDoc),
    #spatial_query_args{
        stale = Stale
    } = QueryArgs = parse_spatial_params(Req),
    % TODO proper calculation of etag
    Etag = couch_uuids:new(),
    couch_stats_collector:increment({httpd, view_reads}),
    chttpd:etag_respond(Req, Etag, fun() ->
        {ok, Resp} = chttpd:start_json_response(Req, 200, [{"Etag",Etag}]),
        CB = fun spatial_callback/2,
        fabric:query_spatial(Db, DDoc, SpatialName, CB, {nil, Resp}, QueryArgs),
        chttpd:end_json_response(Resp)
    end).

handle_spatial(#httpd{method='GET',
        path_parts=[_, _, DName, _, SpatialName]}=Req, Db, DDoc) ->
    ?LOG_DEBUG("Spatial query (~p): ~n~p", [DName, DDoc#doc.id]),
    design_doc_spatial(Req, Db, DDoc, SpatialName);
handle_spatial(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").

% MW counterparts in couch_httpd_spatial are json_spatial_start_resp and
% send_json_spatial_row
spatial_callback({update_seq, UpdateSeq}, {nil, Resp}) ->
    send_chunk(Resp,
            io_lib:format("{\"update_seq\":~w,\"rows\":[\r\n", [UpdateSeq])),
    {ok, {"", Resp}};
spatial_callback({update_seq, _}, Acc) ->
    % a sorted=false view where the message came in late.  Ignore.
    {ok, Acc};
spatial_callback({row, {{Bbox, DocId}, {Geom, Value}}}, {nil, Resp}) ->
    JsonObj = {[
        {<<"id">>, DocId},
        {<<"bbox">>, erlang:tuple_to_list(Bbox)},
        {<<"geometry">>, couch_spatial_updater:geocouch_to_geojsongeom(Geom)},
        {<<"value">>, Value}]},
    send_chunk(Resp, ["{\"rows\":[\r\n", ?JSON_ENCODE(JsonObj)]),
    {ok, {",\r\n", Resp}};
spatial_callback({row, {{Bbox, DocId}, {Geom, Value}}}, {Prepend, Resp}) ->
    JsonObj = {[
        {<<"id">>, DocId},
        {<<"bbox">>, erlang:tuple_to_list(Bbox)},
        {<<"geometry">>, couch_spatial_updater:geocouch_to_geojsongeom(Geom)},
        {<<"value">>, Value}]},
    send_chunk(Resp, [Prepend, ?JSON_ENCODE(JsonObj)]),
    {ok, {",\r\n", Resp}};
spatial_callback(complete, {nil, Resp}) ->
    send_chunk(Resp, "{\"rows\":[]}");
spatial_callback(complete, {_, Resp}) ->
    send_chunk(Resp, "\r\n]}");
spatial_callback({count, Count}, {_, Resp}) ->
    send_chunk(Resp, {[{"count",Count}]});
spatial_callback({error, Reason}, {_, Resp}) ->
    {Code, ErrorStr, ReasonStr} = chttpd:error_info(Reason),
    Json = {[{code,Code}, {error,ErrorStr}, {reason,ReasonStr}]},
    send_chunk(Resp, [$\n, ?JSON_ENCODE(Json), $\n]).

parse_spatial_params(Req) ->
    QueryList = couch_httpd:qs(Req),
    QueryParams = lists:foldl(fun({K, V}, Acc) ->
        parse_spatial_param(K, V) ++ Acc
    end, [], QueryList),
    QueryArgs = lists:foldl(fun({K, V}, Args2) ->
        validate_spatial_query(K, V, Args2)
    end, #spatial_query_args{}, lists:reverse(QueryParams)),

    #spatial_query_args{
        bbox = Bbox,
        bounds = Bounds
    } = QueryArgs,
    case {Bbox, Bounds} of
    % Coordinates of the bounding box are flipped and no bounds for the
    % cartesian plane were set
    {{W, S, E, N}, nil} when E < W; N < S ->
        Msg = <<"Coordinates of the bounding box are flipped, but no bounds "
                "for the cartesian plane were specified "
                "(use the `plane_bounds` parameter)">>,
        throw({query_parse_error, Msg});
    _ ->
        QueryArgs
    end.

parse_spatial_param("bbox", Bbox) ->
    [{bbox, list_to_tuple(?JSON_DECODE("[" ++ Bbox ++ "]"))}];
parse_spatial_param("stale", "ok") ->
    [{stale, ok}];
parse_spatial_param("stale", "update_after") ->
    [{stale, update_after}];
parse_spatial_param("stale", _Value) ->
    throw({query_parse_error,
            <<"stale only available as stale=ok or as stale=update_after">>});
parse_spatial_param("count", "true") ->
    [{count, true}];
parse_spatial_param("count", _Value) ->
    throw({query_parse_error, <<"count only available as count=true">>});
parse_spatial_param("plane_bounds", Bounds) ->
    [{bounds, list_to_tuple(?JSON_DECODE("[" ++ Bounds ++ "]"))}];
parse_spatial_param(Key, Value) ->
    [{extra, {Key, Value}}].

validate_spatial_query(bbox, Value, Args) ->
    Args#spatial_query_args{bbox=Value};
validate_spatial_query(stale, ok, Args) ->
    Args#spatial_query_args{stale=ok};
validate_spatial_query(stale, update_after, Args) ->
    Args#spatial_query_args{stale=update_after};
validate_spatial_query(stale, _, Args) ->
    Args;
validate_spatial_query(count, true, Args) ->
    Args#spatial_query_args{count=true};
validate_spatial_query(bounds, Value, Args) ->
    Args#spatial_query_args{bounds=Value};
validate_spatial_query(extra, _Value, Args) ->
    Args.