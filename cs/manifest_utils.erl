-module(manifest_utils).

-export([display_manifest_bucket/1,
         display_manifest_pbc/0,
         display_object_manifests/1,
         display_active_manifest/1]).

checkout_client() ->
    {ok, CPid} = riak_cs_riak_client:checkout(),
    CPid.

checkin_client(RcPid) ->
    riak_cs_riak_client:checkin(RcPid).

get_manifest_bucket(BucketName) ->
    ManifesBucket = riak_cs_utils:to_bucket_name(objects, BucketName),
    ManifesBucket.

display_manifest_bucket(BucketName) ->
    io:format("~nManifest Bucket:~n~p~n~n", [get_manifest_bucket(BucketName)]).

get_manifest_pbc(RcPid) ->
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    ManifestPbc.

display_manifest_pbc() ->
    RcPid = checkout_client(),
    io:format("~nManifest Pbc:~n~p~n", [get_manifest_pbc(RcPid)]),
    checkin_client(RcPid).

get_object_manifest({Bucket, Key}) ->
    RcPid = checkout_client(),
    ManifesBucket = get_manifest_bucket(Bucket),
    ok = riak_cs_riak_client:set_bucket_name(RcPid, Bucket),
    ManifestPbc = get_manifest_pbc(RcPid),
    ObjectManifests =
        case riakc_pb_socket:get(ManifestPbc, ManifesBucket, Key, [{r, all}])
        of
            {ok, RiakObject} ->
                riak_cs_manifest:manifests_from_riak_object(RiakObject);
            {error, Reason} ->
                Reason
        end,
    ObjectManifests.

display_object_manifests({Bucket, Key}) ->
    io:format("~nObject Manifests:~n~p~n", [get_object_manifest({Bucket, Key})]).

display_active_manifest({Bucket, Key}) ->
    Manifests = get_object_manifest({Bucket, Key}),
    io:format("~nActive Manifest:~n~p~n", [riak_cs_manifest_utils:active_manifest(Manifests)]).