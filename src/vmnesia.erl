%%%-------------------------------------------------------------------
%%% @author vladimirb
%%% @doc
%%% Custom mnesia helpers
%%% @end
%%%-------------------------------------------------------------------
-module(vmnesia).

-author("vladimirb").

-export([
    frag_names/2,
    frag_transaction/1,
    create_tab_default/1,
    create_tab_rocks/1,
    validate_table_creation/1,
    bootstrap/0,
    bootstrap/1
]).

-export_type([
    create_tab_discram_params/0,
    create_tab_disconly_params/0
]).

%%%===================================================================
%%% PUBLIC
%%%===================================================================

-type create_tab_discram_params() :: #{
    nodes := [atom()],
    tab := atom(),
    record := atom(),
    fields := [atom()],
    type := set | bag
}.

-type create_tab_disconly_params() :: #{
    nodes := [atom()],
    tab := atom(),
    record := atom(),
    fields := [atom()],
    type := set | bag | ordered_set,
    % [1...2n)
    n_fragments := 1..1024,
    % [node()]
    frag_node_pool := [atom()]
}.

-spec frag_names(TabName :: atom(), NFragments :: non_neg_integer()) -> [atom()].
frag_names(TabName, NFragments) ->
    [
        binary_to_atom(
            <<(atom_to_binary(TabName))/binary, "_frag", (integer_to_binary(Frag))/binary>>
        )
        || Frag <- lists:seq(2, NFragments)
    ].

-spec frag_transaction(fun()) -> any().
frag_transaction(Fn) ->
    % http://erlang.org/pipermail/erlang-questions/2006-January/018611.html
    % http://erlang.org/doc/man/mnesia.html#activity-4
    mnesia:activity(
        sync_transaction,
        Fn,
        [],
        mnesia_frag
    ).

-spec create_tab_default(create_tab_discram_params() | create_tab_disconly_params()) ->
    mnesia:t_result('ok').
create_tab_default(#{
    nodes := Nodes,
    tab := Tab,
    record := RecordAtom,
    fields := Fields,
    type := Type
}) ->
    %%  Fields = record_info(fields, RecordAtom) in caller
    mnesia:create_table(Tab, [
        {type, Type},
        {attributes, Fields},
        {record_name, RecordAtom},
        {disc_copies, Nodes},
        {storage_properties, [
            {ets, [
                compressed,
                {write_concurrency, true},
                {read_concurrency, true}
            ]},
            {dets, [{auto_save, 5000}]}
        ]}
    ]);
%% http://erlang.org/doc/apps/mnesia/Mnesia_chap5.html#table-fragmentation
create_tab_default(#{
    nodes := Nodes,
    tab := Tab,
    record := RecordAtom,
    fields := Fields,
    type := Type,
    % [1...2n)
    n_fragments := NFragments,
    % [node()]
    frag_node_pool := FragNodePool
}) ->
    %%  Fields = record_info(fields, RecordAtom) in caller
    mnesia:create_table(Tab, [
        {type, Type},
        {attributes, Fields},
        {record_name, RecordAtom},
        {storage_properties, [
            {dets, [
                {auto_save, 5000}
                %{ram_file, true}
            ]}
        ]},
        {disc_only_copies, Nodes},
        case NFragments of
            1 ->
                {frag_properties, []};
            _ ->
                {frag_properties, [
                    {n_fragments, NFragments},
                    {n_disc_only_copies, 1},
                    {node_pool, FragNodePool}
                ]}
        end
    ]).

-spec create_tab_rocks(create_tab_discram_params()) -> mnesia:t_result('ok').
create_tab_rocks(#{
    nodes := Nodes,
    tab := Tab,
    record := RecordAtom,
    fields := Fields,
    type := Type
}) ->
    %%  Fields = record_info(fields, RecordAtom) in caller
    mnesia:create_table(Tab, [
        {type, Type},
        {attributes, Fields},
        {record_name, RecordAtom},
        {rocksdb_copies, Nodes}
    ]).

%% THROWS
-spec bootstrap() -> ok.
%% @doc Creates schema on the current node
bootstrap() ->
    bootstrap(node()).

-spec bootstrap(Node | [Node]) -> ok when Node :: atom().
%% @doc Creates schema on the target nodes
bootstrap(Nodes) when is_list(Nodes) ->
    [ok = bootstrap(Node) || Node <- Nodes],
    ok;
bootstrap(Node) when is_atom(Node) ->
    case mnesia:create_schema([Node]) of
        {error, {_, {already_exists, _}}} ->
            ok;
        _ ->
            _ = mnesia:stop(),
            _ = mnesia:delete_schema([Node]),
            _ = mnesia:create_schema([Node]),
            ok = mnesia:start()
    end.

-spec validate_table_creation(mnesia:t_result('ok')) -> ok | {error, any()}.
validate_table_creation(Res) ->
    case Res of
        {aborted, {already_exists, _}} -> ok;
        {atomic, ok} -> ok;
        Reason -> {error, Reason}
    end.
