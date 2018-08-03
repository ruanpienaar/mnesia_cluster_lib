-module(mnesia_cluster_lib).
-export([
    start/0,
    add_node/1,
    remove_node/1,
    resume_node/0
]).


%% TODO: Mnesia DIR should be deleted when nodes removed, or disapeared.

start() ->
    % {ok, _} = application:ensure_all_started(mnesia_cluster_lib),
    % ok = mnesia_cluster_node_mon:add_node(node(), erlang:get_cookie())
    {_, []} = rpc:multicall(mnesia_cluster_node_mon, add_node, [node(), erlang:get_cookie()]),
    % none = mnesia:set_debug_level(verbose),
    ExtraNodes = lists:sort(nodes()),
    io:format("[~p] Extra Nodes! ~p ~n", [?MODULE, ExtraNodes]),
    %% Starting mnesia on cluster
    ok = mnesia_start_on_cluster(ExtraNodes),
    % Moved Nodes here.
    % Mnesia somehow helps discover new nodes,
    % kernel's sync_nodes produces a diff value at startup...
    Nodes = lists:sort([node() | nodes()]),
    io:format("[~p] Nodes! ~p ~n", [?MODULE, Nodes]),
    {SS, _} = rpc:multicall(application, get_env, [mnesia_cluster_lib, system_started]),
    case lists:any(fun({ok,true}) -> true; (_) -> false end, SS) of
    % At least 1 node has started when the system was installed
        true ->
            add_node();
    % No node has ever started when installing
        false ->
            rest_of_setup(Nodes, ExtraNodes)
    end.

rest_of_setup(Nodes, ExtraNodes) ->
    Node = node(),
    % Db Nodes
    AlreadyRunningDbNodes =
        mnesia:system_info(running_db_nodes),
    io:format("[~p] Already running_db_nodes ~p~n", [?MODULE, AlreadyRunningDbNodes]),
    {ok, _ExtraNodesReturn} =
        mnesia:change_config(extra_db_nodes, ExtraNodes),
    ok = poll(
        10000,
        fun() ->
            {AllRunningNodes,_B} = rpc:multicall(mnesia, system_info, [running_db_nodes]),
            lists:all(fun(NodeToCheck) ->
                lists:all(fun(RunningNodesOnNode) ->
                    lists:member(NodeToCheck, RunningNodesOnNode)
                end, AllRunningNodes)
            end, Nodes)
        end,
        true
    ),
    io:format("[~p] All db nodes running ~n", [?MODULE]),
    %% Added db nodes step check
    ok = application:set_env(mnesia_cluster_lib, added_running_db_nodes, true),
    ok = poll(
        10000,
        fun() ->
            {E, _B} = rpc:multicall(application, get_env, [mnesia_cluster_lib, added_running_db_nodes]),
            lists:all(fun({ok, true}) ->
                true;
                         (_) ->
                false
            end, E)
        end,
        %[ {ok, true} || _N <- Nodes ]
        true
    ),
    io:format("[~p] All nodes step set env ~n", [?MODULE]),
    %% Stop mnesia on cluster
    io:format("[~p] Stopping mnesia before schema creation ~n", [?MODULE]),
    stopped = mnesia:stop(),
    ok = lists:foreach(fun(EN) ->
        ok = poll(
            10000,
            fun() ->
                rpc:call(EN, mnesia, system_info, [is_running])
            end,
            no
        )
    end, ExtraNodes),
    io:format("[~p] Mnesia is stopped everywhere~n", [?MODULE]),
    %% Schema ( i think only 1 node has to create it... )
    io:format("[~p] ~p~n", [?MODULE, mnesia:system_info(all)]),
    case mnesia:create_schema(Nodes) of
        ok ->
            io:format("[~p] Schema created ...\n", [?MODULE]);
        {error, {Node, {already_exists, Node}}} ->
            io:format("[~p] Schema already created on ~p ...\n",[?MODULE, Node]);
        EEE ->
            EEE,
            io:format("[~p] Schema Create failed ~p~n", [?MODULE, EEE])
            % exit(2)
    end,
    %% Schema Created step
    ok = application:set_env(mnesia_cluster_lib, schema_created, true),
    ok = poll(
        10000,
        fun() ->
            {E, _B} = rpc:multicall(application, get_env, [mnesia_cluster_lib, schema_created]),
            lists:all(fun({ok, true}) ->
                true;
                         (_) ->
                false
            end, E)
        end,
        %[ {ok, true} || _N <- Nodes ]
        true
    ),
    io:format("[~p] Schema Created step check done ~n", [?MODULE]),
    %% Starting mnesia on cluster
    ok = mnesia_start_on_cluster(ExtraNodes),

    Steps = application:get_env(mnesia_cluster_lib, post_schema_create_mnesia_start_steps, []),
    lists:foreach(fun(Step) ->
        case Step of
            {{M,F,[]}, ExpectedResponse} ->
                ExpectedResponse = M:F();
            {{M,F,Args}, ExpectedResponse} ->
                ExpectedResponse = erlang:apply(M, F, Args);
            {M,F,Args} ->
                erlang:apply(M, F, Args)
        end
    end, Steps),

    %% Startup after schema Created step
    ok = application:set_env(mnesia_cluster_lib, startup_after_schema_create, true),
    ok = poll(
        10000,
        fun() ->
            {E, _B} = rpc:multicall(application, get_env, [mnesia_cluster_lib, startup_after_schema_create]),
            lists:all(fun({ok, true}) ->
                true;
                         (_) ->
                false
            end, E)
        end,
        %[ {ok, true} || _N <- Nodes ]
        true
    ),
    io:format("[~p] Startup after Schema Created step check done ~n", [?MODULE]),
    % %% Tables
    Tables = application:get_env(mnesia_cluster_lib, tables, []),
    % TblOpts = [],
    ok = lists:foreach(fun(Tbl) ->
        Tbl:create_table(Nodes)
    end, Tables),
    ok = mnesia:wait_for_tables(Tables, infinity),
    io:format("[~p] Mnesia wait_for_tables completed ... ~n", [?MODULE]),
    ok = application:set_env(mnesia_cluster_lib, system_started, true),
    mnesia:info().

poll(C, F, Expected) when C > 0 ->
    case F() of
        Expected ->
            % io:format("[~p] Done Polling ~n",
            %     [?MODULE]),
            ok;
        Answer ->
            io:format("[~p] Answer was ~p Still polling ~n", [?MODULE, Answer]),
            timer:sleep(250),
            poll(C-1, F, Expected)
    end;
poll(_, _, _) ->
    exit(1).

%% @doc Adding a node to a running cluster, by assuming the current cluster
%% @end

add_node() ->
    add_node(nodes()).

%% @doc Adding a node to a running cluster, by specifying the current cluster
%%      This is useful for adding new nodes, but be sure to connect
%%      to the cluster nodes first
%% @end

add_node(ClusterNodes) ->
    io:format("[~p] ClusterNodes ~p~n", [?MODULE, ClusterNodes]),
    Node = node(),
    Cookie = erlang:get_cookie(),
    %% Nodes
    Nodes = lists:sort([Node | nodes()]),
    % Match on the expected nodes... Nodes should be visible first.
    case lists:sort(ClusterNodes) of
        Nodes ->
            % io:format("[~p] Nodes! ~p ~n", [?MODULE, Nodes]),
            ExtraNodes = lists:sort(nodes()),
            %% Check that no existing Schema Dir exists...
            MnesiaDir = mnesia_lib:dir(),
            case filelib:is_dir(MnesiaDir) of
                false ->
                    do_add_node(Node, Nodes, ExtraNodes);
                true ->
                    io:format("!!! Mnesia Schema Directory Still exists !!!~n", []),
                    io:format("!!! Delete ~p to add node~n", [MnesiaDir]),
                    io:format(" rm -rf ~p~n", [MnesiaDir])
            end;
        _ ->
            lists:foreach(fun
                (N) when N == Node ->
                    true;
                (N) ->
                    % Let's assume the cookie will be set the same.....
                    true = erlang:set_cookie(N, Cookie),
                    A = net_kernel:connect(N),
                    io:format("Tried connecting to ~p result ~p~n", [N, A])
            end, ClusterNodes),
            case lists:sort([Node | nodes()]) of
                ClusterNodes ->
                    io:format("[~p] Connected to cluster nodes, going to reattempt add_node/1",
                            [?MODULE]),
                    timer:sleep(1000),
                    add_node(ClusterNodes);
                ActualNodes ->
                    io:format("[~p] Cannot install to specified cluster ~n", [?MODULE]),
                    io:format("[~p]  Suggested Cluster:~n~p~n", [?MODULE, ActualNodes])
            end
    end.

do_add_node(Node, Nodes, ExtraNodes) ->
    ok = mnesia:start(),

    % Db Nodes
    AlreadyRunningDbNodes =
        mnesia:system_info(running_db_nodes),
    io:format("[~p] Already running_db_nodes ~p~n", [?MODULE, AlreadyRunningDbNodes]),
    {ok, ExtraNodesReturn} =
        mnesia:change_config(extra_db_nodes, ExtraNodes),
    ExtraNodes = lists:sort(ExtraNodesReturn),
    case mnesia:change_table_copy_type(schema, node(), disc_copies) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, schema, Node, disc_copies}} ->
            % for nodes that were added, then deleted, then added again...
            ok
    end,

    % %% Tables
    Tables = application:get_env(mnesia_cluster_lib, tables, []),
    % TblOpts = [],
    ok = lists:foreach(fun(Tbl) ->
        Tbl:create_table(Nodes),
        case mnesia:add_table_copy(Tbl, node(), disc_copies) of
            {atomic, ok} ->
                ok;
            {aborted, {already_exists, Tbl, Node}} ->
                ok
        end
    end, Tables),
    ok = mnesia:wait_for_tables(Tables, infinity),
    io:format("[~p] Mnesia wait_for_tables completed ... ~n", [?MODULE]),
    % Needed for startup.
    ok = application:set_env(mnesia_cluster_lib, added_running_db_nodes, true),
    ok = application:set_env(mnesia_cluster_lib, schema_created, true),
    ok = application:set_env(mnesia_cluster_lib, startup_after_schema_create, true),
    ok = application:set_env(mnesia_cluster_lib, system_started, true),
    mnesia:info().

%% @doc Start mnesia on all other nodes ( from node() ).
%% @end
mnesia_start_on_cluster(ExtraNodes) ->
    %% Starting mnesia on cluster
    ok = mnesia:start(),
    ok = lists:foreach(fun(EN) ->
        % io:format("[~p] Going to check that mnesia is started @ ~p~n", [?MODULE, EN]),
        ok = poll(
            100,
            fun() -> rpc:call(EN, application, get_application, [mnesia]) /= undefined end,
            true
        )
        % io:format("[~p] Node ~p done polling ~n", [?MODULE, EN])
    end, ExtraNodes),
    io:format("[~p] Mnesia is started everywhere~n", [?MODULE]).

%% @doc Remove a stopped node while on a running db node
%% @end

remove_node(Node) ->
    Tables = application:get_env(mnesia_cluster_lib, tables, []),
    RN = mnesia:system_info(running_db_nodes),
    case lists:member(Node, RN) of
        true ->
            [ {atomic, ok} = mnesia:del_table_copy(Tbl, Node) || Tbl <- Tables ];
        false ->
            ok
    end,
    case rpc:call(Node, erlang, time, []) of
        {badrpc,nodedown} ->
            remove_dead_node(Node);
        {_, _, _} ->
            remove_alive_node(Node)
    end.

remove_alive_node(Node) ->
    Dir = rpc:call(Node, mnesia, system_info, [directory]),
    stopped = rpc:call(Node, mnesia, stop, []),
    ok = rpc:call(Node, mnesia, delete_schema, [[Node]]),
    MC = rpc:multicall(mnesia, del_table_copy, [schema, Node]),
    io:format("[~p] Delete schema table copy ~p~n", [?MODULE, MC]),
    rpc:call(Node, file, del_dir, [Dir]).

remove_dead_node(Node) ->
    MC = rpc:multicall(mnesia, del_table_copy, [schema, Node]),
    io:format("[~p] Delete schema table copy ~p~n", [?MODULE, MC]).

resume_node() ->
    MnesiaDir = mnesia_lib:dir(),
    case filelib:is_dir(MnesiaDir) of
        false ->
            io:format("!!! No existing Mnesia schema dir !!!~n", []);
        true ->
            ok = mnesia:start(),
            mnesia:info()
    end.