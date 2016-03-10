-module(cq).
-export([in/1, out/0]).
-on_load(nif_init/0).


%%====================================================================
%% API functions
%%====================================================================


%% Block at 5 elements in queue

%% Take one message, blocks until a message returns
out() ->
    exit(not_loaded).


%% Publish, might block

in(_Msg) ->
    exit(not_loaded).


%%====================================================================
%% Internal functions
%%====================================================================

nif_init() ->
   SoName = case code:priv_dir(cq) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, cq]);
                _ ->
                    filename:join([priv, cq])
            end;
        Dir ->
            filename:join(Dir, cq)
    end,
    erlang:load_nif(SoName, 0).
