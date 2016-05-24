-module(cq).
-export([new/3, free/1, push/2, pop/1, size/1]).
-export([debug/1, debug_poppers/1, print_bits/0]).
-on_load(nif_init/0).


%%====================================================================
%% API functions
%%====================================================================


new(_QueueId, _QueueSize, _OverflowSize) ->
    exit(nif_not_loaded).

free(_QueueId) ->
    exit(nif_not_loaded).

push(_QueueId, _Term) ->
    exit(nif_not_loaded).


async_pop(_QueueId) ->
    exit(nif_not_loaded).


pop(QueueId) ->
    case async_pop(QueueId) of
        wait_for_msg ->
            error_logger:info_msg("Blocking Erlang process in receive, waiting for reply from async_pop~n"),
            receive
                {pop, Term} ->
                    {ok, Term}
            end;
        {pop, Term} ->
            {ok, Term}
    end.


size(_QueueId) ->
    exit(nif_not_loaded).


debug(_QueueId) ->
    exit(nif_not_loaded).

debug_poppers(_QueueId) ->
    exit(nif_not_loaded).


print_bits() ->
    exit(nif_not_loaded).



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
