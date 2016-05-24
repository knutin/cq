-module(cq_test).
-include_lib("eunit/include/eunit.hrl").

-define(QUEUE_ID, 2).

%% does_it_work_test() ->
%%     {ok, _} = cq:new(?QUEUE_ID, 8, 8),
%%     ?assertEqual(ok, cq:push(?QUEUE_ID, foo)),
%%     ?assertEqual({ok, foo}, cq:pop(?QUEUE_ID)),
%%     ok = cq:free(?QUEUE_ID).


    %%?assertEqual({error, queue_id_already_exists}, cq:new(QueueId, 3, 3)),


%% debug_state_test() ->
%%     {ok, _} = cq:new(?QUEUE_ID, 8, 8),
%%     ?assertEqual({0, 0, n_zeros(8), n_nulls(8)}, cq:debug(?QUEUE_ID)),

%%     %% Push
%%     ?assertEqual(ok, cq:push(?QUEUE_ID, foo1)),
%%     ?assertEqual({0, 0, [3 | n_zeros(7)], [foo1 | n_nulls(7)]},
%%                  cq:debug(?QUEUE_ID)),

%%     ?assertEqual(ok, cq:push(?QUEUE_ID, foo2)),
%%     ?assertEqual({0, 1, [3, 3 | n_zeros(6)], [foo1, foo2 | n_nulls(6)]},
%%                  cq:debug(?QUEUE_ID)),

%%     ?assertEqual(ok, cq:push(?QUEUE_ID, foo3)),
%%     ?assertEqual({0, 2, [3, 3, 3 | n_zeros(5)], [foo1, foo2, foo3 | n_nulls(5)]},
%%                  cq:debug(?QUEUE_ID)),

%%     %% Pop
%%     ?assertEqual({ok, foo1}, cq:pop(?QUEUE_ID)),
%%     ?assertEqual({0, 2, [0, 3, 3 | n_zeros(5)], [null, foo2, foo3 | n_nulls(5)]},
%%                  cq:debug(?QUEUE_ID)),

%%     ?assertEqual({ok, foo2}, cq:pop(?QUEUE_ID)),
%%     ?assertEqual({1, 2, [0, 0, 3 | n_zeros(5)], [null, null, foo3 | n_nulls(5)]},
%%                  cq:debug(?QUEUE_ID)),

%%     ?assertEqual({ok, foo3}, cq:pop(?QUEUE_ID)),
%%     ?assertEqual({2, 2, n_zeros(8), n_nulls(8)},
%%                  cq:debug(?QUEUE_ID)).


pop_empty_blocks_test() ->
    Parent = self(),
    {ok, _} = cq:new(?QUEUE_ID, 8, 8),
    ?assertEqual([], cq:debug_poppers(?QUEUE_ID)),

    Popper = spawn(fun () ->
                           Parent ! go,
                           Ret = cq:pop(?QUEUE_ID),
                           Parent ! {popped, Ret}
                   end),

    receive go ->
            timer:sleep(100), % ensure popper is in pop/1

            ?assertEqual([Popper], cq:debug_poppers(?QUEUE_ID)),

            ?assertEqual(ok, cq:push(?QUEUE_ID, foo))
    end,

    receive M1 ->
            ?assertEqual({popped, {ok, foo}}, M1)
    end.

%% print_bits_test() ->
%%     cq:print_bits(),
%%     cq:print_bits(),
%%     cq:print_bits(),
%%     cq:print_bits(),
%%     cq:print_bits(),
%%     cq:print_bits(),
%%     cq:print_bits(),
%%     cq:print_bits(),
%%     cq:print_bits(),
%%     cq:print_bits(),
%%     cq:print_bits(),

%%     ok.

%% wraparound_test() ->
%%     {ok, _} = cq:new(?QUEUE_ID, 8, 8),
%%     lists:foreach(fun (I) -> cq:push(?QUEUE_ID, {hello, I}) end,
%%                   lists:seq(0, 12)),

%%     ?assertEqual({2, 2, n_zeros(8), n_nulls(8)},
%%                  cq:debug(?QUEUE_ID)).



%% longer_queue_test() ->
%%     ?assertEqual(ok, cq:in(foo1)),
%%     ?assertEqual(ok, cq:in(foo2)),
%%     ?assertEqual(ok, cq:in(foo3)),
%%     ?assertEqual(foo1, cq:out()),
%%     ?assertEqual(foo2, cq:out()),
%%     ?assertEqual(ok, cq:in(foo4)),
%%     ?assertEqual(foo3, cq:out()),
%%     ?assertEqual(foo4, cq:out()),
%%     ?assertEqual(ok, cq:in(foo5)),
%%     ?assertEqual(foo5, cq:out()).


%% parallel_test() ->
%%     Parent = self(),
%%     _Subscriber = spawn(fun Loop() ->
%%                                 case cq:out() of
%%                                     kill -> exit;
%%                                     Any -> Parent ! Any, Loop()
%%                                 end
%%                         end),
%%     ok = cq:in(foobar),
%%     ok = cq:in(kill),
%%     receive Msg ->
%%             ?assertEqual(Msg, foobar)
%%     end,
%%     ok = cq:in(kill).


n_nulls(N) -> lists:duplicate(N, null).
n_zeros(N)  -> lists:duplicate(N, 0).
