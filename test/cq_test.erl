-module(cq_test).
-include_lib("eunit/include/eunit.hrl").


does_it_work_test() ->
    ?assertEqual(ok, cq:in(foo)),
    ?assertEqual(foo, cq:out()).


longer_queue_test() ->
    ?assertEqual(ok, cq:in(foo1)),
    ?assertEqual(ok, cq:in(foo2)),
    ?assertEqual(ok, cq:in(foo3)),
    ?assertEqual(foo1, cq:out()),
    ?assertEqual(foo2, cq:out()),
    ?assertEqual(ok, cq:in(foo4)),
    ?assertEqual(foo3, cq:out()),
    ?assertEqual(foo4, cq:out()),
    ?assertEqual(ok, cq:in(foo5)),
    ?assertEqual(foo5, cq:out()).


parallel_test() ->
    Parent = self(),
    _Subscriber = spawn(fun Loop() ->
                                case cq:out() of
                                    kill -> exit;
                                    Any -> Parent ! Any, Loop()
                                end
                        end),
    ok = cq:in(foobar),
    ok = cq:in(kill),
    receive Msg ->
            ?assertEqual(Msg, foobar)
    end,
    ok = cq:in(kill).
