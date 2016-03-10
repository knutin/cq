Concurrent Queue
=====

Have you ever struggled with a problem where a concurrent queue would
be perfect? Using NIFs and low-level concurrency primitives, this
project implements a mutable queue where multiple Erlang processes can
produce and consume terms, without using a central Erlang process or
ETS table for coordination.
