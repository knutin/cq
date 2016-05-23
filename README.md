Concurrent Queue
=====

Have you ever struggled with a problem where a concurrent queue would
be perfect? Using NIFs and low-level concurrency primitives, this
project implements a mutable queue where multiple Erlang processes can
produce and consume terms, without using a central Erlang process or
ETS table for coordination.



Why a custom algorithm?
======

Need to integrate nicely with the Erlang schedulers, which would *not*
be the case if using mutexes, semaphores or spinlocks. Even with dirty
schedulers, you could quickly end up with all threads waiting on
locks. (Want to support the same amount of blocking Erlang processes
as you would expect the BEAM to handle without this NIF.)
