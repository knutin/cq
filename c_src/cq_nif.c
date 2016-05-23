#include <stdio.h>
#include <unistd.h>

#include "erl_nif.h"
#include "cq_nif.h"


/* #ifndef ERL_NIF_DIRTY_SCHEDULER_SUPPORT
# error Requires dirty schedulers
#endif */





ERL_NIF_TERM
mk_atom(ErlNifEnv* env, const char* atom)
{
    ERL_NIF_TERM ret;

    if(!enif_make_existing_atom(env, atom, &ret, ERL_NIF_LATIN1))
        return enif_make_atom(env, atom);

    return ret;
}

ERL_NIF_TERM
mk_error(ErlNifEnv* env, const char* mesg)
{
    return enif_make_tuple2(env, mk_atom(env, "error"), mk_atom(env, mesg));
}


static ERL_NIF_TERM
queue_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cq_t *q = enif_alloc_resource(CQ_RESOURCE, sizeof(cq_t));
    if (q == NULL)
        return mk_error(env, "priv_alloc_error");

    ERL_NIF_TERM ret = enif_make_resource(env, q);
    /* enif_release_resource(ret); */

    uint32_t queue_id = 0;
    uint32_t queue_size = 0;
    uint32_t overflow_size = 0;

    if (!enif_get_uint(env, argv[0], &queue_id) ||
        !enif_get_uint(env, argv[1], &queue_size) ||
        !enif_get_uint(env, argv[2], &overflow_size))
        return mk_error(env, "badarg");

    if (queue_id > 8)
        return mk_error(env, "bad_queue_id");

    /* TODO: Check that queue_size is power of 2 */

    if (QUEUES[queue_id] != NULL)
        return mk_error(env, "queue_id_already_exists");

    q->id             = queue_id;
    q->queue_size     = queue_size;
    q->overflow_size  = overflow_size;
    q->tail           = 0;
    q->head           = 0;
    q->slots_states   = calloc(q->queue_size, CACHE_LINE_SIZE);
    q->slots_terms    = calloc(q->queue_size, CACHE_LINE_SIZE);
    q->slots_envs     = calloc(q->queue_size, CACHE_LINE_SIZE);
    q->overflow_terms = calloc(q->overflow_size, CACHE_LINE_SIZE);
    q->overflow_envs  = calloc(q->queue_size, CACHE_LINE_SIZE);

    /* TODO: Check calloc return */


    for (int i = 0; i < q->queue_size; i++) {
        ErlNifEnv *slot_env = enif_alloc_env();

        q->slots_envs[i*CACHE_LINE_SIZE] = slot_env;
        //q->overflow_envs[i*CACHE_LINE_SIZE] = (ErlNifEnv *) enif_alloc_env();
    }

    QUEUES[q->id] = q;

    return enif_make_tuple2(env, mk_atom(env, "ok"), ret);
}


static ERL_NIF_TERM
queue_free(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint32_t queue_id = 0;

    if (!enif_get_uint(env, argv[0], &queue_id))
        return mk_error(env, "badarg");

    if (queue_id > 8)
        return mk_error(env, "badarg");

    cq_t *q = QUEUES[queue_id];
    if (q == NULL)
        return mk_error(env, "bad_queue_id");


    /* TODO: Free all the things! */
    QUEUES[queue_id] = NULL;

    return enif_make_atom(env, "ok");

}

/* Push to the head of the queue. */
static ERL_NIF_TERM
queue_push(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint32_t queue_id = 0;

    if (!enif_get_uint(env, argv[0], &queue_id))
        return mk_error(env, "badarg");

    if (queue_id > 8)
        return mk_error(env, "badarg");

    /* Load the queue */
    cq_t *q = QUEUES[queue_id];
    if (q == NULL)
        return mk_error(env, "bad_queue_id");

    if (q->id != queue_id)
        return mk_error(env, "not_identical_queue_id");


    for (int i = 0; i < q->queue_size; i++) {
        fprintf(stderr, "queue slot %d, index %d, state %d\n",
                i, i*CACHE_LINE_SIZE, q->slots_states[i*CACHE_LINE_SIZE]);
    }

    /* Increment head and attempt to claim the slot by marking it as
       busy. This ensures no other thread will attempt to modify this
       slot. If we cannot lock it, another thread must have */

    uint64_t head = __sync_add_and_fetch(&q->head, 1);
    size_t size = q->queue_size;

    while (1) {
        uint64_t index = SLOT_INDEX(head, size);
        uint64_t ret = __sync_val_compare_and_swap(&q->slots_states[index],
                                                   STATE_EMPTY,
                                                   STATE_WRITE);

        switch (ret) {

        case STATE_EMPTY:
            head = __sync_add_and_fetch(&q->head, 1);
            fprintf(stderr, "new head %lu, state %d\n",
                    SLOT_INDEX(head, size), q->slots_states[SLOT_INDEX(head, size)]);

        case STATE_WRITE:
            /* We acquired the write lock, go ahead with the write. */
            break;

        case STATE_FULL:
            /* We have caught up with the tail and the buffer is
               full. Block the producer until a consumer reads the
               item. */
            return mk_error(env, "full_not_implemented");
        }
    }

    fprintf(stderr, "found free slot at head %lu\n", head);

    /* If head catches up with tail, the queue is full. Add to
       overflow instead */


    /* Copy term to slot-specific temporary process env. */
    ERL_NIF_TERM copy = enif_make_copy(q->slots_envs[SLOT_INDEX(head, size)], argv[1]);
    q->slots_terms[SLOT_INDEX(head, size)] = copy;

    __sync_synchronize(); /* Or compiler memory barrier? */


    /* TODO: Do we need to collect garbage? */


    /* Mark the slot ready to be consumed */
    if (__sync_bool_compare_and_swap(&q->slots_states[SLOT_INDEX(head, size)],
                                     STATE_WRITE,
                                     STATE_FULL)) {
        return mk_atom(env, "ok");
    } else {
        return mk_error(env, "could_not_update_slots_after_insert");
    }

}



static ERL_NIF_TERM
queue_async_pop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    /* Load queue */

    uint32_t queue_id = 0;

    if (!enif_get_uint(env, argv[0], &queue_id))
        return mk_error(env, "badarg");

    if (queue_id > 8)
        return mk_error(env, "badarg");

    cq_t *q = QUEUES[queue_id];
    if (q == NULL)
        return mk_error(env, "bad_queue_id");

    fprintf(stderr, "q->id %d, queue_id %d\n", q->id, queue_id);
    if (q->id != queue_id)
        return mk_error(env, "not_identical_queue_id");

    uint64_t qsize = q->queue_size;
    uint64_t tail = q->tail;

    /* Walk the buffer starting the tail position until we are either
       able to consume a term or find an empty slot. */
    while (1) {
        uint64_t index = SLOT_INDEX(tail, qsize);
        uint64_t ret = __sync_val_compare_and_swap(&q->slots_states[index],
                                                   STATE_FULL,
                                                   STATE_READ);

        switch (ret) {

        case STATE_READ:
            /* We were able to mark the term as read in progress. We
               now have an exclusive lock. */
            break;

        case STATE_WRITE:
            /* We found an item with a write in progress. We could
               spin on acquiring a read lock, but that would break the
               fixed number of instructions guarantee. */

        case STATE_EMPTY:
            /* We found an empty item. Queue must be empty. Add
               calling Erlang consumer process to queue of waiting
               processes. When the next producer comes along, it will
               pick up waiting consumers and call enif_send */
            return mk_atom(env, "wait_for_msg");

        default:
            tail = __sync_add_and_fetch(&q->tail, 1);
            fprintf(stderr, "new tail %lu\n", tail);

        }
    }


    /* Copy term into calling process env. The NIF env can now be
       gargbage collected. */
    ERL_NIF_TERM copy = enif_make_copy(env, q->slots_terms[SLOT_INDEX(tail, qsize)]);


    /* Mark the slot as free. Note: We don't increment the tail
       position here, as another thread also walking the buffer might
       have incremented it multiple times */
    q->slots_terms[SLOT_INDEX(tail, qsize)] = 0;
    if (__sync_bool_compare_and_swap(&q->slots_states[SLOT_INDEX(tail, qsize)],
                                     STATE_READ,
                                     STATE_EMPTY)) {
        return enif_make_tuple2(env, mk_atom(env, "ok"), copy);
    } else {
        return mk_error(env, "could_not_update_slots_after_pop");
    }
}


static ERL_NIF_TERM
queue_debug(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

    uint32_t queue_id = 0;

    if (!enif_get_uint(env, argv[0], &queue_id))
        return mk_error(env, "badarg");

    if (queue_id > 8)
        return mk_error(env, "badarg");

    cq_t *q = QUEUES[queue_id];
    if (q == NULL)
        return mk_error(env, "bad_queue_id");



    ERL_NIF_TERM *slots_states = enif_alloc(sizeof(ERL_NIF_TERM) * q->queue_size);
    ERL_NIF_TERM *slots_terms = enif_alloc(sizeof(ERL_NIF_TERM) * q->queue_size);
    for (int i = 0; i < q->queue_size; i++) {
        slots_states[i] = enif_make_int(env, q->slots_states[i * CACHE_LINE_SIZE]);

        if (q->slots_terms[i * CACHE_LINE_SIZE] == 0) {
            slots_terms[i] = mk_atom(env, "null");
        } else {
            slots_terms[i] = enif_make_copy(env, q->slots_terms[i * CACHE_LINE_SIZE]);
        }
    }

    return enif_make_tuple4(env,
                            enif_make_uint64(env, q->tail),
                            enif_make_uint64(env, q->head),
                            enif_make_list_from_array(env, slots_states, q->queue_size),
                            enif_make_list_from_array(env, slots_terms, q->queue_size));
}



static ERL_NIF_TERM
print_bits(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

    uint64_t *p1 = malloc(8);
    *p1 = 0;


    for (int bit = 63; bit >= 0; bit--) {
        uint64_t power = 1 << bit;
        //uint64_t byte = *p1;
        uint64_t byte = p1;
        fprintf(stderr, "%d", (byte & power) >> bit);
    }
    fprintf(stderr, "\n");

    //enif_free(p1);

    return mk_atom(env, "ok");
}

void free_resource(ErlNifEnv* env, void* arg)
{
    //cq_t *cq = (cq_t *) arg;

    fprintf(stderr, "free_resource\n");
}



int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
    /* Initialize global array mapping id to cq_t ptr */
    QUEUES = (cq_t **) calloc(8, sizeof(cq_t **));
    if (QUEUES == NULL)
        return -1;


    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    CQ_RESOURCE = enif_open_resource_type(env, "cq", "cq",
                                          &free_resource, flags, NULL);

    if (CQ_RESOURCE == NULL)
        return -1;

    return 0;
}


static ErlNifFunc nif_funcs[] = {
    {"new"      , 3, queue_new},
    {"free"     , 1, queue_free},
    {"push"     , 2, queue_push},
    {"async_pop", 1, queue_async_pop},
    {"debug"    , 1, queue_debug},
    {"print_bits", 0, print_bits}
};

ERL_NIF_INIT(cq, nif_funcs, load, NULL, NULL, NULL);
