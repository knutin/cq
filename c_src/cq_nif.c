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

    q->push_queue = new_queue();
    q->pop_queue = new_queue();

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

    /* If there's consumers waiting, the queue must be empty and we
       should directly pick a consumer to notify. */

    ErlNifPid *waiting_consumer;
    int dequeue_ret = dequeue(q->pop_queue, &waiting_consumer);
    if (dequeue_ret) {
        ErlNifEnv *msg_env = enif_alloc_env();
        ERL_NIF_TERM copy = enif_make_copy(msg_env, argv[1]);
        ERL_NIF_TERM tuple = enif_make_tuple2(msg_env, mk_atom(env, "pop"), copy);

        if (enif_send(env, waiting_consumer, msg_env, tuple)) {
            enif_free_env(msg_env);
            return mk_atom(env, "ok");
        } else {
            return mk_error(env, "notify_failed");
        }
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

    if (q->id != queue_id)
        return mk_error(env, "not_identical_queue_id");

    uint64_t qsize = q->queue_size;
    uint64_t tail = q->tail;
    uint64_t num_busy = 0;

    /* Walk the buffer starting the tail position until we are either
       able to consume a term or find an empty slot. */
    while (1) {
        uint64_t index = SLOT_INDEX(tail, qsize);
        uint64_t ret = __sync_val_compare_and_swap(&q->slots_states[index],
                                                   STATE_FULL,
                                                   STATE_READ);

        if (ret == STATE_READ) {
            /* We were able to mark the term as read in progress. We
               now have an exclusive lock. */
            break;

        } else if (ret == STATE_WRITE) {
            /* We found an item with a write in progress. If that
               thread progresses, it will eventually mark the slot as
               full. We can spin until that happens.

               This can take an arbitrary amount of time and multiple
               reading threads will compete for the same slot.

               Instead we add the caller to the queue of blocking
               consumers. When the next producer comes it will "help"
               this thread by calling enif_send on the current
               in-progress term *and* handle it's own terms. If
               there's no new push to the queue, this will block
               forever. */
            return mk_atom(env, "write_in_progress_not_implemented");

        } else if (ret == STATE_EMPTY) {
            /* We found an empty item. Queue must be empty. Add
               calling Erlang consumer process to queue of waiting
               processes. When the next producer comes along, it first
               checks the waiting consumers and calls enif_send
               instead of writing to the slots. */

            ErlNifPid *pid = enif_alloc(sizeof(ErlNifPid));
            pid = enif_self(env, pid);
            enqueue(q->pop_queue, pid);

            return mk_atom(env, "wait_for_msg");

        } else {
            tail = __sync_add_and_fetch(&q->tail, 1);
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
queue_debug_poppers(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    uint32_t queue_id = 0;

    if (!enif_get_uint(env, argv[0], &queue_id))
        return mk_error(env, "badarg");

    if (queue_id > 8)
        return mk_error(env, "badarg");

    cq_t *q = QUEUES[queue_id];
    if (q == NULL)
        return mk_error(env, "bad_queue_id");


    uint64_t pop_queue_size = 0;
    cq_node_t *node = q->pop_queue->head;
    if (node->value == NULL) {
        node = node->next;
        node = Q_PTR(node);
    }

    while (node != NULL) {
        pop_queue_size++;
        node = node->next;
        node = Q_PTR(node);
    }

    ERL_NIF_TERM *pop_queue_pids = enif_alloc(sizeof(ERL_NIF_TERM) * pop_queue_size);

    node = q->pop_queue->head;
    node = Q_PTR(node);
    if (node->value == NULL) {
        node = node->next;
        node = Q_PTR(node);
    }

    uint64_t i = 0;
    while (node != NULL) {
        if (node->value == 0) {
            pop_queue_pids[i] = mk_atom(env, "null");
        }
        else {
            pop_queue_pids[i] = enif_make_pid(env, node->value);
        }

        i++;
        node = node->next;
        node = Q_PTR(node);
    }

    ERL_NIF_TERM list = enif_make_list_from_array(env, pop_queue_pids, pop_queue_size);
    enif_free(pop_queue_pids);

    return list;
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


cq_queue_t * new_queue()
{
    cq_queue_t *queue = enif_alloc(sizeof(cq_queue_t));
    cq_node_t *node = enif_alloc(sizeof(cq_node_t));
    node->next = NULL;
    //node->env = NULL;
    node->value = NULL;
    queue->head = node;
    queue->tail = node;

    return queue;
}



void enqueue(cq_queue_t *queue, ErlNifPid *pid)
{
    cq_node_t *node = enif_alloc(sizeof(cq_node_t));
    //node->env = enif_alloc_env();
    //node->term = enif_make_copy(node->env, term);
    node->value = pid;
    node->next = NULL;
    fprintf(stderr, "node %lu\n", node);

    cq_node_t *tail = NULL;
    uint64_t tail_count = 0;
    while (1) {
        tail = queue->tail;
        cq_node_t *tail_ptr = Q_PTR(tail);
        tail_count = Q_COUNT(tail);

        cq_node_t *next = tail->next;
        cq_node_t *next_ptr = Q_PTR(next);
        uint64_t next_count = Q_COUNT(next);

        if (tail == queue->tail) {
            fprintf(stderr, "tail == queue->tail\n");
            if (next_ptr == NULL) {
                fprintf(stderr, "next_ptr == NULL\n");
                if (__sync_bool_compare_and_swap(&tail_ptr->next,
                                                 next,
                                                 Q_SET_COUNT(node, next_count+1)))
                    fprintf(stderr, "CAS(tail_ptr->next, next, (node, next_count+1)) -> true\n");
                    break;
            } else {
                __sync_bool_compare_and_swap(&queue->tail,
                                             tail,
                                             Q_SET_COUNT(next_ptr, next_count+1));
                    fprintf(stderr, "CAS(queue->tail, tail, (next_ptr, next_count+1))\n");
            }
        }
    }

    cq_node_t *node_with_count = Q_SET_COUNT(node, tail_count+1);
    int ret = __sync_bool_compare_and_swap(&queue->tail,
                                           tail,
                                           node_with_count);
    fprintf(stderr, "CAS(queue->tail, tail, %lu) -> %d\n", node_with_count, ret);
}


int dequeue(cq_queue_t *queue, ErlNifPid **pid)
{
    fprintf(stderr, "dequeue\n");
    cq_node_t *head, *head_ptr, *tail, *tail_ptr, *next, *next_ptr;

    while (1) {
        head = queue->head;
        head_ptr = Q_PTR(head);
        tail = queue->tail;
        tail_ptr = Q_PTR(tail);
        next = head->next;
        next_ptr = Q_PTR(next);
        fprintf(stderr, "head %lu, tail %lu, next %lu\n", head, tail, next);

        if (head == queue->head) {
            if (head_ptr == tail_ptr) {
                if (next_ptr == NULL) {
                    return 0; /* Queue is empty */
                }
                fprintf(stderr, "CAS(queue->tail, tail, (next_ptr, tail+1))\n");
                __sync_bool_compare_and_swap(&queue->tail,
                                             tail,
                                             Q_SET_COUNT(next_ptr, Q_COUNT(tail)+1));
            } else {
                fprintf(stderr, "next->value %lu\n", next_ptr->value);
                *pid = next_ptr->value;
                fprintf(stderr, "CAS(queue->head, head, (next_ptr, head+1))\n");
                if (__sync_bool_compare_and_swap(&queue->head,
                                                 head,
                                                 Q_SET_COUNT(next_ptr, Q_COUNT(head)+1)))
                    break;
            }
        }
    }
    // free pid
    //enif_free(Q_PTR(head));
    return 1;
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
    {"debug_poppers", 1, queue_debug_poppers},
    {"print_bits", 0, print_bits}
};

ERL_NIF_INIT(cq, nif_funcs, load, NULL, NULL, NULL);
