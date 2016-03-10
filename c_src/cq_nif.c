#include <stdio.h>

#include "erl_nif.h"
#include "cq_nif.h"


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
queue_in(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if(argc != 1)
        return enif_make_badarg(env);

    priv_t *priv = (priv_t *) enif_priv_data(env);

    /* If the queue is full, return immediately */

    /* Copy the term into a separate environment where it can live on,
       even if the original term is garbage collected */
    ERL_NIF_TERM copy = enif_make_copy(priv->env, argv[0]);

    enif_mutex_lock(priv->mutex);
    if (priv->rear < priv->max_size) {
        if(priv->array[priv->rear] != 0)
            return mk_error(env, "slot_not_empty");

        priv->array[priv->rear] = copy;
        priv->rear++;
        if (priv->rear == priv->max_size)
            priv->rear = 0;

        if (priv->num_waiters > 0)
            enif_cond_signal(priv->cond);

        enif_mutex_unlock(priv->mutex);
        return mk_atom(env, "ok");
    }

    enif_mutex_unlock(priv->mutex);
    return mk_atom(env, "in_not_implemented");
}

static ERL_NIF_TERM
queue_out(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    priv_t *priv = (priv_t *) enif_priv_data(env);
    /* Read lock */
    enif_mutex_lock(priv->mutex);

    /* If the queue is empty, block until something writes into
       it. There's no way to have a timeout on this operation with the
       NIF api, so the consumer will block forever if nothing enters
       the queue. */
    if (priv->array[priv->front] == 0) {
        priv->num_waiters++;
        enif_cond_wait(priv->cond, priv->mutex);
        priv->num_waiters--;

        return mk_atom(env, "got_something");
    } else {
        ERL_NIF_TERM out = priv->array[priv->front];
        priv->array[priv->front] = 0;

        priv->front++;
        if (priv->front == priv->max_size)
            priv->front = 0;

        ERL_NIF_TERM copy = enif_make_copy(env, out);
        enif_mutex_unlock(priv->mutex);

        /* Copy to current process env */
        /* TODO: Free temporary env, or GC */

        return copy;
    }
}


int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
    priv_t *priv = calloc(1, sizeof(priv_t));
    if (priv == NULL)
        return mk_error(env, "priv_alloc_error");

    priv->mutex = enif_mutex_create("queue");
    priv->cond = enif_cond_create("queue");
    priv->env = enif_alloc_env();
    priv->array = calloc(3, sizeof(ERL_NIF_TERM));
    priv->max_size = 3;
    priv->front = 0;
    priv->rear = 0;
    priv->num_waiters = 0;

    *priv_data = priv;

    return 0;
}


static ErlNifFunc nif_funcs[] = {
    {"in", 1, queue_in},
    {"out", 0, queue_out}
};

ERL_NIF_INIT(cq, nif_funcs, load, NULL, NULL, NULL);
