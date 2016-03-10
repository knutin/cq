#include <stdint.h>
#include "erl_nif.h"


typedef struct priv {
    ErlNifMutex* mutex;
    ErlNifCond* cond;
    ErlNifEnv *env;
    ERL_NIF_TERM *array;
    uint32_t max_size;
    uint32_t front;
    uint32_t rear;
    uint32_t num_waiters;
} priv_t;


ERL_NIF_TERM mk_atom(ErlNifEnv* env, const char* atom);
ERL_NIF_TERM mk_error(ErlNifEnv* env, const char* msg);
int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info);
