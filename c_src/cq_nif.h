#include <stdint.h>
#include "erl_nif.h"


#define CACHE_LINE_SIZE 64

#define SLOT_INDEX(__index, __size) __index & (__size - 1)

#define STATE_EMPTY 0
#define STATE_WRITE 1
#define STATE_READ  2
#define STATE_FULL  3


ErlNifResourceType* CQ_RESOURCE;

// TODO: Add padding between the fields
typedef struct cq {
    uint32_t id;
    uint64_t queue_size;
    uint64_t overflow_size;
    uint64_t head;
    uint64_t tail;

    uint8_t       *slots_states;
    ERL_NIF_TERM  *slots_terms;
    ErlNifEnv    **slots_envs;

    uint8_t       *overflow_states;
    ERL_NIF_TERM  *overflow_terms;
    ErlNifEnv    **overflow_envs;

} cq_t;

cq_t **QUEUES = NULL; /* Initialized on nif load */


ERL_NIF_TERM mk_atom(ErlNifEnv* env, const char* atom);
ERL_NIF_TERM mk_error(ErlNifEnv* env, const char* msg);
int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info);
void free_resource(ErlNifEnv*, void*);
