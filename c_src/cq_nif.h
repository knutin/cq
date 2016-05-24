#include <stdint.h>
#include "erl_nif.h"


#define CACHE_LINE_SIZE 64

#define SLOT_INDEX(__index, __size) __index & (__size - 1)

#define Q_MASK 3L
#define Q_PTR(__ptr) (cq_node_t *) (((uint64_t)__ptr) & (~Q_MASK))
#define Q_COUNT(__ptr) ((uint64_t) __ptr & Q_MASK)
#define Q_SET_COUNT(__ptr, __val) (cq_node_t *) ((uint64_t) __ptr | (__val & Q_MASK))


#define STATE_EMPTY 0
#define STATE_WRITE 1
#define STATE_READ  2
#define STATE_FULL  3


ErlNifResourceType* CQ_RESOURCE;

typedef struct cq_node cq_node_t;

struct cq_node {
    ErlNifEnv *env;
    //ERL_NIF_TERM term;
    ErlNifPid *value;
    cq_node_t *next;
};



typedef struct cq_queue {
    cq_node_t *head;
    cq_node_t *tail;
} cq_queue_t;


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

    cq_queue_t *push_queue;
    cq_queue_t *pop_queue;

    uint8_t       *overflow_states;
    ERL_NIF_TERM  *overflow_terms;
    ErlNifEnv    **overflow_envs;

} cq_t;

cq_t **QUEUES = NULL; /* Initialized on nif load */


ERL_NIF_TERM mk_atom(ErlNifEnv* env, const char* atom);
ERL_NIF_TERM mk_error(ErlNifEnv* env, const char* msg);
int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info);
void free_resource(ErlNifEnv*, void*);


cq_queue_t* new_queue(void);
void enqueue(cq_queue_t *q, ErlNifPid *pid);
