/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */

#include "tasklet.h"

//#define SHARED_POOL 1
//#define ROUND_ROBIN 1
#define REUSE_TASKLET 1

#ifdef _USE_HWLOC
#include <hwloc.h>
#endif

#define TASKLET_MAX_ARGS  20

typedef struct _tasklet_list {
    struct _tasklet_list *next;
    struct _tasklet *tasklet;
    void *key_addr;
    int is_out;
} *Tasklet_list;

static Tasklet_list tasklet_list_free;

static Tasklet_list new_tasklet_list();
static void free_tasklet_list(Tasklet_list lp);

typedef struct _tasklet {
    struct _tasklet *next;  // for ready list and free list
    struct _tasklet_list *depends;
    unsigned int depend_count;  // input count
    unsigned int ref_count;  // reference count
    int done;  // already executed
    cfunc f;
    void *args[TASKLET_MAX_ARGS];
    ABT_mutex mutex;
#ifdef ROUND_ROBIN
    int pool_id;
#endif
} Tasklet;

static Tasklet *tasklet_free_list = NULL;

// for checking
static int tasklet_n_created = 0;
static int tasklet_n_executed = 0;
static int tasklet_n_reclaimed = 0;

static ABT_mutex tasklet_g_mutex;   // global lock
static ABT_mutex tasklet_count_mutex;
static ABT_cond tasklet_count_cond;
static int tasklet_running_count = 0;

int _xmp_num_xstreams = 1;  // number of execution stream
int _xmp_num_pools;
int _xmp_num_procs;
static int _xmp_proc_num;
int _xmp_enable_comm_thread = FALSE;
volatile int _xmp_comm_task_finish_flag = FALSE;
#ifdef ROUND_ROBIN
static int _xmp_current_pool_id = 0;
#endif
static ABT_xstream *_xmp_ess; // execution stream

static ABT_sched *_xmp_scheds;
ABT_pool *_xmp_shared_pool;  // shared global pool

static Tasklet *new_tasklet();
static void free_tasklet(Tasklet *tp);
static void tasklet_put_ready(Tasklet *tp);
static Tasklet *tasklet_get_ready();

static void tasklet_make_dependency(Tasklet *from_tp, Tasklet *to_tp);
static void tasklet_put_keyaddr(int index,Tasklet *tp,void *key_addr,int is_out);
static void tasklet_remove_keyaddr(int index,void *key_addr);

static void tasklet_g_spinlock();
static void tasklet_g_lock();
static void tasklet_g_unlock();
static void tasklet_spinlock();
static void tasklet_lock();
static void tasklet_unlock();
static void tasklet_ref_inc(Tasklet *tp);
static void tasklet_ref_dec(Tasklet *tp);

/* hash table for dependency */
#define N_HASH_TBL_SIZE (1 << 9)   /* 512: must be the power of 2 */
static Tasklet_list tasklet_hash_table[N_HASH_TBL_SIZE];

unsigned int tasklet_hash_index(void *a)
{
    unsigned int h = (unsigned long long int)(a);
    h = h >> 4;
    h = h + (h>>16);
    return h & (N_HASH_TBL_SIZE-1);
}
    
#define HASH_ADDR_INDEX(a)  ((a)) & (N_DEPEND_TBL_SIZE-1))

void tasklet_create(cfunc f, int narg, void **args, 
                    int n_in, void **in_data, int n_out, void **out_data)
{
    Tasklet *tp;
    Tasklet_list lp;
    int i,h;
    void *key_addr;

    tasklet_g_spinlock();
    tp = new_tasklet();
    tp->ref_count = 1;  /* just created */
    tp->done = FALSE;
    tp->f = f;
#ifdef ROUND_ROBIN
    tp->pool_id = _xmp_current_pool_id++;
    if (_xmp_current_pool_id >= _xmp_num_pools)
        _xmp_current_pool_id = 0;
#endif

    if(narg >= TASKLET_MAX_ARGS){
        fprintf(stderr,"too many task arg: %d > %d\n",narg,TASKLET_MAX_ARGS);
        exit(1);
    }
    ABT_mutex_create(&tp->mutex);

    for(i = 0; i < narg; i++) tp->args[i] = args[i];

    if(n_in > 0){
        /* handling IN-dependency, search conresponding OUT */
        for(i = 0; i < n_in; i++){
            key_addr = in_data[i];
            h = tasklet_hash_index(key_addr);
            for(lp = tasklet_hash_table[h]; lp != NULL; lp = lp->next){
                if(lp->is_out && lp->key_addr == key_addr){
                    tasklet_make_dependency(lp->tasklet,tp);
                    tasklet_put_keyaddr(h,tp,key_addr,FALSE);
                    break;
                }
            }
        }
    }
    if(n_out > 0){
        /* handling OUT dependecy, search IN and OUT */
        for(i = 0; i < n_out; i++){
            key_addr = out_data[i];
            h = tasklet_hash_index(key_addr);
            for(lp = tasklet_hash_table[h]; lp != NULL; lp = lp->next){
                if(lp->key_addr == key_addr){
                    tasklet_make_dependency(lp->tasklet,tp);
                }
            }
            tasklet_remove_keyaddr(h,key_addr);
            tasklet_put_keyaddr(h,tp,key_addr,TRUE);
        }
    }
    tasklet_n_created++;
    tasklet_g_unlock();
    
    /* if no dependecy, ready to execute */
    if(tp->depend_count == 0) tasklet_put_ready(tp);
}

/*
 * Tasklet management
 */
static Tasklet *new_tasklet()
{
    Tasklet *tp;
#ifdef REUSE_TASKLET
    if((tp = tasklet_free_list) != NULL){
        tasklet_free_list = tasklet_free_list->next;
    } else {
        if((tp = (Tasklet *)malloc(sizeof(Tasklet))) == NULL){
            fprintf(stderr,"malloc failed: Tasklet\n");
            exit(1);
        }
    }
#else
    if((tp = (Tasklet *)malloc(sizeof(Tasklet))) == NULL){
        fprintf(stderr,"malloc failed: Tasklet\n");
        exit(1);
    }
#endif
    memset((void *)tp,0,sizeof(*tp));
    return tp;
}

static void free_tasklet(Tasklet *tp)
{
#ifdef REUSE_TASKET
    tp->next = tasklet_free_list;
    tasklet_free_list = tp;
    ABT_mutex_free(&tp->mutex);
#else
    free(tp);
#endif
}

/*
 * dependency management
 */
Tasklet_list new_tasklet_list()
{
    Tasklet_list lp;
#ifdef REUSE_TASKLET
    if((lp = tasklet_list_free) != NULL){
        tasklet_list_free = lp->next;
    } else {
        if((lp = (Tasklet_list)malloc(sizeof(*lp))) == NULL){
            fprintf(stderr,"malloc failed: Tasklet_list\n");
            exit(1);
        }
    }
#else
    if((lp = (Tasklet_list)malloc(sizeof(*lp))) == NULL){
        fprintf(stderr,"malloc failed: Tasklet_list\n");
        exit(1);
    }
#endif
    memset((void *)lp,0,sizeof(*lp));
    return lp;
}

static void free_tasklet_list(Tasklet_list lp)
{
#ifdef REUSE_TASKLET
    lp->next = tasklet_list_free;
    tasklet_list_free = lp;
#else
    free(lp);
#endif
}

static void tasklet_make_dependency(Tasklet *from_tp, Tasklet *to_tp)
{
    Tasklet_list lp;

    if(from_tp->done) return;

    lp = new_tasklet_list();
    lp->tasklet = to_tp;
    lp->next = from_tp->depends;
    from_tp->depends = lp;
    to_tp->depend_count++;
    tasklet_ref_inc(to_tp); // inc reference count
}

static void tasklet_put_keyaddr(int index,Tasklet *tp,void *key_addr,int is_out)
{
    Tasklet_list lp;

    lp = new_tasklet_list();
    lp->next = tasklet_hash_table[index];
    tasklet_hash_table[index] = lp;
    lp->tasklet = tp;
    lp->key_addr = key_addr;
    lp->is_out = is_out;
    tasklet_ref_inc(tp);  // inc reference count
}

static void tasklet_remove_keyaddr(int index,void *key_addr)
{
    Tasklet_list lp, lq, lpp;
    
    lp = tasklet_hash_table[index]; 
    lq = NULL;
    while(lp != NULL){
        if(lp->key_addr == key_addr){ // remove this entry
            lpp = lp;
            lp = lp->next;
            if(lq == NULL) // head
                tasklet_hash_table[index] = lp;
            else
                lq->next = lp;
            tasklet_ref_dec(lpp->tasklet); // dec reference count
            free_tasklet_list(lpp);
        } else {
            lq = lp;
            lp = lp->next;
        }
    }
}

static void tasklet_ref_inc(Tasklet *tp)
{
    tp->ref_count++;
}

static void tasklet_ref_dec(Tasklet *tp) 
{
    tp->ref_count--;

    if(tp->ref_count == 0) {
        free_tasklet(tp);
        tasklet_n_reclaimed++;
    }
}

static void ABT_error(char *msg)
{
    fprintf(stderr,"ABT error: %s\n",msg);
    exit(1);
}


#ifdef _USE_HWLOC
static hwloc_topology_t topo;
static int hwloc_ncores;
static int hwloc_nnodes;
void hwloc_setup(int es_num)
{
    /* Compact */
    //int thread_num = es_num;
    int thread_num = es_num+2;

    hwloc_obj_t core = hwloc_get_obj_by_type(topo, HWLOC_OBJ_CORE, thread_num);
    hwloc_cpuset_t set = hwloc_bitmap_dup(core->cpuset);

    int res;
    res = hwloc_set_cpubind(topo, set, HWLOC_CPUBIND_THREAD | HWLOC_CPUBIND_STRICT);
    if (res) {
        int err = errno;
        printf("[%d] error: hwloc_set_cpubind(): %d\n", es_num, err);
    }

    res = hwloc_set_membind(topo, set, HWLOC_MEMBIND_FIRSTTOUCH, HWLOC_MEMBIND_THREAD | HWLOC_MEMBIND_STRICT);
    if (res) {
        int err = errno;
        printf("[%d] error: hwloc_set_membind(): %d\n", es_num, err);
    }
    hwloc_bitmap_free(set);
}
#endif
void thread_initialize(void *arg)
{
    int es_num = (int)(intptr_t)arg;
#ifdef _USE_HWLOC
    hwloc_setup(es_num);
#endif
}

void tasklet_initialize(int argc, char *argv[])
{
    int i, j, k, provided, init_flag = FALSE;
    ABT_thread *threads;

    /* MPI Initialization */
    MPI_Initialized(&init_flag);

    if (!init_flag) {
        MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
        if (provided != MPI_THREAD_MULTIPLE) {
            exit(0);
        }
    }

    MPI_Comm_size(MPI_COMM_WORLD, &_xmp_num_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &_xmp_proc_num);

    /* initialization */
    ABT_init(argc, argv);

    char *cp;
    int val;
    cp = getenv("XMP_NUM_THREADS");
    if (cp != NULL) {
        sscanf(cp, "%d", &val);
        _xmp_num_xstreams = val;
    }
    cp = getenv("XMP_ENABLE_COMM_THREAD");
    if (cp != NULL && (strcmp(cp, "1") == 0 || strcmp(cp, "yes") == 0)) {
        if (_xmp_num_xstreams > 2 && _xmp_num_procs != 1) {
            _xmp_enable_comm_thread = TRUE;
        }
    } 

    _xmp_ess = (ABT_xstream *)malloc(sizeof(ABT_xstream) * _xmp_num_xstreams);

#ifdef SHARED_POOL
    _xmp_shared_pool = (ABT_pool *) malloc(sizeof(ABT_pool) * 1);

    ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC, ABT_TRUE, &_xmp_shared_pool[0]);

    ABT_xstream_self(&_xmp_ess[0]);
    ABT_xstream_set_main_sched_basic(_xmp_ess[0], ABT_SCHED_DEFAULT, 1, &_xmp_shared_pool[0]);
    for (i = 1; i < _xmp_num_xstreams; i++) {
        ABT_xstream_create_basic(ABT_SCHED_DEFAULT, 1, &_xmp_shared_pool[0],
                                 ABT_SCHED_CONFIG_NULL, &_xmp_ess[i]);
        ABT_xstream_start(_xmp_ess[i]);
    }
#else
    _xmp_num_pools = (_xmp_num_xstreams != 1) ? _xmp_num_xstreams-1 : 1;

    _xmp_shared_pool = (ABT_pool *) malloc(sizeof(ABT_pool)  * _xmp_num_pools);
    _xmp_scheds      = (ABT_sched *)malloc(sizeof(ABT_sched) * _xmp_num_xstreams);

    /* create pool */
    for (i = 0; i < _xmp_num_pools; i++) {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC, ABT_TRUE, &_xmp_shared_pool[i]);
    }

    /* create scheduler */
    ABT_sched_config_var cv_freq = {
        .idx = 0,
        .type = ABT_SCHED_CONFIG_INT
    };

    ABT_sched_config config;
    int freq = (_xmp_num_xstreams < 100) ? 100 : _xmp_num_xstreams;
    ABT_sched_config_create(&config, cv_freq, freq, ABT_sched_config_var_end);

    ABT_sched_def sched_def = {
        .type = ABT_SCHED_TYPE_ULT,
        .init = _abt_sched_init,
        .run  = _abt_sched_run,
        .free = _abt_sched_free,
        .get_migr_pool = NULL
    };

    ABT_pool *my_pools = (ABT_pool *)malloc(sizeof(ABT_pool) * _xmp_num_pools);

    /* XMP_ENABLE_COMM_THREAD=1 */
    if (_xmp_enable_comm_thread) {

        for (k = 0; k < _xmp_num_pools-1; k++) {
            my_pools[k] = _xmp_shared_pool[k % (_xmp_num_pools-1)];
        }
        ABT_sched_create(&sched_def, _xmp_num_pools-1, my_pools, config, &_xmp_scheds[0]);

        for (i = 0; i < _xmp_num_xstreams-2; i++) {
            for (k = 0; k < _xmp_num_pools-1; k++) {
                my_pools[k] = _xmp_shared_pool[(i + k) % (_xmp_num_pools-1)];
            }
            ABT_sched_create(&sched_def, _xmp_num_pools-1, my_pools, config, &_xmp_scheds[i+1]);
        }
        my_pools[0] = _xmp_shared_pool[_xmp_num_pools-1];
        ABT_sched_create(&sched_def, 1, my_pools, config, &_xmp_scheds[_xmp_num_xstreams-1]);

    } else {

        for (k = 0; k < _xmp_num_pools; k++) {
            my_pools[k] = _xmp_shared_pool[k % _xmp_num_pools];
        }
        ABT_sched_create(&sched_def, _xmp_num_pools, my_pools, config, &_xmp_scheds[0]);

        for (i = 0; i < _xmp_num_xstreams-1; i++) {
            for (k = 0; k < _xmp_num_pools; k++) {
                my_pools[k] = _xmp_shared_pool[(i + k) % _xmp_num_pools];
            }
            ABT_sched_create(&sched_def, _xmp_num_pools, my_pools, config, &_xmp_scheds[i+1]);
        }

    }
    free(my_pools);

    ABT_sched_config_free(&config);

    /* create xstreams */
    ABT_xstream_self(&_xmp_ess[0]);
    ABT_xstream_set_main_sched(_xmp_ess[0], _xmp_scheds[0]);
    for (i = 1; i < _xmp_num_xstreams; i++) {
        ABT_xstream_create(_xmp_scheds[i], &_xmp_ess[i]);
        ABT_xstream_start(_xmp_ess[i]);
    }
#endif

#ifdef _USE_HWLOC
    hwloc_topology_init(&topo);
    hwloc_topology_load(topo);
    hwloc_ncores = hwloc_get_nbobjs_by_type(topo, HWLOC_OBJ_CORE);
    hwloc_nnodes = hwloc_get_nbobjs_by_type(topo, HWLOC_OBJ_NODE);
#endif

    /* setup thread affinity if enable hwloc */
    threads = (ABT_thread *) malloc(sizeof(ABT_thread) * _xmp_num_xstreams);
    thread_initialize(0);
    for (i = 1; i < _xmp_num_xstreams; i++) {
        ABT_thread_create_on_xstream(_xmp_ess[i], thread_initialize, (void *)i, ABT_THREAD_ATTR_NULL, &threads[i]);
    }
    for (i = 1; i < _xmp_num_xstreams; i++) {
        ABT_thread_join(threads[i]);
        ABT_thread_free(&threads[i]);
    }
    free(threads);

    if(ABT_mutex_create(&tasklet_g_mutex) != ABT_SUCCESS) goto err;
    if(ABT_mutex_create(&tasklet_count_mutex) != ABT_SUCCESS) goto err;
    if(ABT_cond_create(&tasklet_count_cond) != ABT_SUCCESS) goto err;
    tasklet_running_count = 0;

    /* initialize tasklet hash table */
    for (int h = 0; h < N_HASH_TBL_SIZE; h++) {
        //tasklet_hash_table[h] = NULL;
    }

    /* initialize comm thread */
    if (_xmp_enable_comm_thread) {
        comm_thread_create();
    }

    return;
 err:
    ABT_error("tasklet_initialize");
}

void tasklet_exec_main(cfunc main_func)
{
    ABT_thread thread;
    ABT_pool pool;
    int dummy;

    pool = _xmp_shared_pool[0];

    /* lauch main proram */
    ABT_thread_create(pool, (void (*)(void *))main_func, (void *)&dummy, ABT_THREAD_ATTR_NULL, &thread);
    //ABT_thread_create_on_xstream(_xmp_ess[0], (void (*)(void *))main_func, (void *)&dummy, ABT_THREAD_ATTR_NULL, &thread);

    /* wait and join the thread */
    ABT_thread_join(thread);
    ABT_thread_free(&thread);
#ifdef _DEBUG
    printf("tasklet_exec_main: main_func end created=%d executed=%d reclaimed=%d ...\n",
           tasklet_n_created, tasklet_n_executed, tasklet_n_reclaimed);
#endif
}

void tasklet_finalize()
{
    int i;

    /* finish comm thread */
    if (_xmp_enable_comm_thread) {
        _xmp_comm_task_finish_flag = TRUE;
        while (_xmp_comm_task_finish_flag != FALSE);
    }

    /* join ESs */
    for (i = 1; i < _xmp_num_xstreams; i++){
        ABT_xstream_join(_xmp_ess[i]);
        ABT_xstream_free(&_xmp_ess[i]);
    }

    ABT_finalize();

    free(_xmp_ess);
    free(_xmp_scheds);
    free(_xmp_shared_pool);
}

static void tasklet_wrapper(Tasklet *tp)
{
    Tasklet *tq;
    Tasklet_list lp, lq;

    (*tp->f)(tp->args);  // exec tasklet

    tasklet_g_spinlock();
    tp->done = TRUE;  // executed
    // resolve dependency
    for(lp = tp->depends; lp != NULL; lp = lp->next){
        tq = lp->tasklet;

        tq->depend_count--;
        // printf("tasklet(%p) depend_count %d\n",tq,tq->depend_count);
        assert(tq->depend_count >= 0);
        if(tq->depend_count < 0){
            fprintf(stderr,"depend_count (%d) < 0\n",tq->depend_count);
            exit(1);
        }
        tq->ref_count--;
        if(tq->ref_count <= 0){ // must be > 0 
            fprintf(stderr,"ref_count (%d) <= 0\n",tq->ref_count);
            exit(1);
        }
        if(tq->depend_count == 0) tasklet_put_ready(tq);
    }

    lp = tp->depends;
    while(lp != NULL){
        lq = lp->next;
        free_tasklet_list(lp);
        lp = lq;
    }
    tasklet_n_executed++;
    tasklet_ref_dec(tp);  // dec refcount, executed

    tasklet_g_unlock();

    ABT_mutex_spinlock(tasklet_count_mutex);
    tasklet_running_count--;
    if(tasklet_running_count == 0) 
        ABT_cond_signal(tasklet_count_cond);
    ABT_mutex_unlock(tasklet_count_mutex);
}

static void tasklet_put_ready(Tasklet *tp)
{
    ABT_pool pool;

    /* lauch main proram */
    if(ABT_mutex_spinlock(tasklet_count_mutex) != ABT_SUCCESS) goto err;
    tasklet_running_count++;
    if(ABT_mutex_unlock(tasklet_count_mutex) != ABT_SUCCESS) goto err;

    int rank;
#ifdef SHARED_POOL
    rank = 0;
#else
#ifdef ROUND_ROBIN
    rank = tp->pool_id;
    if (_xmp_enable_comm_thread) {
        if (rank == _xmp_num_pools-1) {
            rank = 0;
        }
    }
#else
    rank = xmp_thread_num();
    if (_xmp_enable_comm_thread) {

        if (rank == _xmp_num_xstreams-1) {
            rank = 0;
        } else if (rank != 0) {
            rank = rank - 1;
        }

    } else {

        if (_xmp_num_xstreams != 1 && rank != 0)
            rank = rank - 1; 
    }
#endif
#endif
    pool = _xmp_shared_pool[rank];
    ABT_thread_create(pool, (void (*)(void *))tasklet_wrapper, tp, ABT_THREAD_ATTR_NULL, NULL);
    
    return;
 err:
    ABT_error("task_put_ready");
}

void tasklet_wait_all()
{
    if(ABT_mutex_lock(tasklet_count_mutex) != ABT_SUCCESS) goto err;
    while(tasklet_running_count != 0 || tasklet_n_executed < tasklet_n_created) {
        if(ABT_cond_wait(tasklet_count_cond,tasklet_count_mutex) != ABT_SUCCESS) goto err;
    }
    if(ABT_mutex_unlock(tasklet_count_mutex) != ABT_SUCCESS) goto err;

    /* finish and recreate comm thread */
    if (_xmp_enable_comm_thread) {
        _xmp_comm_task_finish_flag = TRUE;
        while (_xmp_comm_task_finish_flag != FALSE);
        comm_thread_create();
    }

    /* clean up hash table */
    for(int h = 0; h < N_HASH_TBL_SIZE; h++) {
        Tasklet_list lp;
        for(lp = tasklet_hash_table[h]; lp != NULL; lp = lp->next) {
            tasklet_ref_dec(lp->tasklet);
        }
        //tasklet_hash_table[h] = NULL;
    }

    // printf("tasklet_wait_all end: taslet_running count=%d\n",tasklet_running_count);
    return;
 err:
    ABT_error("task_wait_all");
}

static void tasklet_g_spinlock()
{
    if(ABT_mutex_spinlock(tasklet_g_mutex) != ABT_SUCCESS) 
        ABT_error("tasklet_g_spinlock");
}

static void tasklet_g_lock()
{
    if(ABT_mutex_lock(tasklet_g_mutex) != ABT_SUCCESS) 
        ABT_error("tasklet_g_lock");
}

static void tasklet_g_unlock()
{
    if(ABT_mutex_unlock(tasklet_g_mutex) != ABT_SUCCESS) 
        ABT_error("tasklet_g_unlock");
}

static void tasklet_spinlock(ABT_mutex mutex)
{
    if(ABT_mutex_spinlock(mutex) != ABT_SUCCESS) 
        ABT_error("tasklet_spinlock");
}

static void tasklet_lock(ABT_mutex mutex)
{
    if(ABT_mutex_lock(mutex) != ABT_SUCCESS) 
        ABT_error("tasklet_lock");
}

static void tasklet_unlock(ABT_mutex mutex)
{
    if(ABT_mutex_unlock(mutex) != ABT_SUCCESS) 
        ABT_error("tasklet_unlock");
}

void tasklet_yield()
{
    ABT_thread_yield();
}

int xmp_thread_num()
{
    int tnum;
    ABT_xstream_self_rank(&tnum);
    return tnum;
}

int xmp_num_threads()
{
    return _xmp_num_xstreams;
}

