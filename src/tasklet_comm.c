
#include "tasklet.h"

struct _comm_req {
    double *buf;
    int size;
    int target;
    int tag;
    MPI_Datatype data_type;
    int disp;
    MPI_Win win;
    MPI_Request req;
    int comm_comp;
    int comm_type; /* 0: Send, 1: Recv, 2: Put */
    struct _comm_req *next;
};

extern int _xmp_num_pools;
extern int _xmp_num_procs;
extern int _xmp_enable_comm_thread;
extern volatile int _xmp_comm_task_finish_flag;
extern ABT_pool *_xmp_shared_pool;

static struct _comm_req *top_send_req;
static struct _comm_req *top_recv_req;
static struct _comm_req *top_put_req;

static void check_comm_status(struct _comm_req *req);
static void push_comm_req(struct _comm_req *req, struct _comm_req **top);
static struct _comm_req* pop_comm_req(struct _comm_req **top);
static void set_comm_req(struct _comm_req *req, double *buf, int size, int target,
                         int tag, MPI_Datatype data_type, int disp, MPI_Win win, int comm_type);
static void comm_task(void **args);

void _mpi_send(double *buf, int size, int target, int tag, MPI_Datatype data_type)
{
    if (!_xmp_enable_comm_thread) {

        int comp = 0;
        MPI_Request req;

        MPI_Isend(buf, size, data_type, target, tag, MPI_COMM_WORLD, &req);

        MPI_Test(&req, &comp, MPI_STATUS_IGNORE);
        while (!comp) {
            ABT_thread_yield();
            MPI_Test(&req, &comp, MPI_STATUS_IGNORE);
        }

    } else {

        struct _comm_req *req = malloc(sizeof(struct _comm_req));
        set_comm_req(req, buf, size, target, tag, data_type, 0, MPI_WIN_NULL, 0/* Send */);
        push_comm_req(req, &top_send_req);
        check_comm_status(req);
        free(req);
    }
}

void _mpi_recv(double *buf, int size, int target, int tag, MPI_Datatype data_type)
{
    if (!_xmp_enable_comm_thread) {

        int comp = 0;
        MPI_Request req;

        MPI_Irecv(buf, size, data_type, target, tag, MPI_COMM_WORLD, &req);

        MPI_Test(&req, &comp, MPI_STATUS_IGNORE);
        while (!comp) {
            ABT_thread_yield();
            MPI_Test(&req, &comp, MPI_STATUS_IGNORE);
        }

    } else {

        struct _comm_req *req = malloc(sizeof(struct _comm_req));
        set_comm_req(req, buf, size, target, tag, data_type, 0, MPI_WIN_NULL, 1/* Recv */);
        push_comm_req(req, &top_recv_req);
        check_comm_status(req);
        free(req);
    }
}

void _mpi_put(double *buf, int size, int target, int tag, MPI_Datatype data_type,
              int disp, MPI_Win win)
{
    if (!_xmp_enable_comm_thread) {

        int comp = 0;
        MPI_Request req;

        MPI_Rput(buf, size, data_type, target, disp, size, data_type, win, &req);

        MPI_Test(&req, &comp, MPI_STATUS_IGNORE);
        while (!comp) {
            ABT_thread_yield();
            MPI_Test(&req, &comp, MPI_STATUS_IGNORE);
        }
        MPI_Win_flush(target, win);

    } else {

        struct _comm_req *req = malloc(sizeof(struct _comm_req));
        set_comm_req(req, buf, size, target, tag, data_type, disp, win, 2/* Put */);
        push_comm_req(req, &top_put_req);
        check_comm_status(req);
        free(req);
    }
}

static inline void check_comm_status(struct _comm_req *req)
{
    while (!__sync_bool_compare_and_swap(&req->comm_comp, 1, 1)) {
        ABT_thread_yield();
    }
}

static inline void push_comm_req(struct _comm_req *req, struct _comm_req **top)
{
    struct _comm_req *old_top;
    do {
        req->next = old_top = *top;
    } while (!__sync_bool_compare_and_swap(top, old_top, req));
}

static inline struct _comm_req* pop_comm_req(struct _comm_req **top)
{
    struct _comm_req *old_top, *next_top;
    do {
        old_top = *top;
        if (old_top == NULL) return NULL;
        next_top = old_top->next;
    } while (!__sync_bool_compare_and_swap(top, old_top, next_top));
    return old_top;
}

static void set_comm_req(struct _comm_req *req, double *buf, int size, int target,
                         int tag, MPI_Datatype data_type, int disp, MPI_Win win, int comm_type)
{
    req->buf        = buf;
    req->size       = size;
    req->target     = target;
    req->tag        = tag;
    req->data_type  = data_type;
    req->comm_comp  = 0;
    req->disp       = disp;
    req->win        = win;
    req->comm_type  = comm_type;
    req->next       = NULL;
}

void comm_thread_create()
{
    int dummy;
    ABT_thread_create(_xmp_shared_pool[_xmp_num_pools-1], (void (*)(void *))comm_task, (void *)&dummy, ABT_THREAD_ATTR_NULL, NULL);
}

static void comm_task(void **args)
{
    int comm_comp;
    struct _comm_req *head_req, *i_req, *prev_i_req;

    head_req = NULL;

    while (1) {
        while ((i_req = pop_comm_req(&top_send_req)) != NULL) {
            MPI_Isend(i_req->buf, i_req->size, i_req->data_type, i_req->target, i_req->tag, MPI_COMM_WORLD, &i_req->req);
            i_req->next = head_req;
            head_req = i_req;
        }
        while ((i_req = pop_comm_req(&top_recv_req)) != NULL) {
            MPI_Irecv(i_req->buf, i_req->size, i_req->data_type, i_req->target, i_req->tag, MPI_COMM_WORLD, &i_req->req);
            i_req->next = head_req;
            head_req = i_req;
        }
        while ((i_req = pop_comm_req(&top_put_req)) != NULL) {
            MPI_Rput(i_req->buf, i_req->size, i_req->data_type, i_req->target, 0, i_req->size, i_req->data_type, i_req->win, &i_req->req);
            i_req->next = head_req;
            head_req = i_req;
        }

        prev_i_req = NULL;
        for (i_req = head_req; i_req != NULL; i_req = i_req->next) {
            comm_comp = 0;
            MPI_Test(&i_req->req, &comm_comp, MPI_STATUS_IGNORE);
            if (comm_comp) {
                if (i_req->comm_type == 2) { /* Put only */
                    MPI_Win_flush(i_req->target, i_req->win);
                }
                while (!__sync_bool_compare_and_swap(&i_req->comm_comp, 0, 1));
                if (prev_i_req != NULL) {
                    prev_i_req->next = i_req->next;
                } else {
                    head_req = i_req->next;
                }
            } else {
                prev_i_req = i_req;
            }
        }

        if (_xmp_comm_task_finish_flag == TRUE && head_req == NULL) break;
    }
    _xmp_comm_task_finish_flag = FALSE;
    top_send_req = top_recv_req = NULL;
}
