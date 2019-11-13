#ifndef _TASKLET_
#define _TASKLET_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include <sys/times.h>
#include <mpi.h>

#include <abt.h>

#include "timer.h"

#ifndef TRUE
#define TRUE 1
#define FALSE 0
#endif

#define USE_ABT 1

//#define USE_HWLOC 1

typedef void (*cfunc)(void**);

void tasklet_initialize(int argc, char *argv[]);
void tasklet_exec_main(cfunc f);
void tasklet_finalize();

void tasklet_create(cfunc f, int narg, void **args, 
                    int n_in, void **in_data, int n_out, void **out_data);

void tasklet_wait_all();
void tasklet_yield();

int xmp_num_threads();
int xmp_thread_num();

void comm_thread_create();

void _mpi_send(double *buf, int size, int target, int tag, MPI_Datatype data_type);
void _mpi_recv(double *buf, int size, int target, int tag, MPI_Datatype data_type);
void _mpi_put(double *buf, int size, int target, int tag, MPI_Datatype data_type, int disp, MPI_Win win);

int _abt_sched_init(ABT_sched sched, ABT_sched_config config);
void _abt_sched_run(ABT_sched sched);
int _abt_sched_free(ABT_sched sched);

#endif
