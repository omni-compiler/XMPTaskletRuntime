
#include "tasklet.h"

typedef struct {
    uint32_t event_freq;
} _abt_sched_data_t;

int _abt_sched_init(ABT_sched sched, ABT_sched_config config)
{
    _abt_sched_data_t *p_data;
    p_data = (_abt_sched_data_t *)calloc(1, sizeof(_abt_sched_data_t));

    ABT_sched_config_read(config, 1, &p_data->event_freq);
    ABT_sched_set_data(sched, (void *)p_data);

    return ABT_SUCCESS;
}

void _abt_sched_run(ABT_sched sched)
{
    uint32_t work_count = 0;
    _abt_sched_data_t *p_data;
    int num_pools;
    ABT_pool *pools;
    ABT_unit unit;
    int target;
    ABT_bool stop;
    unsigned seed = time(NULL);
    size_t size;
    int pool_last_stolen = -1;

    ABT_sched_get_data(sched, (void **)&p_data);
    ABT_sched_get_num_pools(sched, &num_pools);
    pools = (ABT_pool *)malloc(num_pools * sizeof(ABT_pool));
    ABT_sched_get_pools(sched, num_pools, 0, pools);

    while (1) {
        /* Execute one work unit from the scheduler's pool */
        ABT_pool_get_size(pools[0], &size);
        if (size > 0) {
            ABT_pool_pop_fifo(pools[0], &unit); /* ABT_pool_pop(pools[0], &unit); */
            if (unit != ABT_UNIT_NULL) {
                ABT_xstream_run_unit(unit, pools[0]);
            }
        /* Steal a work unit from other pools */
        } else if (num_pools > 1) {
            unit = ABT_UNIT_NULL;
            target = pool_last_stolen;
            if (target == -1) {
                target = (num_pools != 1) ? rand_r(&seed) % (num_pools-1) + 1 : 0;
            }

            ABT_pool_get_size(pools[target], &size);
            if (size > 0) {
                ABT_pool_pop_filo(pools[target], &unit); /* ABT_pool_pop(pools[0], &unit); */
                //ABT_pool_pop_fifo(pools[target], &unit); /* ABT_pool_pop(pools[0], &unit); */
                if (unit != ABT_UNIT_NULL) {
                    ABT_unit_set_associated_pool(unit, pools[0]);
                    ABT_xstream_run_unit(unit, pools[0]);
                }
            }
            if (unit == ABT_UNIT_NULL) {
                pool_last_stolen = -1;

            }
        }

        if (++work_count >= p_data->event_freq) {
            ABT_xstream_check_events(sched);
            ABT_sched_has_to_stop(sched, &stop);
            if (stop == ABT_TRUE) break;
            work_count = 0;
        }
    }

    free(pools);
}

int _abt_sched_free(ABT_sched sched)
{
    _abt_sched_data_t *p_data;

    ABT_sched_get_data(sched, (void **)&p_data);
    free(p_data);

    return ABT_SUCCESS;
}
