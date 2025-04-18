#include <iostream>
#include <bthread/bthread.h>
#include <atomic>

// 测试 pthread 中能否正确使用 bthread_start_background() 接口
// 从 pthread 代码看出来是可以的

// int bthread_start_urgent(bthread_t* __restrict tid,
//                          const bthread_attr_t* __restrict attr,
//                          void * (*fn)(void*),
//                          void* __restrict arg) {
//     bthread::TaskGroup* g = bthread::tls_task_group;
//     if (g) {
//         // start from worker
//         return bthread::TaskGroup::start_foreground(&g, tid, attr, fn, arg);
//     }
//     return bthread::start_from_non_worker(tid, attr, fn, arg);
// }

// int bthread_start_background(bthread_t* __restrict tid,
//                              const bthread_attr_t* __restrict attr,
//                              void * (*fn)(void*),
//                              void* __restrict arg) {
//     bthread::TaskGroup* g = bthread::tls_task_group;
//     if (g) {
//         // start from worker
//         return g->start_background<false>(tid, attr, fn, arg);
//     }
//     return bthread::start_from_non_worker(tid, attr, fn, arg);
// }

// 用于存储每个线程的递增数字
std::atomic<int> counters[5];

// 线程函数
void* print_bthread_id_and_counter(void* arg) {
    int index = *(static_cast<int*>(arg));
    while (counters[index] < 100) {
        int counter_value = ++counters[index];
        std::cout << "bthread_id: " << bthread_self() << ", counter: " << counter_value << std::endl;
        bthread_usleep(1000);  // 睡眠1秒（可选）
    }
    return nullptr;
}

// 主函数
int main() {
    // 初始化计数器
    for (int i = 0; i < 5; ++i) {
        counters[i] = 0;
    }

    // 启动5个bthreads
    bthread_t bthreads[5];
    int args[5];
    for (int i = 0; i < 5; ++i) {
        args[i] = i;
        bthread_start_background(&bthreads[i], NULL, print_bthread_id_and_counter, &args[i]);
    }

    // 让主线程睡眠以允许bthreads运行（这只是一个简单的示例，实际应用中你可能有更好的方式来处理这个问题）
    bthread_usleep(10000000);  // 睡眠10秒

    return 0;
}
