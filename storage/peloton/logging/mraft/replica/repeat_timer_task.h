
#ifndef CONCENSUS_BENCHMARK_REPEAT_TIMER_TASK_H
#define CONCENSUS_BENCHMARK_REPEAT_TIMER_TASK_H

#include <bthread/unstable.h>
#include <bthread/bthread.h>

class RepeatedTimerTask{
public:
    RepeatedTimerTask();
    virtual ~RepeatedTimerTask();
    // Initialize timer task
    int init(int timeout_ms);

    // Start the timer
    void start();

    // Run timer function once now
    void run_once_now();

    // Stop the timer
    void stop();

    // Reset the timer, and schedule it in the initial timeout_ms
    void reset();

    // Reset the timer and schedule it in |timeout_ms|
    void reset(int timeout_ms);

    // Destroy the timer
    void destroy();

protected:

    // Invoked everytime when it reaches the timeout
    virtual void run() = 0;

    // Invoked when the timer is finally destroyed
    virtual void on_destroy() = 0;

    virtual int adjust_timeout_ms(int timeout_ms) {
        return timeout_ms;
    }

private:

    static void on_timedout(void* arg);
    static void* run_on_timedout_in_new_thread(void* arg);
    void on_timedout();
    //void schedule(std::unique_lock<raft_mutex_t>& lck);
    void schedule();

    //raft_mutex_t _mutex;
    uint64_t _timer;
    timespec _next_duetime;
    int  _timeout_ms;
    bool _stopped;
    bool _running;
    bool _destroyed;
    bool _invoking;
};

#endif //CONCENSUS_BENCHMARK_REPEAT_TIMER_TASK_H
