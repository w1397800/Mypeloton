#include "repeat_timer_task.h"


RepeatedTimerTask::RepeatedTimerTask()
        : _timeout_ms(0)
        , _stopped(true)
        , _running(false)
        , _destroyed(true)
        , _invoking(false)
{}

RepeatedTimerTask::~RepeatedTimerTask()
{
//    CHECK(!_running) << "Is still running";
//    CHECK(_destroyed) << "destroy() must be invoked before descrution";
}

int RepeatedTimerTask::init(int timeout_ms) {
    _timeout_ms = timeout_ms;
    _destroyed = false;
    _stopped = true;
    _running = false;
    _timer = bthread_timer_t();
    return 0;
}

void RepeatedTimerTask::stop() {
//    BAIDU_SCOPED_LOCK(_mutex);
//    BRAFT_RETURN_IF(_stopped);
    do { if ((_stopped)) { return; }} while (0);
    _stopped = true;
    CHECK(_running);
    const int rc = bthread_timer_del(_timer);
    if (rc == 0) {
        _running = false;
        return;
    }
}

void RepeatedTimerTask::on_timedout() {
//    std::unique_lock<raft_mutex_t> lck(_mutex);
    _invoking = true;
//    lck.unlock();
    //   ^^^NOTE: don't invoke run() inside lock to avoid the dead-lock issue
    run();
//    lck.lock();
    _invoking = false;
    CHECK(_running);
    if (_stopped) {
        _running = false;
        if (_destroyed) {
            // this may call the destruction,
            // so do this after setting _running to false
//            lck.unlock();
            on_destroy();
        }
        return;
    }
    return schedule();
}

void RepeatedTimerTask::start() {
    // Implementation considers the following conditions:
    //   - The first time start() was invoked
    //   - stop() was not invoked()
    //   - stop() was invoked and _timer was successfully deleted
    //   - stop() was invoked but _timer was not successfully deleted:
    //       a) _timer is still running right now
    //       b) _timer is finished
//    std::unique_lock<raft_mutex_t> lck(_mutex);
//    BRAFT_RETURN_IF(_destroyed);
    do { if ((_destroyed)) { return; }} while (0);
//    BRAFT_RETURN_IF(!_stopped);
    do { if ((!_stopped)) { return; }} while (0);
    _stopped = false;

//    BRAFT_RETURN_IF(_running);
    do { if ((_running)) { return; }} while (0);
    //  ^^^ _timer was not successfully deleted and the former task
    // is still running, in which case on_timedout would invoke
    // schedule as it would not see _stopped
    _running = true;
    schedule();
}

void RepeatedTimerTask::run_once_now() {
//    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (bthread_timer_del(_timer) == 0) {
//        lck.unlock();
        on_timedout(this);
    }
}

void* RepeatedTimerTask::run_on_timedout_in_new_thread(void* arg) {
    RepeatedTimerTask* m = (RepeatedTimerTask*)arg;
    m->on_timedout();
    return NULL;
}

void RepeatedTimerTask::on_timedout(void* arg) {
    // Start a bthread to invoke run() so we won't block the timer thread.
    // as run() might access the disk so the time it takes is probably beyond
    // expection
    bthread_t tid;
    if (bthread_start_background(
            &tid, NULL, run_on_timedout_in_new_thread, arg) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        run_on_timedout_in_new_thread(arg);
    }
}

void RepeatedTimerTask::schedule() {
    _next_duetime =
            butil::milliseconds_from_now(adjust_timeout_ms(_timeout_ms));
    if (bthread_timer_add(&_timer, _next_duetime, on_timedout, this) != 0) {
//        lck.unlock();
        LOG(ERROR) << "Fail to add timer";
        return on_timedout(this);
    }
}

void RepeatedTimerTask::reset() {
//    std::unique_lock<raft_mutex_t> lck(_mutex);
//    BRAFT_RETURN_IF(_stopped);
    do { if ((_stopped)) { return; }} while (0);
    CHECK(_running);
    const int rc = bthread_timer_del(_timer);
    if (rc == 0) {
        return schedule();
    }
    // else on_timedout would invoke schdule
}

void RepeatedTimerTask::reset(int timeout_ms) {
//    std::unique_lock<raft_mutex_t> lck(_mutex);
    _timeout_ms = timeout_ms;
//    BRAFT_RETURN_IF(_stopped);
    do { if ((_stopped)) { return; }} while (0);
    CHECK(_running);
    const int rc = bthread_timer_del(_timer);
    if (rc == 0) {
        return schedule();
    }
    // else on_timedout would invoke schdule
}

void RepeatedTimerTask::destroy() {
//    std::unique_lock<raft_mutex_t> lck(_mutex);
//    BRAFT_RETURN_IF(_destroyed);
    do { if ((_destroyed)) { return; }} while (0);
    _destroyed = true;
    if (!_running) {
        CHECK(_stopped);
//        lck.unlock();
        on_destroy();
        return;
    }
//    BRAFT_RETURN_IF(_stopped);
    do { if ((_stopped)) { return; }} while (0);
    _stopped = true;
    const int rc = bthread_timer_del(_timer);
    if (rc == 0) {
        _running = false;
//        lck.unlock();
        on_destroy();
        return;
    }
    CHECK(_running);
}


