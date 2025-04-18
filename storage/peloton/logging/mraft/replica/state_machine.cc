#include "replica/state_machine.h"

#include <butil/logging.h>

#include "replica/election_module.h"

namespace mraft {

void StateMachineCaller::init(const StateMachineCallerOptions& options) {
    CHECK(options.state_machine != nullptr);
    _state_machine = options.state_machine;
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
    CHECK_EQ(0, bthread::execution_queue_start(&_queue_id, &queue_options, run,
                                               this));
}

void StateMachineCaller::shutdown() {
    bthread::execution_queue_stop(_queue_id);
    CHECK_EQ(0, bthread::execution_queue_join(_queue_id));
}

void StateMachineCaller::on_leader_start(int64_t term) {
    StateMachineTask task;
    task.type = LEADER_START;
    task.leader_start_context = new LeaderStartContext(term);
    CHECK_EQ(0, bthread::execution_queue_execute(_queue_id, task));
}

void StateMachineCaller::on_leader_stop(const butil::Status& status) {
    StateMachineTask task;
    task.type = LEADER_STOP;
    task.status = new butil::Status(status);
    CHECK_EQ(0, bthread::execution_queue_execute(_queue_id, task));
}

void StateMachineCaller::on_start_following(
    const LeaderChangeContext& start_following_context) {
    StateMachineTask task;
    task.type = START_FOLLOWING;
    task.leader_change_context = new LeaderChangeContext(
        start_following_context.leader_id(), start_following_context.term());
    CHECK_EQ(0, bthread::execution_queue_execute(_queue_id, task));
}

void StateMachineCaller::on_stop_following(
    const LeaderChangeContext& stop_following_context) {
    StateMachineTask task;
    task.type = STOP_FOLLOWING;
    task.leader_change_context = new LeaderChangeContext(
        stop_following_context.leader_id(), stop_following_context.term());
    CHECK_EQ(0, bthread::execution_queue_execute(_queue_id, task));
}

void StateMachine::on_apply() {}

void StateMachine::on_shutdown() { LOG(INFO) << "StateMachine::on_shutdown()"; }

void StateMachine::on_leader_start(const LeaderStartContext& ctx) {
    (void)ctx;
    LOG(INFO) << "StateMachine::on_leader_start()";
}

void StateMachine::on_leader_stop(const butil::Status& status) {
    (void)status;
    LOG(INFO) << "StateMachine::on_leader_stop()";
}

void StateMachine::on_stop_following(const LeaderChangeContext& ctx) {
    (void)ctx;
    LOG(INFO) << "StateMachine::on_stop_following()";
}

void StateMachine::on_start_following(const LeaderChangeContext& ctx) {
    (void)ctx;
    LOG(INFO) << "StateMachine::on_start_following()";
}

int StateMachineCaller::run(void* meta,
                            bthread::TaskIterator<StateMachineTask>& iter) {
    StateMachineCaller* caller = static_cast<StateMachineCaller*>(meta);
    auto state_machine = caller->_state_machine;
    if (iter.is_queue_stopped()) {
        state_machine->on_shutdown();
        return 0;
    }
    for (; iter; ++iter) {
        switch (iter->type) {
            case COMMITTED:
                break;
            case LEADER_STOP:
                caller->_cur_task = LEADER_STOP;
                state_machine->on_leader_stop(*(iter->status));
                delete iter->status;
                break;
            case LEADER_START:
                state_machine->on_leader_start(*(iter->leader_start_context));
                delete iter->leader_start_context;
                break;
            case START_FOLLOWING:
                caller->_cur_task = START_FOLLOWING;
                state_machine->on_start_following(
                    *(iter->leader_change_context));
                delete iter->leader_change_context;
                break;
            case STOP_FOLLOWING:
                caller->_cur_task = STOP_FOLLOWING;
                state_machine->on_stop_following(
                    *(iter->leader_change_context));
                delete iter->leader_change_context;
                break;
            case ERROR:
                CHECK(false) << "Can't reach here";
                break;
            case IDLE:
                CHECK(false) << "Can't reach here";
                break;
        };
    }
    return 0;
}

}  // namespace mraft