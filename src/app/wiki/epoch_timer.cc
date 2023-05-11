#include "utils/util.h"
#include "epoch_timer.h"

using namespace livegraph;

namespace nocc {
namespace oltp {
namespace wiki {

EpochTimer::EpochTimer(std::shared_ptr<SegGraph> graph, double epoch_sec) 
    : graph_(graph),
      epoch_sec_(epoch_sec),
      SECOND_CYCLE_(nocc::util::Breakdown_Timer::get_one_second_cycle()) {
    
}

void EpochTimer::run() {
    /* Bind core */
    // int cpu_num = BindToCore(0);

    std::cout << "[Epoch Timer start working]" << std::endl;

    const uint64_t epoch_interval = SECOND_CYCLE_ * epoch_sec_;
    uint64_t begin = rdtsc(), end;

    // main loop for epoch timer
    while(true) {
        end = rdtsc();
        if (end - begin > epoch_interval) {
            auto epoch_num = graph_->epoch_id.load(std::memory_order_acquire);
            std::cout << "Epoch [" << epoch_num << "]" << std::endl;

            // epoch_id begin from 1
            graph_->epoch_id.fetch_add(1, std::memory_order_relaxed) + 1;
    
            begin = end;
        }
    }  // end main loop
}

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
