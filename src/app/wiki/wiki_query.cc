#include "wiki_query.h"

using namespace livegraph;

namespace nocc {
namespace oltp {
namespace wiki {

WikiQueryWorker::WikiQueryWorker(uint32_t worker_id, uint32_t num_thread, 
                                 std::shared_ptr<SegGraph> graph) 
    : worker_id_(worker_id), num_thread_(num_thread), graph_(graph) {
    
}

void WikiQueryWorker::run() {
    // TODO
}

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc

