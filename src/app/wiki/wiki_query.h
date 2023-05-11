#pragma once

#include <memory>

#include "core/segment_graph.hpp"
#include "utils/thread.h"

namespace nocc {
namespace oltp {
namespace wiki {

class WikiQueryWorker : public ndb_thread {
public:
    WikiQueryWorker(uint32_t worker_id, uint32_t num_thread, 
                    std::shared_ptr<livegraph::SegGraph> graph);

    int get_worker_id() const { return worker_id_; }
    void run();

protected:
    int worker_id_;
    int num_thread_;
    
    std::shared_ptr<livegraph::SegGraph> graph_;
};

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
